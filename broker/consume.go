package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/time/rate"
)

// consumeSetupTimeout 是建立消费（开 channel、声明、绑定、basic.consume）的上限，超时视为连接不可用。
const consumeSetupTimeout = 30 * time.Second

const channelCloseTimeout = 5 * time.Second

// errDeliveryClosed 表示 broker 关闭了投递通道, 需要由上层重新发起消费。
var errDeliveryClosed = errors.New("delivery channel closed")

var consumerSeq atomic.Uint64

type sessionSpec struct {
	queue    string
	bindings []string
	prefetch int
	limiter  *rate.Limiter
	handle   func([]byte) Status
}

// deliveryLoop 逐条处理 delivery，直到 ctx 结束或投递通道关闭。
//
// 返回值恒为非 nil：上层据此重建消费，返回 nil 会让重连循环误判为正常结束而空转。
// ctx 在 channel 关闭、连接断开或停机时被取消，限流等待也会随之立刻返回。
func deliveryLoop(ctx context.Context, deliveries <-chan amqp.Delivery, limiter *rate.Limiter, process func(amqp.Delivery)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			// 通道关闭后接收会立即返回零值, 必须退出, 否则会空转并对零值消息反复调用 handle。
			if !ok {
				return errDeliveryClosed
			}
			// 已要求退出时不再处理新消息：未 ack 的消息随 channel 关闭由 broker 重新入队
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if limiter != nil {
				if err := limiter.Wait(ctx); err != nil {
					return err
				}
			}
			process(d)
		}
	}
}

// runSession 建立一次消费并持续处理，直到 channel / 连接关闭或停机。
func (a *AmqpBroker) runSession(spec sessionSpec) error {
	if !a.enterSession() {
		return ErrShutdown
	}
	defer a.sessions.Done()

	conn, gen, err := a.main.get(a.ctx)
	if err != nil {
		return a.sessionErr(err)
	}

	var ch *amqp.Channel
	if err := a.main.withWatchdog(gen, consumeSetupTimeout, "setup consumer "+spec.queue, func() (err error) {
		ch, err = a.openConsumeChannel(conn, spec)
		return err
	}); err != nil {
		return a.sessionErr(err)
	}
	defer closeChannel(ch)

	retryReady := a.declareRetryTopology(conn, gen, spec.queue)

	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	tag := consumerTag(spec.queue)
	var deliveries <-chan amqp.Delivery
	if err := a.main.withWatchdog(gen, consumeSetupTimeout, "consume "+spec.queue, func() (err error) {
		deliveries, err = ch.Consume(spec.queue, tag, false, false, false, false, nil)
		return err
	}); err != nil {
		return a.sessionErr(err)
	}

	ctx, cancel := context.WithCancel(a.ctx)
	defer cancel()
	closeErr := make(chan error, 1)
	var chanClosed atomic.Bool
	go func() {
		select {
		case err := <-chClosed:
			chanClosed.Store(true)
			if err != nil {
				closeErr <- err
			}
			cancel()
		case <-ctx.Done():
		}
	}()

	r := &redeliverer{
		queue:  spec.queue,
		handle: spec.handle,
		wait:   func(d time.Duration) bool { return sleepCtx(ctx, d) },
	}
	if retryReady {
		r.schedule = func(d amqp.Delivery, next int, delay time.Duration) error {
			return a.scheduleRedelivery(spec.queue, d, next, delay)
		}
		r.persist = func(d amqp.Delivery, next int, delay time.Duration) bool {
			// 只在停机且 channel 仍可用时尝试：断线时原消息无法 ack，再发一份只会制造重复
			if a.ctx.Err() == nil || chanClosed.Load() {
				return false
			}
			return a.scheduleRedelivery(spec.queue, d, next, delay) == nil
		}
	}

	err = deliveryLoop(ctx, deliveries, spec.limiter, r.process)
	if !chanClosed.Load() {
		// 在途消息此时已处理完并确认。先取消订阅再关闭 channel：阿里云 RabbitMQ 版在 basic.cancel 时
		// 立即把已投递未确认的预取消息重新入队，而仅关闭连接时要等消费超时（默认 5 分钟）才重投。
		cancelConsumer(ch, tag)
	}
	if a.isShutdown() {
		return ErrShutdown
	}
	select {
	case e := <-closeErr:
		return e
	default:
		return err
	}
}

func (a *AmqpBroker) openConsumeChannel(conn *amqp.Connection, spec sessionSpec) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*amqp.Channel, error) {
		go ch.Close()
		return nil, err
	}
	if err := ch.ExchangeDeclare(a.options.Exchange, a.options.ExchangeType, true, false, false, false, nil); err != nil {
		return fail(err)
	}
	if _, err := ch.QueueDeclare(spec.queue, true, false, false, false, nil); err != nil {
		return fail(err)
	}
	if err := ch.Qos(spec.prefetch, 0, false); err != nil {
		return fail(err)
	}
	for _, rk := range spec.bindings {
		if err := ch.QueueBind(spec.queue, rk, a.options.Exchange, false, nil); err != nil {
			return fail(err)
		}
	}
	return ch, nil
}

// declareRetryTopology 声明重投 exchange 并把队列以自身名字绑定上去。
// 在独立的临时 channel 上执行：失败（例如没有 configure 权限）不影响正常消费，
// 只是失败消息改为在进程内退避重试。
func (a *AmqpBroker) declareRetryTopology(conn *amqp.Connection, gen uint64, queue string) bool {
	err := a.main.withWatchdog(gen, consumeSetupTimeout, "declare retry topology for "+queue, func() error {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		if err := ch.ExchangeDeclare(retryExchange, "direct", true, false, false, false, nil); err != nil {
			return err
		}
		return ch.QueueBind(queue, queue, retryExchange, false, nil)
	})
	if err != nil {
		log.Printf("rabbitmq: queue %s retry topology unavailable: %v; failed messages will be retried in-process", queue, err)
		return false
	}
	return true
}

func (a *AmqpBroker) sessionErr(err error) error {
	if a.isShutdown() {
		return ErrShutdown
	}
	return err
}

func consumerTag(queue string) string {
	tag := fmt.Sprintf("%s.%d.%d.%s", hostname, os.Getpid(), consumerSeq.Add(1), queue)
	if len(tag) > 255 {
		tag = tag[:255]
	}
	return tag
}

func cancelConsumer(ch *amqp.Channel, tag string) {
	done := make(chan struct{})
	go func() {
		_ = ch.Cancel(tag, false)
		close(done)
	}()
	t := time.NewTimer(channelCloseTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
	}
}

// closeChannel 关闭 channel，最多等待 channelCloseTimeout：
// 网络黑洞时 Close 会挂起直到心跳超时，不能让消费循环跟着挂住。
func closeChannel(ch *amqp.Channel) {
	done := make(chan struct{})
	go func() {
		_ = ch.Close()
		close(done)
	}()
	t := time.NewTimer(channelCloseTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
