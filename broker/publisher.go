package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// publishConfirmTimeout 是等待连接就绪/阻塞解除、以及等待 broker publisher confirm 的上限。
// 超时会被当作发送失败返回，避免在 broker 侧静默丢消息。
const publishConfirmTimeout = 5 * time.Second

// publishQueueTimeout 是调用方未设置 deadline 时，排队等待前一条发布完成的上限。
const publishQueueTimeout = 30 * time.Second

// abandonCloseTimeout 是后台关闭被放弃 channel 的上限。broker 长期不响应（持续告警、半死连接）时
// 强制关闭该代连接，保证后台 goroutine 一定退出，且不会在每次放弃时立刻触发重连抖动。
const abandonCloseTimeout = 30 * time.Second

var errUnroutable = errors.New("amqp message unroutable")

// errNotSent 标记“消息确定没有写出”的失败（开 channel、声明失败，或写入前发现连接已关闭），
// 这类失败可以安全地重发，不会产生重复消息。
var errNotSent = errors.New("amqp message not sent")

type notSentError struct{ err error }

func (e notSentError) Error() string        { return e.err.Error() }
func (e notSentError) Unwrap() error        { return e.err }
func (e notSentError) Is(target error) bool { return target == errNotSent }

func notSent(err error) error {
	if err == nil {
		return nil
	}
	return notSentError{err}
}

type queueDecl struct {
	name       string
	durable    bool
	autoDelete bool
	args       amqp.Table
	// cacheFor > 0 时在该时长内复用声明结果，减少 QueueDeclare 调用（阿里云按接口 TPS 限流）。
	// 依赖 x-expires 续期的队列必须为 0，每次发布前都重新声明。
	cacheFor time.Duration
}

type exchangeDecl struct {
	name string
	kind string
}

type pubRequest struct {
	exchange  string
	key       string
	mandatory bool
	msg       amqp.Publishing
	declare   *queueDecl
}

// publishLanes 是每条连接上并发发布使用的 channel 数上限。
const publishLanes = 8

// publisher 在一条连接上发布消息，并保证每条消息都拿到 broker 确认。
//
// 内部有最多 publishLanes 条 lane，每条 lane 是一个 confirm 模式的长生命周期 channel，
// 同一时刻只有一条未确认消息，便于把确认与发布一一对应；多个调用方可并发占用不同 lane，
// 吞吐不再被单个往返时延（RTT）锁死。空闲 lane 后进先出复用，低并发时只会打开一个 channel。
//
// 所有可能阻塞的 streadway 调用都在看门狗下执行；调用方只受自身 ctx 与固定超时约束，
// 不会因为 broker 阻塞、网络黑洞或 channel 关闭而无限期挂起。
type publisher struct {
	cm      *connManager
	timeout time.Duration
	// failFastBlocked 为 true 时，连接被 broker 阻塞立即返回 ErrBlocked，不等待解除。
	// 用于消费侧重投：调用方有进程内兜底，不能让消费循环陪着 broker 告警一起等待。
	failFastBlocked bool
	// exchange 非空时，每次新建 channel 都声明一次（与旧版本行为及参数一致），
	// 保证向全新 vhost 发布时 exchange 存在，而不是被 broker 以 404 关闭 channel。
	exchange *exchangeDecl

	slots chan struct{}
	mu    sync.Mutex
	idle  []*lane
}

// lane 的字段只由当前占用它的调用方访问。
type lane struct {
	p        *publisher
	ch       *amqp.Channel
	gen      uint64
	acks     chan amqp.Confirmation
	rets     chan amqp.Return
	closed   chan *amqp.Error
	seq      uint64
	declared map[string]time.Time
}

func newPublisher(cm *connManager, timeout time.Duration) *publisher {
	p := &publisher{cm: cm, timeout: timeout, slots: make(chan struct{}, publishLanes)}
	for i := 0; i < publishLanes; i++ {
		p.idle = append(p.idle, &lane{p: p, declared: make(map[string]time.Time)})
	}
	return p
}

func (p *publisher) acquire(ctx context.Context) (*lane, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	l := p.idle[len(p.idle)-1]
	p.idle = p.idle[:len(p.idle)-1]
	p.mu.Unlock()
	return l, nil
}

func (p *publisher) release(l *lane) {
	p.mu.Lock()
	p.idle = append(p.idle, l)
	p.mu.Unlock()
	<-p.slots
}

func (p *publisher) publish(ctx context.Context, r pubRequest) error {
	// 1. 等待连接可用、阻塞解除：各调用方并发等待，不占用 lane
	waitCtx, cancelWait := context.WithTimeout(ctx, p.timeout)
	_, gen, err := p.cm.get(waitCtx)
	if err == nil {
		err = p.waitUnblocked(waitCtx, gen)
	}
	cancelWait()
	if err != nil {
		return mapWaitErr(ctx, err, errNotConnected)
	}

	// 2. 占用一条 lane：lane 都忙时排队，排队时响应调用方 ctx
	queueCtx, cancelQueue := context.WithTimeout(ctx, publishQueueTimeout)
	l, err := p.acquire(queueCtx)
	cancelQueue()
	if err != nil {
		return mapWaitErr(ctx, err, errors.New("amqp publish timeout waiting for in-flight publishes"))
	}
	defer p.release(l)

	// 3. 发布并等待确认
	opCtx, cancelOp := context.WithTimeout(ctx, p.timeout)
	defer cancelOp()
	err = l.publishOnce(ctx, opCtx, r)
	switch {
	case errors.Is(err, errUnroutable) && r.declare != nil:
		// 目标队列可能已过期或被删除：失效缓存，重新声明后再发一次
		delete(l.declared, r.declare.name)
		err = l.publishOnce(ctx, opCtx, r)
	case errors.Is(err, errNotSent) && opCtx.Err() == nil:
		// 连接刚断开（k8s 网络抖动、broker 重启）：消息确定没发出，等重连后在同一超时预算内重发一次
		err = l.publishOnce(ctx, opCtx, r)
	}
	return err
}

func (l *lane) publishOnce(ctx, opCtx context.Context, r pubRequest) error {
	p := l.p
	conn, gen, err := p.cm.get(opCtx)
	if err != nil {
		return mapWaitErr(ctx, err, errNotConnected)
	}
	if err := p.waitUnblocked(opCtx, gen); err != nil {
		return mapWaitErr(ctx, err, ErrBlocked)
	}
	if err := l.ensureChannel(ctx, opCtx, conn, gen); err != nil {
		return notSentUnlessTimeout(opCtx, err)
	}
	if r.declare != nil {
		if err := l.ensureDeclared(ctx, opCtx, gen, r.declare); err != nil {
			return notSentUnlessTimeout(opCtx, err)
		}
	}

	ch := l.ch
	l.seq++
	expectedTag := l.seq
	if err := l.call(ctx, opCtx, gen, "publish", func() error {
		return ch.Publish(r.exchange, r.key, r.mandatory, false, r.msg)
	}); err != nil {
		l.abandon()
		// 写入超时时消息可能已部分写出，结果不确定，不能标记为未发送
		return notSentUnlessTimeout(opCtx, err)
	}
	return l.waitConfirm(ctx, opCtx, gen, expectedTag)
}

func notSentUnlessTimeout(opCtx context.Context, err error) error {
	if opCtx.Err() != nil {
		return err
	}
	return notSent(err)
}

func (p *publisher) waitUnblocked(ctx context.Context, gen uint64) error {
	if p.failFastBlocked {
		if blocked, why := p.cm.blockedReason(gen); blocked {
			return fmt.Errorf("%w: %s", ErrBlocked, why)
		}
		return nil
	}
	return p.cm.waitUnblocked(ctx, gen)
}

func (l *lane) waitConfirm(ctx, opCtx context.Context, gen, expectedTag uint64) error {
	p := l.p
	var blockedCh <-chan struct{}
	if p.failFastBlocked {
		blockedCh = p.cm.blockedChan(gen)
	}
	select {
	case c, ok := <-l.acks:
		if !ok {
			l.reset()
			return errors.New("amqp publish confirm channel closed")
		}
		if c.DeliveryTag != expectedTag {
			l.abandon()
			return fmt.Errorf("amqp publish confirm out of order, want=%d got=%d", expectedTag, c.DeliveryTag)
		}
		if !c.Ack {
			return fmt.Errorf("amqp message nacked, deliveryTag=%d", c.DeliveryTag)
		}
		// streadway 在同一个读循环里先投递 basic.return 再投递 ack，这里非阻塞探测即可精确识别 unroutable。
		// channel 关闭时 streadway 会 close 该通道，读到零值不能误判为 unroutable。
		select {
		case ret, ok := <-l.rets:
			if ok {
				return unroutable(ret)
			}
			return nil
		default:
			return nil
		}
	case ret, ok := <-l.rets:
		if !ok {
			acked := l.alreadyAcked(expectedTag)
			l.reset()
			if acked {
				return nil
			}
			return errors.New("amqp publish channel closed before confirm")
		}
		// return 之后紧跟 ack，同样带超时兜底
		select {
		case <-l.acks:
		case <-opCtx.Done():
			l.abandon()
		}
		return unroutable(ret)
	case err := <-l.closed:
		acked := l.alreadyAcked(expectedTag)
		l.reset()
		if acked {
			return nil
		}
		if err != nil {
			return err
		}
		return errors.New("amqp publish channel closed before confirm")
	case <-blockedCh:
		l.abandon()
		_, why := p.cm.blockedReason(gen)
		return fmt.Errorf("%w: %s", ErrBlocked, why)
	case <-opCtx.Done():
		// 未消费的 confirm 留在旧 channel 上，必须放弃该 channel，避免与后续发布错位
		l.abandon()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if blocked, why := p.cm.blockedReason(gen); blocked {
			return fmt.Errorf("%w: %s", ErrBlocked, why)
		}
		return errors.New("amqp publish confirm timeout")
	}
}

// alreadyAcked 在 channel 关闭时非阻塞检查确认是否已经到达：select 在多个就绪分支间随机选择，
// 不能因为先选中了关闭通知就把已确认的消息报告为失败（调用方重发会造成重复）。
func (l *lane) alreadyAcked(expectedTag uint64) bool {
	select {
	case c, ok := <-l.acks:
		return ok && c.DeliveryTag == expectedTag && c.Ack
	default:
		return false
	}
}

func unroutable(r amqp.Return) error {
	return fmt.Errorf("%w, replyCode=%d replyText=%s exchange=%s routingKey=%s",
		errUnroutable, r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey)
}

func (l *lane) ensureChannel(ctx, opCtx context.Context, conn *amqp.Connection, gen uint64) error {
	p := l.p
	if l.ch != nil {
		if l.gen != gen {
			l.abandon()
		} else {
			select {
			case err := <-l.closed:
				// 被 broker 关闭（例如阿里云超出 TPS 限流返回 530）：丢弃后重建
				log.Printf("rabbitmq: %s publish channel closed: %v, will recreate", p.cm.name, err)
				l.reset()
			default:
				return nil
			}
		}
	}

	type result struct {
		ch  *amqp.Channel
		err error
	}
	res := make(chan result, 1)
	go func() {
		wd := p.cm.watchdog(gen, rpcTimeout, "open publish channel")
		defer wd.Stop()
		ch, err := conn.Channel()
		if err == nil {
			err = ch.Confirm(false)
			if err == nil && p.exchange != nil && p.exchange.name != "" {
				err = ch.ExchangeDeclare(p.exchange.name, p.exchange.kind, true, false, false, false, nil)
			}
			if err != nil {
				l.closeAsync(ch, gen)
				ch = nil
			}
		}
		res <- result{ch, err}
	}()

	select {
	case r := <-res:
		if r.err != nil {
			return r.err
		}
		l.ch, l.gen, l.seq = r.ch, gen, 0
		// 缓冲足够：同一 channel 上最多只有一条未确认消息，放弃的 channel 也不会因写满而卡住连接读循环
		l.acks = r.ch.NotifyPublish(make(chan amqp.Confirmation, 16))
		l.rets = r.ch.NotifyReturn(make(chan amqp.Return, 16))
		l.closed = r.ch.NotifyClose(make(chan *amqp.Error, 1))
		return nil
	case <-opCtx.Done():
		go func() {
			if r := <-res; r.ch != nil {
				l.closeAsync(r.ch, gen)
			}
		}()
		return mapWaitErr(ctx, opCtx.Err(), errors.New("amqp open publish channel timeout"))
	}
}

func (l *lane) ensureDeclared(ctx, opCtx context.Context, gen uint64, d *queueDecl) error {
	if d.cacheFor > 0 {
		if at, ok := l.declared[d.name]; ok && time.Since(at) < d.cacheFor {
			return nil
		}
	}
	ch := l.ch
	if err := l.call(ctx, opCtx, gen, "declare queue "+d.name, func() error {
		_, err := ch.QueueDeclare(d.name, d.durable, d.autoDelete, false, false, d.args)
		return err
	}); err != nil {
		// 声明失败时 broker 会关闭 channel（如参数冲突 406），一律丢弃重建
		l.abandon()
		return err
	}
	if d.cacheFor > 0 {
		l.declared[d.name] = time.Now()
	}
	return nil
}

// call 在看门狗下执行一次 streadway 调用并响应 ctx。
// 调用方放弃等待时，后台调用仍受看门狗约束，最终一定返回。
func (l *lane) call(ctx, opCtx context.Context, gen uint64, what string, fn func() error) error {
	errCh := make(chan error, 1)
	go func() {
		wd := l.p.cm.watchdog(gen, rpcTimeout, what)
		defer wd.Stop()
		errCh <- fn()
	}()
	select {
	case err := <-errCh:
		return err
	case <-opCtx.Done():
		l.abandon()
		return mapWaitErr(ctx, opCtx.Err(), fmt.Errorf("amqp %s timeout", what))
	}
}

// abandon 丢弃当前 channel 并在后台关闭。
// 不同步关闭：broker 阻塞连接时 Close 本身会挂起，同步关闭正是旧实现阻塞所有发布的根因。
func (l *lane) abandon() {
	ch, gen := l.ch, l.gen
	l.reset()
	if ch != nil {
		l.closeAsync(ch, gen)
	}
}

func (l *lane) closeAsync(ch *amqp.Channel, gen uint64) {
	go func() {
		wd := l.p.cm.watchdog(gen, abandonCloseTimeout, "close abandoned publish channel")
		defer wd.Stop()
		_ = ch.Close()
	}()
}

func (l *lane) reset() {
	l.ch, l.acks, l.rets, l.closed, l.seq = nil, nil, nil, nil, 0
}

// mapWaitErr 区分调用方 ctx 结束与内部超时：前者原样返回 ctx 错误，后者返回 timeoutErr。
func mapWaitErr(ctx context.Context, err, timeoutErr error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return timeoutErr
	}
	return err
}
