package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

// publishConfirmTimeout 是等待 broker publisher confirm 的超时时间。
// 超时会被当作发送失败返回，避免在 broker 侧静默丢消息。
const publishConfirmTimeout = 5 * time.Second

type AmqpBrokerOptions struct {
	Url          string
	Exchange     string
	ExchangeType string
}

type AmqpBroker struct {
	m        sync.Mutex
	conn     *amqp.Connection
	notifies map[string]chan *amqp.Error
	options  *AmqpBrokerOptions

	// publish 路径独立状态，开启 publisher confirm 后保证消息真正落地
	pubMu     sync.Mutex
	pubCh     *amqp.Channel
	pubAck    chan amqp.Confirmation
	pubReturn chan amqp.Return
	pubClosed chan *amqp.Error
	// pubSeq 跟踪当前 channel 上下一条 publish 的 DeliveryTag（从 1 开始），
	// 用来与 confirm 一一配对，发现错位立刻报错。channel 重建时归零。
	pubSeq uint64
}

func NewAmqpBroker(option *AmqpBrokerOptions) *AmqpBroker {
	conn, err := amqp.Dial(option.Url)
	if err != nil {
		panic(err)
	}

	ab := &AmqpBroker{
		conn:     conn,
		options:  option,
		notifies: make(map[string]chan *amqp.Error),
	}

	go ab.keepAlive()

	return ab
}

func (a *AmqpBroker) keepAlive() {
	a.m.Lock()
	conn := a.conn
	a.m.Unlock()

	if conn != nil {
		cc := conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("amqp conn close: %v", <-cc)
	}

	newConn, err := amqp.Dial(a.options.Url)
	if err != nil {
		log.Printf("amqp redial faild: %v", err)
		time.AfterFunc(5*time.Second, a.keepAlive)
		return
	}
	log.Println("amqp redial success...")

	a.m.Lock()
	a.conn = newConn
	notifies := make([]chan *amqp.Error, 0, len(a.notifies))
	for _, n := range a.notifies {
		notifies = append(notifies, n)
	}
	a.m.Unlock()

	// 重连后强制重建 publish channel
	a.resetPubChannel()

	for _, n := range notifies {
		select {
		case n <- amqp.ErrClosed:
		default:
		}
	}
	a.keepAlive()
}

func (a *AmqpBroker) Health() bool {
	a.m.Lock()
	defer a.m.Unlock()
	return a.conn != nil && !a.conn.IsClosed()
}

// defaultPrefetch 是未显式配置时的未 Ack 消息上限。
const defaultPrefetch = 10

// ConsumeOption 消费配置。
//
// 限流语义只有一条：每 Interval 最多消费 Limit 条。
// Interval <= 0 表示不限流；此时只有 Prefetch 生效。
type ConsumeOption struct {
	// Prefetch 未 Ack 消息上限, <=0 时取 10。
	// 开启限流时会自动对齐为 Limit, 无需手动设置。
	Prefetch int

	Interval time.Duration // 限流窗口
	Limit    int           // 每个窗口最多消费的条数, <=0 时取 1
}

// Every 构造「每 d 最多 n 条」的消费配置。
func Every(d time.Duration, n int) *ConsumeOption {
	return &ConsumeOption{Interval: d, Limit: n}
}

// PerSecond 构造「每秒最多 n 条」的消费配置。
func PerSecond(n int) *ConsumeOption {
	return Every(time.Second, n)
}

// normalize 返回补齐默认值后的副本，不会修改调用方传入的配置。
func normalize(optChain ...*ConsumeOption) ConsumeOption {
	opt := ConsumeOption{}
	if len(optChain) > 0 && optChain[0] != nil {
		opt = *optChain[0]
	}

	if opt.Interval > 0 {
		if opt.Limit <= 0 {
			opt.Limit = 1
		}
		// 限流时未 Ack 消息数与窗口配额对齐，避免预取过多打乱消费节奏
		opt.Prefetch = opt.Limit
	} else if opt.Prefetch <= 0 {
		opt.Prefetch = defaultPrefetch
	}

	return opt
}

// newLimiter 按「每 Interval 最多 Limit 条」构造限流器，未开启限流时返回 nil。
func newLimiter(opt ConsumeOption) *rate.Limiter {
	if opt.Interval <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(float64(opt.Limit)/opt.Interval.Seconds()), opt.Limit)
}

// errDeliveryClosed 表示 broker 关闭了投递通道, 需要由上层重新发起消费。
var errDeliveryClosed = errors.New("delivery channel closed")

// consumeLoop 按限流节奏消费 delivery。
//
// 返回值恒为非 nil：delivery 被关闭或收到连接关闭通知都需要上层重连，
// 返回 nil 会让调用方的重连循环误判为正常结束而空转。
func consumeLoop(
	ctx context.Context,
	delivery <-chan amqp.Delivery,
	notify <-chan *amqp.Error,
	limiter *rate.Limiter,
	handle func(amqp.Delivery),
) error {
	for {
		select {
		case err := <-notify:
			if err != nil {
				return err
			}
			return amqp.ErrClosed
		case d, ok := <-delivery:
			// 通道关闭后接收会立即返回零值, 必须退出,
			// 否则会空转并对零值消息反复调用 handle。
			if !ok {
				return errDeliveryClosed
			}
			if limiter != nil {
				if err := limiter.Wait(ctx); err != nil {
					return err
				}
			}
			handle(d)
		}
	}
}

func (a *AmqpBroker) Consume(queue *Queue, optChain ...*ConsumeOption) error {

	opt := normalize(optChain...)
	ctx := context.Background()

	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil {
		return errors.New("conn is nil")
	}
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(
		a.options.Exchange,
		a.options.ExchangeType,
		true, false, false, false, nil,
	); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(queue.Name,
		true, false, false, false, nil,
	); err != nil {
		return err
	}
	if err := channel.Qos(opt.Prefetch, 0, false); err != nil {
		return err
	}

	if err := channel.QueueBind(queue.Name, queue.RouteKey, a.options.Exchange, false, nil); err != nil {
		return err
	}

	delivery, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	notify := make(chan *amqp.Error, 1)

	a.m.Lock()
	a.notifies[queue.Name] = notify
	a.m.Unlock()

	// 只摘除注册、不 close：keepAlive 可能仍持有该 channel 的引用，
	// close 后再写入会 panic。
	defer func() {
		a.m.Lock()
		delete(a.notifies, queue.Name)
		a.m.Unlock()
	}()

	return consumeLoop(ctx, delivery, notify, newLimiter(opt), func(d amqp.Delivery) {
		switch status := queue.Handle(d.Body); status {
		case Retry:
			if err := a.retry(queue, d); err != nil {
				d.Nack(false, true)
			} else {
				d.Ack(false)
			}
		default:
			d.Ack(false)
		}
	})
}

type GroupConsumeOption struct {
	Group       string   // 消费组名（同组共享队列）
	RoutingKeys []string // 需要订阅的多个 topic / pattern
	Prefetch    int      // 每实例并行未 Ack 上限
}

func (a *AmqpBroker) ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error {
	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}
	if opt.Group == "" || len(opt.RoutingKeys) == 0 {
		return fmt.Errorf("group or routing keys empty")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// 1. 声明 topic exchange（幂等）
	if err := ch.ExchangeDeclare(a.options.Exchange, a.options.ExchangeType, true, false, false, false, nil); err != nil {
		return err
	}

	// 2. 队列名（同组保证一致）
	queueName := opt.Group

	// 3. 声明共享队列（持久化, 不自动删除）
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return err
	}

	// 4. 绑定多个 routing key（可含 \* 或 \# 通配）
	for _, rk := range opt.RoutingKeys {
		if err := ch.QueueBind(queueName, rk, a.options.Exchange, false, nil); err != nil {
			return err
		}
	}

	// 5. QoS
	prefetch := opt.Prefetch
	if prefetch <= 0 {
		prefetch = 10
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}

	// 6. 开始消费（手动 Ack）
	delivery, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for d := range delivery {
		if debug := os.Getenv("DEBUG"); debug == "TRUE" {
			log.Printf("AmqpBroker ConsumerTopic receive msg: %s", d.Body)
		}
		retry := handle(d.Body)
		if retry == Retry {
			// 可接你现有的延迟重试逻辑
			d.Nack(false, true) // 直接 requeue 简易重试（可能导致顺序抖动）
			continue
		}
		d.Ack(false)
	}

	return nil
}

func (a *AmqpBroker) retry(queue *Queue, d amqp.Delivery) error {
	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil {
		return errors.New("conn is nil")
	}
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	retryCount, _ := d.Headers["x-retry-count"].(int32)

	if int(retryCount) >= len(queue.RetryQueue) {
		return nil
	}

	delay := queue.RetryQueue[retryCount]
	delayDuration := time.Duration(delay) * time.Millisecond
	delayQ := fmt.Sprintf("delay.%s.%s.%s", delayDuration.String(), a.options.Exchange, queue.Name)

	if _, err := channel.QueueDeclare(delayQ,
		true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    a.options.Exchange,
			"x-dead-letter-routing-key": queue.RouteKey,
			"x-message-ttl":             delay,
			"x-expires":                 delay * 2,
		},
	); err != nil {
		return err
	}

	return channel.Publish("", delayQ, false, false, amqp.Publishing{
		Headers:      amqp.Table{"x-retry-count": retryCount + 1},
		Body:         d.Body,
		DeliveryMode: amqp.Persistent,
	})
}

// ensurePubChannelLocked 必须在持有 a.pubMu 的情况下调用。
// 它返回一个开启了 publisher confirm 的长生命周期 channel，以及该 channel 上的 ack / return 通知 channel。
func (a *AmqpBroker) ensurePubChannelLocked() (*amqp.Channel, chan amqp.Confirmation, chan amqp.Return, error) {
	if a.pubCh != nil {
		select {
		case err, ok := <-a.pubClosed:
			if ok && err != nil {
				log.Printf("amqp publish channel closed: %v, will recreate", err)
			}
			a.pubCh = nil
			a.pubAck = nil
			a.pubReturn = nil
			a.pubClosed = nil
		default:
			return a.pubCh, a.pubAck, a.pubReturn, nil
		}
	}

	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil || conn.IsClosed() {
		return nil, nil, nil, errors.New("amqp connection is not ready")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, nil, err
	}

	if err := ch.ExchangeDeclare(
		a.options.Exchange,
		a.options.ExchangeType,
		true, false, false, false, nil,
	); err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}

	a.pubCh = ch
	a.pubAck = ch.NotifyPublish(make(chan amqp.Confirmation, 256))
	a.pubReturn = ch.NotifyReturn(make(chan amqp.Return, 16))
	a.pubClosed = ch.NotifyClose(make(chan *amqp.Error, 1))
	a.pubSeq = 0
	return a.pubCh, a.pubAck, a.pubReturn, nil
}

func (a *AmqpBroker) resetPubChannelLocked() {
	if a.pubCh != nil {
		_ = a.pubCh.Close()
	}
	a.pubCh = nil
	a.pubAck = nil
	a.pubReturn = nil
	a.pubClosed = nil
	a.pubSeq = 0
}

func (a *AmqpBroker) resetPubChannel() {
	a.pubMu.Lock()
	defer a.pubMu.Unlock()
	a.resetPubChannelLocked()
}

// DeclareQueue 在 publisher 启动 / 首次发送时声明队列并绑定到 exchange，
// 避免 consumer 尚未启动时出现 unroutable 静默丢消息。幂等。
func (a *AmqpBroker) DeclareQueue(name, routingKey string) error {
	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil || conn.IsClosed() {
		return errors.New("amqp connection is not ready")
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(
		a.options.Exchange, a.options.ExchangeType,
		true, false, false, false, nil,
	); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		return err
	}
	if routingKey == "" {
		routingKey = name
	}
	return ch.QueueBind(name, routingKey, a.options.Exchange, false, nil)
}

func (a *AmqpBroker) Publish(ctx context.Context, key string, body []byte) error {
	opId := tool.OpId(ctx)
	hds := amqp.Table{"opId": opId}
	if tool.EnvEnabled("TRACE") {
		tracer := otel.Tracer("rabbitmq")
		_, span := tracer.Start(ctx, "rabbitmq.Publish")
		defer span.End()
		span.SetAttributes(
			attribute.String("exchange", a.options.Exchange),
			attribute.String("exchange_type", a.options.ExchangeType),
			attribute.String("routing_key", key),
			attribute.String("opId", opId),
		)
	}

	a.pubMu.Lock()
	defer a.pubMu.Unlock()

	ch, confirms, returns, err := a.ensurePubChannelLocked()
	if err != nil {
		return err
	}

	// 记录本次 publish 对应的 DeliveryTag（broker 从 1 开始递增），
	// 便于和后续 confirm 配对，发现错位立刻报错暴露问题。
	a.pubSeq++
	expectedTag := a.pubSeq

	// mandatory=true: 路由不到队列时 broker 通过 basic.return 退回，
	// 否则即便开启了 publisher confirm，broker 仍会返回 Ack=true 但消息已被丢弃。
	if err := ch.Publish(a.options.Exchange, key, true, false, amqp.Publishing{
		Headers:      hds,
		ContentType:  "",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		a.resetPubChannelLocked()
		return err
	}

	// 等待 broker 真正确认收到，避免 channel 关闭/网络层缓冲导致的静默丢失。
	// 注意：confirms / returns 是按 publish 顺序与 channel 共享的串行队列，
	// 任何提前返回都必须 reset channel，避免把残留的 confirm 错位给下一次调用。
	select {
	case c, ok := <-confirms:
		if !ok {
			a.resetPubChannelLocked()
			return errors.New("amqp publish confirm channel closed")
		}
		if c.DeliveryTag != expectedTag {
			a.resetPubChannelLocked()
			return fmt.Errorf("amqp publish confirm out of order, want=%d got=%d", expectedTag, c.DeliveryTag)
		}
		if !c.Ack {
			return fmt.Errorf("amqp message nacked, deliveryTag=%d", c.DeliveryTag)
		}
		// streadway/amqp 保证 basic.return 在 ack 之前到达，
		// 这里非阻塞探测 returns，能精确识别 unroutable。
		select {
		case r := <-returns:
			return fmt.Errorf("amqp message unroutable, replyCode=%d replyText=%s exchange=%s routingKey=%s",
				r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey)
		default:
			return nil
		}
	case r := <-returns:
		// return 帧到达后 ack 紧随其后，但仍要带超时兜底防止阻塞。
		select {
		case <-confirms:
		case <-time.After(publishConfirmTimeout):
			a.resetPubChannelLocked()
		}
		return fmt.Errorf("amqp message unroutable, replyCode=%d replyText=%s exchange=%s routingKey=%s",
			r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey)
	case err := <-a.pubClosed:
		a.resetPubChannelLocked()
		if err != nil {
			return err
		}
		return errors.New("amqp publish channel closed before confirm")
	case <-time.After(publishConfirmTimeout):
		a.resetPubChannelLocked()
		return errors.New("amqp publish confirm timeout")
	case <-ctx.Done():
		// 必须 reset：否则未消费的 confirm 会留在 channel 上，
		// 与下一次 Publish 错位，导致后续消息被静默"误判为成功"而真正丢失。
		a.resetPubChannelLocked()
		return ctx.Err()
	}
}

func (a *AmqpBroker) PublishDelay(ctx context.Context, queue string, body []byte, delay int64) error {
	a.m.Lock()
	conn := a.conn
	a.m.Unlock()
	if conn == nil {
		return errors.New("conn is nil")
	}

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	delayQ := fmt.Sprintf("delay.%d.%s.%s", delay, a.options.Exchange, queue)

	opId := tool.OpId(ctx)
	hd := amqp.Table{
		"opId":                      opId,
		"x-dead-letter-exchange":    a.options.Exchange,
		"x-dead-letter-routing-key": queue,
		"x-message-ttl":             delay * 1000,
		"x-expires":                 delay * 2 * 1000,
	}

	if tool.EnvEnabled("TRACE") {
		tracer := otel.Tracer("rabbitmq")
		_, span := tracer.Start(ctx, "rabbitmq.PublishDelay")
		defer span.End()
		span.SetAttributes(
			attribute.String("exchange", a.options.Exchange),
			attribute.String("exchange_type", a.options.ExchangeType),
			attribute.String("queue", queue),
			attribute.String("opId", opId),
		)
	}

	if _, err := channel.QueueDeclare(delayQ,
		true, true, false, false, hd,
	); err != nil {
		return err
	}

	if err := channel.Confirm(false); err != nil {
		return err
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	closed := channel.NotifyClose(make(chan *amqp.Error, 1))

	if err := channel.Publish("", delayQ, false, false, amqp.Publishing{
		Headers:      amqp.Table{"opId": opId},
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		return err
	}

	select {
	case c, ok := <-confirms:
		if !ok {
			return errors.New("amqp publish confirm channel closed")
		}
		if !c.Ack {
			return fmt.Errorf("amqp delay message nacked, deliveryTag=%d", c.DeliveryTag)
		}
		return nil
	case err := <-closed:
		if err != nil {
			return err
		}
		return errors.New("amqp publish channel closed before confirm")
	case <-time.After(publishConfirmTimeout):
		return errors.New("amqp publish confirm timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}
