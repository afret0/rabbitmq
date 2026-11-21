package broker

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// shutdownTimeout 是 Close 等待在途消息处理完成的上限。
const shutdownTimeout = 10 * time.Second

type AmqpBrokerOptions struct {
	Url          string
	Exchange     string
	ExchangeType string
}

// AmqpBroker 是 Broker 的 RabbitMQ 实现。
//
// 连接划分：
//   - main：NewAmqpBroker 时建立。只用于发布的实例（Producer）在它上面发布；
//     一旦开始消费，它就只用于消费，从不发布，因此不会被 broker 的内存/磁盘告警阻塞，
//     ack 与消费不受发布侧影响。
//   - side：懒加载的独立连接，发布消费失败的重投消息；同一实例既消费又发布时，业务发布也走这里。
type AmqpBroker struct {
	options *AmqpBrokerOptions

	main *connManager
	pub  *publisher

	ctx    context.Context // Shutdown 时取消
	cancel context.CancelFunc

	mu           sync.Mutex
	shutdown     bool
	connsClosed  bool
	consuming    bool
	sessions     sync.WaitGroup
	sessionsOnce sync.Once
	sessionsDone chan struct{}
	sideConn     *connManager
	retryPub     *publisher
	sidePub      *publisher
}

func NewAmqpBroker(option *AmqpBrokerOptions) *AmqpBroker {
	main := newConnManager(option.Url, "main")
	if err := main.connect(); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pub := newPublisher(main, publishConfirmTimeout)
	pub.exchange = &exchangeDecl{name: option.Exchange, kind: option.ExchangeType}
	return &AmqpBroker{
		options: option,
		main:    main,
		pub:     pub,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (a *AmqpBroker) Health() bool {
	return a.main.healthy()
}

// Consume 持续消费 queue，直到 channel / 连接关闭（返回非 nil error，由调用方重建消费）
// 或 Shutdown（返回 ErrShutdown）。
//
// handler 返回 Retry 或 panic 时，消息按 1s / 5s / 10s 退避重投，同一条消息最多重投 3 次，
// 之后丢弃（队列配置了死信则进入死信）并打印一条日志。Queue.RetryQueue 已废弃，不再生效。
func (a *AmqpBroker) Consume(queue *Queue, optChain ...*ConsumeOption) error {
	opt := normalize(optChain...)
	return a.runSession(sessionSpec{
		queue:    queue.Name,
		bindings: []string{queue.RouteKey},
		prefetch: opt.Prefetch,
		limiter:  newLimiter(opt),
		handle:   queue.Handle,
	})
}

type GroupConsumeOption struct {
	Group       string   // 消费组名（同组共享队列）
	RoutingKeys []string // 需要订阅的多个 topic / pattern
	Prefetch    int      // 每实例并行未 Ack 上限
}

// ConsumerTopic 以消费组方式消费：同组的多个实例共享以 Group 命名的队列。
// 返回值与重投语义同 Consume。
func (a *AmqpBroker) ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error {
	if opt.Group == "" || len(opt.RoutingKeys) == 0 {
		return fmt.Errorf("group or routing keys empty")
	}
	prefetch := opt.Prefetch
	if prefetch <= 0 {
		prefetch = defaultPrefetch
	}
	h := handle
	if os.Getenv("DEBUG") == "TRUE" {
		h = func(body []byte) Status {
			log.Printf("AmqpBroker ConsumerTopic receive msg: %s", body)
			return handle(body)
		}
	}
	return a.runSession(sessionSpec{
		queue:    opt.Group,
		bindings: opt.RoutingKeys,
		prefetch: prefetch,
		handle:   h,
	})
}

// ConsumerBroadcast 以广播方式消费：每个实例独享一个 server-named 的非持久 / exclusive / auto-delete
// 临时队列，绑定到 fanout exchange，所有实例都收到全量消息。auto-ack、无重投拓扑，
// 断线后重建的是全新队列，断线期间的消息不补发——语义与 redis pubsub 对齐。
//
// 要求 Options.ExchangeType 为 fanout。handler panic 时打印日志并丢弃该条消息。
// 返回值语义同 Consume（断线返回非 nil error 由调用方重建，Shutdown 返回 ErrShutdown）。
func (a *AmqpBroker) ConsumerBroadcast(handle func(headers amqp.Table, body []byte)) error {
	if handle == nil {
		return fmt.Errorf("broadcast handle nil")
	}
	h := handle
	if os.Getenv("DEBUG") == "TRUE" {
		h = func(headers amqp.Table, body []byte) {
			log.Printf("AmqpBroker ConsumerBroadcast receive msg: %s", body)
			handle(headers, body)
		}
	}
	return a.runSession(sessionSpec{
		broadcast: true,
		broadcastHandle: func(d amqp.Delivery) {
			defer func() {
				if p := recover(); p != nil {
					log.Printf("rabbitmq: broadcast handler panic: %v\n%s", p, debug.Stack())
				}
			}()
			h(d.Headers, d.Body)
		},
	})
}

// ConsumerDirected 以定向广播方式消费：每个实例独享临时独占队列（server-named、非持久、自动删），
// 队列以 routingKey 绑定到配置的 exchange（应为 direct/topic），broker 只把匹配 routingKey 的消息投给本实例。
// 与 ConsumerBroadcast 共用独占自动删 + auto-ack + 断线重建新队列语义，区别仅在绑定键不为空：
// 目标实例不在（队列已随断连自动删除）时消息 unroutable 直接丢弃，适合可丢的定向通知。
func (a *AmqpBroker) ConsumerDirected(routingKey string, handle func(headers amqp.Table, body []byte)) error {
	if routingKey == "" {
		return fmt.Errorf("directed routingKey empty")
	}
	if handle == nil {
		return fmt.Errorf("directed handle nil")
	}
	h := handle
	if os.Getenv("DEBUG") == "TRUE" {
		h = func(headers amqp.Table, body []byte) {
			log.Printf("AmqpBroker ConsumerDirected receive msg: %s", body)
			handle(headers, body)
		}
	}
	return a.runSession(sessionSpec{
		broadcast: true,
		bindKey:   routingKey,
		broadcastHandle: func(d amqp.Delivery) {
			defer func() {
				if p := recover(); p != nil {
					log.Printf("rabbitmq: directed handler panic: %v\n%s", p, debug.Stack())
				}
			}()
			h(d.Headers, d.Body)
		},
	})
}

func (a *AmqpBroker) scheduleRedelivery(queue string, d amqp.Delivery, next int, delay time.Duration) error {
	pub, err := a.retryPublisher()
	if err != nil {
		return err
	}
	decl := delayQueueDecl(queue, delay)
	return pub.publish(context.Background(), pubRequest{
		exchange:  "",
		key:       decl.name,
		mandatory: true,
		msg:       redeliveryPublishing(d, next),
		declare:   decl,
	})
}

// retryPublisher 返回发布重投消息的发布器。连接被阻塞时立即失败，由调用方进程内兜底。
func (a *AmqpBroker) retryPublisher() (*publisher, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.connsClosed {
		return nil, ErrShutdown
	}
	if a.retryPub == nil {
		a.retryPub = newPublisher(a.sideConnLocked(), publishConfirmTimeout)
		a.retryPub.failFastBlocked = true
	}
	return a.retryPub, nil
}

// producerPublisher 返回业务发布使用的发布器：实例开始消费后切换到独立连接，避免发布被阻塞时拖住消费。
func (a *AmqpBroker) producerPublisher() *publisher {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.consuming || a.connsClosed {
		return a.pub
	}
	if a.sidePub == nil {
		a.sidePub = newPublisher(a.sideConnLocked(), publishConfirmTimeout)
		a.sidePub.exchange = a.pub.exchange
	}
	return a.sidePub
}

func (a *AmqpBroker) sideConnLocked() *connManager {
	if a.sideConn == nil {
		a.sideConn = newConnManager(a.options.Url, "side")
	}
	return a.sideConn
}

// DeclareQueue 在 publisher 启动 / 首次发送时声明队列并绑定到 exchange，
// 避免 consumer 尚未启动时出现 unroutable 静默丢消息。幂等。
func (a *AmqpBroker) DeclareQueue(name, routingKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishConfirmTimeout)
	defer cancel()
	conn, gen, err := a.main.get(ctx)
	if err != nil {
		return mapWaitErr(context.Background(), err, errNotConnected)
	}
	if routingKey == "" {
		routingKey = name
	}
	return a.main.withWatchdog(gen, rpcTimeout, "declare queue "+name, func() error {
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
		return ch.QueueBind(name, routingKey, a.options.Exchange, false, nil)
	})
}

// Publish 发布消息并等待 broker 确认。
//
// 返回时间受 ctx 约束；ctx 没有 deadline 时，等待连接就绪 / 阻塞解除 / 确认各自最多 5s。
// 连接被 broker 阻塞（内存/磁盘告警）且 5s 内未解除时返回 ErrBlocked。
// 注意：返回超时类错误时消息仍可能已被投递（at-least-once），重发需业务幂等。
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

	// mandatory=true: 路由不到队列时 broker 通过 basic.return 退回，
	// 否则即便开启了 publisher confirm，broker 仍会返回 Ack=true 但消息已被丢弃。
	return a.producerPublisher().publish(ctx, pubRequest{
		exchange:  a.options.Exchange,
		key:       key,
		mandatory: true,
		msg: amqp.Publishing{
			Headers:      hds,
			ContentType:  "",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	})
}

// PublishBroadcast 广播一条消息：transient + mandatory=false，无订阅者时 broker 直接确认并丢弃，
// 返回 nil（与 redis pubsub 无订阅者的语义一致）。要求 exchange 为 fanout。
func (a *AmqpBroker) PublishBroadcast(ctx context.Context, body []byte) error {
	opId := tool.OpId(ctx)
	if tool.EnvEnabled("TRACE") {
		tracer := otel.Tracer("rabbitmq")
		_, span := tracer.Start(ctx, "rabbitmq.PublishBroadcast")
		defer span.End()
		span.SetAttributes(
			attribute.String("exchange", a.options.Exchange),
			attribute.String("exchange_type", a.options.ExchangeType),
			attribute.String("opId", opId),
		)
	}
	return a.producerPublisher().publish(ctx, pubRequest{
		exchange:  a.options.Exchange,
		key:       "",
		mandatory: false,
		msg: amqp.Publishing{
			Headers:      amqp.Table{"opId": opId},
			Body:         body,
			DeliveryMode: amqp.Transient,
		},
	})
}

// PublishDelay 延迟投递, delaySeconds 单位为秒。
// 内部换算为 AMQP 的 x-message-ttl(毫秒)。
// delaySeconds <= 0 时等同于 Publish 立即投递。
func (a *AmqpBroker) PublishDelay(ctx context.Context, queue string, body []byte, delaySeconds int64) error {
	if delaySeconds <= 0 {
		return a.Publish(ctx, queue, body)
	}

	delayQ := fmt.Sprintf("delay.%d.%s.%s", delaySeconds, a.options.Exchange, queue)

	opId := tool.OpId(ctx)
	// 队列参数必须与旧版本完全一致，否则滚动升级期间新旧 Pod 声明同名队列会 PRECONDITION_FAILED
	hd := amqp.Table{
		"opId":                      opId,
		"x-dead-letter-exchange":    a.options.Exchange,
		"x-dead-letter-routing-key": queue,
		"x-message-ttl":             delaySeconds * 1000,
		"x-expires":                 delaySeconds * 2 * 1000,
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

	// 延迟队列依赖 x-expires 自动清理，每次发布前都重新声明以续期（cacheFor=0），
	// 否则队列可能在消息到期前过期，消息随之丢失。
	return a.producerPublisher().publish(ctx, pubRequest{
		exchange:  "",
		key:       delayQ,
		mandatory: true,
		msg: amqp.Publishing{
			Headers:      amqp.Table{"opId": opId},
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
		declare: &queueDecl{name: delayQ, durable: true, autoDelete: true, args: hd},
	})
}

// Shutdown 优雅停机：停止接收新消息，等待在途消息处理并确认完成（受 ctx 约束），然后关闭连接。
// 未处理的预取消息随 channel 关闭由 broker 重新入队，交给其他实例处理。
// 适合在 k8s 收到 SIGTERM 后调用。可重复调用。
func (a *AmqpBroker) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	a.shutdown = true
	a.mu.Unlock()
	a.cancel()

	// 多次超时调用 Shutdown 时只保留一个等待 goroutine
	a.sessionsOnce.Do(func() {
		a.sessionsDone = make(chan struct{})
		go func() {
			a.sessions.Wait()
			close(a.sessionsDone)
		}()
	})
	var err error
	select {
	case <-a.sessionsDone:
	case <-ctx.Done():
		err = ctx.Err()
	}

	a.mu.Lock()
	a.connsClosed = true
	sideConn := a.sideConn
	a.mu.Unlock()
	if sideConn != nil {
		sideConn.close()
	}
	a.main.close()
	return err
}

// Close 等同于 Shutdown，最多等待 10s。
func (a *AmqpBroker) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return a.Shutdown(ctx)
}

func (a *AmqpBroker) enterSession() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.shutdown {
		return false
	}
	a.consuming = true
	a.sessions.Add(1)
	return true
}

func (a *AmqpBroker) isShutdown() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.shutdown
}
