package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/afret0/rabbitmq/broker"
	"github.com/afret0/rabbitmq/internal/backoff"
	log2 "github.com/afret0/wheel/log"
)

const tracerName = "rabbitmq"

// traceEnvKey 与 sample 中 traceSvc.Init 的开关一致：环境变量 TRACE。
const traceEnvKey = "TRACE"

// tool.HostId() 在没有 HOSTNAME 时每次都会生成新的 uuid，这里只取一次，保证同一进程内稳定。
var hostId = tool.HostId()

var RetryError = errors.New("job retry")

type ExchangeOption struct {
	Name string
	Type string
}

type ConsumerOptions struct {
	ExchangeOpt    *ExchangeOption
	BrokerURL      string
	MonitorAddress string
}

type Consumer struct {
	broker broker.Broker

	stopOnce sync.Once
	stop     chan struct{}
	//log *log.Logger
	//config    *viper.Viper
	// metrics struct {
	// 	JobHandleDuration metrics.Histogram metric:`job_handle_duration"labels:"name,Err"
	// }
}

func NewConsumer(opt *ConsumerOptions) *Consumer {
	b := broker.NewAmqpBroker(&broker.AmqpBrokerOptions{
		Url:          opt.BrokerURL,
		Exchange:     opt.ExchangeOpt.Name,
		ExchangeType: opt.ExchangeOpt.Type,
	})

	consumer := &Consumer{broker: b, stop: make(chan struct{})}
	// metrics.MustInit(&consumer.metrics, prometheus.New())

	//go func() {
	//	consumer.monitoring(opt.MonitorAddress)
	//}()
	return consumer
}

type Job func([]byte) error

type Message[T any] struct {
	//OpId string `json:"opId" required:"true"`
	MsgId string `json:"msgId" required:"true"`
	Data  T      `json:"data"`
}

func NewJob[T any](f func(ctx context.Context, p T) error) Job {
	name := handlerName(f)
	return func(msgS []byte) error {
		c1, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		c1, span := startConsumeSpan(c1, name)
		if span != nil {
			defer span.End()
		}

		lg1 := log2.CtxLogger(c1).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg1.Printf("receive msg: %s", string(msgS))
		}

		M := &Message[T]{}
		err := tool.Unmarshal(string(msgS), M)
		if err != nil {
			lg1.Printf("unmarshal message error: %s, msg: %s", err, string(msgS))
			recordSpanError(span, err)
			return err
		}

		ctx := context.WithValue(c1, "opId", M.MsgId)
		setSpanMsgId(span, M.MsgId)
		lg := log2.CtxLogger(ctx).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg.Infof("start process message: %s", string(msgS))
		}

		err = f(ctx, M.Data)
		if err != nil {
			lg.Errorf("process message error: %s", err)
			recordSpanError(span, err)
			return err
		}

		if tool.Debug() {
			lg.Infof("process message success: %s", string(msgS))
		}

		return nil
	}
}

// startConsumeSpan 为每条消息开启一个全新的 trace。
// WithNewRoot 保证 worker 不会复用上游生产消息时的 trace，
// 即每次消费到的新消息都是独立的一条链路。
// 是否开启由环境变量 TRACE 控制（与 sample 中 traceSvc.Init 的开关一致），
// 未开启时返回 nil span，调用方需要判空。
func startConsumeSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if !traceEnabled() {
		return ctx, nil
	}

	ctx, span := otel.Tracer(tracerName).Start(
		ctx,
		fmt.Sprintf("rabbitmq.Consume %s", name),
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String("job", name),
		attribute.String("hostId", hostId),
	)
	return ctx, span
}

// traceEnabled 复用 sample 里 traceSvc.Init / handler 使用的 TRACE 环境变量开关，
// 取值 true/TRUE/1/yes/YES 时开启。
func traceEnabled() bool {
	return tool.EnvEnabled(traceEnvKey)
}

// setSpanMsgId 把消息的 msgId 挂到 span 上，
// 同时写入 opId 属性，和 producer 端 Publish、sample handler 的字段命名保持一致。
func setSpanMsgId(span trace.Span, msgId string) {
	if span == nil || msgId == "" {
		return
	}
	span.SetAttributes(
		attribute.String("msgId", msgId),
		attribute.String("opId", msgId),
	)
}

func recordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// handlerName 取业务 handler 的函数名作为 span 名，方便在链路上区分不同的 job。
func handlerName(f interface{}) string {
	v := reflect.ValueOf(f)
	if v.Kind() != reflect.Func || v.Pointer() == 0 {
		return "unknown"
	}

	fn := runtime.FuncForPC(v.Pointer())
	if fn == nil {
		return "unknown"
	}

	name := fn.Name()
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, "-fm")
	return name
}

//type params struct {
//	retryQueue []int64
//}
//
//type Param func(*params)

//func Retry(strategy help.RetryStrategy, retry help.Retry) Param {
//	return func(p *params) {
//		if strategy == help.CUSTOMQUEUE {
//			for _, delay := range retry.Queue {
//				d, err := time.ParseDuration(delay)
//				if err != nil {
//					panic(err)
//				}
//				p.retryQueue = append(p.retryQueue, int64(d/time.Millisecond))
//			}
//			return
//		}
//		d, err := time.ParseDuration(retry.Delay)
//		if err != nil {
//			panic(err)
//		}
//		p.retryQueue = help.GetRetryQueue(int64(d/time.Millisecond), retry.Max, strategy)
//	}
//}

//func evaParam(param []Param) *params {
//	ps := &params{}
//	for _, p := range param {
//		p(ps)
//	}
//	return ps
//}

type LaunchJobOpt = broker.ConsumeOption

// LimitEvery 限制消费速率为「每 d 最多 n 条」。
//
//	consumer.LaunchJob(key, queue, job, LimitEvery(time.Second, 5))
func LimitEvery(d time.Duration, n int) *LaunchJobOpt {
	return broker.Every(d, n)
}

// LimitPerSecond 限制消费速率为「每秒最多 n 条」。
//
//	consumer.LaunchJob(key, queue, job, LimitPerSecond(5))
func LimitPerSecond(n int) *LaunchJobOpt {
	return broker.PerSecond(n)
}

// RetryAfter 曾用于配置重试退避序列。
//
// Deprecated: 不再生效。handler 返回 RetryError（含 errors.Is 可识别的包装错误）或 panic 时，
// 统一按 1s / 5s / 10s 退避重投，同一条消息最多重投 3 次，之后丢弃（队列配置了死信则进入死信）
// 并打印一条日志。保留该函数仅为兼容旧代码编译。
func RetryAfter(delays ...time.Duration) *LaunchJobOpt {
	return (&LaunchJobOpt{}).WithRetry(delays...)
}

// LaunchJob 持续消费 queue（绑定 key），阻塞直到 Shutdown。
// 连接断开、channel 被关闭等错误会按带抖动的指数退避（约 0.5s 起、最长 30s）自动重建消费。
//
// handler 返回 RetryError 或 panic 时按 1s / 5s / 10s 退避重投，同一条消息最多重投 3 次，
// 之后丢弃（队列配置了死信则进入死信）并打印一条日志；返回其他错误或 nil 都会 ack。
func (c *Consumer) LaunchJob(key, queue string, job Job, optChain ...*LaunchJobOpt) {
	if len(optChain) > 0 && optChain[0] != nil && len(optChain[0].Retry) > 0 {
		log.Printf("job %s: RetryAfter is deprecated and ignored, failed messages are redelivered after 1s/5s/10s (at most 3 times)", queue)
	}

	q := &broker.Queue{
		Name:     queue,
		RouteKey: key,
		Handle: func(body []byte) broker.Status {
			if errors.Is(job(body), RetryError) {
				return broker.Retry
			}
			return broker.Success
		},
	}

	c.runForever(fmt.Sprintf("job %s", queue), func() error {
		return c.broker.Consume(q, optChain...)
	})
}

// LaunchTopicJob  group: 即队列名, topic: routing key
// 阻塞直到 Shutdown，重建消费与重投语义同 LaunchJob。
func (c *Consumer) LaunchTopicJob(group string, topic string, job Job) {

	if group == "" {
		panic("group is empty")
	}

	opt := &broker.GroupConsumeOption{
		Group:       group,
		RoutingKeys: []string{topic},
		Prefetch:    10,
	}

	handle := func(body []byte) broker.Status {
		if tool.Debug() {
			log.Printf("group: %s, topic: %s, hostId: %s, receive msg: %s", group, topic, hostId, body)
		}

		if errors.Is(job(body), RetryError) {
			return broker.Retry
		}
		return broker.Success
	}

	c.runForever(fmt.Sprintf("group: %s, topic: %s, hostId: %s,", group, topic, hostId), func() error {
		return c.broker.ConsumerTopic(opt, handle)
	})
}

// LaunchBroadcastJob 以广播方式消费：每个实例独享临时独占队列，所有实例都收到全量消息。
// 消息体是发布方的原始字节，没有 Message 信封；ctx 的 opId 取自 AMQP header（发布方写入）。
// handler panic 时打印日志并丢弃该条消息，不重投；断线后重建新队列，断线期间的消息不补发。
// 语义对齐 redis pubsub，适合跨实例通知总线，不适合不能丢消息的场景。
// 阻塞直到 Shutdown。要求 ExchangeOpt.Type 为 fanout。
func (c *Consumer) LaunchBroadcastJob(f func(ctx context.Context, body []byte)) {
	handle := func(headers amqp.Table, body []byte) {
		ctx := context.Background()
		if opId, ok := headers["opId"].(string); ok && opId != "" {
			ctx = context.WithValue(ctx, "opId", opId)
		}
		if tool.Debug() {
			lg := log2.CtxLogger(ctx).WithFields(logrus.Fields{})
			lg.Infof("recv, hostId: %s, body: %s", hostId, body)
		}
		f(ctx, body)
	}
	c.runForever(fmt.Sprintf("broadcast hostId: %s", hostId), func() error {
		return c.broker.ConsumerBroadcast(handle)
	})
}

// LaunchDirectedJob 以定向广播方式消费：每个实例独享临时独占队列（server-named、非持久、自动删），
// 队列以 routingKey 绑定到配置的 exchange（应为 direct/topic），只收到匹配 routingKey 的消息。
// 消息体是发布方原始字节，没有 Message 信封；ctx 的 opId 取自 AMQP header。
// handler panic 时打印日志并丢弃，不重投；断线后重建新队列，断线期间消息不补发；
// 本实例不在（队列已自动删）时发布方消息 unroutable 直接丢。语义对齐 redis 按节点 channel 订阅，
// 适合可丢的定向通知（如按 nodeId 投到目标网关），不适合不能丢消息的场景。
// 阻塞直到 Shutdown。routingKey 不能为空。
func (c *Consumer) LaunchDirectedJob(routingKey string, f func(ctx context.Context, body []byte)) {
	handle := func(headers amqp.Table, body []byte) {
		ctx := context.Background()
		if opId, ok := headers["opId"].(string); ok && opId != "" {
			ctx = context.WithValue(ctx, "opId", opId)
		}
		if tool.Debug() {
			lg := log2.CtxLogger(ctx).WithFields(logrus.Fields{})
			lg.Infof("recv, hostId: %s, routingKey: %s, body: %s", hostId, routingKey, body)
		}
		f(ctx, body)
	}
	c.runForever(fmt.Sprintf("directed hostId: %s routingKey: %s", hostId, routingKey), func() error {
		return c.broker.ConsumerDirected(routingKey, handle)
	})
}

// runForever 反复执行一次消费会话，直到 Shutdown。
// 会话运行超过 1 分钟视为稳定，退避从头开始；否则逐步拉长，避免持续失败时空转、
// 以及 broker 重启后所有 Pod 同时重建消费造成惊群。
func (c *Consumer) runForever(name string, consume func() error) {
	bo := backoff.New(time.Second, 30*time.Second)
	for {
		if c.stopped() {
			log.Printf("%s stopped", name)
			return
		}
		log.Printf("%s start consume...", name)
		start := time.Now()
		err := consume()
		if errors.Is(err, broker.ErrShutdown) || c.stopped() {
			log.Printf("%s stopped", name)
			return
		}
		if time.Since(start) > time.Minute {
			bo.Reset()
		}
		wait := bo.Next()
		log.Printf("%s consume error: %v ,retrying consume after %v", name, err, wait)
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-c.stopCh():
			t.Stop()
		}
	}
}

// Shutdown 优雅停机，适合在 k8s 收到 SIGTERM 时调用：
// 停止接收新消息，等待在途消息处理并确认（受 ctx 约束），然后关闭连接；
// LaunchJob / LaunchTopicJob 随之返回。预取但未处理的消息由 broker 重新入队给其他实例。
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		if c.stop == nil {
			c.stop = make(chan struct{})
		}
		close(c.stop)
	})
	if s, ok := c.broker.(interface{ Shutdown(context.Context) error }); ok {
		return s.Shutdown(ctx)
	}
	return nil
}

func (c *Consumer) stopCh() <-chan struct{} {
	return c.stop
}

func (c *Consumer) stopped() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

//func (c *Consumer) LaunchDirectJob(key string, job Job, param ...Param) {
//	c.LaunchJob(key, key, job, param...)
//}

func (c *Consumer) monitoring(address string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		status := "UP"
		if !c.broker.Health() {
			status = "DOWN"
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Print(status)
		_, _ = w.Write([]byte(status))
	}))

	log.Printf("monitoring server listen on port %s...\n", address)
	if err := http.ListenAndServe(address, mux); err != nil {
		panic(err)
	}
}
