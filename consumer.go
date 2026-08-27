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
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/afret0/rabbitmq/broker"
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

	consumer := &Consumer{broker: b}
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

// RetryAfter 配置重试退避序列：handler 返回 RetryError 时，
// 第 n 次重试等待 delays[n]，用尽后不再重试。
//
//	consumer.LaunchJob(key, queue, job, RetryAfter(time.Second, 5*time.Second))
func RetryAfter(delays ...time.Duration) *LaunchJobOpt {
	return (&LaunchJobOpt{}).WithRetry(delays...)
}

func (c *Consumer) LaunchJob(key, queue string, job Job, optChain ...*LaunchJobOpt) {
	retryQueue := make([]int64, 0)
	if len(optChain) > 0 && optChain[0] != nil {
		retryQueue = optChain[0].RetryQueueMillis()
	}

	q := &broker.Queue{
		Name:       queue,
		RouteKey:   key,
		RetryQueue: retryQueue,
		Handle: func(body []byte) broker.Status {
			var err error

			defer func(begin time.Time) {
				// c.metrics.JobHandleDuration.ObserveWith(map[string]string{
				// 	"name": queue,
				// 	"Err":  fmt.Sprintf("%v", Err),
				// }, time.Since(begin).Seconds())
			}(time.Now())

			switch err = job(body); err {
			case RetryError:
				return broker.Retry
			default:
				return broker.Success
			}
		},
	}

	for {
		log.Printf("job %s start consume...", queue)
		if err := c.broker.Consume(q, optChain...); err != nil {
			log.Printf("job %s consume error: %v ,retrying consume after 30s", queue, err)
			time.Sleep(30 * time.Second)
		}
	}
}

// LaunchTopicJob  group: 即队列名, topic: routing key
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
			log.Printf("group: %s, topic: %s, hostId: %s, receive msg: %s", group, topic, tool.HostId(), body)
		}

		switch err := job(body); err {
		case RetryError:
			return broker.Retry
		default:
			return broker.Success
		}
	}

	for {
		log.Printf("group: %s, topic: %s, hostId: %s, start consume...", group, topic, tool.HostId())
		if err := c.broker.ConsumerTopic(opt, handle); err != nil {
			log.Printf("group: %s, topic: %s, hostId: %s, consume error: %v ,retrying consume after 30s", group, topic, tool.HostId(), err)
			time.Sleep(30 * time.Second)
		}

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
