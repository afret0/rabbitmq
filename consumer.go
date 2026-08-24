package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/afret0/rabbitmq/broker"
	log2 "github.com/afret0/wheel/log"
)

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
	return func(msgS []byte) error {
		c1, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		lg1 := log2.CtxLogger(c1).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg1.Printf("receive msg: %s", string(msgS))
		}

		M := &Message[T]{}
		err := tool.Unmarshal(string(msgS), M)
		if err != nil {
			lg1.Printf("unmarshal message error: %s, msg: %s", err, string(msgS))
			return err
		}

		ctx := context.WithValue(c1, "opId", M.MsgId)
		lg := log2.CtxLogger(ctx).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg.Infof("start process message: %s", string(msgS))
		}

		err = f(ctx, M.Data)
		if err != nil {
			lg.Errorf("process message error: %s", err)
			return err
		}

		if tool.Debug() {
			lg.Infof("process message success: %s", string(msgS))
		}

		return nil
	}
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
