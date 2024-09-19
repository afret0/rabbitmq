package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/afret0/rabbitmq/broker"
	"github.com/afret0/rabbitmq/help"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

type params struct {
	retryQueue []int64
}

type Param func(*params)

func Retry(strategy help.RetryStrategy, retry help.Retry) Param {
	return func(p *params) {
		if strategy == help.CUSTOMQUEUE {
			for _, delay := range retry.Queue {
				d, err := time.ParseDuration(delay)
				if err != nil {
					panic(err)
				}
				p.retryQueue = append(p.retryQueue, int64(d/time.Millisecond))
			}
			return
		}
		d, err := time.ParseDuration(retry.Delay)
		if err != nil {
			panic(err)
		}
		p.retryQueue = help.GetRetryQueue(int64(d/time.Millisecond), retry.Max, strategy)
	}
}

func evaParam(param []Param) *params {
	ps := &params{}
	for _, p := range param {
		p(ps)
	}
	return ps
}

func (c *Consumer) LaunchJob(key, queue string, job Job, param ...Param) {
	ps := evaParam(param)

	q := &broker.Queue{
		Name:       queue,
		RouteKey:   key,
		RetryQueue: ps.retryQueue,
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
		if err := c.broker.Consume(q); err != nil {
			log.Printf("job %s consume error: %v ,retrying consume after 30s", queue, err)
			time.Sleep(30 * time.Second)
		}
	}
}

func (c *Consumer) LaunchDirectJob(key string, job Job, param ...Param) {
	c.LaunchJob(key, key, job, param...)
}

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
