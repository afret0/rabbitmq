package broker

import (
	"context"

	"github.com/streadway/amqp"
)

type Status int

const (
	Success Status = iota
	Retry
)

type Queue struct {
	Name     string
	RouteKey string
	// Deprecated: 不再生效。Handle 返回 Retry 时统一按 1s / 5s / 10s 退避重投,
	// 同一条消息最多重投 3 次, 之后丢弃(队列配置了死信则进入死信)并打印日志。
	RetryQueue []int64
	Handle     func([]byte) Status
}

type Broker interface {
	Consume(queue *Queue, optChain ...*ConsumeOption) error
	ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error
	ConsumerBroadcast(handle func(headers amqp.Table, body []byte)) error
	ConsumerDirected(routingKey string, handle func(headers amqp.Table, body []byte)) error
	Publish(ctx context.Context, key string, body []byte) error
	// PublishDelay 延迟投递, delaySeconds 单位为秒。
	PublishDelay(ctx context.Context, queue string, body []byte, delaySeconds int64) error
	PublishBroadcast(ctx context.Context, body []byte) error
	DeclareQueue(name, routingKey string) error
	Health() bool
}
