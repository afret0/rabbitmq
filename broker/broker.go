package broker

import "context"

type Status int

const (
	Success Status = iota
	Retry
)

type Queue struct {
	Name     string
	RouteKey string
	// RetryQueue 是重试退避队列, 第 n 次重试等待 RetryQueue[n]。
	// 单位为毫秒(与 AMQP 的 x-message-ttl 一致), 注意与 PublishDelay 的秒不同。
	// 为空时不重试。
	RetryQueue []int64
	Handle     func([]byte) Status
}

type Broker interface {
	Consume(queue *Queue, optChain ...*ConsumeOption) error
	ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error
	Publish(ctx context.Context, key string, body []byte) error
	// PublishDelay 延迟投递, delaySeconds 单位为秒。
	// 注意与 Queue.RetryQueue 的单位(毫秒)不同。
	PublishDelay(ctx context.Context, queue string, body []byte, delaySeconds int64) error
	DeclareQueue(name, routingKey string) error
	Health() bool
}
