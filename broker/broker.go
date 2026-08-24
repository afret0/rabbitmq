package broker

import "context"

type Status int

const (
	Success Status = iota
	Retry
)

type Queue struct {
	Name       string
	RouteKey   string
	RetryQueue []int64
	Handle     func([]byte) Status
}

type Broker interface {
	Consume(queue *Queue, optChain ...*ConsumeOption) error
	ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error
	Publish(ctx context.Context, key string, body []byte) error
	PublishDelay(ctx context.Context, queue string, body []byte, delay int64) error
	DeclareQueue(name, routingKey string) error
	Health() bool
}
