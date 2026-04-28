package rabbitmq

import (
	"context"
	"encoding/json"

	"github.com/afret0/rabbitmq/broker"
)

type ProducerOptions struct {
	ExchangeOpt *ExchangeOption
	BrokerURL   string
}

type Producer struct {
	broker broker.Broker
}

func NewProducer(opt *ProducerOptions) *Producer {
	broker := broker.NewAmqpBroker(&broker.AmqpBrokerOptions{
		Url:          opt.BrokerURL,
		Exchange:     opt.ExchangeOpt.Name,
		ExchangeType: opt.ExchangeOpt.Type,
	})
	return &Producer{broker: broker}
}

// DeclareQueue 在 publisher 端预先声明队列并绑定到 exchange。
// 推荐在服务启动时对所有要发送的 routing key / queue 调用一次，
// 这样即便 consumer 还没起，broker 也不会因为 unroutable 把消息丢掉。
func (p *Producer) DeclareQueue(name, routingKey string) error {
	return p.broker.DeclareQueue(name, routingKey)
}

func (p *Producer) Publish(ctx context.Context, key string, data interface{}) error {
	var body []byte
	switch d := data.(type) {
	case string:
		body = []byte(d)
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body = b
	}
	return p.broker.Publish(ctx, key, body)
}

func (p *Producer) PublishDelay(ctx context.Context, key string, data interface{}, delay int64) error {
	var body []byte
	switch d := data.(type) {
	case string:
		body = []byte(d)
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body = b
	}
	return p.broker.PublishDelay(ctx, key, body, delay)
}
