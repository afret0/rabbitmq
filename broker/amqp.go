package broker

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

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
	if a.conn != nil {
		cc := a.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("amqp conn close: %v", <-cc)
	}

	var err error
	if a.conn, err = amqp.Dial(a.options.Url); err != nil {
		log.Printf("amqp redial faild: %v", err)
		time.AfterFunc(5*time.Second, a.keepAlive)
		return
	}
	log.Println("amqp redial success...")

	for _, n := range a.notifies {
		n <- amqp.ErrClosed
	}
	a.keepAlive()
}

func (a *AmqpBroker) Health() bool {
	return a.conn != nil
}

func (a *AmqpBroker) Consume(queue *Queue) error {
	if a.conn == nil {
		return errors.New("conn is nil")
	}
	channel, err := a.conn.Channel()
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
	if err := channel.Qos(10, 0, false); err != nil {
		return err
	}

	if err := channel.QueueBind(queue.Name, queue.RouteKey, a.options.Exchange, false, nil); err != nil {
		return err
	}

	delivery, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	notify := make(chan *amqp.Error)
	defer close(notify)

	a.m.Lock()
	a.notifies[queue.Name] = notify
	a.m.Unlock()

	for {
		select {
		case err := <-notify:
			return err
		case d := <-delivery:
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
		}
	}
}

type GroupConsumeOption struct {
	Group       string   // 消费组名（同组共享队列）
	RoutingKeys []string // 需要订阅的多个 topic / pattern
	Prefetch    int      // 每实例并行未 Ack 上限
}

func (a *AmqpBroker) ConsumerTopic(opt *GroupConsumeOption, handle func([]byte) Status) error {
	if a.conn == nil {
		return fmt.Errorf("conn is nil")
	}
	if opt.Group == "" || len(opt.RoutingKeys) == 0 {
		return fmt.Errorf("group or routing keys empty")
	}

	ch, err := a.conn.Channel()
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
	channel, err := a.conn.Channel()
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

func (a *AmqpBroker) Publish(key string, body []byte) error {
	channel, err := a.conn.Channel()
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

	return channel.Publish(a.options.Exchange, key, false, false, amqp.Publishing{
		Headers:      amqp.Table{},
		ContentType:  "",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

func (a *AmqpBroker) PublishDelay(queue string, body []byte, delay int64) error {
	channel, err := a.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	delayQ := fmt.Sprintf("delay.%d.%s.%s", delay, a.options.Exchange, queue)

	if _, err := channel.QueueDeclare(delayQ,
		true, true, false, false, amqp.Table{
			"x-dead-letter-exchange":    a.options.Exchange,
			"x-dead-letter-routing-key": queue,
			"x-message-ttl":             delay * 1000,
			"x-expires":                 delay * 2 * 1000,
		},
	); err != nil {
		return err
	}

	return channel.Publish("", delayQ, false, false, amqp.Publishing{
		Headers:      amqp.Table{},
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}
