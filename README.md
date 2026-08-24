# rabbitmq

基于 [streadway/amqp](https://github.com/streadway/amqp) 的 RabbitMQ 生产者 / 消费者封装。

- 生产端开启 publisher confirm，断线自动重建 channel
- 消费端断线自动重连，支持重试队列
- 支持消费限流（每 N 时间最多消费 M 条）
- 泛型消息体，handler 直接拿到具体类型

## 安装

```bash
go get github.com/afret0/rabbitmq
```

## 快速开始

```go
exchange := &rabbitmq.ExchangeOption{Name: "order-exchange", Type: "direct"}

producer := rabbitmq.NewProducer(&rabbitmq.ProducerOptions{
	ExchangeOpt: exchange,
	BrokerURL:   "amqp://user:pass@127.0.0.1:5672/",
})

consumer := rabbitmq.NewConsumer(&rabbitmq.ConsumerOptions{
	ExchangeOpt: exchange,
	BrokerURL:   "amqp://user:pass@127.0.0.1:5672/",
})
```

## 消息体

`Message[T]` 是消息信封，`MsgId` 会被透传到 handler 的 `ctx`（key 为 `opId`）用于串联日志。

```go
type Order struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

// 生产
producer.Publish(ctx, "order.created", &rabbitmq.Message[*Order]{
	MsgId: "20260824-0001",
	Data:  &Order{OrderID: "o-9", Amount: 100},
})

// 消费：NewJob 用泛型解出 Data，handler 直接拿到 *Order
job := rabbitmq.NewJob(func(ctx context.Context, o *Order) error {
	// 返回 rabbitmq.RetryError 会进入重试队列
	return nil
})
```

## 消费限流

限流语义只有一条：**每 `Interval` 最多消费 `Limit` 条**。

```go
// 每秒最多 5 条
consumer.LaunchJob("order.created", "order-queue", job, rabbitmq.LimitPerSecond(5))

// 每 2 秒最多 1 条
consumer.LaunchJob("order.created", "order-queue", job, rabbitmq.LimitEvery(2*time.Second, 1))

// 不限流（默认预取 10）
consumer.LaunchJob("order.created", "order-queue", job)

// 不限流，只调大预取
consumer.LaunchJob("order.created", "order-queue", job, &rabbitmq.LaunchJobOpt{Prefetch: 50})
```

`LaunchJobOpt` 就是 `broker.ConsumeOption`：

| 字段 | 说明 |
| --- | --- |
| `Prefetch` | 未 Ack 消息上限（QoS），`<=0` 时取 `10`；**开启限流时会被自动对齐为 `Limit`**，不用手动设 |
| `Interval` | 限流窗口，`<=0` 表示不限流 |
| `Limit` | 每个窗口最多消费的条数，`<=0` 时取 `1` |

说明：

- 限流器是令牌桶，速率为 `Limit/Interval`、桶容量为 `Limit`。所以启动瞬间最多放行 `Limit` 条，之后按窗口配额平滑供给。
- 限流作用于「取到消息之后、执行 handler 之前」，因此实际吞吐是 `min(限流速率, 1/handler 耗时)`。handler 本身慢于限流速率时，限流不会额外生效。
- 限流目前只对 `LaunchJob` 生效，`LaunchTopicJob` 暂不支持。

## 重试

handler 返回 `rabbitmq.RetryError` 表示这条消息需要重试。用 `RetryAfter` 配置退避序列：

```go
// 第 1 次重试等 1 秒，第 2 次等 5 秒，第 3 次等 30 秒，之后不再重试
consumer.LaunchJob(key, queue, job, rabbitmq.RetryAfter(time.Second, 5*time.Second, 30*time.Second))

// 也可以和限流一起用
consumer.LaunchJob(key, queue, job,
	rabbitmq.LimitPerSecond(5).WithRetry(time.Second, 5*time.Second))
```

行为：

- 每次重试会把消息投递到一个带 TTL 的延迟队列，到期后经死信路由回原队列，重试次数记在 `x-retry-count` 头上。
- **没有配置退避序列时**，`RetryError` 会让消息立即重新入队（`Nack` + requeue）。这能保证消息不丢，但如果 handler 一直失败就会持续重投，所以需要重试时请显式配置 `RetryAfter`。
- **重试次数用尽时**，消息被 `Nack`（不重新入队），有死信配置则进死信，否则丢弃，同时打印日志。

## 广播 / 消费组

同一个 `group` 内的实例共享队列（竞争消费），不同 `group` 各收一份（广播）。

```go
consumer.LaunchTopicJob("order-group", "order.*", job)
```

## 生产端预声明队列

消息发出时若没有任何队列绑定该 routing key，broker 会直接丢弃。建议启动时预声明：

```go
producer.DeclareQueue("order-queue", "order.created")
```

## 延迟消息

```go
producer.PublishDelay(ctx, "order.created", data, 5) // 5 秒后投递
```

注意单位不一致：`PublishDelay` 的 `delay` 单位是**秒**，而 `broker.Queue.RetryQueue`（重试退避队列）里的值单位是**毫秒**。