# rabbitmq

基于 [streadway/amqp](https://github.com/streadway/amqp) 的 RabbitMQ 生产者 / 消费者封装。

- 生产端开启 publisher confirm，断线自动重连，所有调用都有超时，不会无限期阻塞
- 消费端断线自动重连（带抖动的指数退避），失败消息按 1s / 5s / 10s 退避重投，最多 3 次
- 支持消费限流（每 N 时间最多消费 M 条）
- 支持优雅停机，适配 k8s 滚动发布
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
	// 返回 rabbitmq.RetryError（或用 %w 包装它）会退避重投，见「重试」
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
- 等待令牌期间消息处于未确认状态，`Interval` 应明显小于 broker 的消费超时（开源 RabbitMQ 默认 30 分钟，阿里云 RabbitMQ 版默认 5~30 分钟），否则消息会被 broker 判定超时后重投。

## 重试

handler 返回 `rabbitmq.RetryError`（包括用 `fmt.Errorf("...: %w", rabbitmq.RetryError)` 包装的错误）或发生 panic 时，消息会被重投。规则固定，`LaunchJob` 与 `LaunchTopicJob` 一致：

- 第 1 / 2 / 3 次重投前分别等待 **1s / 5s / 10s**；
- 同一条消息**最多重投 3 次**（handler 最多执行 4 次），第 4 次仍失败则 `Nack` 不重新入队：队列配置了死信则进入死信，否则丢弃，并打印一条日志：

```
rabbitmq: drop message after 3 redeliveries (max 3): queue=order-queue exchange=order-exchange routingKey=order.created messageId= opId=... bodyBytes=128 body="..."
```

- handler 返回 `nil` 或其他错误时消息都会被确认，不会重投。

实现与分布式部署：

- 重投次数记在消息头 `x-retry-count`（int32）上，随消息在 Pod 之间流转，多副本竞争消费时计数依然准确。
- 重投消息先进入延迟队列 `delay.v1.<1s|5s|10s>.<队列名>`，到期后死信到 direct exchange `afret0.rabbitmq.retry`，按队列名精确投回原队列，**不会**重复投给绑定了同一 routing key 的其他队列（其他服务、其他消费组）。
- 重投消息通过独立连接发布并等待 broker 确认后，才确认原消息。
- broker 暂时无法接收重投消息（内存/磁盘告警、限流、没有声明权限等）时，改为在进程内按同样的退避重试，次数照常累计，消息不会丢，上限依然生效。
- panic 会被捕获并记录堆栈，按重投处理，不会让进程崩溃。

`RetryAfter` 与 `LaunchJobOpt.WithRetry` 已废弃，传入不会报错但不再生效（会打印一次提示）；`broker.Queue.RetryQueue` 同样不再生效。

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

`delay` 单位是**秒**，`<= 0` 时等同于 `Publish` 立即投递。

## 发布的超时与错误

`Publish` / `PublishDelay` / `DeclareQueue` 都不会无限期阻塞：

- 调用受 `ctx` 约束；`ctx` 没有 deadline 时，等待连接恢复、等待 broker 解除阻塞、等待确认各自最多 5 秒。
- broker 因内存/磁盘告警阻塞连接且 5 秒内未解除时返回 `broker.ErrBlocked`（可用 `errors.Is` 判断）。
- 路由不到任何队列时返回 `amqp message unroutable, ...` 错误（消息被 broker 退回，不会静默丢失）。
- 连接刚断开、消息确定还没发出时，会在同一个超时预算内等待重连并自动重发一次。
- 返回超时类错误时，消息仍可能已经到达 broker（at-least-once），重发请确保业务幂等。
- 同一个 `Producer` 可被多个 goroutine 并发使用，内部最多 8 个 channel 并行等待确认。

## 优雅停机（k8s）

```go
sig := make(chan os.Signal, 1)
signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
<-sig

ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // 小于 terminationGracePeriodSeconds
defer cancel()
_ = consumer.Shutdown(ctx) // 停止接收新消息，等待在途消息处理并确认，LaunchJob / LaunchTopicJob 随之返回
_ = producer.Close()
```

- 预取但还没处理的消息会通过 `basic.cancel` 交还给 broker，由其他 Pod 继续处理。
- `ctx` 到期时仍未处理完的消息不会被确认，由 broker 重新投递（at-least-once）。
- 不调用 `Shutdown` 的旧代码行为不变，进程退出时未确认消息由 broker 重新投递。

## 连接与重连

- `Consumer` 的主连接只用于消费，从不发布，因此 broker 告警阻塞发布时消费和 ack 不受影响；重投消息走另一条按需建立的连接。
- 断线后按 0.25s 起、最长 30s 的带抖动指数退避重连，避免 broker 重启后所有 Pod 同时重连形成惊群；连接恢复后消费自动重建，不再固定等待 30 秒。
- 心跳 10 秒，网络黑洞约 30 秒内被发现并重连。
- 连接名（`connection_name`）为 `<hostname>/main`、`<hostname>/side`，在 k8s 中即 Pod 名，便于在管理台定位。

## broker 上会创建的对象

除业务自己的 exchange / queue 外，本库还会声明：

| 对象 | 说明 |
| --- | --- |
| exchange `afret0.rabbitmq.retry`（direct，持久化） | 重投消息回到原队列的通道，每个消费队列以自身名字绑定 |
| 队列 `delay.v1.<1s\|5s\|10s>.<队列名>`（持久化） | 重投用的延迟队列，闲置 7 天后由 broker 自动删除 |
| 队列 `delay.<秒>.<exchange>.<routingKey>` | `PublishDelay` 的延迟队列（与旧版本相同） |

如果 RabbitMQ 账号的 configure 权限受限，需放行上述名字；未放行时失败消息退化为进程内重试，规则不变。

## 滚动升级

新旧版本可以同时运行：队列声明参数保持一致，重投计数与旧版本共用 `x-retry-count`，新旧 Pod 混跑时不会出现 `PRECONDITION_FAILED`，有毒消息的重投次数依然有上限。旧版本创建的 `delay.<时长>.<exchange>.<队列>` 重试队列会按原有的 `x-expires` 自动过期。

## 阿里云 RabbitMQ 版注意事项

以下为平台行为，与客户端实现无关：

- **TTL 精度**：延迟队列到期存在数秒偏差（实测 1s 的 TTL 实际 1.3s~8s），所以 `PublishDelay` 和重投间隔在阿里云上会比设定值长几秒。
- **未确认消息的重投**：连接或 channel 异常断开（Pod 被杀、网络中断）时，阿里云要等到队列的「消费超时时间」（控制台可配，默认 5~30 分钟，测试实例实测 30 分钟）才重投其中未确认的消息；`basic.cancel` 则会在数秒到约 40 秒内重投。因此请使用 `Consumer.Shutdown` 停机，并按业务需要在控制台调小消费超时时间（需大于 handler 最长耗时 + 16 秒退避）。
- **服务端投递上限**：阿里云自身的「最大投递次数」默认 16 次，本库的 3 次上限先生效。
- **接口限流**：超出实例 TPS 时 broker 以 530 关闭 channel，本库会自动重建 channel；发布返回错误时请退避后重试。

## 链路追踪

`NewJob` 内置了 OpenTelemetry 埋点，由环境变量 **`TRACE`** 控制开关（取值 `true` / `TRUE` / `1` / `yes` / `YES` 时开启），和 sample 里 `traceSvc.Init`、handler 用的是同一个开关。关闭时不会创建任何 span，也不会有额外开销。

TracerProvider 仍由业务方在 worker 启动时初始化（如 `traceSvc.Init(cfg, svc)`），本库只负责生成 span。

- 每条消息都会开启一条**全新的 trace**（`trace.WithNewRoot()`），不会和上游生产消息时的 trace 串在一起。
- span 名为 `rabbitmq.Consume <handler 函数名>`，`SpanKind` 为 `consumer`。
- span 属性：`msgId`（消息里的 `msgId`）、`opId`（同 `msgId`，与 producer 端 `Publish` 和 sample handler 的字段命名保持一致）、`job`、`hostId`。
- 消息反序列化失败或 handler 返回错误时，会记录到 span 上并把状态置为 `Error`。
- span 通过 ctx 传递给 handler，handler 内继续 `otel.Tracer(...).Start(ctx, ...)` 即可挂到同一条链路上。
## 测试

```bash
# 单元测试，无需 broker
go test -race ./broker ./internal/...

# 集成测试与端到端测试（未设置 RABBITMQ_TEST_URL 时自动跳过）
docker run -d --name rmq -p 5672:5672 rabbitmq:3-management
RABBITMQ_TEST_URL=amqp://guest:guest@127.0.0.1:5672/ \
RABBITMQ_TEST_CONTAINER=rmq \
go test -race -timeout 30m ./broker ./e2e
```

- `RABBITMQ_TEST_CONTAINER`：docker 容器名，开启依赖 `rabbitmqctl` 的故障注入（内存告警、强制断开连接、重启 broker、受限权限）。网络黑洞与连接重置通过内置 TCP 代理注入，对任意 broker 都可运行。
- 针对阿里云 RabbitMQ 版运行时追加 `RABBITMQ_TEST_TTL_SLACK=20s RABBITMQ_TEST_REQUEUE_WAIT=90s RABBITMQ_TEST_SLOW_REQUEUE=1`，以适配上文的平台行为。
- 根目录下原有的测试需要可用的 broker 地址，按需单独运行。
