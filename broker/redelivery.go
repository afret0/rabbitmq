package broker

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

const (
	// retryCountHeader 记录消息已被重投的次数，随消息在 Pod 之间流转，保证分布式部署下计数准确。
	// 与旧版本共用同一个 header 与 int32 类型，滚动升级期间新旧 Pod 对计数的理解一致。
	retryCountHeader = "x-retry-count"

	originalExchangeHeader   = "x-original-exchange"
	originalRoutingKeyHeader = "x-original-routing-key"

	// retryExchange 是重投专用的 direct exchange：延迟队列到期后死信到这里，再按 routing key = 队列名
	// 精确投回原队列。旧实现死信回主 exchange 重新路由，会把重试消息重复投给绑定了同一 routing key
	// 的其他队列（其他服务、其他消费组），fanout exchange 下则是所有队列。
	retryExchange = "afret0.rabbitmq.retry"

	// maxRedeliveries 同一条消息最多重投的次数，超过后丢弃（队列配置了死信则进入死信）。
	maxRedeliveries = 3

	// 延迟队列闲置 7 天后由 broker 自动删除；发布前每小时至少重新声明一次以续期，
	// 保证队列里还有消息时不会过期。
	delayQueueIdleExpiry   = 7 * 24 * time.Hour
	delayQueueDeclareCache = time.Hour

	dropLogBodyLimit = 512
)

// redeliveryBackoff 第 n 次重投前的等待时间。
var redeliveryBackoff = []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}

var errRetryTopologyUnavailable = errors.New("retry topology unavailable")

// redeliveryCount 读取消息已被重投的次数，兼容 broker 可能改写的各种整数类型。
func redeliveryCount(h amqp.Table) int {
	v, ok := h[retryCountHeader]
	if !ok {
		return 0
	}
	var n int64
	switch x := v.(type) {
	case int8:
		n = int64(x)
	case int16:
		n = int64(x)
	case int32:
		n = int64(x)
	case int64:
		n = x
	case int:
		n = int64(x)
	case uint8:
		n = int64(x)
	case uint16:
		n = int64(x)
	case uint32:
		n = int64(x)
	case uint64:
		if x > math.MaxInt32 {
			return math.MaxInt32
		}
		n = int64(x)
	case float32:
		n = int64(x)
	case float64:
		n = int64(x)
	case string:
		p, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0
		}
		n = p
	default:
		return 0
	}
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(n)
}

func backoffFor(count int) time.Duration {
	if count < 0 {
		count = 0
	}
	if count >= len(redeliveryBackoff) {
		count = len(redeliveryBackoff) - 1
	}
	return redeliveryBackoff[count]
}

// delayQueueDecl 返回某个队列在给定退避时长下的延迟队列。
// 名称与参数只由 (queue, delay) 决定：所有 Pod 声明完全一致，不会触发 PRECONDITION_FAILED。
// 使用 delay.v1. 前缀，与旧版本 delay.<dur>.<exchange>.<queue> 的队列互不冲突，滚动升级期间可共存。
func delayQueueDecl(queue string, delay time.Duration) *queueDecl {
	name := fmt.Sprintf("delay.v1.%s.%s", delay, queue)
	if len(name) > 255 {
		sum := sha1.Sum([]byte(queue))
		name = fmt.Sprintf("delay.v1.%s.%x", delay, sum[:10])
	}
	ttl := delay.Milliseconds()
	return &queueDecl{
		name:    name,
		durable: true,
		args: amqp.Table{
			"x-dead-letter-exchange":    retryExchange,
			"x-dead-letter-routing-key": queue,
			"x-message-ttl":             ttl,
			"x-expires":                 delayQueueIdleExpiry.Milliseconds() + ttl,
		},
		cacheFor: delayQueueDeclareCache,
	}
}

// redeliveryPublishing 复制原消息用于重投，并把重投次数写入 header。
func redeliveryPublishing(d amqp.Delivery, next int) amqp.Publishing {
	headers := make(amqp.Table, len(d.Headers)+3)
	for k, v := range d.Headers {
		// x-death 等由 broker 维护，不随重投复制
		if isDeathHeader(k) {
			continue
		}
		headers[k] = v
	}
	headers[retryCountHeader] = int32(next)
	if _, ok := headers[originalExchangeHeader]; !ok {
		headers[originalExchangeHeader] = d.Exchange
	}
	if _, ok := headers[originalRoutingKeyHeader]; !ok {
		headers[originalRoutingKeyHeader] = d.RoutingKey
	}
	// 不复制 Expiration（会与延迟队列 TTL 相互影响）和 UserId（broker 会校验与当前连接用户一致）
	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}

func isDeathHeader(k string) bool {
	return k == "x-death" || strings.HasPrefix(k, "x-first-death-") || strings.HasPrefix(k, "x-last-death-")
}

// redeliverer 处理单条消息：调用 handler，失败时按 1s/5s/10s 退避重投，最多 3 次后丢弃。
type redeliverer struct {
	queue  string
	handle func([]byte) Status
	// schedule 把消息作为第 next 次重投发到 delay 后到期的延迟队列；nil 表示重投拓扑不可用
	schedule func(d amqp.Delivery, next int, delay time.Duration) error
	// wait 等待 d，被停机或断线打断时返回 false
	wait func(d time.Duration) bool
	// persist 在进程内重试被停机打断时，尽力把累加后的次数交给 broker（成功返回 true），
	// 避免消息带着旧计数回到队列、在其他 Pod 上多出重投次数。nil 表示不尝试。
	persist func(d amqp.Delivery, next int, delay time.Duration) bool
}

func (r *redeliverer) process(d amqp.Delivery) {
	count := redeliveryCount(d.Headers)
	for {
		if r.invoke(d.Body) != Retry {
			r.ack(d)
			return
		}
		if count >= maxRedeliveries {
			r.logDrop(d, count)
			r.nack(d, false)
			return
		}

		delay := backoffFor(count)
		err := errRetryTopologyUnavailable
		if r.schedule != nil {
			if err = r.schedule(d, count+1, delay); err == nil {
				r.ack(d)
				return
			}
		}

		// 无法通过 broker 调度（连接被阻塞、限流、权限不足等）：不丢消息，
		// 在本进程内按同样的退避重试，次数照常累计，保证上限仍然生效。
		log.Printf("rabbitmq: queue %s schedule redelivery %d/%d failed: %v, retrying in-process after %v",
			r.queue, count+1, maxRedeliveries, err, delay)
		if !r.wait(delay) {
			if r.persist != nil && r.persist(d, count+1, delay) {
				r.ack(d)
				return
			}
			// 放回队列由其他消费者处理（channel 已关闭时 broker 会自动重新入队）
			r.nack(d, true)
			return
		}
		count++
	}
}

func (r *redeliverer) invoke(body []byte) (status Status) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("rabbitmq: queue %s handler panic: %v\n%s", r.queue, p, debug.Stack())
			status = Retry
		}
	}()
	return r.handle(body)
}

func (r *redeliverer) ack(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		log.Printf("rabbitmq: queue %s ack failed: %v", r.queue, err)
	}
}

func (r *redeliverer) nack(d amqp.Delivery, requeue bool) {
	if err := d.Nack(false, requeue); err != nil {
		log.Printf("rabbitmq: queue %s nack(requeue=%v) failed: %v", r.queue, requeue, err)
	}
}

func (r *redeliverer) logDrop(d amqp.Delivery, count int) {
	exchange, routingKey := d.Exchange, d.RoutingKey
	if v, ok := d.Headers[originalExchangeHeader].(string); ok {
		exchange = v
	}
	if v, ok := d.Headers[originalRoutingKeyHeader].(string); ok {
		routingKey = v
	}
	body := d.Body
	if len(body) > dropLogBodyLimit {
		body = body[:dropLogBodyLimit]
	}
	log.Printf("rabbitmq: drop message after %d redeliveries (max %d): queue=%s exchange=%s routingKey=%s messageId=%s opId=%v bodyBytes=%d body=%q",
		count, maxRedeliveries, r.queue, exchange, routingKey, d.MessageId, d.Headers["opId"], len(d.Body), body)
}
