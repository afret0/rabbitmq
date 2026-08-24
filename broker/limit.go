package broker

import (
	"time"

	"golang.org/x/time/rate"
)

// defaultPrefetch 是未显式配置时的未 Ack 消息上限。
const defaultPrefetch = 10

// ConsumeOption 消费配置。
//
// 限流语义只有一条：每 Interval 最多消费 Limit 条。
// Interval <= 0 表示不限流；此时只有 Prefetch 生效。
//
// 一般不需要手写这个结构体，直接用 Every / PerSecond 构造：
//
//	Every(2*time.Second, 1) // 每 2 秒最多 1 条
//	PerSecond(5)            // 每秒最多 5 条
//	&ConsumeOption{Prefetch: 50} // 不限流, 只调大预取
type ConsumeOption struct {
	// Prefetch 未 Ack 消息上限, <=0 时取 10。
	// 开启限流时会自动对齐为 Limit, 无需手动设置。
	Prefetch int

	Interval time.Duration // 限流窗口
	Limit    int           // 每个窗口最多消费的条数, <=0 时取 1

	// Retry 是重试退避序列: handler 返回 RetryError 时,
	// 第 n 次重试会等待 Retry[n] 之后重投, 用尽后不再重试。
	// 为空表示不做延迟重试, 此时 RetryError 会让消息立即重新入队。
	Retry []time.Duration
}

// WithRetry 设置重试退避序列，返回自身以便链式调用：
//
//	PerSecond(5).WithRetry(time.Second, 5*time.Second, 30*time.Second)
func (o *ConsumeOption) WithRetry(delays ...time.Duration) *ConsumeOption {
	o.Retry = delays
	return o
}

// RetryQueueMillis 把 Retry 换算成 Queue.RetryQueue 需要的毫秒序列。
// 非正数的退避时长会被忽略, 避免生成 ttl 为 0 的延迟队列。
func (o ConsumeOption) RetryQueueMillis() []int64 {
	out := make([]int64, 0, len(o.Retry))
	for _, d := range o.Retry {
		if d <= 0 {
			continue
		}
		out = append(out, int64(d/time.Millisecond))
	}
	return out
}

// Every 构造「每 d 最多 n 条」的消费配置。
func Every(d time.Duration, n int) *ConsumeOption {
	return &ConsumeOption{Interval: d, Limit: n}
}

// PerSecond 构造「每秒最多 n 条」的消费配置。
func PerSecond(n int) *ConsumeOption {
	return Every(time.Second, n)
}

// normalize 返回补齐默认值后的副本，不会修改调用方传入的配置。
func normalize(optChain ...*ConsumeOption) ConsumeOption {
	opt := ConsumeOption{}
	if len(optChain) > 0 && optChain[0] != nil {
		opt = *optChain[0]
	}

	if opt.Interval > 0 {
		if opt.Limit <= 0 {
			opt.Limit = 1
		}
		// 限流时未 Ack 消息数与窗口配额对齐，避免预取过多打乱消费节奏
		opt.Prefetch = opt.Limit
	} else if opt.Prefetch <= 0 {
		opt.Prefetch = defaultPrefetch
	}

	return opt
}

// newLimiter 按「每 Interval 最多 Limit 条」构造限流器，未开启限流时返回 nil。
//
// 令牌桶速率为 Limit/Interval、桶容量为 Limit：
// 启动瞬间最多放行 Limit 条，之后按窗口配额平滑供给。
func newLimiter(opt ConsumeOption) *rate.Limiter {
	if opt.Interval <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(float64(opt.Limit)/opt.Interval.Seconds()), opt.Limit)
}
