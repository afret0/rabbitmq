// Package backoff 提供带抖动的指数退避，供重连与重启消费复用。
package backoff

import (
	"math/rand"
	"time"
)

// Backoff 按 Min 起步、每次翻倍、封顶 Max 的节奏退避。
//
// 返回值带 50% 抖动（区间 [d/2, d]），避免 k8s 里大量 Pod 在
// broker 重启后同一时刻重连、重新声明队列，形成惊群。
// 非并发安全，每个循环持有自己的实例。
type Backoff struct {
	Min time.Duration
	Max time.Duration
	cur time.Duration
}

func New(min, max time.Duration) *Backoff {
	return &Backoff{Min: min, Max: max}
}

func (b *Backoff) Next() time.Duration {
	switch {
	case b.cur <= 0:
		b.cur = b.Min
	case b.cur < b.Max:
		b.cur *= 2
		if b.cur > b.Max {
			b.cur = b.Max
		}
	}
	half := b.cur / 2
	if half <= 0 {
		return b.cur
	}
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func (b *Backoff) Reset() {
	b.cur = 0
}
