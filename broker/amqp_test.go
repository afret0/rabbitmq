package broker

import (
	"context"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func Test_normalize_Default(t *testing.T) {
	opt := normalize()
	if opt.Prefetch != defaultPrefetch {
		t.Fatalf("expected default prefetch %d, got %d", defaultPrefetch, opt.Prefetch)
	}
	if opt.Interval != 0 || opt.Limit != 0 {
		t.Fatalf("expected no rate limiting, got %+v", opt)
	}
}

func Test_normalize_NilOptionUsesDefault(t *testing.T) {
	if opt := normalize(nil); opt.Prefetch != defaultPrefetch {
		t.Fatalf("expected default prefetch %d, got %d", defaultPrefetch, opt.Prefetch)
	}
}

func Test_normalize_EmptyOptionKeepsDefaultPrefetch(t *testing.T) {
	if opt := normalize(&ConsumeOption{}); opt.Prefetch != defaultPrefetch {
		t.Fatalf("expected default prefetch %d, got %d", defaultPrefetch, opt.Prefetch)
	}
}

func Test_normalize_KeepsExplicitPrefetch(t *testing.T) {
	if opt := normalize(&ConsumeOption{Prefetch: 3}); opt.Prefetch != 3 {
		t.Fatalf("expected prefetch 3, got %d", opt.Prefetch)
	}
}

func Test_normalize_IntervalDefaultsLimitToOne(t *testing.T) {
	opt := normalize(&ConsumeOption{Interval: time.Second})
	if opt.Limit != 1 || opt.Prefetch != 1 {
		t.Fatalf("expected limit/prefetch 1, got %+v", opt)
	}
}

func Test_normalize_IntervalAlignsPrefetchToLimit(t *testing.T) {
	opt := normalize(&ConsumeOption{Interval: time.Second, Limit: 5, Prefetch: 100})
	if opt.Prefetch != 5 {
		t.Fatalf("expected prefetch aligned to limit 5, got %d", opt.Prefetch)
	}
}

func Test_normalize_DoesNotMutateCaller(t *testing.T) {
	in := &ConsumeOption{Interval: time.Second, Prefetch: 100}
	_ = normalize(in)
	if in.Prefetch != 100 || in.Limit != 0 {
		t.Fatalf("caller option was mutated: %+v", in)
	}
}

func Test_Every_And_PerSecond(t *testing.T) {
	if opt := Every(2*time.Second, 3); opt.Interval != 2*time.Second || opt.Limit != 3 {
		t.Fatalf("unexpected option: %+v", opt)
	}
	if opt := PerSecond(7); opt.Interval != time.Second || opt.Limit != 7 {
		t.Fatalf("unexpected option: %+v", opt)
	}
}

func Test_newLimiter_DisabledWithoutInterval(t *testing.T) {
	if l := newLimiter(normalize(&ConsumeOption{Limit: 10})); l != nil {
		t.Fatalf("expected nil limiter when Interval is 0, got %v", l)
	}
}

func Test_newLimiter_RateMatchesLimitPerInterval(t *testing.T) {
	limiter := newLimiter(normalize(Every(time.Second, 5)))
	if got := limiter.Burst(); got != 5 {
		t.Fatalf("expected burst 5, got %d", got)
	}
	if got := float64(limiter.Limit()); got < 4.99 || got > 5.01 {
		t.Fatalf("expected ~5 events/sec, got %v", got)
	}
}

func Test_newLimiter_SubSecondInterval(t *testing.T) {
	limiter := newLimiter(normalize(Every(100*time.Millisecond, 10)))
	if got := float64(limiter.Limit()); got < 99.9 || got > 100.1 {
		t.Fatalf("expected ~100 events/sec, got %v", got)
	}
}

// 投递通道关闭后必须退出, 否则会空转并对零值消息反复调用 handle。
func Test_consumeLoop_ReturnsWhenDeliveryClosed(t *testing.T) {
	delivery := make(chan amqp.Delivery)
	close(delivery)

	handled := 0
	done := make(chan error, 1)
	go func() {
		done <- consumeLoop(context.Background(), delivery, make(chan *amqp.Error, 1), nil,
			func(amqp.Delivery) { handled++ })
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error so the caller reconnects")
		}
	case <-time.After(time.Second):
		t.Fatal("consumeLoop did not return on closed delivery channel (busy loop)")
	}

	if handled != 0 {
		t.Fatalf("handle must not run for zero-value deliveries, ran %d times", handled)
	}
}

func Test_consumeLoop_ReturnsOnNotify(t *testing.T) {
	notify := make(chan *amqp.Error, 1)
	notify <- amqp.ErrClosed

	err := consumeLoop(context.Background(), make(chan amqp.Delivery), notify, nil,
		func(amqp.Delivery) {})
	if err == nil {
		t.Fatal("expected non-nil error")
	}
}

// notify 被关闭时也必须返回非 nil, 否则上层重连循环会空转。
func Test_consumeLoop_ClosedNotifyReturnsError(t *testing.T) {
	notify := make(chan *amqp.Error)
	close(notify)

	if err := consumeLoop(context.Background(), make(chan amqp.Delivery), notify, nil,
		func(amqp.Delivery) {}); err == nil {
		t.Fatal("expected non-nil error on closed notify channel")
	}
}

func Test_consumeLoop_NoLimiterProcessesAll(t *testing.T) {
	delivery := make(chan amqp.Delivery, 10)
	for i := 0; i < 10; i++ {
		delivery <- amqp.Delivery{}
	}
	close(delivery)

	handled := 0
	_ = consumeLoop(context.Background(), delivery, make(chan *amqp.Error, 1), nil,
		func(amqp.Delivery) { handled++ })

	if handled != 10 {
		t.Fatalf("expected 10 handled, got %d", handled)
	}
}

// 真正验证限流生效: 窗口 100ms / 2 条, 在持续供给下 300ms 内不应放行过多。
func Test_consumeLoop_LimitsThroughput(t *testing.T) {
	delivery := make(chan amqp.Delivery, 1024)
	for i := 0; i < 1024; i++ {
		delivery <- amqp.Delivery{}
	}

	limiter := newLimiter(normalize(Every(100*time.Millisecond, 2)))

	handled := make(chan struct{}, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = consumeLoop(ctx, delivery, make(chan *amqp.Error, 1), limiter,
		func(amqp.Delivery) { handled <- struct{}{} })

	got := len(handled)
	// burst 2 + 300ms * 20/s = 8, 留一点调度余量
	if got == 0 || got > 12 {
		t.Fatalf("expected roughly 8 messages in 300ms, got %d", got)
	}
}

// 限流器等待期间 ctx 结束时必须返回错误, 而不是阻塞或继续无节制消费。
func Test_consumeLoop_ReturnsWhenLimiterContextDone(t *testing.T) {
	delivery := make(chan amqp.Delivery, 8)
	for i := 0; i < 8; i++ {
		delivery <- amqp.Delivery{}
	}

	limiter := newLimiter(normalize(Every(time.Hour, 1)))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumeLoop(ctx, delivery, make(chan *amqp.Error, 1), limiter,
			func(amqp.Delivery) {})
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error when limiter context expires")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumeLoop blocked forever on limiter")
	}
}
