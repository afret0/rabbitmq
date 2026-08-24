package broker

import (
	"context"
	"testing"
	"time"
)

func Test_normalize_Default(t *testing.T) {
	opt := normalize()
	if opt.QosPrefetchCount != defaultQosPrefetchCount {
		t.Fatalf("expected default prefetch %d, got %d", defaultQosPrefetchCount, opt.QosPrefetchCount)
	}
	if opt.Interval != 0 || opt.Limit != 0 {
		t.Fatalf("expected no rate limiting, got %+v", opt)
	}
}

func Test_normalize_NilOptionUsesDefault(t *testing.T) {
	opt := normalize(nil)
	if opt.QosPrefetchCount != defaultQosPrefetchCount {
		t.Fatalf("expected default prefetch %d, got %d", defaultQosPrefetchCount, opt.QosPrefetchCount)
	}
}

func Test_normalize_EmptyOptionKeepsDefaultPrefetch(t *testing.T) {
	opt := normalize(&ConsumeOption{})
	if opt.QosPrefetchCount != defaultQosPrefetchCount {
		t.Fatalf("expected default prefetch %d, got %d", defaultQosPrefetchCount, opt.QosPrefetchCount)
	}
}

func Test_normalize_KeepsExplicitPrefetch(t *testing.T) {
	opt := normalize(&ConsumeOption{QosPrefetchCount: 3, QosPrefetchSize: 1024})
	if opt.QosPrefetchCount != 3 || opt.QosPrefetchSize != 1024 {
		t.Fatalf("unexpected option: %+v", opt)
	}
}

func Test_normalize_IntervalDefaultsLimitToOne(t *testing.T) {
	opt := normalize(&ConsumeOption{Interval: time.Second})
	if opt.Limit != 1 {
		t.Fatalf("expected limit 1, got %d", opt.Limit)
	}
	if opt.QosPrefetchCount != 1 {
		t.Fatalf("expected prefetch aligned to limit 1, got %d", opt.QosPrefetchCount)
	}
}

func Test_normalize_IntervalAlignsPrefetchToLimit(t *testing.T) {
	opt := normalize(&ConsumeOption{Interval: time.Second, Limit: 5, QosPrefetchCount: 100})
	if opt.QosPrefetchCount != 5 {
		t.Fatalf("expected prefetch aligned to limit 5, got %d", opt.QosPrefetchCount)
	}
}

func Test_normalize_DoesNotMutateCaller(t *testing.T) {
	in := &ConsumeOption{Interval: time.Second, QosPrefetchCount: 100}
	_ = normalize(in)
	if in.QosPrefetchCount != 100 || in.Limit != 0 {
		t.Fatalf("caller option was mutated: %+v", in)
	}
}

func Test_newLimiter_DisabledWithoutInterval(t *testing.T) {
	if l := newLimiter(normalize(&ConsumeOption{Limit: 10})); l != nil {
		t.Fatalf("expected nil limiter when Interval is 0, got %v", l)
	}
}

// Limit 条应当在一个 Interval 周期内放行, 而不是每条都等一个 Interval。
func Test_newLimiter_BurstAllowsLimitPerInterval(t *testing.T) {
	opt := normalize(&ConsumeOption{Interval: time.Second, Limit: 5})
	limiter := newLimiter(opt)

	if got := limiter.Burst(); got != 5 {
		t.Fatalf("expected burst 5, got %d", got)
	}
	if got := float64(limiter.Limit()); got < 4.99 || got > 5.01 {
		t.Fatalf("expected ~5 events/sec, got %v", got)
	}

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := limiter.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("first %d messages should pass immediately, took %v", opt.Limit, elapsed)
	}
}

func Test_newLimiter_SubSecondInterval(t *testing.T) {
	limiter := newLimiter(normalize(&ConsumeOption{Interval: 100 * time.Millisecond, Limit: 10}))
	if got := float64(limiter.Limit()); got < 99.9 || got > 100.1 {
		t.Fatalf("expected ~100 events/sec, got %v", got)
	}
}
