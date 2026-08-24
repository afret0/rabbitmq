package broker

import (
	"testing"
	"time"
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
