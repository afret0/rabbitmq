package backoff

import (
	"testing"
	"time"
)

func TestBackoff_GrowsWithJitterAndCaps(t *testing.T) {
	b := New(500*time.Millisecond, 4*time.Second)
	bases := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	for i := 0; i < 200; i++ {
		b.Reset()
		for step, base := range bases {
			d := b.Next()
			if d < base/2 || d > base {
				t.Fatalf("step %d: %v not within [%v, %v]", step, d, base/2, base)
			}
		}
	}
}

func TestBackoff_JitterSpreadsValues(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[New(time.Second, time.Second).Next()] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected jitter to produce varied delays, got %d distinct values", len(seen))
	}
}
