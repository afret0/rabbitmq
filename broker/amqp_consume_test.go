package broker

import (
	"context"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

// 投递通道关闭后必须退出, 否则会空转并对零值消息反复调用 handle。
func Test_deliveryLoop_ReturnsWhenDeliveryClosed(t *testing.T) {
	delivery := make(chan amqp.Delivery)
	close(delivery)

	handled := 0
	done := make(chan error, 1)
	go func() {
		done <- deliveryLoop(context.Background(), delivery, nil,
			func(amqp.Delivery) { handled++ })
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error so the caller reconnects")
		}
	case <-time.After(time.Second):
		t.Fatal("deliveryLoop did not return on closed delivery channel (busy loop)")
	}

	if handled != 0 {
		t.Fatalf("handle must not run for zero-value deliveries, ran %d times", handled)
	}
}

// channel 关闭、连接断开或停机时 ctx 被取消, 循环必须立即返回非 nil。
func Test_deliveryLoop_ReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := deliveryLoop(ctx, make(chan amqp.Delivery), nil, func(amqp.Delivery) {}); err == nil {
		t.Fatal("expected non-nil error")
	}
}

// 已要求退出后, 即使通道里还有消息也不能再处理: 未 ack 的消息由 broker 重新入队。
func Test_deliveryLoop_DoesNotProcessAfterCancel(t *testing.T) {
	delivery := make(chan amqp.Delivery, 4)
	for i := 0; i < 4; i++ {
		delivery <- amqp.Delivery{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	handled := 0
	for i := 0; i < 50; i++ {
		_ = deliveryLoop(ctx, delivery, nil, func(amqp.Delivery) { handled++ })
	}
	if handled != 0 {
		t.Fatalf("handled %d messages after cancel", handled)
	}
}

func Test_deliveryLoop_NoLimiterProcessesAll(t *testing.T) {
	delivery := make(chan amqp.Delivery, 10)
	for i := 0; i < 10; i++ {
		delivery <- amqp.Delivery{}
	}
	close(delivery)

	handled := 0
	_ = deliveryLoop(context.Background(), delivery, nil,
		func(amqp.Delivery) { handled++ })

	if handled != 10 {
		t.Fatalf("expected 10 handled, got %d", handled)
	}
}

// 真正验证限流生效: 窗口 100ms / 2 条, 在持续供给下 300ms 内不应放行过多。
func Test_deliveryLoop_LimitsThroughput(t *testing.T) {
	delivery := make(chan amqp.Delivery, 1024)
	for i := 0; i < 1024; i++ {
		delivery <- amqp.Delivery{}
	}

	limiter := newLimiter(normalize(Every(100*time.Millisecond, 2)))

	handled := make(chan struct{}, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = deliveryLoop(ctx, delivery, limiter,
		func(amqp.Delivery) { handled <- struct{}{} })

	got := len(handled)
	// burst 2 + 300ms * 20/s = 8, 留一点调度余量
	if got == 0 || got > 12 {
		t.Fatalf("expected roughly 8 messages in 300ms, got %d", got)
	}
}

// 限流器等待期间 ctx 结束时必须返回错误, 而不是阻塞或继续无节制消费。
func Test_deliveryLoop_ReturnsWhenLimiterContextDone(t *testing.T) {
	delivery := make(chan amqp.Delivery, 8)
	for i := 0; i < 8; i++ {
		delivery <- amqp.Delivery{}
	}

	limiter := newLimiter(normalize(Every(time.Hour, 1)))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- deliveryLoop(ctx, delivery, limiter,
			func(amqp.Delivery) {})
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error when limiter context expires")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deliveryLoop blocked forever on limiter")
	}
}
