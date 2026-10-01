package broker

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

// 返回一个立即拒绝连接的地址。
func refusedURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "amqp://guest:guest@" + addr + "/"
}

func Test_connManager_DialFailureBacksOffInsteadOfSpinning(t *testing.T) {
	buf := captureLog(t)
	m := newConnManager(refusedURL(t), "test")
	defer m.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, _, err := m.get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if el := time.Since(start); el < 2*time.Second || el > 4*time.Second {
		t.Fatalf("get must honor ctx, took %v", el)
	}
	// 退避 0.25~0.5s, 0.5~1s, 1~2s...：2.5s 内最多 5 次拨号，热循环会是成千上万次
	if n := strings.Count(buf.String(), "dial failed"); n < 1 || n > 5 {
		t.Fatalf("unexpected dial attempts in 2.5s: %d", n)
	}
}

func Test_connManager_CloseUnblocksWaiters(t *testing.T) {
	captureLog(t)
	m := newConnManager(refusedURL(t), "test")
	errCh := make(chan error, 1)
	go func() {
		_, _, err := m.get(context.Background())
		errCh <- err
	}()
	time.Sleep(200 * time.Millisecond)
	m.close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("expected ErrShutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("get did not return after close")
	}
	if _, _, err := m.get(context.Background()); !errors.Is(err, ErrShutdown) {
		t.Fatalf("expected ErrShutdown after close, got %v", err)
	}
}

func Test_publisher_NotConnectedReturnsWithinTimeout(t *testing.T) {
	captureLog(t)
	m := newConnManager(refusedURL(t), "test")
	defer m.close()
	p := newPublisher(m, 700*time.Millisecond)

	start := time.Now()
	err := p.publish(context.Background(), pubRequest{key: "k"})
	if !errors.Is(err, errNotConnected) {
		t.Fatalf("expected errNotConnected, got %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("publish took %v", el)
	}
}

// 调用方 ctx 必须被尊重，且多个调用方并发等待，而不是排队串行等待。
func Test_publisher_ConcurrentCallersHonorContext(t *testing.T) {
	captureLog(t)
	m := newConnManager(refusedURL(t), "test")
	defer m.close()
	p := newPublisher(m, 5*time.Second)

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			errs <- p.publish(ctx, pubRequest{key: "k"})
		}()
	}
	wg.Wait()
	if el := time.Since(start); el > time.Second {
		t.Fatalf("%d callers with 300ms ctx took %v (serialized?)", n, el)
	}
	close(errs)
	for err := range errs {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected ctx deadline error, got %v", err)
		}
	}
}

func Test_mapWaitErr(t *testing.T) {
	timeout := errors.New("timeout")
	if err := mapWaitErr(context.Background(), context.DeadlineExceeded, timeout); err != timeout {
		t.Fatalf("internal timeout should map to %v, got %v", timeout, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapWaitErr(ctx, context.DeadlineExceeded, timeout); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller ctx error must win, got %v", err)
	}
	other := errors.New("other")
	if err := mapWaitErr(context.Background(), other, timeout); err != other {
		t.Fatalf("other errors pass through, got %v", err)
	}
}

func Test_connManager_NoDialAfterClose(t *testing.T) {
	buf := captureLog(t)
	m := newConnManager(refusedURL(t), "test")
	m.close()
	for i := 0; i < 5; i++ {
		if _, _, err := m.get(context.Background()); !errors.Is(err, ErrShutdown) {
			t.Fatalf("expected ErrShutdown, got %v", err)
		}
	}
	time.Sleep(700 * time.Millisecond)
	if strings.Contains(buf.String(), "dial failed") {
		t.Fatalf("closed manager must not dial:\n%s", buf.String())
	}
}

func newTestPublisherWithChannels() (*lane, chan amqp.Confirmation, chan amqp.Return, chan *amqp.Error) {
	m := newConnManager("amqp://unused/", "test")
	l := &lane{p: newPublisher(m, time.Second), declared: map[string]time.Time{}}
	acks, rets, closed := make(chan amqp.Confirmation, 1), make(chan amqp.Return, 1), make(chan *amqp.Error, 1)
	l.acks, l.rets, l.closed = acks, rets, closed
	return l, acks, rets, closed
}

// channel 关闭时 streadway 会 close NotifyReturn：读到零值不能当成 NO_ROUTE（否则会触发重复发布）。
func Test_waitConfirm_ClosedReturnChannelIsNotUnroutable(t *testing.T) {
	p, _, rets, _ := newTestPublisherWithChannels()
	close(rets)
	ctx := context.Background()
	opCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := p.waitConfirm(ctx, opCtx, 0, 1)
	if err == nil || errors.Is(err, errUnroutable) {
		t.Fatalf("expected channel-closed error, got %v", err)
	}
}

// 确认已到达时，无论 select 先选中哪个分支（已关闭的 rets / closed 通知），都必须报告成功。
func Test_waitConfirm_AckWinsOverConcurrentChannelClose(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		p, acks, rets, closed := newTestPublisherWithChannels()
		acks <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
		if i%2 == 0 {
			close(rets)
		} else {
			closed <- &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"}
		}
		opCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := p.waitConfirm(ctx, opCtx, 0, 1)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: acked message reported as failure: %v", i, err)
		}
	}
}

func Test_waitConfirm_RealReturnIsUnroutable(t *testing.T) {
	p, acks, rets, _ := newTestPublisherWithChannels()
	rets <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: "ex", RoutingKey: "rk"}
	acks <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	ctx := context.Background()
	opCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := p.waitConfirm(ctx, opCtx, 0, 1)
	if !errors.Is(err, errUnroutable) || !strings.Contains(err.Error(), "amqp message unroutable, replyCode=312 replyText=NO_ROUTE exchange=ex routingKey=rk") {
		t.Fatalf("unexpected error %v", err)
	}
}

// lane 池：并发上限为 publishLanes，空闲 lane 后进先出复用（低并发时只用到一个 channel）。
func Test_publisher_LanesBoundConcurrencyAndReuseLIFO(t *testing.T) {
	p := newPublisher(newConnManager("amqp://unused/", "test"), time.Second)
	held := make([]*lane, 0, publishLanes)
	for i := 0; i < publishLanes; i++ {
		l, err := p.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire beyond %d lanes must wait, got %v", publishLanes, err)
	}
	for _, l := range held {
		p.release(l)
	}
	first, _ := p.acquire(context.Background())
	p.release(first)
	second, _ := p.acquire(context.Background())
	p.release(second)
	if first != second {
		t.Fatal("idle lanes must be reused LIFO so low concurrency keeps a single open channel")
	}
}

func Test_notSent_PreservesErrorAndMarksRetryable(t *testing.T) {
	base := amqp.ErrClosed
	err := notSent(base)
	if !errors.Is(err, errNotSent) || !errors.Is(err, amqp.ErrClosed) || err.Error() != base.Error() {
		t.Fatalf("unexpected wrapping: %v", err)
	}
	if notSent(nil) != nil {
		t.Fatal("notSent(nil) must be nil")
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if errors.Is(notSentUnlessTimeout(expired, base), errNotSent) {
		t.Fatal("failures after the deadline are ambiguous and must not be retried")
	}
}
