// Package e2e 通过对外 API（rabbitmq.NewProducer / NewConsumer / LaunchJob ...）做端到端测试。
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@127.0.0.1:5672/ go test ./e2e -v
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"

	"github.com/afret0/rabbitmq"
)

var (
	brokerURL = os.Getenv("RABBITMQ_TEST_URL")
	// ttlSlack 是 TTL 到期死信的额外延迟上限（阿里云 RabbitMQ 版实测有数秒偏差），只放宽上界。
	ttlSlack = func() time.Duration {
		if d, err := time.ParseDuration(os.Getenv("RABBITMQ_TEST_TTL_SLACK")); err == nil {
			return d
		}
		return 5 * time.Second
	}()
)

type Order struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

var seq atomic.Uint64

func names(t *testing.T) (exchange, queue string) {
	t.Helper()
	if brokerURL == "" {
		t.Skip("set RABBITMQ_TEST_URL to run end-to-end tests")
	}
	n := fmt.Sprintf("rmqtest-e2e-%d%d", time.Now().UnixNano()%100000, seq.Add(1))
	ex, q := n+"-ex", n+"-q"
	t.Cleanup(func() { cleanup(ex, q) })
	return ex, q
}

// cleanup 删除测试资源，整体最多 60s：管理连接直接使用 streadway，RPC 没有超时。
func cleanup(exchange string, queues ...string) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		cleanupSync(exchange, queues...)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
	}
}

func cleanupSync(exchange string, queues ...string) {
	c, err := amqp.Dial(brokerURL)
	if err != nil {
		return
	}
	defer c.Close()
	del := func(fn func(ch *amqp.Channel) error) {
		if ch, err := c.Channel(); err == nil {
			_ = fn(ch)
			_ = ch.Close()
		}
	}
	for _, q := range queues {
		for _, n := range []string{q, "delay.v1.1s." + q, "delay.v1.5s." + q, "delay.v1.10s." + q, fmt.Sprintf("delay.1.%s.%s", exchange, q)} {
			n := n
			del(func(ch *amqp.Channel) error { _, err := ch.QueueDelete(n, false, false, false); return err })
		}
	}
	del(func(ch *amqp.Channel) error { return ch.ExchangeDelete(exchange, false, false) })
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *logBuffer {
	buf := &logBuffer{}
	prev := log.Writer()
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(prev)
		if t.Failed() {
			t.Logf("captured logs:\n%s", buf.String())
		}
	})
	return buf
}

func newPair(t *testing.T, exchange, kind string) (*rabbitmq.Producer, *rabbitmq.Consumer) {
	ex := &rabbitmq.ExchangeOption{Name: exchange, Type: kind}
	p := rabbitmq.NewProducer(&rabbitmq.ProducerOptions{ExchangeOpt: ex, BrokerURL: brokerURL})
	c := rabbitmq.NewConsumer(&rabbitmq.ConsumerOptions{ExchangeOpt: ex, BrokerURL: brokerURL})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
		_ = p.Close()
	})
	return p, c
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %v: %s", timeout, msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestE2E_NewJobRoundTripAndShutdown(t *testing.T) {
	ex, q := names(t)
	producer, consumer := newPair(t, ex, "direct")
	if err := producer.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}

	type got struct {
		order *Order
		opId  interface{}
	}
	ch := make(chan got, 4)
	launched := make(chan struct{})
	go func() {
		defer close(launched)
		consumer.LaunchJob(q, q, rabbitmq.NewJob(func(ctx context.Context, o *Order) error {
			ch <- got{o, ctx.Value("opId")}
			return nil
		}))
	}()

	err := producer.Publish(context.Background(), q, &rabbitmq.Message[*Order]{
		MsgId: "20261002-0001",
		Data:  &Order{OrderID: "o-9", Amount: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-ch:
		if g.order == nil || g.order.OrderID != "o-9" || g.order.Amount != 100 || g.opId != "20261002-0001" {
			t.Fatalf("unexpected delivery: %+v opId=%v", g.order, g.opId)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("message not delivered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-launched:
	case <-time.After(5 * time.Second):
		t.Fatal("LaunchJob did not return after Shutdown")
	}

	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(context.Background(), q, "after close"); err == nil {
		t.Fatal("Publish after Close should fail")
	}
}

// 真实退避 1s / 5s / 10s：RetryAfter 已废弃不再生效，%w 包装的 RetryError 同样触发重投。
func TestE2E_RetryErrorUsesFixedBackoffAndCap(t *testing.T) {
	ex, q := names(t)
	logs := captureLog(t)
	producer, consumer := newPair(t, ex, "direct")
	if err := producer.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var attempts []time.Time
	job := func(body []byte) error {
		mu.Lock()
		attempts = append(attempts, time.Now())
		mu.Unlock()
		return fmt.Errorf("downstream unavailable: %w", rabbitmq.RetryError)
	}
	go consumer.LaunchJob(q, q, job, rabbitmq.RetryAfter(100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond))

	if err := producer.Publish(context.Background(), q, "poison"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second+3*ttlSlack, func() bool { return strings.Contains(logs.String(), "drop message") }, "no drop log")
	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 4 {
		t.Fatalf("expected 4 attempts (1 + 3 redeliveries), got %d", len(attempts))
	}
	want := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	for i, w := range want {
		gap := attempts[i+1].Sub(attempts[i])
		if gap < w-50*time.Millisecond || gap > w+ttlSlack {
			t.Fatalf("gap %d = %v, want about %v", i, gap, w)
		}
		t.Logf("redelivery %d after %v", i+1, gap.Round(10*time.Millisecond))
	}
	out := logs.String()
	if !strings.Contains(out, "RetryAfter is deprecated and ignored") {
		t.Fatalf("expected deprecation warning:\n%s", out)
	}
	if strings.Count(out, "drop message") != 1 || !strings.Contains(out, "after 3 redeliveries") || !strings.Contains(out, "queue="+q) {
		t.Fatalf("unexpected drop log:\n%s", out)
	}
}

func TestE2E_NonRetryErrorIsAckedOnce(t *testing.T) {
	ex, q := names(t)
	captureLog(t)
	producer, consumer := newPair(t, ex, "direct")
	if err := producer.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	go consumer.LaunchJob(q, q, func([]byte) error { n.Add(1); return errors.New("bad payload") })
	if err := producer.Publish(context.Background(), q, "x"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return n.Load() == 1 }, "not delivered")
	time.Sleep(2 * time.Second)
	if n.Load() != 1 {
		t.Fatalf("non-RetryError must be acked, delivered %d times", n.Load())
	}
}

func TestE2E_LaunchTopicJobGroups(t *testing.T) {
	ex, q := names(t)
	g1, g2 := q+"-g1", q+"-g2"
	t.Cleanup(func() { cleanup(ex, g1, g2) })
	producer, consumer := newPair(t, ex, "topic")
	if err := producer.DeclareQueue(g1, "order.*"); err != nil {
		t.Fatal(err)
	}
	if err := producer.DeclareQueue(g2, "order.*"); err != nil {
		t.Fatal(err)
	}
	var c1, c2 atomic.Int32
	done := make(chan struct{}, 3)
	go func() {
		consumer.LaunchTopicJob(g1, "order.*", func([]byte) error { c1.Add(1); return nil })
		done <- struct{}{}
	}()
	go func() {
		consumer.LaunchTopicJob(g1, "order.*", func([]byte) error { c1.Add(1); return nil })
		done <- struct{}{}
	}()
	go func() {
		consumer.LaunchTopicJob(g2, "order.*", func([]byte) error { c2.Add(1); return nil })
		done <- struct{}{}
	}()

	for i := 0; i < 3; i++ {
		if err := producer.Publish(context.Background(), "order.created", map[string]int{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, func() bool { return c1.Load() == 3 && c2.Load() == 3 }, fmt.Sprintf("g1=%d g2=%d", c1.Load(), c2.Load()))
	time.Sleep(time.Second)
	if c1.Load() != 3 || c2.Load() != 3 {
		t.Fatalf("each group must receive each message once: g1=%d g2=%d", c1.Load(), c2.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("LaunchTopicJob did not return after Shutdown")
		}
	}
}

func TestE2E_PublishDelay(t *testing.T) {
	ex, q := names(t)
	producer, consumer := newPair(t, ex, "direct")
	if err := producer.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	arrived := make(chan time.Time, 1)
	go consumer.LaunchJob(q, q, func([]byte) error { arrived <- time.Now(); return nil })
	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	if err := producer.PublishDelay(context.Background(), q, map[string]string{"k": "v"}, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-arrived:
		if el := at.Sub(start); el < 900*time.Millisecond || el > 2*time.Second+ttlSlack {
			t.Fatalf("arrived after %v, want ~1s", el)
		}
	case <-time.After(10*time.Second + ttlSlack):
		t.Fatal("delayed message not delivered")
	}
}
