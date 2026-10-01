package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"

	"github.com/afret0/rabbitmq/internal/faultproxy"
)

// 集成测试：
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@127.0.0.1:5672/ go test ./broker -run TestIT -v
//
// RABBITMQ_TEST_CONTAINER 指定 docker 容器名时，额外运行需要 rabbitmqctl 的故障注入
// （内存告警、强制断开所有连接、重启 broker、受限权限用户）。
//
// 平台差异（下界在任何平台都严格校验，只放宽上界）：
//   - RABBITMQ_TEST_TTL_SLACK：TTL 到期死信的额外延迟上限，默认 5s；阿里云 RabbitMQ 版实测有数秒偏差。
//   - RABBITMQ_TEST_REQUEUE_WAIT：未确认消息被 broker 重新投递的等待上限，默认 60s。
//   - RABBITMQ_TEST_SLOW_REQUEUE=1：broker 在连接/channel 异常关闭后要等队列的消费超时才重投未确认消息
//     （阿里云 RabbitMQ 版，默认 5~30 分钟），此时只校验断开瞬间在途的消息数量有界，其余消息必须全部送达。

var (
	itURL         = os.Getenv("RABBITMQ_TEST_URL")
	itContainer   = os.Getenv("RABBITMQ_TEST_CONTAINER")
	itTTLSlack    = envDuration("RABBITMQ_TEST_TTL_SLACK", 5*time.Second)
	itRequeueWait = envDuration("RABBITMQ_TEST_REQUEUE_WAIT", 60*time.Second)
	itSlowRequeue = os.Getenv("RABBITMQ_TEST_SLOW_REQUEUE") == "1"
)

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

var fastBackoff = []time.Duration{300 * time.Millisecond, 600 * time.Millisecond, 900 * time.Millisecond}

func TestMain(m *testing.M) {
	code := m.Run()
	if itURL != "" {
		if c, err := amqp.Dial(itURL); err == nil {
			if ch, err := c.Channel(); err == nil {
				_ = ch.ExchangeDelete(retryExchange, true, false)
			}
			_ = c.Close()
		}
	}
	os.Exit(code)
}

func requireBroker(t *testing.T) {
	t.Helper()
	if itURL == "" {
		t.Skip("set RABBITMQ_TEST_URL to run integration tests")
	}
}

func requireContainer(t *testing.T) {
	t.Helper()
	requireBroker(t)
	if itContainer == "" {
		t.Skip("set RABBITMQ_TEST_CONTAINER to run broker-side fault injection")
	}
}

var nameSeq atomic.Uint64
var nameSanitizer = regexp.MustCompile(`[^a-z0-9]+`)

func testName(t *testing.T, suffix string) string {
	base := nameSanitizer.ReplaceAllString(strings.ToLower(strings.TrimPrefix(t.Name(), "TestIT_")), "-")
	if len(base) > 50 {
		base = base[:50]
	}
	return fmt.Sprintf("rmqtest-%s-%s-%d%d", base, suffix, time.Now().UnixNano()%100000, nameSeq.Add(1))
}

// admin 是直连 broker 的管理连接，用于检查队列、清理测试资源；断开后自动重连。
type admin struct {
	t    *testing.T
	mu   sync.Mutex
	conn *amqp.Connection
}

func newAdmin(t *testing.T) *admin {
	a := &admin{t: t}
	t.Cleanup(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.conn != nil {
			_ = a.conn.Close()
		}
	})
	return a
}

// do 在新 channel 上执行一次管理操作。管理连接直接使用 streadway，RPC 没有超时
// （阿里云上实测出现过 channel.open 永不返回），因此这里自带超时，超时后丢弃该连接。
func (a *admin) do(fn func(ch *amqp.Channel) error) error {
	a.mu.Lock()
	if a.conn == nil || a.conn.IsClosed() {
		c, err := amqp.Dial(itURL)
		if err != nil {
			a.mu.Unlock()
			return err
		}
		a.conn = c
	}
	conn := a.conn
	a.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		ch, err := conn.Channel()
		if err != nil {
			errCh <- err
			return
		}
		defer ch.Close()
		errCh <- fn(ch)
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(20 * time.Second):
		a.mu.Lock()
		if a.conn == conn {
			a.conn = nil
		}
		a.mu.Unlock()
		go conn.Close()
		return errors.New("admin operation timed out")
	}
}

func (a *admin) depth(name string) int {
	n := -1
	_ = a.do(func(ch *amqp.Channel) error {
		q, err := ch.QueueInspect(name)
		if err == nil {
			n = q.Messages
		}
		return err
	})
	return n
}

func (a *admin) capability(name string) bool {
	ok := false
	_ = a.do(func(*amqp.Channel) error {
		caps, _ := a.conn.Properties["capabilities"].(amqp.Table)
		ok, _ = caps[name].(bool)
		return nil
	})
	return ok
}

// cleanup 在测试结束时删除交换机、队列及其所有延迟队列。必须在创建 broker 之前调用，
// 保证（LIFO 的）清理顺序是先关闭 broker 再删资源。
func (a *admin) cleanup(exchange string, queues ...string) {
	a.t.Cleanup(func() {
		var names []string
		for _, q := range queues {
			names = append(names, q)
			for _, d := range append(append([]time.Duration{}, fastBackoff...), time.Second, 5*time.Second, 10*time.Second) {
				names = append(names, delayQueueDecl(q, d).name)
			}
		}
		for _, n := range names {
			n := n
			_ = a.do(func(ch *amqp.Channel) error { _, err := ch.QueueDelete(n, false, false, false); return err })
		}
		if exchange != "" {
			_ = a.do(func(ch *amqp.Channel) error { return ch.ExchangeDelete(exchange, false, false) })
		}
	})
}

func newBroker(t *testing.T, rawURL, exchange, kind string) *AmqpBroker {
	t.Helper()
	b := NewAmqpBroker(&AmqpBrokerOptions{Url: rawURL, Exchange: exchange, ExchangeType: kind})
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func withFastBackoff(t *testing.T) {
	prev := redeliveryBackoff
	redeliveryBackoff = fastBackoff
	t.Cleanup(func() { redeliveryBackoff = prev })
}

func proxied(t *testing.T) (*faultproxy.Proxy, string) {
	t.Helper()
	u, err := url.Parse(itURL)
	if err != nil {
		t.Fatal(err)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "5672")
	}
	p, err := faultproxy.New(host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	u.Host = p.Addr()
	return p, u.String()
}

func rabbitmqctl(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"exec", itContainer, "rabbitmqctl"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("rabbitmqctl %v: %v\n%s", args, err, out)
	}
}

func setAlarm(t *testing.T) {
	rabbitmqctl(t, "set_vm_memory_high_watermark", "0")
	t.Cleanup(func() { rabbitmqctl(t, "set_vm_memory_high_watermark", "0.4") })
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, format string, args ...interface{}) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %v: "+format, append([]interface{}{timeout}, args...)...)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// consumeForever 模拟 LaunchJob：消费会话结束后重建，直到 broker 关闭。
func consumeForever(b *AmqpBroker, q *Queue, opts ...*ConsumeOption) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			start := time.Now()
			err := b.Consume(q, opts...)
			if errors.Is(err, ErrShutdown) {
				return
			}
			log.Printf("test: session on %s ended after %v: %v", q.Name, time.Since(start).Round(time.Millisecond), err)
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return done
}

func topicForever(b *AmqpBroker, opt *GroupConsumeOption, h func([]byte) Status) {
	go func() {
		for {
			if err := b.ConsumerTopic(opt, h); errors.Is(err, ErrShutdown) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

type recorder struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
	total    int
}

func newRecorder() *recorder { return &recorder{attempts: map[string][]time.Time{}} }

func (r *recorder) add(body []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := string(body)
	r.attempts[k] = append(r.attempts[k], time.Now())
	r.total++
	return len(r.attempts[k])
}

func (r *recorder) count(body string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts[body])
}

func (r *recorder) times(body string) []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.attempts[body]...)
}

func (r *recorder) sum() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

func (r *recorder) uniq() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

func publishOK(t *testing.T, b *AmqpBroker, key, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Publish(ctx, key, []byte(body)); err != nil {
		t.Fatalf("publish %s: %v", body, err)
	}
}

// ---------------------------------------------------------------------------
// 基本收发
// ---------------------------------------------------------------------------

func TestIT_RoundTrip(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})

	const n = 200
	for i := 0; i < n; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}
	waitFor(t, 30*time.Second, func() bool { return rec.uniq() == n }, "received %d/%d", rec.uniq(), n)
	time.Sleep(500 * time.Millisecond)
	if rec.sum() != n {
		t.Fatalf("expected exactly %d deliveries, got %d", n, rec.sum())
	}
	if !prod.Health() || !cons.Health() {
		t.Fatal("brokers should be healthy")
	}
}

func TestIT_DeclareQueueBeforeConsumer(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, itURL, ex, "topic")
	if err := prod.DeclareQueue(q, "orders.*"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		publishOK(t, prod, "orders.created", fmt.Sprintf("m-%d", i))
	}
	cons := newBroker(t, itURL, ex, "topic")
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: "orders.*", Handle: func(b []byte) Status { rec.add(b); return Success }})
	waitFor(t, 20*time.Second, func() bool { return rec.uniq() == 5 }, "received %d/5", rec.uniq())
}

func TestIT_PublishUnroutableAndChannelErrorRecovery(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}

	err := prod.Publish(context.Background(), "no-such-key", []byte("x"))
	if !errors.Is(err, errUnroutable) || !strings.Contains(err.Error(), "amqp message unroutable, replyCode=312") {
		t.Fatalf("expected unroutable error, got %v", err)
	}
	publishOK(t, prod, q, "after-unroutable")

	// broker 主动关闭 channel（如阿里云超出限流返回 530，这里用不存在的 exchange 触发 404）后必须能自愈
	err = prod.pub.publish(context.Background(), pubRequest{exchange: testName(t, "missing"), key: q, msg: amqp.Publishing{Body: []byte("x")}})
	if err == nil {
		t.Fatal("expected error publishing to a missing exchange")
	}
	for i := 0; i < 5; i++ {
		publishOK(t, prod, q, fmt.Sprintf("after-channel-error-%d", i))
	}
	waitFor(t, 10*time.Second, func() bool { return adm.depth(q) == 6 }, "queue depth %d, want 6", adm.depth(q))
}

// 旧版本在发布 channel 建立时声明 exchange：向全新 vhost / 新 exchange 发布应得到 unroutable，而不是 404。
func TestIT_PublishToFreshExchangeDeclaresIt(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex := testName(t, "ex")
	adm.cleanup(ex)
	prod := newBroker(t, itURL, ex, "topic")
	err := prod.Publish(context.Background(), "order.created", []byte("x"))
	if !errors.Is(err, errUnroutable) {
		t.Fatalf("expected unroutable on a fresh exchange, got %v", err)
	}
	if err := adm.do(func(ch *amqp.Channel) error {
		return ch.ExchangeDeclarePassive(ex, "topic", true, false, false, false, nil)
	}); err != nil {
		t.Fatalf("exchange was not declared by the publisher: %v", err)
	}
}

func TestIT_PublishDelay(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	adm.t.Cleanup(func() {
		_ = adm.do(func(ch *amqp.Channel) error {
			_, err := ch.QueueDelete(fmt.Sprintf("delay.2.%s.%s", ex, q), false, false, false)
			return err
		})
	})

	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	arrived := make(chan time.Time, 16)
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		rec.add(b)
		arrived <- time.Now()
		return Success
	}})

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := prod.PublishDelay(context.Background(), q, []byte(fmt.Sprintf("d-%d", i)), 2); err != nil {
				t.Errorf("PublishDelay: %v", err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 5; i++ {
		select {
		case at := <-arrived:
			if el := at.Sub(start); el < 1900*time.Millisecond || el > 3*time.Second+itTTLSlack {
				t.Fatalf("delayed message arrived after %v, want ~2s", el)
			}
		case <-time.After(10*time.Second + itTTLSlack):
			t.Fatal("delayed message not delivered")
		}
	}

	start = time.Now()
	if err := prod.PublishDelay(context.Background(), q, []byte("now"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-arrived:
		if el := at.Sub(start); el > 2*time.Second {
			t.Fatalf("delaySeconds=0 should deliver immediately, took %v", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("immediate message not delivered")
	}
}

// ---------------------------------------------------------------------------
// 重投：1s/5s/10s 退避、最多 3 次、跨实例计数、不扇出
// ---------------------------------------------------------------------------

func TestIT_RedeliveryCapExactAttemptsAndBackoff(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Retry }})

	publishOK(t, prod, q, "poison")
	waitFor(t, 30*time.Second+3*itTTLSlack, func() bool { return strings.Contains(logs.String(), "drop message") }, "no drop log")
	time.Sleep(3 * time.Second)
	if got := rec.count("poison"); got != 4 {
		t.Fatalf("expected exactly 4 attempts (1 + 3 redeliveries), got %d", got)
	}
	ts := rec.times("poison")
	for i := 0; i < 3; i++ {
		gap := ts[i+1].Sub(ts[i])
		if gap < fastBackoff[i]-50*time.Millisecond || gap > fastBackoff[i]+itTTLSlack {
			t.Fatalf("gap %d = %v, want about %v", i, gap, fastBackoff[i])
		}
	}
	if n := strings.Count(logs.String(), "drop message"); n != 1 {
		t.Fatalf("expected one drop log line, got %d:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "queue="+q) || !strings.Contains(logs.String(), "routingKey="+q) {
		t.Fatalf("drop log lacks identification:\n%s", logs.String())
	}
	waitFor(t, 10*time.Second, func() bool { return adm.depth(q) == 0 }, "queue not empty: %d", adm.depth(q))
	for _, d := range fastBackoff {
		if n := adm.depth(delayQueueDecl(q, d).name); n != 0 {
			t.Fatalf("delay queue %s still holds %d messages", delayQueueDecl(q, d).name, n)
		}
	}
}

func TestIT_RedeliveryCapAcrossPods(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	var perPod [3]atomic.Int32
	for i := 0; i < 3; i++ {
		i := i
		pod := newBroker(t, itURL, ex, "direct")
		consumeForever(pod, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
			perPod[i].Add(1)
			rec.add(b)
			return Retry
		}}, &ConsumeOption{Prefetch: 1})
	}
	const msgs = 6
	for i := 0; i < msgs; i++ {
		publishOK(t, prod, q, fmt.Sprintf("poison-%d", i))
	}
	waitFor(t, 60*time.Second+3*itTTLSlack, func() bool { return strings.Count(logs.String(), "drop message") == msgs }, "drops=%d", strings.Count(logs.String(), "drop message"))
	time.Sleep(3 * time.Second)
	for i := 0; i < msgs; i++ {
		if got := rec.count(fmt.Sprintf("poison-%d", i)); got != 4 {
			t.Fatalf("message %d attempted %d times across pods, want 4", i, got)
		}
	}
	busy := 0
	for i := range perPod {
		if perPod[i].Load() > 0 {
			busy++
		}
	}
	t.Logf("attempts per pod: %d/%d/%d", perPod[0].Load(), perPod[1].Load(), perPod[2].Load())
	if busy < 2 {
		t.Fatalf("expected attempts to spread across pods, got %d busy pods", busy)
	}
}

func TestIT_RetryThenSuccess(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		if rec.add(b) < 3 {
			return Retry
		}
		return Success
	}})
	publishOK(t, prod, q, "flaky")
	waitFor(t, 20*time.Second+2*itTTLSlack, func() bool { return rec.count("flaky") == 3 }, "attempts=%d", rec.count("flaky"))
	time.Sleep(2 * time.Second)
	if rec.count("flaky") != 3 || strings.Contains(logs.String(), "drop message") {
		t.Fatalf("attempts=%d, logs:\n%s", rec.count("flaky"), logs.String())
	}
	waitFor(t, 10*time.Second, func() bool { return adm.depth(q) == 0 }, "queue not empty")
}

// 旧实现把重试消息死信回主 exchange 重新路由，会重复投给绑定同一 routing key 的其他队列。
func TestIT_RetryDoesNotFanOutToOtherQueues(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	for _, kind := range []string{"direct", "topic", "fanout"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			adm := newAdmin(t)
			ex, qa, qb := testName(t, "ex"), testName(t, "qa"), testName(t, "qb")
			adm.cleanup(ex, qa, qb)
			prod := newBroker(t, itURL, ex, kind)
			if err := prod.DeclareQueue(qa, "k"); err != nil {
				t.Fatal(err)
			}
			if err := prod.DeclareQueue(qb, "k"); err != nil {
				t.Fatal(err)
			}
			recA, recB := newRecorder(), newRecorder()
			consA := newBroker(t, itURL, ex, kind)
			consB := newBroker(t, itURL, ex, kind)
			consumeForever(consA, &Queue{Name: qa, RouteKey: "k", Handle: func(b []byte) Status {
				if recA.add(b) < 3 {
					return Retry
				}
				return Success
			}})
			consumeForever(consB, &Queue{Name: qb, RouteKey: "k", Handle: func(b []byte) Status { recB.add(b); return Success }})

			publishOK(t, prod, "k", "event")
			waitFor(t, 20*time.Second+2*itTTLSlack, func() bool { return recA.count("event") == 3 }, "A attempts=%d", recA.count("event"))
			time.Sleep(2 * time.Second)
			if got := recB.count("event"); got != 1 {
				t.Fatalf("queue B received %d copies, want exactly 1", got)
			}
		})
	}
}

func TestIT_PanicIsRecoveredRetriedAndCapped(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		rec.add(b)
		if string(b) == "panic" {
			panic("handler exploded")
		}
		return Success
	}})
	publishOK(t, prod, q, "panic")
	waitFor(t, 30*time.Second+3*itTTLSlack, func() bool { return strings.Contains(logs.String(), "drop message") }, "no drop log")
	if rec.count("panic") != 4 || strings.Count(logs.String(), "handler panic: handler exploded") != 4 {
		t.Fatalf("attempts=%d panics logged=%d", rec.count("panic"), strings.Count(logs.String(), "handler panic"))
	}
	publishOK(t, prod, q, "after-panic")
	waitFor(t, 10*time.Second, func() bool { return rec.count("after-panic") == 1 }, "consumer dead after panic")
}

func TestIT_ConsumerTopicGroups(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, g1, g2 := testName(t, "ex"), testName(t, "g1"), testName(t, "g2")
	adm.cleanup(ex, g1, g2)
	prod := newBroker(t, itURL, ex, "topic")
	if err := prod.DeclareQueue(g1, "order.*"); err != nil {
		t.Fatal(err)
	}
	if err := prod.DeclareQueue(g2, "order.*"); err != nil {
		t.Fatal(err)
	}
	rec1, rec2 := newRecorder(), newRecorder()
	for i := 0; i < 2; i++ {
		topicForever(newBroker(t, itURL, ex, "topic"), &GroupConsumeOption{Group: g1, RoutingKeys: []string{"order.*"}},
			func(b []byte) Status { rec1.add(b); return Retry })
	}
	topicForever(newBroker(t, itURL, ex, "topic"), &GroupConsumeOption{Group: g2, RoutingKeys: []string{"order.*"}},
		func(b []byte) Status { rec2.add(b); return Success })

	for i := 0; i < 3; i++ {
		publishOK(t, prod, "order.created", fmt.Sprintf("o-%d", i))
	}
	waitFor(t, 40*time.Second+3*itTTLSlack, func() bool { return strings.Count(logs.String(), "drop message") == 3 }, "drops=%d", strings.Count(logs.String(), "drop message"))
	time.Sleep(2 * time.Second)
	if rec1.sum() != 12 {
		t.Fatalf("group1 attempts=%d, want 12 (3 messages x 4)", rec1.sum())
	}
	if rec2.sum() != 3 || rec2.uniq() != 3 {
		t.Fatalf("group2 got %d deliveries of %d messages, want exactly 3", rec2.sum(), rec2.uniq())
	}
}

// ---------------------------------------------------------------------------
// 故障注入：连接重置、网络黑洞（本地与阿里云均可运行）
// ---------------------------------------------------------------------------

func TestIT_ConnectionResetNoMessageLoss(t *testing.T) {
	requireBroker(t)
	proxy, purl := proxied(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, purl, ex, "direct")
	cons := newBroker(t, purl, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})

	const n = 300
	var maxRecovery time.Duration
	for i := 0; i < n; i++ {
		if i == 100 || i == 200 {
			proxy.ResetAll()
		}
		body := fmt.Sprintf("m-%d", i)
		start := time.Now()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := prod.Publish(ctx, q, []byte(body))
			cancel()
			if err == nil {
				break
			}
			if time.Since(start) > 60*time.Second {
				t.Fatalf("publish did not recover: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if el := time.Since(start); el > maxRecovery {
			maxRecovery = el
		}
	}
	// 重置瞬间已投递未确认的消息由 broker 重新投递：开源 RabbitMQ 立即重投，阿里云要等消费超时
	if itSlowRequeue {
		const resets, prefetch = 2, defaultPrefetch
		deadline := time.Now().Add(itRequeueWait)
		for rec.uniq() < n && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
		missing := n - rec.uniq()
		t.Logf("slow-requeue broker: %d message(s) held by the dropped consumer connections, redelivered after the queue's consume timeout", missing)
		if missing > resets*prefetch {
			t.Fatalf("missing %d messages, more than the %d that can be in flight on dropped connections", missing, resets*prefetch)
		}
	} else {
		waitFor(t, itRequeueWait, func() bool { return rec.uniq() == n }, "received %d/%d", rec.uniq(), n)
	}
	t.Logf("max publish recovery after reset: %v, deliveries=%d (unique %d)", maxRecovery, rec.sum(), rec.uniq())
	if maxRecovery > 15*time.Second {
		t.Fatalf("recovery too slow: %v", maxRecovery)
	}
}

func TestIT_NetworkBlackholeIsBounded(t *testing.T) {
	requireBroker(t)
	proxy, purl := proxied(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)

	prod := newBroker(t, purl, ex, "direct")
	cons := newBroker(t, purl, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})
	publishOK(t, prod, q, "before")
	waitFor(t, 10*time.Second, func() bool { return rec.count("before") == 1 }, "baseline delivery")

	proxy.SetBlackhole(true)
	healed := false
	defer func() {
		if !healed {
			proxy.SetBlackhole(false)
		}
	}()

	start := time.Now()
	err := prod.Publish(context.Background(), q, []byte("blackholed"))
	if el := time.Since(start); err == nil || el > 7*time.Second {
		t.Fatalf("Publish without deadline must fail within ~5s, got err=%v after %v", err, el)
	}
	t.Logf("publish without deadline returned after %v: %v", time.Since(start), err)

	var wg sync.WaitGroup
	start = time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := prod.Publish(ctx, q, []byte("x")); err == nil {
				t.Error("publish into blackhole must fail")
			}
		}()
	}
	wg.Wait()
	if el := time.Since(start); el > 2500*time.Millisecond {
		t.Fatalf("10 concurrent publishes with 1s ctx took %v", el)
	}

	waitFor(t, 60*time.Second, func() bool { return !cons.Health() }, "consumer did not detect the dead connection")
	t.Logf("consumer detected dead connection after %v", time.Since(start))

	proxy.SetBlackhole(false)
	healed = true
	healedAt := time.Now()
	waitFor(t, 90*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return prod.Publish(ctx, q, []byte("after")) == nil
	}, "publisher did not recover")
	waitFor(t, 90*time.Second, func() bool { return rec.count("after") >= 1 }, "consumer did not recover")
	t.Logf("recovered %v after heal", time.Since(healedAt))
}

// ---------------------------------------------------------------------------
// broker 侧故障注入（需要 docker 容器）
// ---------------------------------------------------------------------------

func TestIT_MemoryAlarm(t *testing.T) {
	requireContainer(t)
	withFastBackoff(t)
	logs := captureLog(t)
	adm := newAdmin(t)
	ex, qOK, qFail := testName(t, "ex"), testName(t, "ok"), testName(t, "fail")
	adm.cleanup(ex, qOK, qFail)

	prod := newBroker(t, itURL, ex, "direct")
	for _, q := range []string{qOK, qFail} {
		if err := prod.DeclareQueue(q, q); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		publishOK(t, prod, qOK, fmt.Sprintf("ok-%d", i))
	}
	publishOK(t, prod, qFail, "poison")

	setAlarm(t)
	time.Sleep(time.Second)

	start := time.Now()
	err := prod.Publish(context.Background(), qOK, []byte("during-alarm"))
	el := time.Since(start)
	if !errors.Is(err, ErrBlocked) || el > 7*time.Second {
		t.Fatalf("expected ErrBlocked within ~5s, got %v after %v", err, el)
	}
	t.Logf("blocked publish returned after %v: %v", el, err)

	var wg sync.WaitGroup
	start = time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := prod.Publish(ctx, qOK, []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected ctx deadline, got %v", err)
			}
		}()
	}
	wg.Wait()
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("concurrent publishes during alarm took %v", el)
	}

	// 消费连接从不发布，告警期间照常消费；失败消息的重投连接被阻塞，退化为进程内重试，上限照常生效
	cons := newBroker(t, itURL, ex, "direct")
	okRec, failRec := newRecorder(), newRecorder()
	// 先让失败消息触发一次重投发布（若误用消费连接发布，该连接会被 broker 阻塞），再验证同一实例的其他队列仍能消费
	consumeForever(cons, &Queue{Name: qFail, RouteKey: qFail, Handle: func(b []byte) Status { failRec.add(b); return Retry }})
	waitFor(t, 15*time.Second, func() bool { return failRec.count("poison") >= 2 }, "poison attempts=%d", failRec.count("poison"))
	consumeForever(cons, &Queue{Name: qOK, RouteKey: qOK, Handle: func(b []byte) Status { okRec.add(b); return Success }})
	waitFor(t, 15*time.Second, func() bool { return okRec.uniq() == 5 }, "consumed %d/5 during alarm", okRec.uniq())
	waitFor(t, 30*time.Second, func() bool { return strings.Contains(logs.String(), "drop message") }, "poison not dropped during alarm")
	if failRec.count("poison") != 4 {
		t.Fatalf("poison attempts during alarm=%d, want 4", failRec.count("poison"))
	}
	if !strings.Contains(logs.String(), ErrBlocked.Error()) {
		t.Fatalf("expected redelivery scheduling to fail fast with ErrBlocked:\n%s", logs.String())
	}

	rabbitmqctl(t, "set_vm_memory_high_watermark", "0.4")
	waitFor(t, 20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return prod.Publish(ctx, qOK, []byte("after-alarm")) == nil
	}, "publisher did not recover after alarm cleared")
	waitFor(t, 10*time.Second, func() bool { return okRec.count("after-alarm") >= 1 }, "consumer did not get post-alarm message")
}

// 同一个 AmqpBroker 既消费又发布：开始消费后业务发布走独立连接，发布被阻塞不能拖住消费。
func TestIT_MixedUseBrokerKeepsConsumingDuringAlarm(t *testing.T) {
	requireContainer(t)
	adm := newAdmin(t)
	ex, qWarm, q := testName(t, "ex"), testName(t, "warm"), testName(t, "q")
	adm.cleanup(ex, qWarm, q)
	prod := newBroker(t, itURL, ex, "direct")
	for _, name := range []string{qWarm, q} {
		if err := prod.DeclareQueue(name, name); err != nil {
			t.Fatal(err)
		}
	}
	mixed := newBroker(t, itURL, ex, "direct")
	warm := newRecorder()
	consumeForever(mixed, &Queue{Name: qWarm, RouteKey: qWarm, Handle: func(b []byte) Status { warm.add(b); return Success }})
	publishOK(t, mixed, qWarm, "warm")
	waitFor(t, 10*time.Second, func() bool { return warm.count("warm") == 1 }, "warm-up")
	for i := 0; i < 5; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}

	setAlarm(t)
	time.Sleep(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := mixed.Publish(ctx, qWarm, []byte("blocked")); err == nil {
		t.Fatal("publish during alarm should fail")
	}
	cancel()

	rec := newRecorder()
	consumeForever(mixed, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})
	waitFor(t, 15*time.Second, func() bool { return rec.uniq() == 5 }, "mixed broker consumed %d/5 during alarm", rec.uniq())
}

func TestIT_CloseAllConnectionsRecovery(t *testing.T) {
	requireContainer(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})
	publishOK(t, prod, q, "before")
	waitFor(t, 10*time.Second, func() bool { return rec.count("before") == 1 }, "baseline")

	rabbitmqctl(t, "close_all_connections", "rmqtest")
	start := time.Now()
	waitFor(t, 30*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return prod.Publish(ctx, q, []byte("after")) == nil
	}, "publisher did not recover")
	waitFor(t, 30*time.Second, func() bool { return rec.count("after") >= 1 }, "consumer did not recover")
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("recovery took %v, want well under the old fixed 30s pause", el)
	}
	t.Logf("recovered in %v", time.Since(start))
}

func TestIT_BrokerRestartNoMessageLoss(t *testing.T) {
	requireContainer(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		rec.add(b)
		time.Sleep(20 * time.Millisecond)
		return Success
	}})
	const n = 100
	for i := 0; i < n; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}
	if out, err := exec.Command("docker", "restart", itContainer).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v %s", err, out)
	}
	restarted := time.Now()
	waitFor(t, 120*time.Second, func() bool { return rec.uniq() == n }, "received %d/%d after restart", rec.uniq(), n)
	waitFor(t, 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return prod.Publish(ctx, q, []byte("after-restart")) == nil
	}, "publisher did not recover")
	waitFor(t, 60*time.Second, func() bool { return rec.count("after-restart") >= 1 }, "consumer did not recover")
	t.Logf("fully recovered %v after restart; deliveries=%d unique=%d", time.Since(restarted), rec.sum(), rec.uniq())
}

func TestIT_RetryTopologyUnavailableFallsBackInProcess(t *testing.T) {
	requireContainer(t)
	withFastBackoff(t)
	logs := captureLog(t)
	user := testName(t, "user")
	rabbitmqctl(t, "add_user", user, "pw")
	t.Cleanup(func() { rabbitmqctl(t, "delete_user", user) })
	// 只允许配置 rmqtest- 开头的资源：重投 exchange 与 delay.v1 队列都无法声明
	rabbitmqctl(t, "set_permissions", "-p", "/", user, "^rmqtest-.*", ".*", ".*")

	u, _ := url.Parse(itURL)
	u.User = url.UserPassword(user, "pw")
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	cons := newBroker(t, u.String(), ex, "direct")
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Retry }})
	publishOK(t, prod, q, "poison")
	waitFor(t, 30*time.Second, func() bool { return strings.Contains(logs.String(), "drop message") }, "no drop")
	if rec.count("poison") != 4 || !strings.Contains(logs.String(), "retry topology unavailable") {
		t.Fatalf("attempts=%d logs:\n%s", rec.count("poison"), logs.String())
	}
}

// ---------------------------------------------------------------------------
// 停机、限流、并发与资源泄漏
// ---------------------------------------------------------------------------

func TestIT_GracefulShutdown(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}

	rec := newRecorder()
	started := make(chan string, 10)
	cons := NewAmqpBroker(&AmqpBrokerOptions{Url: itURL, Exchange: ex, ExchangeType: "direct"})
	done := consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		started <- string(b)
		time.Sleep(1500 * time.Millisecond)
		rec.add(b)
		return Success
	}})
	first := <-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := cons.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if el := time.Since(start); el > 4*time.Second {
		t.Fatalf("shutdown took %v", el)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Consume did not return ErrShutdown")
	}
	if rec.count(first) != 1 || rec.sum() != 1 {
		t.Fatalf("in-flight message must finish exactly once before shutdown: %v / total %d", rec.count(first), rec.sum())
	}

	rec2 := newRecorder()
	cons2 := newBroker(t, itURL, ex, "direct")
	consumeForever(cons2, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec2.add(b); return Success }})
	// 停机时先 basic.cancel 再关 channel：开源 RabbitMQ 立即重投预取消息，阿里云约 40s 后重投
	waitFor(t, itRequeueWait, func() bool { return rec2.uniq() == 4 }, "remaining %d/4", rec2.uniq())
	time.Sleep(time.Second)
	if rec2.count(first) != 0 || rec2.sum() != 4 {
		t.Fatalf("acked message redelivered or duplicates: first=%d total=%d", rec2.count(first), rec2.sum())
	}
	if err := cons.Consume(&Queue{Name: q, RouteKey: q, Handle: func([]byte) Status { return Success }}); !errors.Is(err, ErrShutdown) {
		t.Fatalf("Consume after shutdown should return ErrShutdown, got %v", err)
	}
	if err := cons.Publish(context.Background(), q, []byte("x")); err == nil {
		t.Fatal("Publish after shutdown should fail")
	}
}

func TestIT_ShutdownTimeoutRequeuesInFlight(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	publishOK(t, prod, q, "slow")
	started := make(chan struct{}, 1)
	cons := NewAmqpBroker(&AmqpBrokerOptions{Url: itURL, Exchange: ex, ExchangeType: "direct"})
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		started <- struct{}{}
		time.Sleep(5 * time.Second)
		return Success
	}})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := cons.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if el := time.Since(start); el > 4*time.Second {
		t.Fatalf("Shutdown must honor ctx, took %v", el)
	}
	before := runtime.NumGoroutine()
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	for i := 0; i < 50; i++ {
		_ = cons.Shutdown(expired)
	}
	if grown := runtime.NumGoroutine() - before; grown > 3 {
		t.Fatalf("repeated Shutdown leaked %d goroutines", grown)
	}
	rec := newRecorder()
	cons2 := newBroker(t, itURL, ex, "direct")
	consumeForever(cons2, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})
	if itSlowRequeue {
		t.Log("slow-requeue broker: the in-flight message is redelivered after the queue's consume timeout; not waiting for it")
		return
	}
	waitFor(t, itRequeueWait, func() bool { return rec.count("slow") == 1 }, "unacked in-flight message was not redelivered")
}

func TestIT_RateLimit(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}
	cons := newBroker(t, itURL, ex, "direct")
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }}, PerSecond(5))
	waitFor(t, 10*time.Second, func() bool { return rec.sum() > 0 }, "no delivery")
	time.Sleep(2 * time.Second)
	if got := rec.sum(); got < 8 || got > 20 {
		t.Fatalf("PerSecond(5) delivered %d in ~2s", got)
	}

	// 限流等待中也必须能立即停机
	ex2, q2 := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex2, q2)
	prod2 := newBroker(t, itURL, ex2, "direct")
	if err := prod2.DeclareQueue(q2, q2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		publishOK(t, prod2, q2, fmt.Sprintf("m-%d", i))
	}
	slow := NewAmqpBroker(&AmqpBrokerOptions{Url: itURL, Exchange: ex2, ExchangeType: "direct"})
	rec2 := newRecorder()
	done := consumeForever(slow, &Queue{Name: q2, RouteKey: q2, Handle: func(b []byte) Status { rec2.add(b); return Success }}, Every(time.Hour, 1))
	waitFor(t, 10*time.Second, func() bool { return rec2.sum() == 1 }, "first message")
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := slow.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("shutdown blocked on limiter for %v", el)
	}
}

func TestIT_ConcurrentPublishThroughput(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }}, &ConsumeOption{Prefetch: 100})

	const workers, per = 20, 100
	var failed atomic.Int32
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := prod.Publish(ctx, q, []byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					failed.Add(1)
					t.Errorf("publish: %v", err)
				}
				cancel()
			}
		}(w)
	}
	wg.Wait()
	el := time.Since(start)
	waitFor(t, 60*time.Second, func() bool { return rec.uniq() == workers*per }, "received %d/%d", rec.uniq(), workers*per)
	t.Logf("published %d confirmed messages from %d goroutines in %v (%.0f msg/s)", workers*per, workers, el, float64(workers*per)/el.Seconds())
	if failed.Load() > 0 {
		t.Fatalf("%d publishes failed", failed.Load())
	}
}

func TestIT_QueueDeletedWhileConsuming(t *testing.T) {
	requireBroker(t)
	adm := newAdmin(t)
	if !adm.capability("consumer_cancel_notify") {
		t.Skip("broker does not support consumer_cancel_notify")
	}
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	prod := newBroker(t, itURL, ex, "direct")
	cons := newBroker(t, itURL, ex, "direct")
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status { rec.add(b); return Success }})
	publishOK(t, prod, q, "before")
	waitFor(t, 10*time.Second, func() bool { return rec.count("before") == 1 }, "baseline")
	if err := adm.do(func(ch *amqp.Channel) error { _, err := ch.QueueDelete(q, false, false, false); return err }); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return prod.Publish(ctx, q, []byte("after")) == nil
	}, "queue was not re-declared by the consumer")
	waitFor(t, 10*time.Second, func() bool { return rec.count("after") == 1 }, "consumer did not resume")
}

func TestIT_NoGoroutineLeakAfterShutdown(t *testing.T) {
	requireBroker(t)
	withFastBackoff(t)
	captureLog(t)
	adm := newAdmin(t)
	ex, q := testName(t, "ex"), testName(t, "q")
	adm.cleanup(ex, q)
	time.Sleep(time.Second)
	baseline := runtime.NumGoroutine()

	proxy, purl := proxied(t)
	prod := NewAmqpBroker(&AmqpBrokerOptions{Url: purl, Exchange: ex, ExchangeType: "direct"})
	cons := NewAmqpBroker(&AmqpBrokerOptions{Url: purl, Exchange: ex, ExchangeType: "direct"})
	if err := prod.DeclareQueue(q, q); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	done := consumeForever(cons, &Queue{Name: q, RouteKey: q, Handle: func(b []byte) Status {
		if rec.add(b) < 2 {
			return Retry
		}
		return Success
	}})
	for i := 0; i < 20; i++ {
		publishOK(t, prod, q, fmt.Sprintf("m-%d", i))
	}
	waitFor(t, 30*time.Second, func() bool { return rec.sum() >= 40 }, "processed %d", rec.sum())
	accepted := proxy.Accepted()
	proxy.ResetAll()
	waitFor(t, 30*time.Second, func() bool { return proxy.Accepted() >= accepted+3 && prod.Health() && cons.Health() }, "reconnect")
	// 断开瞬间发布的消息在阿里云上可能被派给已失效的旧消费者、等消费超时才重投，因此持续发送直到任意一条被消费
	waitFor(t, 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = prod.Publish(ctx, q, []byte("final"))
		time.Sleep(time.Second)
		return rec.count("final") >= 1
	}, "consumer did not resume after reset")

	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prod.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	proxy.Close()
	waitFor(t, 15*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+2 },
		"goroutines %d > baseline %d", runtime.NumGoroutine(), baseline)
}
