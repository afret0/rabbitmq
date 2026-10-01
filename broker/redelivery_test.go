package broker

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

type fakeAck struct {
	mu      sync.Mutex
	acks    int
	nacks   int
	requeue []bool
}

func (f *fakeAck) Ack(uint64, bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	return nil
}

func (f *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks++
	f.requeue = append(f.requeue, requeue)
	return nil
}

func (f *fakeAck) Reject(tag uint64, requeue bool) error { return f.Nack(tag, false, requeue) }

type scheduled struct {
	next  int
	delay time.Duration
}

type harness struct {
	ack       *fakeAck
	calls     int
	schedules []scheduled
	waits     []time.Duration
	r         *redeliverer
}

// newHarness: statuses 依次作为每次 handler 调用的返回值（用尽后重复最后一个）；
// scheduleErr 为 nil 表示调度总是成功；waitOK 控制进程内等待是否被打断。
func newHarness(statuses []Status, scheduleErr error, withSchedule, waitOK bool) *harness {
	h := &harness{ack: &fakeAck{}}
	h.r = &redeliverer{
		queue: "q",
		handle: func([]byte) Status {
			i := h.calls
			h.calls++
			if i >= len(statuses) {
				i = len(statuses) - 1
			}
			return statuses[i]
		},
		wait: func(d time.Duration) bool {
			h.waits = append(h.waits, d)
			return waitOK
		},
	}
	if withSchedule {
		h.r.schedule = func(_ amqp.Delivery, next int, delay time.Duration) error {
			h.schedules = append(h.schedules, scheduled{next, delay})
			return scheduleErr
		}
	}
	return h
}

func (h *harness) delivery(headers amqp.Table) amqp.Delivery {
	return amqp.Delivery{Acknowledger: h.ack, Headers: headers, Body: []byte(`{"msgId":"m1"}`), RoutingKey: "rk", Exchange: "ex"}
}

// logBuffer 是并发安全的日志缓冲：后台重连循环等 goroutine 会同时写日志。
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

func (b *logBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func captureLog(t *testing.T) *logBuffer {
	t.Helper()
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

func Test_redeliveryCount_ParsesHeaderTypes(t *testing.T) {
	cases := []struct {
		v    interface{}
		want int
	}{
		{nil, 0},
		{int8(1), 1}, {int16(2), 2}, {int32(3), 3}, {int64(4), 4}, {int(5), 5},
		{uint8(1), 1}, {uint16(2), 2}, {uint32(3), 3}, {uint64(4), 4},
		{float32(2), 2}, {float64(3), 3},
		{"2", 2}, {" 3 ", 3}, {"x", 0},
		{int32(-1), 0}, {int64(1 << 40), 1<<31 - 1}, {uint64(1 << 40), 1<<31 - 1},
		{true, 0},
	}
	for _, c := range cases {
		h := amqp.Table{}
		if c.v != nil {
			h[retryCountHeader] = c.v
		}
		if got := redeliveryCount(h); got != c.want {
			t.Errorf("redeliveryCount(%T %v) = %d, want %d", c.v, c.v, got, c.want)
		}
	}
	if got := redeliveryCount(nil); got != 0 {
		t.Errorf("nil table: got %d", got)
	}
}

func Test_backoffSchedule_Is1s5s10sAndCapIs3(t *testing.T) {
	want := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	if len(redeliveryBackoff) != len(want) || maxRedeliveries != 3 {
		t.Fatalf("unexpected policy: backoff=%v max=%d", redeliveryBackoff, maxRedeliveries)
	}
	for i, d := range want {
		if redeliveryBackoff[i] != d || backoffFor(i) != d {
			t.Fatalf("backoff[%d] = %v, want %v", i, redeliveryBackoff[i], d)
		}
	}
	if backoffFor(-1) != time.Second || backoffFor(99) != 10*time.Second {
		t.Fatal("backoffFor must clamp out-of-range counts")
	}
}

func Test_process_SuccessAcks(t *testing.T) {
	h := newHarness([]Status{Success}, nil, true, true)
	h.r.process(h.delivery(nil))
	if h.ack.acks != 1 || h.ack.nacks != 0 || len(h.schedules) != 0 {
		t.Fatalf("acks=%d nacks=%d schedules=%v", h.ack.acks, h.ack.nacks, h.schedules)
	}
}

func Test_process_RetrySchedulesWithBackoffAndAcksOriginal(t *testing.T) {
	for count, delay := range []time.Duration{time.Second, 5 * time.Second, 10 * time.Second} {
		h := newHarness([]Status{Retry}, nil, true, true)
		h.r.process(h.delivery(amqp.Table{retryCountHeader: int32(count)}))
		if len(h.schedules) != 1 || h.schedules[0] != (scheduled{count + 1, delay}) {
			t.Fatalf("count=%d: schedules=%v", count, h.schedules)
		}
		if h.ack.acks != 1 || h.ack.nacks != 0 || h.calls != 1 || len(h.waits) != 0 {
			t.Fatalf("count=%d: acks=%d nacks=%d calls=%d waits=%v", count, h.ack.acks, h.ack.nacks, h.calls, h.waits)
		}
	}
}

func Test_process_DropsAfterThreeRedeliveriesWithOneLogLine(t *testing.T) {
	buf := captureLog(t)
	h := newHarness([]Status{Retry}, nil, true, true)
	d := h.delivery(amqp.Table{
		retryCountHeader:         int32(3),
		"opId":                   "op-9",
		originalExchangeHeader:   "orders",
		originalRoutingKeyHeader: "order.created",
	})
	d.MessageId = "mid-1"
	h.r.process(d)

	if len(h.schedules) != 0 || h.ack.acks != 0 || h.ack.nacks != 1 || h.ack.requeue[0] {
		t.Fatalf("must nack without requeue: schedules=%v acks=%d nacks=%d requeue=%v", h.schedules, h.ack.acks, h.ack.nacks, h.ack.requeue)
	}
	out := buf.String()
	if strings.Count(out, "drop message") != 1 {
		t.Fatalf("expected exactly one drop log line, got:\n%s", out)
	}
	for _, want := range []string{"after 3 redeliveries", "queue=q", "exchange=orders", "routingKey=order.created", "messageId=mid-1", "opId=op-9", `body="{\"msgId\":\"m1\"}"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("drop log missing %q:\n%s", want, out)
		}
	}
}

func Test_process_DropLogTruncatesBody(t *testing.T) {
	buf := captureLog(t)
	h := newHarness([]Status{Retry}, nil, true, true)
	d := h.delivery(amqp.Table{retryCountHeader: int32(3)})
	d.Body = bytes.Repeat([]byte("a"), 5000)
	h.r.process(d)
	if !strings.Contains(buf.String(), "bodyBytes=5000") || strings.Count(buf.String(), "a") > dropLogBodyLimit+50 {
		t.Fatalf("body not truncated in log: %d bytes", buf.Len())
	}
}

func Test_process_PanicIsRecoveredAndRetried(t *testing.T) {
	buf := captureLog(t)
	h := newHarness(nil, nil, true, true)
	h.r.handle = func([]byte) Status { h.calls++; panic("boom") }
	h.r.process(h.delivery(nil))
	if h.calls != 1 || len(h.schedules) != 1 || h.schedules[0].next != 1 || h.ack.acks != 1 {
		t.Fatalf("panic must be treated as Retry: calls=%d schedules=%v acks=%d", h.calls, h.schedules, h.ack.acks)
	}
	if !strings.Contains(buf.String(), "handler panic: boom") {
		t.Fatalf("panic not logged:\n%s", buf.String())
	}
}

func Test_process_PanicAtLimitIsDropped(t *testing.T) {
	captureLog(t)
	h := newHarness(nil, nil, true, true)
	h.r.handle = func([]byte) Status { panic("boom") }
	h.r.process(h.delivery(amqp.Table{retryCountHeader: int32(3)}))
	if h.ack.nacks != 1 || h.ack.requeue[0] {
		t.Fatalf("expected drop, nacks=%d requeue=%v", h.ack.nacks, h.ack.requeue)
	}
}

// broker 侧无法调度（阻塞/限流/断线）时，在进程内按同样的退避重试，次数照常累计。
func Test_process_ScheduleFailureFallsBackInProcessAndStillCaps(t *testing.T) {
	buf := captureLog(t)
	h := newHarness([]Status{Retry}, errors.New("blocked"), true, true)
	h.r.process(h.delivery(nil))

	if h.calls != 4 {
		t.Fatalf("expected 4 handler invocations (1 + 3 redeliveries), got %d", h.calls)
	}
	wantWaits := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	if len(h.waits) != 3 || h.waits[0] != wantWaits[0] || h.waits[1] != wantWaits[1] || h.waits[2] != wantWaits[2] {
		t.Fatalf("waits=%v want %v", h.waits, wantWaits)
	}
	if len(h.schedules) != 3 || h.schedules[0].next != 1 || h.schedules[1].next != 2 || h.schedules[2].next != 3 {
		t.Fatalf("each attempt should try the broker first: %v", h.schedules)
	}
	if h.ack.acks != 0 || h.ack.nacks != 1 || h.ack.requeue[0] {
		t.Fatalf("expected final drop: acks=%d nacks=%d requeue=%v", h.ack.acks, h.ack.nacks, h.ack.requeue)
	}
	if strings.Count(buf.String(), "drop message") != 1 {
		t.Fatalf("expected one drop log:\n%s", buf.String())
	}
}

func Test_process_ScheduleRecoversMidway(t *testing.T) {
	captureLog(t)
	h := newHarness([]Status{Retry}, nil, true, true)
	fails := 1
	h.r.schedule = func(_ amqp.Delivery, next int, delay time.Duration) error {
		h.schedules = append(h.schedules, scheduled{next, delay})
		if fails > 0 {
			fails--
			return errors.New("blocked")
		}
		return nil
	}
	h.r.process(h.delivery(nil))
	if h.calls != 2 || len(h.waits) != 1 || len(h.schedules) != 2 || h.schedules[1] != (scheduled{2, 5 * time.Second}) || h.ack.acks != 1 {
		t.Fatalf("calls=%d waits=%v schedules=%v acks=%d", h.calls, h.waits, h.schedules, h.ack.acks)
	}
}

func Test_process_NoTopologyRetriesInProcess(t *testing.T) {
	captureLog(t)
	h := newHarness([]Status{Retry, Retry, Success}, nil, false, true)
	h.r.process(h.delivery(nil))
	if h.calls != 3 || len(h.waits) != 2 || h.ack.acks != 1 || h.ack.nacks != 0 {
		t.Fatalf("calls=%d waits=%v acks=%d nacks=%d", h.calls, h.waits, h.ack.acks, h.ack.nacks)
	}
}

func Test_process_InterruptedWaitRequeues(t *testing.T) {
	captureLog(t)
	h := newHarness([]Status{Retry}, errors.New("down"), true, false)
	h.r.process(h.delivery(nil))
	if h.calls != 1 || h.ack.nacks != 1 || !h.ack.requeue[0] {
		t.Fatalf("expected requeue on shutdown: calls=%d nacks=%d requeue=%v", h.calls, h.ack.nacks, h.ack.requeue)
	}
}

func Test_delayQueueDecl_DeterministicAndCompatible(t *testing.T) {
	a, b := delayQueueDecl("orders", 5*time.Second), delayQueueDecl("orders", 5*time.Second)
	if a.name != "delay.v1.5s.orders" || a.name != b.name {
		t.Fatalf("unexpected name %q / %q", a.name, b.name)
	}
	if !a.durable || a.autoDelete || a.cacheFor != time.Hour {
		t.Fatalf("unexpected flags: %+v", a)
	}
	want := amqp.Table{
		"x-dead-letter-exchange":    retryExchange,
		"x-dead-letter-routing-key": "orders",
		"x-message-ttl":             int64(5000),
		"x-expires":                 int64(7*24*3600*1000 + 5000),
	}
	if len(a.args) != len(want) {
		t.Fatalf("args=%v", a.args)
	}
	for k, v := range want {
		if a.args[k] != v {
			t.Fatalf("arg %s = %#v, want %#v", k, a.args[k], v)
		}
	}
	// 新旧命名不能冲突：旧版本是 delay.<dur>.<exchange>.<queue>
	if strings.HasPrefix(a.name, "delay.5s.") {
		t.Fatal("new delay queue name collides with legacy naming")
	}
}

func Test_delayQueueDecl_LongNameIsHashed(t *testing.T) {
	q := strings.Repeat("q", 250)
	d := delayQueueDecl(q, 10*time.Second)
	if len(d.name) > 255 || !strings.HasPrefix(d.name, "delay.v1.10s.") {
		t.Fatalf("bad long name %q (%d)", d.name, len(d.name))
	}
	if d.args["x-dead-letter-routing-key"] != q {
		t.Fatal("routing key must stay the full queue name")
	}
	if delayQueueDecl(q, 10*time.Second).name != d.name {
		t.Fatal("hashed name must be deterministic")
	}
}

func Test_redeliveryPublishing_CopiesMessageAndStripsDeathHeaders(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	d := amqp.Delivery{
		Headers: amqp.Table{
			"opId": "op-1", retryCountHeader: int32(1),
			"x-death": []interface{}{}, "x-first-death-queue": "q", "x-last-death-reason": "expired",
		},
		ContentType: "application/json", ContentEncoding: "gzip", Priority: 3,
		CorrelationId: "c", ReplyTo: "r", MessageId: "m", Timestamp: ts, Type: "t", AppId: "app",
		UserId: "guest", Expiration: "1000", Body: []byte("body"),
		Exchange: "ex", RoutingKey: "rk",
	}
	p := redeliveryPublishing(d, 2)
	if p.Headers[retryCountHeader] != int32(2) {
		t.Fatalf("retry count must be int32(2), got %#v", p.Headers[retryCountHeader])
	}
	for _, k := range []string{"x-death", "x-first-death-queue", "x-last-death-reason"} {
		if _, ok := p.Headers[k]; ok {
			t.Fatalf("header %s must be stripped", k)
		}
	}
	if p.Headers["opId"] != "op-1" || p.Headers[originalExchangeHeader] != "ex" || p.Headers[originalRoutingKeyHeader] != "rk" {
		t.Fatalf("headers not preserved: %v", p.Headers)
	}
	if p.DeliveryMode != amqp.Persistent || p.ContentType != "application/json" || p.ContentEncoding != "gzip" ||
		p.Priority != 3 || p.CorrelationId != "c" || p.ReplyTo != "r" || p.MessageId != "m" ||
		!p.Timestamp.Equal(ts) || p.Type != "t" || p.AppId != "app" || string(p.Body) != "body" {
		t.Fatalf("properties not copied: %+v", p)
	}
	if p.UserId != "" || p.Expiration != "" {
		t.Fatalf("UserId/Expiration must not be copied: %+v", p)
	}
	if d.Headers[retryCountHeader] != int32(1) {
		t.Fatal("original headers must not be mutated")
	}

	// 第二次重投保留首次记录的原始路由
	d2 := amqp.Delivery{Headers: p.Headers, Exchange: retryExchange, RoutingKey: "q"}
	p2 := redeliveryPublishing(d2, 3)
	if p2.Headers[originalExchangeHeader] != "ex" || p2.Headers[originalRoutingKeyHeader] != "rk" {
		t.Fatalf("original routing overwritten: %v", p2.Headers)
	}
}

// 进程内重试被停机打断时，先尝试把累加后的次数交给 broker，成功则 ack 原消息。
func Test_process_InterruptedWaitPersistsCount(t *testing.T) {
	captureLog(t)
	h := newHarness([]Status{Retry}, errors.New("blocked"), true, false)
	var persisted []scheduled
	h.r.persist = func(_ amqp.Delivery, next int, delay time.Duration) bool {
		persisted = append(persisted, scheduled{next, delay})
		return true
	}
	h.r.process(h.delivery(amqp.Table{retryCountHeader: int32(1)}))
	if len(persisted) != 1 || persisted[0] != (scheduled{2, 5 * time.Second}) {
		t.Fatalf("persisted=%v", persisted)
	}
	if h.ack.acks != 1 || h.ack.nacks != 0 {
		t.Fatalf("expected ack after persisting: acks=%d nacks=%d", h.ack.acks, h.ack.nacks)
	}
}

func Test_process_InterruptedWaitPersistFailureRequeues(t *testing.T) {
	captureLog(t)
	h := newHarness([]Status{Retry}, errors.New("blocked"), true, false)
	h.r.persist = func(amqp.Delivery, int, time.Duration) bool { return false }
	h.r.process(h.delivery(nil))
	if h.ack.acks != 0 || h.ack.nacks != 1 || !h.ack.requeue[0] {
		t.Fatalf("acks=%d nacks=%d requeue=%v", h.ack.acks, h.ack.nacks, h.ack.requeue)
	}
}
