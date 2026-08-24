package broker

import (
	"errors"
	"testing"
	"time"
)

// 调度失败必须 requeue, 否则消息丢失。
func Test_retryAction_ScheduleErrorRequeues(t *testing.T) {
	err := errors.New("boom")
	if got := retryAction(false, err, true); got != actionRequeue {
		t.Fatalf("expected requeue, got %v", got)
	}
	// 即便 scheduled 为 true, 只要出错就不能 Ack
	if got := retryAction(true, err, true); got != actionRequeue {
		t.Fatalf("expected requeue, got %v", got)
	}
}

func Test_retryAction_ScheduledAcks(t *testing.T) {
	if got := retryAction(true, nil, true); got != actionAck {
		t.Fatalf("expected ack, got %v", got)
	}
}

// 回归: 未配置重试时曾被静默 Ack 丢弃, 现在必须 requeue。
func Test_retryAction_NotConfiguredRequeues(t *testing.T) {
	if got := retryAction(false, nil, false); got != actionRequeue {
		t.Fatalf("expected requeue when retry is not configured, got %v", got)
	}
}

// 回归: 重试次数用尽时曾被静默 Ack, 现在应显式 reject。
func Test_retryAction_ExhaustedRejects(t *testing.T) {
	if got := retryAction(false, nil, true); got != actionReject {
		t.Fatalf("expected reject when retry exhausted, got %v", got)
	}
}

// 任何情况下都不能在未真正调度重试时 Ack。
func Test_retryAction_NeverAcksWithoutSchedule(t *testing.T) {
	for _, err := range []error{nil, errors.New("boom")} {
		for _, configured := range []bool{true, false} {
			if got := retryAction(false, err, configured); got == actionAck {
				t.Fatalf("must not ack: err=%v configured=%v", err, configured)
			}
		}
	}
}

func Test_RetryQueueMillis_Empty(t *testing.T) {
	if got := (ConsumeOption{}).RetryQueueMillis(); len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func Test_RetryQueueMillis_Converts(t *testing.T) {
	opt := PerSecond(5).WithRetry(time.Second, 5*time.Second, 1500*time.Millisecond)
	got := opt.RetryQueueMillis()

	want := []int64{1000, 5000, 1500}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

// ttl 为 0 的延迟队列会让消息立刻回流, 等于没有退避, 必须过滤。
func Test_RetryQueueMillis_DropsNonPositive(t *testing.T) {
	opt := (&ConsumeOption{}).WithRetry(0, -time.Second, time.Second)
	got := opt.RetryQueueMillis()

	if len(got) != 1 || got[0] != 1000 {
		t.Fatalf("expected [1000], got %v", got)
	}
}

func Test_WithRetry_IsChainableAndKeepsLimit(t *testing.T) {
	opt := PerSecond(5).WithRetry(time.Second)
	if opt.Limit != 5 || opt.Interval != time.Second {
		t.Fatalf("limit config lost: %+v", opt)
	}
	if len(opt.Retry) != 1 {
		t.Fatalf("retry config lost: %+v", opt)
	}
}
