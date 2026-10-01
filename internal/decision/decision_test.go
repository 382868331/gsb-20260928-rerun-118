package decision

import (
	"strings"
	"testing"
	"time"

	"resource-controller/internal/metrics"
)

func TestQueueRateStrategyExplainsDecisions(t *testing.T) {
	strategy := NewQueueRateStrategy()

	up, action, reason := strategy.Decide(metrics.Snapshot{
		Class: "cpu", Min: 2, Max: 6,
		Ready: 2, Starting: 0, Busy: 2, QueueLength: 6, QueueCap: 8,
		ArrivalsInWindow: 12, CompletionsInWindow: 4,
		ArrivalRate:     12.0 / float64(time.Second),
		MeanServiceTime: 500 * time.Millisecond,
	})
	if action != ActionScaleUp || up < 6 {
		t.Fatalf("action=(%d,%s), want scale up to at least 6", up, action)
	}
	if !strings.Contains(reason, "队列") || !strings.Contains(reason, "速率需") {
		t.Fatalf("reason does not explain queue and rate: %q", reason)
	}

	hold, action, reason := strategy.Decide(metrics.Snapshot{
		Class: "cpu", Min: 2, Max: 6,
		Ready: 2, Starting: 2, Busy: 2, QueueLength: 7, QueueCap: 8,
		ArrivalRate:     12.0 / float64(time.Second),
		MeanServiceTime: 500 * time.Millisecond,
	})
	if action != ActionHold || hold != 4 {
		t.Fatalf("action=(%d,%s), want hold at 4 during startup", hold, action)
	}
	if !strings.Contains(reason, "在途扩容") {
		t.Fatalf("reason does not explain in-flight scale up: %q", reason)
	}

	strategy = NewQueueRateStrategy()
	var down int
	for i := 0; i < 3; i++ {
		down, action, _ = strategy.Decide(metrics.Snapshot{
			Class: "cpu", Min: 2, Max: 6,
			Ready: 5, Starting: 0, QueueLength: 0, QueueCap: 8,
			ArrivalRate: 0, MeanServiceTime: 500 * time.Millisecond,
		})
	}
	_, action, reason = strategy.Decide(metrics.Snapshot{
		Class: "cpu", Min: 2, Max: 6,
		Ready: 5, Starting: 0, QueueLength: 0, QueueCap: 8,
		ArrivalRate: 0, MeanServiceTime: 500 * time.Millisecond,
	})
	if action != ActionScaleDown || down != 2 {
		t.Fatalf("action=(%d,%s), want scale down to min 2", down, action)
	}
	if !strings.Contains(reason, "连续") || !strings.Contains(reason, "在途工作") {
		t.Fatalf("reason does not explain dwell and draining: %q", reason)
	}
}

func TestQueueRateStrategyPerClassState(t *testing.T) {
	strategy := NewQueueRateStrategy()
	// gpu 类别高压应扩容，cpu 类别空闲应各自独立累计低流量轮次。
	_, action, _ := strategy.Decide(metrics.Snapshot{
		Class: "gpu", Min: 1, Max: 3,
		Ready: 1, Busy: 1, QueueLength: 3, QueueCap: 4,
		ArrivalRate: 2.0 / float64(time.Second), MeanServiceTime: 2 * time.Second,
	})
	if action != ActionScaleUp {
		t.Fatalf("gpu action = %s, want scale_up", action)
	}
	_, action, reason := strategy.Decide(metrics.Snapshot{
		Class: "cpu", Min: 2, Max: 8,
		Ready: 4, QueueLength: 0, QueueCap: 8,
		ArrivalRate: 0, MeanServiceTime: 500 * time.Millisecond,
	})
	if action != ActionHold || !strings.Contains(reason, "连续确认") {
		t.Fatalf("cpu first low tick = (%s, %q), want dwell hold", action, reason)
	}
}

func TestTotalQueueStrategyIgnoresInflightStartup(t *testing.T) {
	strategy := NewTotalQueueStrategy()
	// 队列超水位且已有 2 人在启动：天真策略仍然继续扩容。
	target, action, reason := strategy.Decide(metrics.Snapshot{
		Class: "gpu", Min: 1, Max: 6,
		Ready: 1, Starting: 2, Busy: 1, QueueLength: 3, QueueCap: 4,
	})
	if action != ActionScaleUp || target != 3 {
		t.Fatalf("action=(%d,%s), want naive scale up to 3 ignoring in-flight", target, action)
	}
	if !strings.Contains(reason, "不计在途扩容") {
		t.Fatalf("reason should admit ignoring in-flight expansion: %q", reason)
	}
}
