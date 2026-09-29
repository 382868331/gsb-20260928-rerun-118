package decision

import (
	"strings"
	"testing"
	"time"

	"resource-controller/internal/metrics"
)

func TestQueueRateStrategyExplainsDecisions(t *testing.T) {
	strategy := NewQueueRateStrategy(2, 6)

	up, action, reason := strategy.Decide(metrics.Snapshot{
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
		Ready: 2, Starting: 2, Busy: 2, QueueLength: 7, QueueCap: 8,
		ArrivalRate:     12.0 / float64(time.Second),
		MeanServiceTime: 500 * time.Millisecond,
	})
	if action != ActionHold || hold != 4 {
		t.Fatalf("action=(%d,%s), want hold at 4 during startup", hold, action)
	}
	if !strings.Contains(reason, "启动") {
		t.Fatalf("reason does not explain startup delay: %q", reason)
	}

	strategy = NewQueueRateStrategy(2, 6)
	var down int
	for i := 0; i < 3; i++ {
		down, action, _ = strategy.Decide(metrics.Snapshot{
			Ready: 5, Starting: 0, QueueLength: 0, QueueCap: 8,
			ArrivalRate: 0, MeanServiceTime: 500 * time.Millisecond,
		})
	}
	_, action, reason = strategy.Decide(metrics.Snapshot{
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
