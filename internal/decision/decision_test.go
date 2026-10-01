package decision

import (
	"strings"
	"testing"
	"time"

	"resource-controller/internal/metrics"
)

var testLimits = []ClassLimit{
	{Name: "short", Min: 2, Max: 6},
	{Name: "long", Min: 1, Max: 3},
}

func findDecision(decisions []ClassDecision, class string) ClassDecision {
	for _, d := range decisions {
		if d.Class == class {
			return d
		}
	}
	return ClassDecision{}
}

func TestQueueRateStrategyExplainsPerClassDecisions(t *testing.T) {
	strategy := NewQueueRateStrategy(testLimits)

	decisions := strategy.Decide(metrics.Snapshot{Classes: []metrics.ClassSnapshot{
		{Name: "short", Ready: 2, Busy: 2, QueueLength: 6, QueueCap: 8,
			ArrivalsInWindow: 12, CompletionsInWindow: 4,
			ArrivalRate: 12.0 / float64(time.Second), MeanServiceTime: 500 * time.Millisecond},
		{Name: "long", Ready: 1, QueueLength: 0, QueueCap: 4,
			ArrivalRate: 0, MeanServiceTime: 2 * time.Second},
	}})

	d := findDecision(decisions, "short")
	if d.Action != ActionScaleUp || d.Desired < 6 {
		t.Fatalf("short decision=(%d,%s), want scale up to at least 6", d.Desired, d.Action)
	}
	if !strings.Contains(d.Reason, "队列") || !strings.Contains(d.Reason, "速率需") {
		t.Fatalf("reason does not explain queue and rate: %q", d.Reason)
	}
	if !strings.Contains(d.Reason, "short") {
		t.Fatalf("reason does not name the class: %q", d.Reason)
	}
}

func TestQueueRateStrategyCountsInflightScaleUp(t *testing.T) {
	strategy := NewQueueRateStrategy(testLimits)
	decisions := strategy.Decide(metrics.Snapshot{Classes: []metrics.ClassSnapshot{
		{Name: "short", Ready: 2, Starting: 2, Busy: 2, QueueLength: 7, QueueCap: 8,
			ArrivalRate: 12.0 / float64(time.Second), MeanServiceTime: 500 * time.Millisecond},
	}})
	d := findDecision(decisions, "short")
	if d.Action != ActionHold || d.Desired != 4 {
		t.Fatalf("decision=(%d,%s), want hold at 4 while scale-up in flight", d.Desired, d.Action)
	}
	if !strings.Contains(d.Reason, "在途扩容") || !strings.Contains(d.Reason, "启动延迟") {
		t.Fatalf("reason does not explain in-flight scale-up: %q", d.Reason)
	}
}

func TestQueueRateStrategyScaleDownAfterDwell(t *testing.T) {
	strategy := NewQueueRateStrategy(testLimits)
	snap := metrics.Snapshot{Classes: []metrics.ClassSnapshot{
		{Name: "short", Ready: 5, QueueLength: 0, QueueCap: 8,
			ArrivalRate: 0, MeanServiceTime: 500 * time.Millisecond},
	}}
	var d ClassDecision
	for i := 0; i < 4; i++ {
		d = findDecision(strategy.Decide(snap), "short")
	}
	if d.Action != ActionScaleDown || d.Desired != 2 {
		t.Fatalf("decision=(%d,%s), want scale down to min 2", d.Desired, d.Action)
	}
	if !strings.Contains(d.Reason, "连续") || !strings.Contains(d.Reason, "在途工作") {
		t.Fatalf("reason does not explain dwell and draining: %q", d.Reason)
	}
}

func TestQueueRateStrategyHoldsAtClassCap(t *testing.T) {
	strategy := NewQueueRateStrategy(testLimits)
	decisions := strategy.Decide(metrics.Snapshot{Classes: []metrics.ClassSnapshot{
		{Name: "long", Ready: 3, Busy: 3, QueueLength: 4, QueueCap: 4,
			ArrivalRate: 4.0 / float64(time.Second), MeanServiceTime: 2500 * time.Millisecond},
	}})
	d := findDecision(decisions, "long")
	if d.Action != ActionHold || d.Desired != 3 {
		t.Fatalf("decision=(%d,%s), want hold at cap 3", d.Desired, d.Action)
	}
	if !strings.Contains(d.Reason, "上限") || !strings.Contains(d.Reason, "拒绝") {
		t.Fatalf("reason does not explain cap and rejection: %q", d.Reason)
	}
}

func TestTotalQueueStrategyIgnoresInflight(t *testing.T) {
	strategy := NewTotalQueueStrategy(testLimits)
	decisions := strategy.Decide(metrics.Snapshot{Classes: []metrics.ClassSnapshot{
		{Name: "short", Ready: 2, Starting: 2, QueueLength: 3, QueueCap: 8,
			MeanServiceTime: 500 * time.Millisecond},
	}})
	d := findDecision(decisions, "short")
	if d.Action != ActionScaleUp {
		t.Fatalf("naive strategy should scale up despite in-flight, got %s", d.Action)
	}
	if !strings.Contains(d.Reason, "未扣除在途扩容") {
		t.Fatalf("reason should admit ignoring in-flight scale-up: %q", d.Reason)
	}
}
