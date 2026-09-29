package executor

import (
	"testing"
	"time"

	"resource-controller/internal/decision"
)

func testConfig() Config {
	return Config{
		MinWorkers:   2,
		MaxWorkers:   4,
		QueueCap:     2,
		StartupDelay: 2 * time.Second,
		Tick:         time.Second,
		Cooldown:     3 * time.Second,
	}
}

func TestStartupDelayAndCompletionAccounting(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(2, 4))
	jobs := []Job{
		{Arrival: 0, Service: 500 * time.Millisecond},
		{Arrival: 100 * time.Millisecond, Service: 500 * time.Millisecond},
	}

	_, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if counters.Submitted != 2 || counters.Completed != 2 || counters.Rejected != 0 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
	// 初始工作者 2 秒后才就绪，因此两个请求都等待过。
	if counters.Waited != 2 {
		t.Fatalf("Waited = %d, want 2", counters.Waited)
	}
}

func TestRejectsOnlyWhenSaturated(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(2, 4))
	// 2 名工作者在启动中；队列容量 2，因此前 4 个请求被接收，第 5 个必须明确拒绝。
	jobs := []Job{
		{Arrival: 100 * time.Millisecond, Service: time.Second},
		{Arrival: 200 * time.Millisecond, Service: time.Second},
		{Arrival: 300 * time.Millisecond, Service: time.Second},
		{Arrival: 400 * time.Millisecond, Service: time.Second},
		{Arrival: 500 * time.Millisecond, Service: time.Second},
	}

	_, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if counters.Submitted != 5 || counters.Completed != 4 || counters.Rejected != 1 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
}

func TestDrainAndNoImmediateFlapping(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(2, 4))
	// 短暂过载触发扩容；之后长时间低流量，允许系统回落，但必须经过冷却和低流量确认。
	var jobs []Job
	for i := 0; i < 12; i++ {
		jobs = append(jobs, Job{
			Arrival: 2*time.Second + time.Duration(i)*80*time.Millisecond,
			Service: 700 * time.Millisecond,
		})
	}
	for i := 0; i < 3; i++ {
		jobs = append(jobs, Job{
			Arrival: 7*time.Second + time.Duration(i)*3*time.Second,
			Service: 300 * time.Millisecond,
		})
	}

	logs, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if counters.Completed+counters.Rejected != counters.Submitted {
		t.Fatalf("accounting mismatch: %+v", counters)
	}

	var changes []decision.Log
	for _, entry := range logs {
		if entry.Action != decision.ActionHold {
			changes = append(changes, entry)
		}
	}
	if len(changes) < 2 {
		t.Fatalf("expected scale up and later scale down, got %d changes", len(changes))
	}
	if changes[0].Action != decision.ActionScaleUp {
		t.Fatalf("first change = %s, want scale_up", changes[0].Action)
	}
	foundDown := false
	for _, entry := range changes[1:] {
		if entry.Action == decision.ActionScaleDown {
			foundDown = true
			if entry.At < changes[0].At+3*time.Second {
				t.Fatalf("scale down at %s violates cooldown after scale up at %s", entry.At, changes[0].At)
			}
		}
	}
	if !foundDown {
		t.Fatal("burst cleared but capacity never scaled down")
	}
}
