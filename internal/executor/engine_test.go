package executor

import (
	"testing"
	"time"

	"resource-controller/internal/decision"
)

func testConfig() Config {
	return Config{
		Classes: map[string]ClassConfig{
			"cpu": {Min: 2, Max: 4, QueueCap: 2, StartupDelay: 2 * time.Second},
			"gpu": {Min: 1, Max: 2, QueueCap: 1, StartupDelay: 3 * time.Second},
		},
		Tick:     time.Second,
		Cooldown: 3 * time.Second,
	}
}

func TestStartupDelayAndCompletionAccounting(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: 0, Service: 500 * time.Millisecond, Class: "cpu"},
		{Arrival: 100 * time.Millisecond, Service: 500 * time.Millisecond, Class: "cpu"},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	counters := result.PerClass["cpu"]
	if counters.Submitted != 2 || counters.Completed != 2 || counters.Rejected != 0 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
	// 初始工作者 2 秒后才就绪，因此两个请求都等待过。
	if counters.Waited != 2 {
		t.Fatalf("Waited = %d, want 2", counters.Waited)
	}
}

func TestAffinityConstraint(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy())
	// gpu 任务到达时 cpu 工作者已空闲，但适配约束禁止跨类处理：
	// gpu 工作者 3 秒才就绪，任务必须排队等待同类实例。
	jobs := []Job{
		{Arrival: 2500 * time.Millisecond, Service: 500 * time.Millisecond, Class: "gpu"},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Completed != 1 || gpu.Waited != 1 {
		t.Fatalf("gpu counters = %+v, want 1 completed after waiting for gpu worker", gpu)
	}
	if gpu.MaxWait < 500*time.Millisecond {
		t.Fatalf("gpu MaxWait = %s, want to reflect startup wait", gpu.MaxWait)
	}
}

func TestRejectsOnlyWhenClassSaturated(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy())
	// cpu：2 名启动中 + 队列容量 2，前 4 个接收，第 5 个拒绝。
	var jobs []Job
	for i := 0; i < 5; i++ {
		jobs = append(jobs, Job{
			Arrival: time.Duration(i+1) * 100 * time.Millisecond,
			Service: time.Second,
			Class:   "cpu",
		})
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	cpu := result.PerClass["cpu"]
	if cpu.Submitted != 5 || cpu.Completed != 4 || cpu.Rejected != 1 {
		t.Fatalf("unexpected cpu counters: %+v", cpu)
	}
}

func TestDrainAndNoImmediateFlapping(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy())
	var jobs []Job
	for i := 0; i < 12; i++ {
		jobs = append(jobs, Job{
			Arrival: 2*time.Second + time.Duration(i)*80*time.Millisecond,
			Service: 700 * time.Millisecond,
			Class:   "cpu",
		})
	}
	for i := 0; i < 3; i++ {
		jobs = append(jobs, Job{
			Arrival: 7*time.Second + time.Duration(i)*3*time.Second,
			Service: 300 * time.Millisecond,
			Class:   "cpu",
		})
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	cpu := result.PerClass["cpu"]
	if cpu.Completed+cpu.Rejected != cpu.Submitted {
		t.Fatalf("accounting mismatch: %+v", cpu)
	}

	var changes []decision.Log
	for _, entry := range result.Logs {
		if entry.Class == "cpu" && entry.Action != decision.ActionHold {
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

func TestUnknownClassRejected(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy())
	_, err := engine.Run([]Job{{Arrival: 0, Service: time.Second, Class: "tpu"}})
	if err == nil {
		t.Fatal("want error for job class without worker class")
	}
}
