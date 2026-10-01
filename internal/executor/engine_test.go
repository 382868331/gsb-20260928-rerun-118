package executor

import (
	"testing"
	"time"

	"resource-controller/internal/decision"
)

func testConfig() Config {
	return Config{
		Classes: []ClassConfig{
			{Name: "short", MinWorkers: 2, MaxWorkers: 4, QueueCap: 2, StartupDelay: 2 * time.Second},
			{Name: "long", MinWorkers: 1, MaxWorkers: 2, QueueCap: 1, StartupDelay: 3 * time.Second},
		},
		Tick:     time.Second,
		Cooldown: 3 * time.Second,
	}
}

func testLimits() []decision.ClassLimit {
	return []decision.ClassLimit{
		{Name: "short", Min: 2, Max: 4},
		{Name: "long", Min: 1, Max: 2},
	}
}

func TestClassAffinityAndAccounting(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(testLimits()))
	jobs := []Job{
		{Class: "short", Arrival: 0, Service: 500 * time.Millisecond},
		{Class: "short", Arrival: 100 * time.Millisecond, Service: 500 * time.Millisecond},
		{Class: "long", Arrival: 200 * time.Millisecond, Service: 800 * time.Millisecond},
	}

	_, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if counters.Total.Submitted != 3 || counters.Total.Completed != 3 || counters.Total.Rejected != 0 {
		t.Fatalf("unexpected totals: %+v", counters.Total)
	}
	short := counters.Class("short")
	long := counters.Class("long")
	if short.Completed != 2 || long.Completed != 1 {
		t.Fatalf("per-class completion wrong: short=%+v long=%+v", short, long)
	}
	// 所有类别的工作者都有启动延迟，因此三个请求都等待过。
	if counters.Total.Waited != 3 {
		t.Fatalf("Waited = %d, want 3", counters.Total.Waited)
	}
}

func TestUnknownClassRejectedImmediately(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(testLimits()))
	jobs := []Job{
		{Class: "gpu", Arrival: 0, Service: time.Second},
		{Class: "short", Arrival: 0, Service: 100 * time.Millisecond},
	}
	_, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if counters.Total.Rejected != 1 || counters.Total.Completed != 1 {
		t.Fatalf("unknown class must be rejected explicitly: %+v", counters.Total)
	}
}

func TestRejectsOnlyWhenClassSaturated(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(testLimits()))
	// short 类 2 名在启动；队列容量 2，前 4 个接收，第 5 个必须明确拒绝。
	// long 类任务不受 short 类拥塞影响。
	var jobs []Job
	for i := 0; i < 5; i++ {
		jobs = append(jobs, Job{Class: "short",
			Arrival: time.Duration(i+1) * 100 * time.Millisecond, Service: time.Second})
	}
	jobs = append(jobs, Job{Class: "long", Arrival: 150 * time.Millisecond, Service: time.Second})

	_, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	short := counters.Class("short")
	if short.Submitted != 5 || short.Completed != 4 || short.Rejected != 1 {
		t.Fatalf("short class counters wrong: %+v", short)
	}
	if long := counters.Class("long"); long.Completed != 1 || long.Rejected != 0 {
		t.Fatalf("long class must be unaffected: %+v", long)
	}
}

func TestDrainAndNoImmediateFlapping(t *testing.T) {
	engine := New(testConfig(), decision.NewQueueRateStrategy(testLimits()))
	var jobs []Job
	for i := 0; i < 12; i++ {
		jobs = append(jobs, Job{Class: "short",
			Arrival: 2*time.Second + time.Duration(i)*80*time.Millisecond,
			Service: 700 * time.Millisecond})
	}
	for i := 0; i < 3; i++ {
		jobs = append(jobs, Job{Class: "short",
			Arrival: 7*time.Second + time.Duration(i)*3*time.Second,
			Service: 300 * time.Millisecond})
	}

	logs, counters, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"short", "long"} {
		cc := counters.Class(name)
		if cc.Completed+cc.Rejected != cc.Submitted {
			t.Fatalf("class %s accounting mismatch: %+v", name, cc)
		}
	}

	var changes []struct {
		at     time.Duration
		action string
	}
	for _, entry := range logs {
		if entry.Class == "short" && entry.Action != "hold" {
			changes = append(changes, struct {
				at     time.Duration
				action string
			}{time.Duration(entry.At), string(entry.Action)})
		}
	}
	if len(changes) < 2 {
		t.Fatalf("expected scale up and later scale down, got %d changes", len(changes))
	}
	if changes[0].action != "scale_up" {
		t.Fatalf("first change = %s, want scale_up", changes[0].action)
	}
	foundDown := false
	for _, c := range changes[1:] {
		if c.action == "scale_down" {
			foundDown = true
			if c.at < changes[0].at+3*time.Second {
				t.Fatalf("scale down at %s violates cooldown after scale up at %s", c.at, changes[0].at)
			}
		}
	}
	if !foundDown {
		t.Fatal("burst cleared but capacity never scaled down")
	}
}

func TestReplayDeterministicAcrossStrategies(t *testing.T) {
	jobs := []Job{
		{Class: "short", Arrival: 0, Service: 400 * time.Millisecond},
		{Class: "short", Arrival: 50 * time.Millisecond, Service: 400 * time.Millisecond},
		{Class: "long", Arrival: 0, Service: 1500 * time.Millisecond},
	}
	run := func(s decision.Strategy) (int, int) {
		engine := New(testConfig(), s)
		_, counters, err := engine.Run(jobs)
		if err != nil {
			t.Fatal(err)
		}
		return counters.Total.Completed, counters.Total.Rejected
	}
	c1, r1 := run(decision.NewQueueRateStrategy(testLimits()))
	c2, r2 := run(decision.NewQueueRateStrategy(testLimits()))
	if c1 != c2 || r1 != r2 {
		t.Fatalf("same strategy replay diverged: (%d,%d) vs (%d,%d)", c1, r1, c2, r2)
	}
}
