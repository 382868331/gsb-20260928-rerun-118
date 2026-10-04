package executor

import (
	"reflect"
	"strings"
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

func handoffConfig() Config {
	return Config{
		Classes: map[string]ClassConfig{
			"gpu": {Min: 1, Max: 2, QueueCap: 2, StartupDelay: time.Second},
			"cpu": {Min: 1, Max: 2, QueueCap: 2, StartupDelay: 10 * time.Millisecond, CostFactor: 3},
		},
		Compatibility: map[string][]string{
			"gpu": {"gpu", "cpu"},
		},
		Tick:     500 * time.Millisecond,
		Cooldown: time.Second,
	}
}

func auditKinds(events []AuditEvent, jobID int) []string {
	var kinds []string
	for _, event := range events {
		if event.JobID == jobID {
			kinds = append(kinds, event.Kind)
		}
	}
	return kinds
}

func containsAuditReason(events []AuditEvent, kind, fragment string) bool {
	for _, event := range events {
		if event.Kind == kind && strings.Contains(event.Reason, fragment) {
			return true
		}
	}
	return false
}

func TestFailedWorkerHandoffWaitsForManualOutcome(t *testing.T) {
	cfg := handoffConfig()
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.Confirmations = []OutcomeConfirmation{{At: 1600 * time.Millisecond, JobID: 1}}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2},
		{Arrival: 1010 * time.Millisecond, Service: 100 * time.Millisecond, Class: "gpu"},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Submitted != 2 || gpu.Completed != 2 || gpu.Rejected != 0 || gpu.Unknown != 0 {
		t.Fatalf("unexpected gpu counters after handoff: %+v", gpu)
	}
	if !containsAuditReason(result.Events, AuditReserveStandby, "就绪但不自动认定旧实例停止") {
		t.Fatalf("missing standby reservation audit: %+v", result.Events)
	}
	if !containsAuditReason(result.Events, AuditConfirmEffect, "允许接替执行剩余任务") {
		t.Fatalf("missing manual confirmation audit: %+v", result.Events)
	}
	kinds := auditKinds(result.Events, 1)
	want := []string{AuditEnqueue, AuditAssign, AuditLoseWorker, AuditMarkUnknown, AuditReserveStandby, AuditConfirmEffect, AuditTakeover, AuditComplete, AuditRelease}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("job 1 audit = %v, want %v", kinds, want)
	}
}

func TestConfirmedEffectReleasesStandbyAndKeepsUnstartedQueued(t *testing.T) {
	cfg := handoffConfig()
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.Confirmations = []OutcomeConfirmation{{At: 1600 * time.Millisecond, JobID: 1, EffectObserved: true}}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: time.Second, Service: time.Second, Class: "gpu"},
		{Arrival: 1010 * time.Millisecond, Service: 100 * time.Millisecond, Class: "gpu"},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if result.PerClass["gpu"].Completed != 2 {
		t.Fatalf("gpu counters = %+v, want unstarted job rescheduled after class replacement starts", result.PerClass["gpu"])
	}
	if len(result.Unknown) != 0 {
		t.Fatalf("unknown jobs = %+v, want resolved", result.Unknown)
	}
	if !containsAuditReason(result.Events, AuditRelease, "取消接替执行") {
		t.Fatalf("standby was not released after effect confirmation: %+v", result.Events)
	}
}

func TestReplacementReadyBeforeConfirmationDoesNotExecute(t *testing.T) {
	cfg := Config{
		Classes: map[string]ClassConfig{
			"gpu": {Min: 1, Max: 2, QueueCap: 1, StartupDelay: time.Second},
		},
		Tick:          250 * time.Millisecond,
		Cooldown:      time.Second,
		Failures:      []Failure{{At: 1500 * time.Millisecond, JobID: 1}},
		Confirmations: []OutcomeConfirmation{{At: 2500 * time.Millisecond, JobID: 1}},
	}
	jobs := []Job{{Arrival: time.Second, Service: time.Second, Class: "gpu"}}

	result, err := New(cfg, decision.NewQueueRateStrategy()).Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range result.Events {
		if event.Kind == AuditTakeover && event.At < 2500*time.Millisecond {
			t.Fatalf("replacement executed before confirmation: %+v", event)
		}
	}
	if !containsAuditReason(result.Events, AuditReserveStandby, "已就绪但旧效果尚未确认") {
		t.Fatalf("missing ready-but-unconfirmed standby audit: %+v", result.Events)
	}
	if got := result.PerClass["gpu"]; got.Completed != 1 || got.Unknown != 0 || got.Rejected != 0 {
		t.Fatalf("unexpected counters: %+v", got)
	}
}

func TestLateCompletionAfterObservedEffectIsIgnored(t *testing.T) {
	cfg := Config{
		Classes: map[string]ClassConfig{
			"gpu": {Min: 1, Max: 2, QueueCap: 1, StartupDelay: 500 * time.Millisecond},
		},
		Tick:          250 * time.Millisecond,
		Cooldown:      time.Second,
		Failures:      []Failure{{At: 1500 * time.Millisecond, JobID: 1}},
		Confirmations: []OutcomeConfirmation{{At: 1600 * time.Millisecond, JobID: 1, EffectObserved: true}},
	}
	jobs := []Job{{Arrival: time.Second, Service: time.Second, Class: "gpu"}}

	result, err := New(cfg, decision.NewQueueRateStrategy()).Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.PerClass["gpu"]; got.Completed != 1 || got.Unknown != 0 {
		t.Fatalf("late completion changed accounting: %+v", got)
	}
	if len(auditKinds(result.Events, 1)) != len(uniqueAuditKinds(auditKinds(result.Events, 1))) {
		t.Fatalf("late event produced duplicate accounting audit: %v", auditKinds(result.Events, 1))
	}
}

func TestHandoffReplayIsDeterministic(t *testing.T) {
	run := func() Result {
		cfg := handoffConfig()
		cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
		cfg.Confirmations = []OutcomeConfirmation{{At: 2500 * time.Millisecond, JobID: 1}}
		jobs := []Job{
			{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2},
			{Arrival: 1010 * time.Millisecond, Service: 100 * time.Millisecond, Class: "gpu"},
		}
		result, err := New(cfg, decision.NewQueueRateStrategy()).Run(jobs)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	first, second := run(), run()
	if !reflect.DeepEqual(first.Events, second.Events) {
		t.Fatalf("audit events differ across replays\nfirst=%v\nsecond=%v", first.Events, second.Events)
	}
	if !reflect.DeepEqual(first.PerClass, second.PerClass) {
		t.Fatalf("counters differ across replays\nfirst=%v\nsecond=%v", first.PerClass, second.PerClass)
	}
}

func uniqueAuditKinds(kinds []string) []string {
	seen := map[string]bool{}
	var unique []string
	for _, kind := range kinds {
		if !seen[kind] {
			seen[kind] = true
			unique = append(unique, kind)
		}
	}
	return unique
}

func TestCompatibilityDoesNotRelaxNormalAdmission(t *testing.T) {
	cfg := handoffConfig()
	engine := New(cfg, decision.NewQueueRateStrategy())
	var jobs []Job
	for i := 0; i < 5; i++ {
		jobs = append(jobs, Job{Arrival: time.Duration(i) * time.Millisecond, Service: time.Second, Class: "gpu"})
	}
	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Rejected != 2 {
		t.Fatalf("gpu rejected = %d, want 2 despite cpu compatibility: %+v", gpu.Rejected, gpu)
	}
	if !containsAuditReason(result.Events, AuditReject, "不放宽普通亲和性") {
		t.Fatalf("missing affinity rejection reason: %+v", result.Events)
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
