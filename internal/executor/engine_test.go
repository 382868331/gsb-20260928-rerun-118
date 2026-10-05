package executor

import (
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

// 替代实例在人工确认前就绪：只能进入待命，绝不能提前执行未知任务。
// 确认旧效果已发生时，该任务只记一次完成，不出现 takeover/complete 双份账目。
func TestStandbyReadyBeforeConfirmationDoesNotRun(t *testing.T) {
	cfg := handoffConfig()
	// cpu 替代实例 10ms 就绪，故障 1500ms 发生，确认 3s 才到：就绪远早于确认。
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.Confirmations = []OutcomeConfirmation{{At: 3000 * time.Millisecond, JobID: 1, EffectObserved: true}}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Submitted != 1 || gpu.Completed != 1 || gpu.Unknown != 0 || gpu.Rejected != 0 {
		t.Fatalf("确认已生效后账目应只有一次完成: %+v", gpu)
	}
	if len(result.Unknown) != 0 {
		t.Fatalf("未确认未知残留: %+v", result.Unknown)
	}
	kinds := auditKinds(result.Events, 1)
	for _, kind := range kinds {
		if kind == AuditTakeover {
			t.Fatalf("未确认前替代实例不得执行任务，审计序列=%v", kinds)
		}
	}
	var completes int
	for _, kind := range kinds {
		if kind == AuditComplete {
			completes++
		}
	}
	if completes != 0 {
		t.Fatalf("确认已生效时不应由替代实例再完成一次，审计序列=%v", kinds)
	}
	if !containsAuditReason(result.Events, AuditConfirmEffect, "只保留结果，不重复执行") {
		t.Fatalf("缺少已生效确认审计: %+v", result.Events)
	}
}

// 确认旧效果不存在时，尚未完成启动的替代实例在就绪后执行剩余工作；
// 旧实例的迟到完成事件不得重复记账或访问已释放状态。
func TestConfirmedNotObservedRunsAfterReadyAndLateCompleteIgnored(t *testing.T) {
	cfg := handoffConfig()
	// gpu 替代实例 1s 启动延迟，故障 1500ms 时第二个 gpu 实例仍在启动中。
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.Confirmations = []OutcomeConfirmation{{At: 1600 * time.Millisecond, JobID: 1}}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Submitted != 1 || gpu.Completed != 1 || gpu.Unknown != 0 {
		t.Fatalf("接替执行后应恰好一次完成: %+v", gpu)
	}
	kinds := auditKinds(result.Events, 1)
	var completes, takeovers int
	for _, kind := range kinds {
		switch kind {
		case AuditComplete:
			completes++
		case AuditTakeover:
			takeovers++
		}
	}
	if completes != 1 || takeovers != 1 {
		t.Fatalf("接替审计应各出现一次，审计序列=%v", kinds)
	}
}

// 重复/迟到的确认事件必须幂等：同一任务不会被二次记账。
func TestDuplicateConfirmationIsIdempotent(t *testing.T) {
	cfg := handoffConfig()
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.Confirmations = []OutcomeConfirmation{
		{At: 1600 * time.Millisecond, JobID: 1, EffectObserved: true},
		{At: 2500 * time.Millisecond, JobID: 1, EffectObserved: true},
	}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2}}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	gpu := result.PerClass["gpu"]
	if gpu.Completed != 1 || gpu.Unknown != 0 {
		t.Fatalf("重复确认后账目被二次修改: %+v", gpu)
	}
	var confirms int
	for _, event := range result.Events {
		if event.JobID == 1 && event.Kind == AuditConfirmEffect {
			confirms++
		}
	}
	if confirms != 1 {
		t.Fatalf("确认审计=%d，只应处理一次", confirms)
	}
}

func TestReviewRoundPreviewsConflictsAndPersistsTakeovers(t *testing.T) {
	cfg := handoffConfig()
	gpu := cfg.Classes["gpu"]
	gpu.Min = 2
	gpu.Max = 2
	cfg.Classes["gpu"] = gpu
	cpu := cfg.Classes["cpu"]
	cpu.StartupDelay = 2 * time.Second
	cfg.Classes["cpu"] = cpu
	cfg.Failures = []Failure{
		{At: 1500 * time.Millisecond, JobID: 1},
		{At: 1500 * time.Millisecond, JobID: 2},
	}
	cfg.ReviewRounds = []ReviewRound{
		{
			ID: "round-a", At: 1600 * time.Millisecond, Operator: "alice",
			Decisions: []ReviewDecision{
				{JobID: 1, Outcome: ReviewInsufficient, BaseVersion: 0},
				{JobID: 2, Outcome: ReviewNotOccurred, BaseVersion: 0},
			},
		},
		{
			ID: "round-b", At: 1700 * time.Millisecond, Operator: "bob",
			Decisions: []ReviewDecision{
				{JobID: 1, Outcome: ReviewOccurred, BaseVersion: 0},
			},
		},
		{
			ID: "round-c", At: 1800 * time.Millisecond, Operator: "bob",
			Decisions: []ReviewDecision{
				{JobID: 1, Outcome: ReviewOccurred, BaseVersion: 1},
			},
		},
	}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{
		{Arrival: 0, Service: 2 * time.Second, Class: "gpu"},
		{Arrival: 0, Service: 2 * time.Second, Class: "gpu"},
		{Arrival: 0, Service: 100 * time.Millisecond, Class: "gpu"},
	}

	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ReviewRounds) != 3 {
		t.Fatalf("review rounds = %d, want 3", len(result.ReviewRounds))
	}
	first := result.ReviewRounds[0]
	if len(first.Preview.Waiting) != 1 || first.Preview.Waiting[0].JobID != 1 {
		t.Fatalf("first round waiting preview = %+v", first.Preview.Waiting)
	}
	if len(first.Preview.Takeovers) != 1 || first.Preview.Takeovers[0].JobID != 2 || first.Preview.Takeovers[0].Ready {
		t.Fatalf("first round takeover preview = %+v", first.Preview.Takeovers)
	}
	if len(first.Preview.Releases) != 0 {
		t.Fatalf("insufficient material should not release capacity: %+v", first.Preview.Releases)
	}

	stale := result.ReviewRounds[1].Decisions[0]
	if stale.Accepted || stale.Status != ReviewStatusConflicted || stale.CurrentVersion != 1 {
		t.Fatalf("stale decision = %+v, want conflict at version 1", stale)
	}
	final := result.ReviewRounds[2].Decisions[0]
	if !final.Accepted || len(result.ReviewRounds[2].Preview.Releases) != 1 {
		t.Fatalf("current-version decision = %+v preview=%+v", final, result.ReviewRounds[2].Preview)
	}
	if len(result.Takeovers) != 1 || result.Takeovers[0].JobID != 2 || result.Takeovers[0].RoundID != "round-a" {
		t.Fatalf("actual takeovers = %+v, want only job 2", result.Takeovers)
	}
	if result.Takeovers[0].At < 2*time.Second {
		t.Fatalf("takeover recorded at plan time %s; want actual startup time", result.Takeovers[0].At)
	}
	if len(result.Resolutions) != 3 {
		t.Fatalf("resolutions = %+v, want insufficient, replay and old-result confirmation", result.Resolutions)
	}
	replayed := map[int]bool{}
	for _, resolution := range result.Resolutions {
		replayed[resolution.JobID] = resolution.Replayed
	}
	if !replayed[2] || replayed[1] {
		t.Fatalf("replayed flags = %+v, want only job 2 rerun", replayed)
	}
	if got := auditKinds(result.Events, 1); containsString(got, AuditTakeover) {
		t.Fatalf("job 1 confirmed old result but was executed again: %v", got)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
