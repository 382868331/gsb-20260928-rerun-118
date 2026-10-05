package executor

import (
	"strings"
	"testing"
	"time"

	"resource-controller/internal/decision"
)

// 分批核对：先保留（材料不足），替代实例就绪后状态版本递增；
// 依据旧版本的过期决定被列为冲突，只有当前版本的决定生效，且不重复执行。
func TestReviewRoundKeepsWaitingThenRejectsStaleBasis(t *testing.T) {
	cfg := reviewBasisConfig()
	cfg.Failures = []Failure{{At: 1500 * time.Millisecond, JobID: 1}}
	cfg.ReviewRounds = []ReviewRound{
		{
			ID: 1, Operator: "alice", DecidedAt: 1600 * time.Millisecond,
			Items: []ReviewItem{{JobID: 1, Decision: DecisionInsufficient, Basis: 0}},
		},
		{
			// bob 的依据停留在失联瞬间版本 0：就绪后版本已为 1，该决定过期。
			ID: 2, Operator: "bob", DecidedAt: 3000 * time.Millisecond,
			Items: []ReviewItem{{JobID: 1, Decision: DecisionHappened, Basis: 0}},
		},
		{
			// alice 使用当前版本 1：接受并确认旧效果已发生，释放替代容量。
			ID: 3, Operator: "alice", DecidedAt: 3200 * time.Millisecond,
			Items: []ReviewItem{{JobID: 1, Decision: DecisionHappened, Basis: 1}},
		},
		{
			// 任务已解决：再提任何决定都必须冲突，绝不重复执行。
			ID: 4, Operator: "bob", DecidedAt: 3400 * time.Millisecond,
			Items: []ReviewItem{{JobID: 1, Decision: DecisionNotHappened, Basis: 1}},
		},
	}
	engine := New(cfg, decision.NewQueueRateStrategy())
	result, err := engine.Run([]Job{{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if result.PerClass["gpu"].Completed != 1 || result.PerClass["gpu"].Unknown != 0 {
		t.Fatalf("最终应只确认一次旧结果: %+v", result.PerClass["gpu"])
	}
	rounds := result.Review.Rounds
	if len(rounds) != 4 {
		t.Fatalf("四轮核对都应留痕: %d", len(rounds))
	}
	if len(rounds[0].Accepted) != 1 {
		t.Fatal("第一轮材料不足应被接受为保留等待")
	}
	if len(rounds[1].Accepted) != 0 || len(rounds[1].Preview.Conflicts) != 1 {
		t.Fatalf("旧状态版本决定应冲突且不生效: %+v", rounds[1].Preview)
	}
	if len(rounds[2].Accepted) != 1 {
		t.Fatalf("当前版本决定应生效: %+v", rounds[2].Preview)
	}
	if len(rounds[3].Accepted) != 0 || len(rounds[3].Preview.Conflicts) != 1 {
		t.Fatalf("已解决任务的迟到决定必须冲突: %+v", rounds[3].Preview)
	}
	var takeovers int
	var rerun int
	for _, record := range result.Review.Takeovers {
		takeovers++
		if record.ReRun {
			rerun++
		}
		if record.Status != TakeoverConfirmedOldResult {
			t.Fatalf("该任务只确认旧结果，记录应为 %s: %+v", TakeoverConfirmedOldResult, record)
		}
	}
	if takeovers != 1 || rerun != 0 {
		t.Fatalf("实际接替只应有一条且未重新运行: takeovers=%d rerun=%d", takeovers, rerun)
	}
	if !containsAuditReason(result.Events, AuditKeepWaiting, "保留未知并继续等待") {
		t.Fatalf("缺少保留等待审计: %+v", result.Events)
	}
	if !containsAuditReason(result.Events, AuditReviewConflict, "状态版本") {
		t.Fatalf("缺少过期依据冲突审计: %+v", result.Events)
	}
	kinds := auditKinds(result.Events, 1)
	joined := strings.Join(kinds, ",")
	if strings.Contains(joined, AuditTakeover) {
		t.Fatalf("确认旧结果后不得发生接替执行: %v", kinds)
	}
}

// 一轮内对多个失联任务分别记录三种结论，并区分重新运行与只确认旧结果。
func TestBatchReviewThreeDecisionsInOneRound(t *testing.T) {
	cfg := reviewBasisConfig()
	cfg.Failures = []Failure{
		{At: 1500 * time.Millisecond, JobID: 1},
	}
	cfg.ReviewRounds = []ReviewRound{
		{
			ID: 1, Operator: "alice", DecidedAt: 1700 * time.Millisecond,
			Items: []ReviewItem{
				{JobID: 1, Decision: DecisionInsufficient, Basis: 0},
			},
		},
		{
			ID: 2, Operator: "bob", DecidedAt: 3200 * time.Millisecond,
			// 就绪事件已把版本推进到 1。
			Items: []ReviewItem{{JobID: 1, Decision: DecisionNotHappened, Basis: 1}},
		},
	}
	engine := New(cfg, decision.NewQueueRateStrategy())
	jobs := []Job{{Arrival: time.Second, Service: time.Second, Class: "gpu", Cost: 2}}
	result, err := engine.Run(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Review.Takeovers) != 1 || !result.Review.Takeovers[0].ReRun ||
		result.Review.Takeovers[0].Status != TakeoverCompleted {
		t.Fatalf("应记录一条确已完成的重新运行接替: %+v", result.Review.Takeovers)
	}
}

// reviewBasisConfig 让被选中的替代实例在故障时仍在途启动：
// gpu 原实例 1s 就绪并执行任务，1.5s 失联；保留的 cpu 替代实例 2s 才就绪，
// 就绪事件把失联任务状态版本从 0 推进到 1，用于检验过期依据冲突。
func reviewBasisConfig() Config {
	return Config{
		Classes: map[string]ClassConfig{
			"gpu": {Min: 1, Max: 2, QueueCap: 2, StartupDelay: time.Second},
			"cpu": {Min: 1, Max: 2, QueueCap: 2, StartupDelay: 2 * time.Second, CostFactor: 1},
		},
		Compatibility: map[string][]string{"gpu": {"gpu", "cpu"}},
		Tick:          500 * time.Millisecond,
		Cooldown:      time.Second,
	}
}
