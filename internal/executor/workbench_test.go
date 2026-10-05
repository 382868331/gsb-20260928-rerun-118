package executor

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleTasks() []UnknownTask {
	return []UnknownTask{
		{JobID: 1, Class: "gpu", Basis: 0, FailedAt: time.Second, Remaining: 500 * time.Millisecond, ReplacementID: 7, ReplacementClass: "cpu", ReplacementReady: true},
		{JobID: 2, Class: "gpu", Basis: 1, FailedAt: time.Second, Remaining: 500 * time.Millisecond, ReplacementID: 8, ReplacementClass: "cpu", ReplacementReady: false},
		{JobID: 3, Class: "gpu", Basis: 0, FailedAt: time.Second, Remaining: 500 * time.Millisecond},
	}
}

func TestPreviewListsReleaseTakeoverAndWaiting(t *testing.T) {
	wb := Workbench{}
	round := ReviewRound{
		ID: 1, Operator: "alice", DecidedAt: 2 * time.Second,
		Items: []ReviewItem{
			{JobID: 1, Decision: DecisionHappened, Basis: 0},
			{JobID: 2, Decision: DecisionNotHappened, Basis: 1},
			{JobID: 3, Decision: DecisionInsufficient, Basis: 0},
		},
	}
	preview := wb.Preview(sampleTasks(), nil, round)
	if len(preview.Applicable) != 3 || len(preview.Conflicts) != 0 {
		t.Fatalf("全部依据有效时应都适用: %+v", preview)
	}
	want := map[int]string{1: EffectReleaseCapacity, 2: EffectScheduleTakeover, 3: EffectKeepWaiting}
	for _, effect := range preview.Effects {
		if want[effect.JobID] != effect.Effect {
			t.Fatalf("任务 %d 效果=%s，期望 %s", effect.JobID, effect.Effect, want[effect.JobID])
		}
	}
}

func TestPreviewRejectsStaleBasisAndAlreadyResolved(t *testing.T) {
	wb := Workbench{}
	resolved := []ResolvedOutcome{
		{JobID: 1, Decision: DecisionNotHappened, RoundID: 1, Operator: "bob", ReRun: true},
	}
	round := ReviewRound{
		ID: 2, Operator: "alice", DecidedAt: 3 * time.Second,
		Items: []ReviewItem{
			// 任务 1 已由 bob 安排接替并重新运行：过期的 happened 决定不得再次执行/释放。
			{JobID: 1, Decision: DecisionHappened, Basis: 0},
			// 任务 2 当前 basis=1，alice 依据旧版本 0：冲突，不接受。
			{JobID: 2, Decision: DecisionNotHappened, Basis: 0},
			// 任务 4 不在失联队列：冲突。
			{JobID: 4, Decision: DecisionHappened, Basis: 0},
			// 任务 3 仍适用。
			{JobID: 3, Decision: DecisionInsufficient, Basis: 0},
		},
	}
	preview := wb.Preview(sampleTasks(), resolved, round)
	if len(preview.Applicable) != 1 || preview.Applicable[0].JobID != 3 {
		t.Fatalf("只有任务 3 应被接受: %+v", preview.Applicable)
	}
	if len(preview.Conflicts) != 3 {
		t.Fatalf("应明确列出 3 项冲突: %+v", preview.Conflicts)
	}
	byJob := map[int]ReviewConflict{}
	for _, conflict := range preview.Conflicts {
		byJob[conflict.JobID] = conflict
	}
	if !strings.Contains(byJob[1].Reason, "已在第 1 轮") {
		t.Fatalf("已解决项冲突原因应指向早先轮次: %s", byJob[1].Reason)
	}
	if byJob[2].CurrentBasis != 1 || byJob[2].GivenBasis != 0 {
		t.Fatalf("过期依据冲突应同时记录给定/当前版本: %+v", byJob[2])
	}
}

func TestJournalPersistsRoundsAndTakeoversSeparately(t *testing.T) {
	wb := Workbench{}
	round := ReviewRound{ID: 1, Operator: "alice", DecidedAt: 2 * time.Second,
		Items: []ReviewItem{{JobID: 1, Decision: DecisionNotHappened, Basis: 0}}}
	preview := wb.Preview(sampleTasks(), nil, round)
	journal := ReviewJournal{
		Rounds: []RoundRecord{{Round: round, Preview: preview, Accepted: preview.Applicable}},
		Takeovers: []TakeoverRecord{
			{JobID: 1, RoundID: 1, Operator: "alice", Status: TakeoverCompleted, ReRun: true},
			{JobID: 9, RoundID: 2, Operator: "bob", Status: TakeoverConfirmedOldResult, ReRun: false},
		},
	}
	path := filepath.Join(t.TempDir(), "review.json")
	if err := SaveJournal(path, journal); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Rounds) != 1 || len(loaded.Takeovers) != 2 {
		t.Fatalf("恢复后台账缺失: %+v", loaded)
	}
	reran, onlyConfirmed := 0, 0
	for _, record := range loaded.Takeovers {
		switch {
		case record.ReRun && record.Status == TakeoverCompleted:
			reran++
		case !record.ReRun && record.Status == TakeoverConfirmedOldResult:
			onlyConfirmed++
		}
	}
	if reran != 1 || onlyConfirmed != 1 {
		t.Fatalf("恢复后应能区分确已重新运行与只确认旧结果: %+v", loaded.Takeovers)
	}
}
