package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ReviewDecision 是操作者对单个失联任务的人工核对结论。
type ReviewDecision string

const (
	// DecisionHappened 旧效果确已发生：只确认旧结果，释放替代容量，绝不重放。
	DecisionHappened ReviewDecision = "happened"
	// DecisionNotHappened 旧效果确未发生：安排替代实例接替执行剩余工作。
	DecisionNotHappened ReviewDecision = "not_happened"
	// DecisionInsufficient 材料仍不足：保留未知，继续等待下一轮核对。
	DecisionInsufficient ReviewDecision = "insufficient"
)

// 预览/实际生效动作：释放替代容量、安排接替、保留等待。
const (
	EffectReleaseCapacity  = "release_capacity"
	EffectScheduleTakeover = "schedule_takeover"
	EffectKeepWaiting      = "keep_waiting"
)

// ReviewItem 是一轮核对中对单个失联任务的决定及其依据的状态版本。
type ReviewItem struct {
	JobID    int            `json:"job_id"`
	Decision ReviewDecision `json:"decision"`
	// Basis 是操作者作出判断时所依据的未知任务状态版本；状态推进（如替代实例
	// 由在途启动变为待命就绪）后版本递增，过期依据的决定不再适用。
	Basis int64  `json:"basis"`
	Note  string `json:"note,omitempty"`
}

// ReviewRound 是一名操作者把多个失联任务组成的一轮分批核对。
type ReviewRound struct {
	ID        int           `json:"id"`
	Operator  string        `json:"operator"`
	OpenedAt  time.Duration `json:"opened_at"`
	DecidedAt time.Duration `json:"decided_at"`
	Items     []ReviewItem  `json:"items"`
}

// UnknownTask 是工作台看到的失联任务快照。
type UnknownTask struct {
	JobID            int           `json:"job_id"`
	Class            string        `json:"class"`
	Basis            int64         `json:"basis"`
	FailedAt         time.Duration `json:"failed_at"`
	Remaining        time.Duration `json:"remaining"`
	ReplacementID    int           `json:"replacement_id,omitempty"`
	ReplacementClass string        `json:"replacement_class,omitempty"`
	ReplacementReady bool          `json:"replacement_ready"`
}

// ResolvedOutcome 是某失联任务已被某轮核对确认的最终去向，用于识别过期决定。
type ResolvedOutcome struct {
	JobID    int            `json:"job_id"`
	Decision ReviewDecision `json:"decision"`
	RoundID  int            `json:"round_id"`
	Operator string         `json:"operator"`
	At       time.Duration  `json:"at"`
	// ReRun 为 true 表示任务确已由替代实例重新运行；false 表示只是确认了旧结果。
	ReRun bool   `json:"rerun"`
	Note  string `json:"note,omitempty"`
}

// PreviewEffect 描述某条决定一旦生效将释放/安排/保留什么。
type PreviewEffect struct {
	JobID       int            `json:"job_id"`
	Decision    ReviewDecision `json:"decision"`
	Effect      string         `json:"effect"`
	WorkerID    int            `json:"worker_id,omitempty"`
	WorkerClass string         `json:"worker_class,omitempty"`
	Detail      string         `json:"detail"`
}

// ReviewConflict 明确列出本轮不被接受的过期/冲突决定。
type ReviewConflict struct {
	RoundID      int            `json:"round_id"`
	Operator     string         `json:"operator"`
	JobID        int            `json:"job_id"`
	Decision     ReviewDecision `json:"decision"`
	GivenBasis   int64          `json:"given_basis"`
	CurrentBasis int64          `json:"current_basis,omitempty"`
	Reason       string         `json:"reason"`
}

// RoundPreview 是一轮核对的预览：仍适用的项及其效果，与冲突项分别列出。
type RoundPreview struct {
	RoundID    int              `json:"round_id"`
	Operator   string           `json:"operator"`
	DecidedAt  time.Duration    `json:"decided_at"`
	Applicable []ReviewItem     `json:"applicable"`
	Effects    []PreviewEffect  `json:"effects"`
	Conflicts  []ReviewConflict `json:"conflicts"`
}

// RoundRecord 是一轮核对的持久化记录：决定、预览效果与冲突全部留痕。
type RoundRecord struct {
	Round    ReviewRound  `json:"round"`
	Preview  RoundPreview `json:"preview"`
	Accepted []ReviewItem `json:"accepted"`
}

// TakeoverStatus 描述实际接替的状态。
type TakeoverStatus string

const (
	// TakeoverConfirmedOldResult 只是确认旧效果已发生，任务没有重新运行。
	TakeoverConfirmedOldResult TakeoverStatus = "confirmed_old_result"
	// TakeoverRunning 已确认旧效果不存在，替代实例正在/即将重新运行剩余任务。
	TakeoverRunning TakeoverStatus = "running"
	// TakeoverCompleted 替代实例确已完成接替执行。
	TakeoverCompleted TakeoverStatus = "completed"
)

// TakeoverRecord 与每轮决定分别保存：记录任务是否确已重新运行及其真实时点。
type TakeoverRecord struct {
	JobID       int            `json:"job_id"`
	Class       string         `json:"class"`
	RoundID     int            `json:"round_id"`
	Operator    string         `json:"operator"`
	ConfirmedAt time.Duration  `json:"confirmed_at"`
	StartedAt   time.Duration  `json:"started_at,omitempty"`
	CompletedAt time.Duration  `json:"completed_at,omitempty"`
	Remaining   time.Duration  `json:"remaining"`
	WorkerID    int            `json:"worker_id,omitempty"`
	WorkerClass string         `json:"worker_class,omitempty"`
	ReRun       bool           `json:"rerun"`
	Status      TakeoverStatus `json:"status"`
}

// ReviewJournal 是恢复后可追溯的核对台账：轮次决定与实际接替分别保存。
type ReviewJournal struct {
	Rounds    []RoundRecord     `json:"rounds"`
	Takeovers []TakeoverRecord  `json:"takeovers"`
	Resolved  []ResolvedOutcome `json:"resolved"`
}

// Workbench 是失联任务分批人工核对工作台：预览决定效果、识别冲突、落盘台账。
type Workbench struct{}

// Preview 在不执行任何决定的前提下，预览整轮核对的适用项、效果与冲突。
// 两名操作者依据不同时间的状态作决定时，只有仍适用的项进入 Effects，
// 其余项明确列入 Conflicts，过期决定不会被接受。
func (Workbench) Preview(tasks []UnknownTask, resolved []ResolvedOutcome, round ReviewRound) RoundPreview {
	preview := RoundPreview{RoundID: round.ID, Operator: round.Operator, DecidedAt: round.DecidedAt}
	byJob := map[int]UnknownTask{}
	for _, task := range tasks {
		byJob[task.JobID] = task
	}
	resolvedByJob := map[int]ResolvedOutcome{}
	for _, item := range resolved {
		resolvedByJob[item.JobID] = item
	}
	seen := map[int]bool{}
	for _, item := range round.Items {
		if seen[item.JobID] {
			preview.Conflicts = append(preview.Conflicts, ReviewConflict{
				RoundID: round.ID, Operator: round.Operator, JobID: item.JobID,
				Decision: item.Decision, GivenBasis: item.Basis,
				Reason: "同一轮核对对该任务重复作出决定，只接受第一项",
			})
			continue
		}
		seen[item.JobID] = true
		if prior, ok := resolvedByJob[item.JobID]; ok {
			preview.Conflicts = append(preview.Conflicts, ReviewConflict{
				RoundID: round.ID, Operator: round.Operator, JobID: item.JobID,
				Decision: item.Decision, GivenBasis: item.Basis,
				Reason: fmt.Sprintf("任务已在第 %d 轮由 %s 确认为 %s（重新运行=%t）；过期决定不得重新执行已确认的效果",
					prior.RoundID, prior.Operator, prior.Decision, prior.ReRun),
			})
			continue
		}
		task, ok := byJob[item.JobID]
		if !ok {
			preview.Conflicts = append(preview.Conflicts, ReviewConflict{
				RoundID: round.ID, Operator: round.Operator, JobID: item.JobID,
				Decision: item.Decision, GivenBasis: item.Basis,
				Reason: "任务当前不在失联待核对队列中",
			})
			continue
		}
		if item.Basis != task.Basis {
			preview.Conflicts = append(preview.Conflicts, ReviewConflict{
				RoundID: round.ID, Operator: round.Operator, JobID: item.JobID,
				Decision: item.Decision, GivenBasis: item.Basis, CurrentBasis: task.Basis,
				Reason: fmt.Sprintf("决定依据的状态版本 %d 已过期，当前版本 %d（失联任务状态已推进），只接受仍适用的项",
					item.Basis, task.Basis),
			})
			continue
		}
		preview.Applicable = append(preview.Applicable, item)
		preview.Effects = append(preview.Effects, effectFor(task, item))
	}
	return preview
}

func effectFor(task UnknownTask, item ReviewItem) PreviewEffect {
	switch item.Decision {
	case DecisionHappened:
		detail := "确认旧结果有效：不重新执行任务"
		if task.ReplacementID != 0 {
			detail = fmt.Sprintf("确认旧结果有效：取消接替并释放待命替代实例 %d/%s，不重新执行任务",
				task.ReplacementID, task.ReplacementClass)
		}
		return PreviewEffect{JobID: item.JobID, Decision: item.Decision, Effect: EffectReleaseCapacity,
			WorkerID: task.ReplacementID, WorkerClass: task.ReplacementClass, Detail: detail}
	case DecisionNotHappened:
		if task.ReplacementID != 0 && task.ReplacementReady {
			return PreviewEffect{JobID: item.JobID, Decision: item.Decision, Effect: EffectScheduleTakeover,
				WorkerID: task.ReplacementID, WorkerClass: task.ReplacementClass,
				Detail: fmt.Sprintf("替代实例 %d/%s 已就绪，确认后立即接替执行剩余 %s",
					task.ReplacementID, task.ReplacementClass, task.Remaining)}
		}
		if task.ReplacementID != 0 {
			return PreviewEffect{JobID: item.JobID, Decision: item.Decision, Effect: EffectScheduleTakeover,
				WorkerID: task.ReplacementID, WorkerClass: task.ReplacementClass,
				Detail: fmt.Sprintf("替代实例 %d/%s 仍在途启动，确认后就绪即接替执行剩余 %s",
					task.ReplacementID, task.ReplacementClass, task.Remaining)}
		}
		return PreviewEffect{JobID: item.JobID, Decision: item.Decision, Effect: EffectScheduleTakeover,
			Detail: "需在预声明兼容范围内声明替代容量后接替"}
	default:
		return PreviewEffect{JobID: item.JobID, Decision: item.Decision, Effect: EffectKeepWaiting,
			Detail: "材料仍不足：保持未知，替代实例继续待命，留待后续轮次核对"}
	}
}

// SaveJournal 把每轮决定与实际接替分别保存在同一台账文件中（顶层分字段），
// 恢复时用 LoadJournal 读回，追溯哪些任务确已重新运行、哪些只是确认旧结果。
func SaveJournal(path string, journal ReviewJournal) error {
	journal = cloneJournal(journal)
	data, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, 0o644)
}

// LoadJournal 在恢复后读回核对台账。
func LoadJournal(path string) (ReviewJournal, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ReviewJournal{}, err
	}
	var journal ReviewJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return ReviewJournal{}, err
	}
	return journal, nil
}

func cloneJournal(in ReviewJournal) ReviewJournal {
	sort.SliceStable(in.Rounds, func(i, j int) bool { return in.Rounds[i].Round.ID < in.Rounds[j].Round.ID })
	sort.SliceStable(in.Takeovers, func(i, j int) bool {
		if in.Takeovers[i].ConfirmedAt != in.Takeovers[j].ConfirmedAt {
			return in.Takeovers[i].ConfirmedAt < in.Takeovers[j].ConfirmedAt
		}
		return in.Takeovers[i].JobID < in.Takeovers[j].JobID
	})
	sort.SliceStable(in.Resolved, func(i, j int) bool { return in.Resolved[i].JobID < in.Resolved[j].JobID })
	return in
}
