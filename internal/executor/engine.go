package executor

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/metrics"
)

// Job 携带能力类别：只有同类工作者才能处理它。
type Job struct {
	Arrival time.Duration
	Service time.Duration
	Class   string
	Cost    int
}

// ClassConfig 描述一个工作者类别的容量约束与启动延迟。
type ClassConfig struct {
	Min          int
	Max          int
	QueueCap     int
	StartupDelay time.Duration
	CostFactor   int
}

type Config struct {
	Classes       map[string]ClassConfig
	Compatibility map[string][]string
	Failures      []Failure
	Confirmations []OutcomeConfirmation
	ReviewRounds  []ReviewRound
	Tick          time.Duration
	Cooldown      time.Duration
}

type Failure struct {
	At    time.Duration
	JobID int
}

type OutcomeConfirmation struct {
	At             time.Duration
	JobID          int
	EffectObserved bool
}

type ReviewOutcome string

const (
	ReviewOccurred     ReviewOutcome = "occurred"
	ReviewNotOccurred  ReviewOutcome = "not_occurred"
	ReviewInsufficient ReviewOutcome = "insufficient_material"
)

type ReviewStatus string

const (
	ReviewStatusPending    ReviewStatus = "pending"
	ReviewStatusWaiting    ReviewStatus = "waiting_material"
	ReviewStatusResolved   ReviewStatus = "resolved"
	ReviewStatusConflicted ReviewStatus = "conflicted"
)

type ReviewDecision struct {
	JobID       int           `json:"job_id"`
	Outcome     ReviewOutcome `json:"outcome"`
	BaseVersion int64         `json:"base_version"`
}

type ReviewRound struct {
	ID        string           `json:"id"`
	At        time.Duration    `json:"at"`
	Operator  string           `json:"operator"`
	Decisions []ReviewDecision `json:"decisions"`
}

type ReviewCapacityPlan struct {
	JobID       int    `json:"job_id"`
	WorkerID    int    `json:"worker_id"`
	WorkerClass string `json:"worker_class"`
}

type ReviewTakeoverPlan struct {
	JobID       int           `json:"job_id"`
	WorkerID    int           `json:"worker_id"`
	WorkerClass string        `json:"worker_class"`
	Ready       bool          `json:"ready"`
	StartsAt    time.Duration `json:"starts_at"`
	Remaining   time.Duration `json:"remaining"`
}

type ReviewWaitPlan struct {
	JobID   int    `json:"job_id"`
	Version int64  `json:"version"`
	Reason  string `json:"reason"`
}

type ReviewPreview struct {
	Releases  []ReviewCapacityPlan `json:"releases"`
	Takeovers []ReviewTakeoverPlan `json:"takeovers"`
	Waiting   []ReviewWaitPlan     `json:"waiting"`
}

type ReviewDecisionRecord struct {
	ReviewDecision
	Accepted       bool         `json:"accepted"`
	CurrentVersion int64        `json:"current_version"`
	Status         ReviewStatus `json:"status"`
	ConflictReason string       `json:"conflict_reason,omitempty"`
}

type ReviewRoundRecord struct {
	ReviewRound
	Version   int64                  `json:"version"`
	Decisions []ReviewDecisionRecord `json:"decisions"`
	Preview   ReviewPreview          `json:"preview"`
}

type ReviewResolution struct {
	At       time.Duration `json:"at"`
	RoundID  string        `json:"round_id"`
	Operator string        `json:"operator"`
	JobID    int           `json:"job_id"`
	Outcome  ReviewOutcome `json:"outcome"`
	Replayed bool          `json:"replayed"`
}

type TakeoverRecord struct {
	At          time.Duration `json:"at"`
	RoundID     string        `json:"round_id"`
	JobID       int           `json:"job_id"`
	WorkerID    int           `json:"worker_id"`
	WorkerClass string        `json:"worker_class"`
	Remaining   time.Duration `json:"remaining"`
}

type workerState int

const (
	stateStarting workerState = iota
	stateIdle
	stateBusy
	stateDraining
	stateReserved
	stateLost
)

type worker struct {
	id      int
	gen     int
	class   string
	state   workerState
	readyAt time.Duration
	jobID   int
	job     Job
	started time.Duration
	handoff bool
	// standby 表示该实例是为某个失联任务预声明的替代实例：
	// 就绪后只保留待命，未收到人工确认前绝不开始执行未知任务。
	standby      bool
	standbyReady bool
	// runOnReady 表示人工已确认旧效果不存在：替代实例一就绪就执行剩余任务。
	runOnReady bool
	roundID    string
	operator   string
}

type queuedJob struct {
	id  int
	job Job
}

type unknownJob struct {
	id          int
	job         Job
	oldWorker   int
	oldGen      int
	oldClass    string
	failedAt    time.Duration
	startedAt   time.Duration
	remaining   time.Duration
	replacement *worker
}

type AuditEvent struct {
	At          time.Duration
	Kind        string
	RoundID     string `json:"RoundID,omitempty"`
	JobID       int
	JobClass    string
	WorkerID    int
	WorkerClass string
	Reason      string
}

const (
	AuditEnqueue        = "enqueue"
	AuditReject         = "reject"
	AuditAssign         = "assign"
	AuditComplete       = "complete"
	AuditRelease        = "release"
	AuditLoseWorker     = "lose_worker"
	AuditMarkUnknown    = "mark_unknown"
	AuditReserveStandby = "reserve_standby"
	AuditConfirmEffect  = "confirm_effect"
	AuditTakeover       = "takeover"
	AuditReviewRound    = "review_round"
	AuditReviewConflict = "review_conflict"
	AuditKeepWaiting    = "keep_waiting"
)

type eventKind int

const (
	evArrival eventKind = iota
	evWorkerReady
	evComplete
	evTick
	evLoseWorker
	evConfirm
	evReviewRound
)

type event struct {
	at    time.Duration
	seq   int
	kind  eventKind
	ref   int
	gen   int
	jobID int
	round int
	job   Job
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	return h[i].at < h[j].at || h[i].at == h[j].at && h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]
	return v
}

// classRuntime 是单个类别的运行态：独立队列、独立账目、独立扩缩容状态。
type classRuntime struct {
	cfg         ClassConfig
	queue       []queuedJob
	desired     int
	inflight    int
	lastScaleAt time.Duration
	ewmaService time.Duration
	haveService bool
	counters    metrics.Counters
}

// Result 是一次重放的完整产出：决策依据、分类别账目与总账。
type Result struct {
	Logs         []decision.Log
	Events       []AuditEvent
	Unknown      []UnknownJob
	ReviewRounds []ReviewRoundRecord
	Resolutions  []ReviewResolution
	Takeovers    []TakeoverRecord
	PerClass     map[string]metrics.Counters
	Total        metrics.Counters
}

type UnknownJob struct {
	JobID     int
	Class     string
	WorkerID  int
	At        time.Duration
	Remaining time.Duration
}

type Engine struct {
	cfg      Config
	strategy decision.Strategy

	classes        map[string]*classRuntime
	order          []string
	workers        []*worker
	logs           []decision.Log
	audits         []AuditEvent
	unknown        []*unknownJob
	nextJob        int
	reviewVersions map[int]int64
	reviewStatus   map[int]ReviewStatus
	reviewRounds   []ReviewRoundRecord
	resolutions    []ReviewResolution
	takeovers      []TakeoverRecord
	roundVersion   int64

	events eventHeap
	seq    int
}

func New(cfg Config, strategy decision.Strategy) *Engine {
	if len(cfg.Classes) == 0 {
		panic("executor: at least one worker class is required")
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 3 * cfg.Tick
	}
	e := &Engine{
		cfg:            cfg,
		strategy:       strategy,
		classes:        map[string]*classRuntime{},
		reviewVersions: map[int]int64{},
		reviewStatus:   map[int]ReviewStatus{},
	}
	for name, cc := range cfg.Classes {
		if cc.Min < 1 {
			cc.Min = 1
		}
		if cc.Max < cc.Min {
			cc.Max = cc.Min
		}
		if cc.StartupDelay <= 0 {
			cc.StartupDelay = 2 * time.Second
		}
		e.classes[name] = &classRuntime{cfg: cc, desired: cc.Min}
		e.order = append(e.order, name)
	}
	sort.Strings(e.order)
	if cfg.Compatibility == nil {
		cfg.Compatibility = map[string][]string{}
	}
	for jobClass, candidates := range cfg.Compatibility {
		seen := map[string]bool{jobClass: true}
		allowed := []string{jobClass}
		for _, candidate := range candidates {
			if _, ok := e.classes[candidate]; !ok || seen[candidate] {
				continue
			}
			seen[candidate] = true
			allowed = append(allowed, candidate)
		}
		cfg.Compatibility[jobClass] = allowed
	}
	for _, name := range e.order {
		c := e.classes[name]
		for i := 0; i < c.cfg.Min; i++ {
			id, gen := e.nextWorkerID(name)
			w := &worker{id: id, gen: gen, class: name, state: stateStarting, readyAt: c.cfg.StartupDelay}
			e.workers = append(e.workers, w)
			e.scheduleReady(id, gen, c.cfg.StartupDelay)
		}
	}
	return e
}

func (e *Engine) schedule(ev event) {
	e.seq++
	ev.seq = e.seq
	heap.Push(&e.events, ev)
}

func (e *Engine) scheduleReady(id, gen int, at time.Duration) {
	e.schedule(event{at: at, kind: evWorkerReady, ref: id, gen: gen})
}

func (e *Engine) Run(jobs []Job) (Result, error) {
	for i, job := range jobs {
		if _, ok := e.classes[job.Class]; !ok {
			return Result{}, fmt.Errorf("任务类别 %q 没有对应的工作者类别", job.Class)
		}
		e.schedule(event{at: job.Arrival, kind: evArrival, jobID: i + 1, job: job})
	}
	for _, failure := range e.cfg.Failures {
		if failure.JobID <= 0 || failure.JobID > len(jobs) {
			return Result{}, fmt.Errorf("失联任务 %d 不存在", failure.JobID)
		}
		e.schedule(event{at: failure.At, kind: evLoseWorker, jobID: failure.JobID})
	}
	for _, confirmation := range e.cfg.Confirmations {
		gen := 0
		if confirmation.EffectObserved {
			gen = 1
		}
		e.schedule(event{at: confirmation.At, kind: evConfirm, jobID: confirmation.JobID, gen: gen})
	}
	for i, round := range e.cfg.ReviewRounds {
		e.schedule(event{at: round.At, kind: evReviewRound, round: i})
	}
	e.schedule(event{at: e.cfg.Tick, kind: evTick})

	endAt := time.Duration(0)
	for _, job := range jobs {
		if deadline := job.Arrival + job.Service; deadline > endAt {
			endAt = deadline
		}
	}
	for _, name := range e.order {
		endAt += e.classes[name].cfg.StartupDelay
	}
	endAt += e.cfg.Cooldown + time.Second

	for e.events.Len() > 0 {
		ev := heap.Pop(&e.events).(event)
		if ev.kind == evArrival && ev.at > endAt {
			continue
		}
		switch ev.kind {
		case evArrival:
			e.handleArrival(ev.at, ev.job)
		case evWorkerReady:
			e.handleReady(ev.at, ev.ref, ev.gen)
		case evComplete:
			e.handleComplete(ev.at, ev.ref, ev.gen)
		case evTick:
			if ev.at <= endAt {
				e.handleTick(ev.at)
				e.schedule(event{at: ev.at + e.cfg.Tick, kind: evTick})
			}
		case evLoseWorker:
			e.handleLoseWorker(ev.at, ev.jobID)
		case evConfirm:
			e.handleConfirmation(ev.at, ev.jobID, ev.gen == 1)
		case evReviewRound:
			if ev.round >= 0 && ev.round < len(e.cfg.ReviewRounds) {
				e.SubmitReviewRound(ev.at, e.cfg.ReviewRounds[ev.round])
			}
		}
	}

	result := Result{
		Logs: e.logs, Events: e.audits,
		ReviewRounds: e.reviewRounds, Resolutions: e.resolutions, Takeovers: e.takeovers,
		PerClass: map[string]metrics.Counters{},
	}
	if result.ReviewRounds == nil {
		result.ReviewRounds = []ReviewRoundRecord{}
	}
	if result.Resolutions == nil {
		result.Resolutions = []ReviewResolution{}
	}
	if result.Takeovers == nil {
		result.Takeovers = []TakeoverRecord{}
	}
	for _, name := range e.order {
		result.PerClass[name] = e.classes[name].counters
		result.Total.Add(e.classes[name].counters)
	}
	for _, item := range e.unknown {
		result.Unknown = append(result.Unknown, UnknownJob{
			JobID: item.id, Class: item.job.Class, WorkerID: item.oldWorker,
			At: item.failedAt, Remaining: item.remaining,
		})
	}
	if err := e.checkAccounting(); err != nil {
		return result, err
	}
	return result, nil
}

func (e *Engine) handleArrival(now time.Duration, job Job) {
	c := e.classes[job.Class]
	c.counters.RecordArrival()
	e.nextJob++
	jobID := e.nextJob
	if !c.haveService {
		c.ewmaService = job.Service
		c.haveService = true
	} else {
		c.ewmaService = (c.ewmaService + job.Service) / 2
	}

	// 准入规则（按类别）：无同类空闲者，且已接纳请求达到
	// “忙碌容量 + 该类队列 + 启动中未来容量”时明确拒绝。
	committed := e.countStates(job.Class, stateBusy, stateDraining)
	availableCapacity := committed + c.cfg.QueueCap + e.countStates(job.Class, stateStarting)
	if e.countStates(job.Class, stateIdle) == 0 && c.inflight >= availableCapacity {
		c.counters.RecordRejection()
		e.audit(now, AuditReject, jobID, job, nil, "同类容量和有界队列均已占满；兼容范围只用于显式接替，不放宽普通亲和性")
	} else {
		c.queue = append(c.queue, queuedJob{id: jobID, job: job})
		c.inflight++
		e.audit(now, AuditEnqueue, jobID, job, nil, "任务尚未开始，保留在同类有界队列，可重新安排")
		e.dispatch(now, job.Class)
	}
}

func (e *Engine) idleWorker(class string) *worker {
	for _, w := range e.workers {
		if w.class == class && w.state == stateIdle {
			return w
		}
	}
	return nil
}

// dispatch 只把任务交给同类别的空闲工作者：适配约束是硬约束。
func (e *Engine) dispatch(now time.Duration, class string) {
	c := e.classes[class]
	for len(c.queue) > 0 {
		w := e.idleWorker(class)
		if w == nil {
			return
		}
		entry := c.queue[0]
		c.queue = c.queue[1:]
		w.state = stateBusy
		w.jobID = entry.id
		w.job = entry.job
		w.started = now
		e.schedule(event{at: now + entry.job.Service, kind: evComplete, ref: w.id, gen: w.gen, jobID: entry.id, job: entry.job})
		e.audit(now, AuditAssign, entry.id, entry.job, w, "同类空闲实例领取未开始任务")
	}
}

func (e *Engine) handleReady(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	// 迟到的就绪事件：实例已被释放或状态已推进，忽略，不访问陈旧状态。
	if w == nil || w.state != stateStarting && w.state != stateReserved {
		return
	}
	w.readyAt = now
	if w.state == stateReserved {
		if !w.runOnReady {
			// 替代实例就绪只是接替容量就绪：旧实例是否已停止、旧效果是否发生
			// 都还没有人工确认，因此只记录就绪并继续待命，绝不重投未知任务。
			w.standbyReady = true
			e.audit(now, AuditReserveStandby, w.jobID, w.job, w, "替代实例启动完成进入待命；就绪不等于旧实例停止，继续等待人工确认")
			return
		}
		// 人工已确认旧效果不存在：就绪后立即执行剩余工作。
		w.state = stateBusy
		w.standby = false
		w.runOnReady = false
		e.schedule(event{at: now + w.job.Service, kind: evComplete, ref: w.id, gen: w.gen, jobID: w.jobID, job: w.job})
		e.recordTakeover(now, w)
		e.auditRound(now, AuditTakeover, w.roundID, w.jobID, w.job, w, "人工确认旧效果不存在；替代实例就绪后执行剩余任务")
		for i := range e.resolutions {
			if e.resolutions[i].RoundID == w.roundID && e.resolutions[i].JobID == w.jobID {
				e.resolutions[i].Replayed = true
			}
		}
		return
	}
	w.state = stateIdle
	e.dispatch(now, w.class)
}

func (e *Engine) handleComplete(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	// 迟到的完成事件：实例可能已在失联/确认时释放，或事件属于上一代实例，
	// 一律忽略，避免重复记账或访问已释放状态。
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		return
	}
	jobClass := w.job.Class
	c := e.classes[jobClass]
	wait := w.started - w.job.Arrival
	if wait < 0 {
		wait = 0
	}
	c.counters.RecordCompletion(wait)
	c.inflight--
	completedJob := w.job
	completedID := w.jobID
	completionReason := "任务由原执行实例完成并释放容量"
	if w.handoff {
		completionReason = "接替实例完成剩余任务，旧效果已由人工确认不存在"
	}
	e.audit(now, AuditComplete, w.jobID, w.job, w, completionReason)
	w.jobID = 0
	w.started = 0
	if w.state == stateDraining {
		e.audit(now, AuditRelease, completedID, completedJob, w, "忙碌实例完成缩容前手头工作并退出")
		e.removeWorker(w)
		e.dispatch(now, jobClass)
		e.dispatch(now, w.class)
		return
	}
	if w.handoff {
		if w.class != jobClass {
			e.audit(now, AuditRelease, completedID, completedJob, w, "跨类接替任务完成，释放专用替代实例")
			e.removeWorker(w)
			e.dispatch(now, jobClass)
			return
		}
		w.handoff = false
	}
	w.state = stateIdle
	w.job = Job{}
	e.dispatch(now, jobClass)
	e.dispatch(now, w.class)
}

func (e *Engine) handleTick(now time.Duration) {
	for _, name := range e.order {
		e.tickClass(now, name)
	}
}

func (e *Engine) handleLoseWorker(now time.Duration, jobID int) {
	w := e.workerByJob(jobID)
	// 只有真正在执行该任务的实例才算失联；待命替代实例或已处理的重复/迟到事件不算。
	if w == nil || w.state != stateBusy && w.state != stateDraining || w.standby {
		return
	}
	oldClass := w.class
	job := w.job
	remaining := w.started + job.Service - now
	if remaining < 0 {
		remaining = 0
	}
	item := &unknownJob{
		id: jobID, job: job, oldWorker: w.id, oldGen: w.gen, oldClass: oldClass,
		failedAt: now, startedAt: w.started, remaining: remaining,
	}
	e.unknown = append(e.unknown, item)
	e.reviewVersions[jobID] = 0
	e.reviewStatus[jobID] = ReviewStatusPending
	e.classes[oldClass].counters.RecordUnknown()
	e.audit(now, AuditLoseWorker, jobID, job, w, "工作者失联：旧实例不再接收队列，但失联不等于已停止")
	e.audit(now, AuditMarkUnknown, jobID, job, w, "任务可能已经产生外部效果；在人工确认前不自动重放、不计完成")
	w.state = stateLost
	w.readyAt = now

	c := e.classes[oldClass]
	c.inflight--

	replacement := e.chooseReplacement(now, job, oldClass)
	item.replacement = replacement
	if replacement == nil {
		e.audit(now, AuditReject, jobID, job, nil, "显式兼容范围内没有可声明的接替容量；旧效果保持未知")
		return
	}
	if replacement.state != stateIdle && replacement.state != stateStarting {
		item.replacement = nil
		return
	}
	replacement.state = stateReserved
	replacement.jobID = jobID
	replacement.job = job
	replacement.handoff = true
	replacement.standby = true
	replacement.standbyReady = replacement.readyAt <= now
	if replacement.readyAt <= now {
		e.audit(now, AuditReserveStandby, jobID, job, replacement, "替代实例就绪但不自动认定旧实例停止；等待人工确认旧效果")
	} else {
		e.audit(now, AuditReserveStandby, jobID, job, replacement, "替代实例仍在途启动；启动完成后只进入待命，仍需等待人工确认")
	}
}

func (e *Engine) handleConfirmation(now time.Duration, jobID int, observed bool) {
	outcome := ReviewNotOccurred
	if observed {
		outcome = ReviewOccurred
	}
	e.resolveUnknown(now, jobID, outcome, "legacy-confirmation", "system")
}

func (e *Engine) SubmitReviewRound(now time.Duration, round ReviewRound) ReviewRoundRecord {
	e.roundVersion++
	record := ReviewRoundRecord{
		ReviewRound: round,
		Version:     e.roundVersion,
	}
	if round.ID == "" {
		round.ID = fmt.Sprintf("round-%d", len(e.reviewRounds)+1)
		record.ReviewRound.ID = round.ID
	}
	touched := map[int]bool{}
	for _, decision := range round.Decisions {
		item := e.unknownByJob(decision.JobID)
		entry := ReviewDecisionRecord{
			ReviewDecision: decision,
			Status:         ReviewStatusConflicted,
		}
		if item == nil {
			entry.ConflictReason = "任务已不在未知队列：终局决定已经生效，过期决定不能重复执行"
			entry.Status = e.reviewStatus[decision.JobID]
			if entry.Status == "" {
				entry.Status = ReviewStatusConflicted
			}
			e.auditRound(now, AuditReviewConflict, round.ID, decision.JobID, Job{}, nil, entry.ConflictReason)
			record.Decisions = append(record.Decisions, entry)
			continue
		}
		entry.CurrentVersion = e.reviewVersions[decision.JobID]
		currentVersion := entry.CurrentVersion
		if touched[decision.JobID] {
			currentVersion++
		}
		entry.CurrentVersion = currentVersion
		if decision.BaseVersion != currentVersion {
			entry.ConflictReason = fmt.Sprintf("决定基于版本 %d，当前版本为 %d", decision.BaseVersion, entry.CurrentVersion)
			e.auditRound(now, AuditReviewConflict, round.ID, decision.JobID, item.job, item.replacement, entry.ConflictReason)
			record.Decisions = append(record.Decisions, entry)
			continue
		}
		switch decision.Outcome {
		case ReviewOccurred, ReviewNotOccurred, ReviewInsufficient:
		default:
			entry.ConflictReason = fmt.Sprintf("未知核对结果 %q", decision.Outcome)
			e.auditRound(now, AuditReviewConflict, round.ID, decision.JobID, item.job, item.replacement, entry.ConflictReason)
			record.Decisions = append(record.Decisions, entry)
			continue
		}

		entry.Accepted = true
		entry.Status = ReviewStatusWaiting
		willReplay := decision.Outcome == ReviewNotOccurred && item.replacement != nil
		preview := e.previewDecision(now, decision, item, currentVersion+1)
		record.Preview.Releases = append(record.Preview.Releases, preview.Releases...)
		record.Preview.Takeovers = append(record.Preview.Takeovers, preview.Takeovers...)
		record.Preview.Waiting = append(record.Preview.Waiting, preview.Waiting...)

		e.reviewVersions[decision.JobID] = currentVersion + 1
		touched[decision.JobID] = true
		if decision.Outcome == ReviewInsufficient {
			e.reviewStatus[decision.JobID] = ReviewStatusWaiting
			e.auditRound(now, AuditKeepWaiting, round.ID, decision.JobID, item.job, item.replacement, "材料仍不足：保留未知状态与待命容量，等待补充材料")
		} else {
			e.reviewStatus[decision.JobID] = ReviewStatusResolved
			resolved := e.resolveUnknown(now, decision.JobID, decision.Outcome, round.ID, round.Operator)
			if !resolved {
				entry.Accepted = false
				entry.Status = ReviewStatusConflicted
				entry.ConflictReason = "当前运行态已无法应用该决定"
			} else {
				entry.Status = ReviewStatusResolved
			}
		}
		if entry.Accepted {
			e.resolutions = append(e.resolutions, ReviewResolution{
				At: now, RoundID: round.ID, Operator: round.Operator,
				JobID: decision.JobID, Outcome: decision.Outcome,
				Replayed: willReplay,
			})
		}
		record.Decisions = append(record.Decisions, entry)
	}
	e.reviewRounds = append(e.reviewRounds, record)
	e.auditRound(now, AuditReviewRound, round.ID, 0, Job{}, nil, fmt.Sprintf("保存人工核对轮次：接受 %d 项，冲突 %d 项", countAccepted(record.Decisions), countConflicts(record.Decisions)))
	return record
}

func (e *Engine) PreviewReviewRound(now time.Duration, round ReviewRound) ReviewRoundRecord {
	record := ReviewRoundRecord{
		ReviewRound: round,
		Version:     e.roundVersion + 1,
	}
	if record.ReviewRound.ID == "" {
		record.ReviewRound.ID = fmt.Sprintf("round-%d", len(e.reviewRounds)+1)
	}
	versions := map[int]int64{}
	for _, decision := range round.Decisions {
		item := e.unknownByJob(decision.JobID)
		entry := ReviewDecisionRecord{ReviewDecision: decision, Status: ReviewStatusConflicted}
		if item == nil {
			entry.ConflictReason = "任务已不在未知队列：终局决定已经生效，过期决定不能重复执行"
			entry.Status = e.reviewStatus[decision.JobID]
			if entry.Status == "" {
				entry.Status = ReviewStatusConflicted
			}
			record.Decisions = append(record.Decisions, entry)
			continue
		}
		entry.CurrentVersion = e.reviewVersions[decision.JobID] + versions[decision.JobID]
		if decision.BaseVersion != entry.CurrentVersion {
			entry.ConflictReason = fmt.Sprintf("决定基于版本 %d，当前版本为 %d", decision.BaseVersion, entry.CurrentVersion)
			record.Decisions = append(record.Decisions, entry)
			continue
		}
		switch decision.Outcome {
		case ReviewOccurred, ReviewNotOccurred, ReviewInsufficient:
		default:
			entry.ConflictReason = fmt.Sprintf("未知核对结果 %q", decision.Outcome)
			record.Decisions = append(record.Decisions, entry)
			continue
		}
		entry.Accepted = true
		entry.Status = ReviewStatusWaiting
		preview := e.previewDecision(now, decision, item, entry.CurrentVersion+1)
		record.Preview.Releases = append(record.Preview.Releases, preview.Releases...)
		record.Preview.Takeovers = append(record.Preview.Takeovers, preview.Takeovers...)
		record.Preview.Waiting = append(record.Preview.Waiting, preview.Waiting...)
		versions[decision.JobID]++
		record.Decisions = append(record.Decisions, entry)
	}
	return record
}

func (e *Engine) previewDecision(now time.Duration, decision ReviewDecision, item *unknownJob, version int64) ReviewPreview {
	var preview ReviewPreview
	switch decision.Outcome {
	case ReviewOccurred:
		if item.replacement != nil {
			preview.Releases = append(preview.Releases, ReviewCapacityPlan{
				JobID: item.id, WorkerID: item.replacement.id, WorkerClass: item.replacement.class,
			})
		}
	case ReviewNotOccurred:
		if item.replacement != nil {
			ready := item.replacement.readyAt <= now
			startsAt := item.replacement.readyAt
			if ready {
				startsAt = now
			}
			preview.Takeovers = append(preview.Takeovers, ReviewTakeoverPlan{
				JobID: item.id, WorkerID: item.replacement.id, WorkerClass: item.replacement.class,
				Ready: ready, StartsAt: startsAt, Remaining: item.remaining,
			})
		}
	case ReviewInsufficient:
		preview.Waiting = append(preview.Waiting, ReviewWaitPlan{
			JobID: item.id, Version: version, Reason: "材料仍不足，继续保留未知任务和待命容量",
		})
	}
	return preview
}

func countAccepted(entries []ReviewDecisionRecord) int {
	total := 0
	for _, entry := range entries {
		if entry.Accepted {
			total++
		}
	}
	return total
}

func countConflicts(entries []ReviewDecisionRecord) int {
	total := 0
	for _, entry := range entries {
		if !entry.Accepted {
			total++
		}
	}
	return total
}

func (e *Engine) resolveUnknown(now time.Duration, jobID int, outcome ReviewOutcome, roundID, operator string) bool {
	item := e.unknownByJob(jobID)
	if item == nil {
		return false
	}
	c := e.classes[item.oldClass]
	replacement := item.replacement
	if outcome == ReviewOccurred {
		// 人工确认旧效果已经产生：只把未知确认为完成结果，绝不执行剩余工作。
		wait := item.startedAt - item.job.Arrival
		if wait < 0 {
			wait = 0
		}
		c.counters.ResolveUnknownAsCompleted()
		if wait > 0 {
			c.counters.Waited++
			c.counters.TotalWait += wait
			if wait > c.counters.MaxWait {
				c.counters.MaxWait = wait
			}
		}
		if replacement != nil {
			replacement.handoff = false
			replacement.standby = false
			replacement.standbyReady = false
			replacement.runOnReady = false
			replacement.jobID = 0
			replacement.job = Job{}
			replacement.started = 0
			if replacement.state == stateReserved && replacement.readyAt <= now {
				replacement.state = stateIdle
			} else if replacement.state == stateReserved {
				replacement.state = stateStarting
			}
			replacement.roundID = roundID
			replacement.operator = operator
			e.auditRound(now, AuditRelease, roundID, jobID, item.job, replacement, "操作者确认旧任务已生效；取消接替执行并释放替代实例")
		}
		e.auditRound(now, AuditConfirmEffect, roundID, jobID, item.job, nil, "操作者确认旧效果已产生；只保留结果，不重复执行")
	} else {
		e.auditRound(now, AuditConfirmEffect, roundID, jobID, item.job, nil, "操作者确认旧效果不存在；允许接替执行剩余任务")
		if replacement == nil {
			replacement = e.chooseReplacement(now, item.job, item.oldClass)
			item.replacement = replacement
		}
		if replacement == nil {
			c.counters.Unknown--
			c.counters.Rejected++
			e.auditRound(now, AuditReject, roundID, jobID, item.job, nil, "确认需要接替但无预声明兼容容量")
		} else {
			c.counters.Unknown--
			c.inflight++
			remaining := item.remaining
			runJob := item.job
			runJob.Service = remaining
			replacement.jobID = jobID
			replacement.job = runJob
			replacement.started = now
			replacement.roundID = roundID
			replacement.operator = operator
			replacement.standbyReady = false
			ready := replacement.readyAt <= now
			if ready {
				replacement.state = stateBusy
				replacement.standby = false
				replacement.runOnReady = false
				e.schedule(event{at: now + remaining, kind: evComplete, ref: replacement.id, gen: replacement.gen, jobID: jobID, job: runJob})
				e.recordTakeover(now, replacement)
				e.auditRound(now, AuditTakeover, roundID, jobID, item.job, replacement, "人工确认旧效果不存在；替代实例已就绪，执行剩余任务")
			} else {
				// 替代实例仍在启动：继续待命，就绪后由 handleReady 接替执行。
				replacement.state = stateReserved
				replacement.standby = true
				replacement.runOnReady = true
				e.auditRound(now, AuditReserveStandby, roundID, jobID, item.job, replacement, "人工确认旧效果不存在；替代实例仍在途，就绪后执行剩余任务")
			}
		}
	}
	old := e.workerByID(item.oldWorker, item.oldGen)
	e.removeUnknown(item)
	e.removeWorker(old)
	e.dispatch(now, item.oldClass)
	return true
}

func (e *Engine) chooseReplacement(now time.Duration, job Job, oldClass string) *worker {
	candidates := e.cfg.Compatibility[job.Class]
	if len(candidates) == 0 {
		candidates = []string{job.Class}
	}
	var best *worker
	bestScore := 0
	for _, class := range candidates {
		cfg, ok := e.cfg.Classes[class]
		if !ok || e.activeClassWorkerCount(class) >= cfg.Max {
			continue
		}
		cost := job.Cost
		if cost == 0 {
			cost = 1
		}
		if cfg.CostFactor > 0 {
			cost *= cfg.CostFactor
		}
		classBest := e.bestIdleOrStarting(class)
		if classBest != nil {
			inflight := 0
			if classBest.state == stateStarting {
				inflight = int((classBest.readyAt - now) / time.Millisecond)
			}
			score := -cost - inflight
			if best == nil || score > bestScore {
				best, bestScore = classBest, score
			}
			continue
		}
		id, gen := e.nextWorkerID(class)
		w := &worker{id: id, gen: gen, class: class, state: stateStarting, readyAt: now + cfg.StartupDelay}
		e.workers = append(e.workers, w)
		e.scheduleReady(id, gen, now+cfg.StartupDelay)
		score := -cost - int(cfg.StartupDelay/time.Millisecond)
		if best == nil || score > bestScore {
			best, bestScore = w, score
		}
	}
	return best
}

func (e *Engine) tickClass(now time.Duration, name string) {
	c := e.classes[name]
	arrivals, completions, rejections := c.counters.RotateWindow()
	activeReady := e.countStates(name, stateIdle, stateBusy)
	snap := metrics.Snapshot{
		At:                  now,
		Class:               name,
		Min:                 c.cfg.Min,
		Max:                 c.cfg.Max,
		Ready:               activeReady,
		Starting:            e.countStates(name, stateStarting),
		Busy:                e.countStates(name, stateBusy),
		Draining:            e.countStates(name, stateDraining),
		Unknown:             e.countStates(name, stateLost),
		QueueLength:         len(c.queue),
		QueueCap:            c.cfg.QueueCap,
		ArrivalsInWindow:    arrivals,
		CompletionsInWindow: completions,
		RejectionsInWindow:  rejections,
		ArrivalRate:         float64(arrivals) / float64(e.cfg.Tick),
		MeanServiceTime:     c.ewmaService,
	}

	target, action, reason := e.strategy.Decide(snap)
	current := activeReady + snap.Starting
	effectiveCapacity := current + snap.Draining
	if action == decision.ActionScaleDown && snap.Draining > 0 && effectiveCapacity > target {
		action = decision.ActionHold
		target = c.desired
		reason = fmt.Sprintf("类别 %s 已有 %d 名工作者正在优雅退出，当前生效容量 %d，等待其完成在途工作后再决策",
			name, snap.Draining, effectiveCapacity)
	}
	if action == decision.ActionScaleDown && target >= current {
		action = decision.ActionHold
		target = c.desired
		reason = fmt.Sprintf("类别 %s 策略建议缩容到 %d，但可调度工作者已为 %d；无需重复调整，等待在途工作自然结束",
			name, target, current)
	}
	if target < c.cfg.Min {
		target = c.cfg.Min
	}
	if target > c.cfg.Max {
		target = c.cfg.Max
	}
	if action != decision.ActionHold && now < c.lastScaleAt+e.cfg.Cooldown {
		action = decision.ActionHold
		reason = fmt.Sprintf("类别 %s 距上次容量调整 %s 不足冷却期 %s：本轮保持 %d，防止刚扩又缩",
			name, now-c.lastScaleAt, e.cfg.Cooldown, current)
		target = c.desired
	}

	e.logs = append(e.logs, decision.Log{
		At: now, Class: name, Action: action, From: current, To: c.desired, Reason: reason,
	})

	if action == decision.ActionHold || target == c.desired {
		return
	}

	c.lastScaleAt = now
	e.logs[len(e.logs)-1].To = target
	if target > c.desired {
		for e.activeClassWorkerCount(name) < target {
			id, gen := e.nextWorkerID(name)
			e.workers = append(e.workers, &worker{id: id, gen: gen, class: name, state: stateStarting})
			w := e.workers[len(e.workers)-1]
			w.readyAt = now + c.cfg.StartupDelay
			e.scheduleReady(id, gen, now+c.cfg.StartupDelay)
		}
	} else {
		e.reconcileDown(name, target)
	}
	c.desired = target
}

// reconcileDown 先停空闲实例；忙碌实例标记 draining，
// 只完成手头在途工作，不再领取队列任务，绝不直接消失。
func (e *Engine) reconcileDown(class string, target int) {
	excess := e.activeClassWorkerCount(class) - target
	if excess <= 0 {
		return
	}
	var toRemove []*worker
	for _, w := range e.workers {
		if excess == 0 {
			break
		}
		if w.class == class && w.state == stateIdle {
			toRemove = append(toRemove, w)
			excess--
		}
	}
	for _, w := range toRemove {
		e.removeWorker(w)
	}
	for _, w := range e.workers {
		if excess == 0 {
			return
		}
		if w.class == class && w.state == stateBusy && !w.handoff {
			w.state = stateDraining
			excess--
		}
	}
}

func (e *Engine) workerByID(id, gen int) *worker {
	for _, w := range e.workers {
		if w.id == id && w.gen == gen {
			return w
		}
	}
	return nil
}

func (e *Engine) removeWorker(target *worker) {
	for i, w := range e.workers {
		if w == target {
			e.workers = append(e.workers[:i], e.workers[i+1:]...)
			return
		}
	}
}

func (e *Engine) countStates(class string, states ...workerState) int {
	total := 0
outer:
	for _, w := range e.workers {
		if w.class != class {
			continue
		}
		for _, state := range states {
			if w.state == state {
				total++
				continue outer
			}
		}
	}
	return total
}

func (e *Engine) classWorkerCount(class string) int {
	total := 0
	for _, w := range e.workers {
		if w.class == class {
			total++
		}
	}
	return total
}

func (e *Engine) activeClassWorkerCount(class string) int {
	total := 0
	for _, w := range e.workers {
		if w.class == class && w.state != stateReserved && w.state != stateLost {
			total++
		}
	}
	return total
}

func (e *Engine) nextWorkerID(class string) (int, int) {
	maxID := 0
	byID := map[int]int{}
	for _, w := range e.workers {
		if w.id > maxID {
			maxID = w.id
		}
		byID[w.id] = w.gen
	}
	id := maxID + 1
	return id, byID[id] + 1
}

func (e *Engine) workerByJob(jobID int) *worker {
	for _, w := range e.workers {
		if w.jobID == jobID {
			return w
		}
	}
	return nil
}

func (e *Engine) unknownByJob(jobID int) *unknownJob {
	for _, item := range e.unknown {
		if item.id == jobID {
			return item
		}
	}
	return nil
}

func (e *Engine) removeUnknown(target *unknownJob) {
	for i, item := range e.unknown {
		if item == target {
			e.unknown = append(e.unknown[:i], e.unknown[i+1:]...)
			return
		}
	}
}

func (e *Engine) bestIdleOrStarting(class string) *worker {
	for _, w := range e.workers {
		if w.class == class && w.state == stateIdle {
			return w
		}
	}
	for _, w := range e.workers {
		if w.class == class && w.state == stateStarting {
			return w
		}
	}
	return nil
}

func (e *Engine) audit(now time.Duration, kind string, jobID int, job Job, w *worker, reason string) {
	e.auditRound(now, kind, "", jobID, job, w, reason)
}

func (e *Engine) auditRound(now time.Duration, kind, roundID string, jobID int, job Job, w *worker, reason string) {
	event := AuditEvent{At: now, Kind: kind, JobID: jobID, JobClass: job.Class, Reason: reason}
	event.RoundID = roundID
	if w != nil {
		event.WorkerID = w.id
		event.WorkerClass = w.class
	}
	e.audits = append(e.audits, event)
}

func (e *Engine) recordTakeover(now time.Duration, w *worker) {
	if w == nil {
		return
	}
	for _, record := range e.takeovers {
		if record.RoundID == w.roundID && record.JobID == w.jobID {
			return
		}
	}
	e.takeovers = append(e.takeovers, TakeoverRecord{
		At: now, RoundID: w.roundID, JobID: w.jobID,
		WorkerID: w.id, WorkerClass: w.class, Remaining: w.job.Service,
	})
}

func (e *Engine) checkAccounting() error {
	for _, name := range e.order {
		c := e.classes[name]
		if pending := e.countStates(name, stateStarting, stateBusy, stateDraining); pending != 0 || len(c.queue) != 0 {
			return fmt.Errorf("类别 %s 重放结束时仍有未完成工作：workers=%d queue=%d", name, pending, len(c.queue))
		}
		if c.counters.Completed+c.counters.Rejected+c.counters.Unknown != c.counters.Submitted {
			return fmt.Errorf("类别 %s 账目不平：提交=%d 完成=%d 拒绝=%d 未知=%d",
				name, c.counters.Submitted, c.counters.Completed, c.counters.Rejected, c.counters.Unknown)
		}
		if c.counters.Waited > c.counters.Completed {
			return fmt.Errorf("类别 %s 等待数异常：完成=%d 等待=%d", name, c.counters.Completed, c.counters.Waited)
		}
	}
	return nil
}
