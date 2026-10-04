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

type workerState int

const (
	stateStarting workerState = iota
	stateIdle
	stateBusy
	stateDraining
	stateReserved
	stateStandby
	stateLost
)

type worker struct {
	id         int
	gen        int
	class      string
	state      workerState
	readyAt    time.Duration
	jobID      int
	job        Job
	started    time.Duration
	handoff    bool
	authorized bool
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
)

type eventKind int

const (
	evArrival eventKind = iota
	evWorkerReady
	evComplete
	evTick
	evLoseWorker
	evConfirm
)

type event struct {
	at    time.Duration
	seq   int
	kind  eventKind
	ref   int
	gen   int
	jobID int
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
	Logs     []decision.Log
	Events   []AuditEvent
	Unknown  []UnknownJob
	PerClass map[string]metrics.Counters
	Total    metrics.Counters
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

	classes    map[string]*classRuntime
	order      []string
	workers    []*worker
	logs       []decision.Log
	audits     []AuditEvent
	unknown    []*unknownJob
	nextJob    int
	nextWorker int

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
		cfg:      cfg,
		strategy: strategy,
		classes:  map[string]*classRuntime{},
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
		}
	}

	result := Result{Logs: e.logs, Events: e.audits, PerClass: map[string]metrics.Counters{}}
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
	if w == nil {
		return
	}
	switch w.state {
	case stateStarting:
		w.state = stateIdle
		w.readyAt = now
		e.dispatch(now, w.class)
	case stateReserved:
		if w.authorized {
			w.state = stateBusy
			w.started = now
			e.schedule(event{at: now + w.job.Service, kind: evComplete, ref: w.id, gen: w.gen, jobID: w.jobID, job: w.job})
			e.audit(now, AuditTakeover, w.jobID, w.job, w, "人工已确认旧效果不存在；替代实例就绪后执行剩余任务")
			return
		}
		w.state = stateStandby
		w.readyAt = now
		e.audit(now, AuditReserveStandby, w.jobID, w.job, w, "替代实例已就绪但旧效果尚未确认；继续待命，不自动重投")
	case stateStandby:
	default:
		return
	}
}

func (e *Engine) handleComplete(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		return
	}
	jobClass := w.job.Class
	c := e.classes[jobClass]
	wait := now - w.job.Arrival - w.job.Service
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
	w.authorized = false
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
	if w == nil || w.state != stateBusy && w.state != stateDraining {
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
	replacement.started = now
	replacement.handoff = true
	replacement.authorized = false
	if replacement.readyAt <= now {
		replacement.state = stateStandby
		e.audit(now, AuditReserveStandby, jobID, job, replacement, "替代实例就绪但不自动认定旧实例停止；等待人工确认旧效果")
	} else {
		e.audit(now, AuditReserveStandby, jobID, job, replacement, "替代实例仍在途启动；等待启动和人工确认，不把在途容量计入未来扩容")
	}
}

func (e *Engine) handleConfirmation(now time.Duration, jobID int, observed bool) {
	item := e.unknownByJob(jobID)
	if item == nil {
		return
	}
	c := e.classes[item.oldClass]
	replacement := item.replacement
	if observed {
		c.counters.ResolveUnknownAsCompleted()
		if replacement != nil {
			replacement.handoff = false
			replacement.authorized = false
			replacement.jobID = 0
			replacement.job = Job{}
			replacement.started = 0
			if replacement.state == stateStandby {
				replacement.state = stateIdle
			} else if replacement.state == stateReserved {
				replacement.state = stateStarting
			}
			e.audit(now, AuditRelease, jobID, item.job, replacement, "操作者确认旧任务已生效；取消接替执行并释放替代实例")
		}
		e.audit(now, AuditConfirmEffect, jobID, item.job, nil, "操作者确认旧效果已产生；只保留结果，不重复执行")
	} else {
		c.counters.Unknown--
		e.audit(now, AuditConfirmEffect, jobID, item.job, nil, "操作者确认旧效果不存在；允许接替执行剩余任务")
		if replacement == nil {
			replacement = e.chooseReplacement(now, item.job, item.oldClass)
			item.replacement = replacement
			if replacement != nil {
				replacement.state = stateReserved
				replacement.jobID = jobID
				replacement.job = item.job
				replacement.started = now
				replacement.handoff = true
			}
		}
		if replacement == nil {
			c.counters.Rejected++
			e.audit(now, AuditReject, jobID, item.job, nil, "确认需要接替但无预声明兼容容量")
		} else {
			item.job.Service = item.remaining
			replacement.job = item.job
			replacement.jobID = jobID
			replacement.started = now
			replacement.authorized = true
			if replacement.state == stateStandby {
				replacement.state = stateBusy
				e.schedule(event{at: now + item.remaining, kind: evComplete, ref: replacement.id, gen: replacement.gen, jobID: jobID, job: item.job})
				e.audit(now, AuditTakeover, jobID, item.job, replacement, "替代实例已就绪，执行剩余任务")
			} else {
				replacement.state = stateReserved
				e.audit(now, AuditReserveStandby, jobID, item.job, replacement, "已确认允许接替；替代实例仍在途，就绪后执行剩余任务")
			}
		}
	}
	old := e.workerByID(item.oldWorker, item.oldGen)
	e.removeUnknown(item)
	e.removeWorker(old)
	e.dispatch(now, item.oldClass)
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
	e.nextWorker++
	return e.nextWorker, 1
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
	event := AuditEvent{At: now, Kind: kind, JobID: jobID, JobClass: job.Class, Reason: reason}
	if w != nil {
		event.WorkerID = w.id
		event.WorkerClass = w.class
	}
	e.audits = append(e.audits, event)
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
