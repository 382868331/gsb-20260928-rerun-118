package executor

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/metrics"
)

// Job 携带能力类别：默认只有同类工作者才能处理它，
// 除非类别预先声明了兼容替代范围。
type Job struct {
	ID      int
	Arrival time.Duration
	Service time.Duration
	Class   string
}

// Fault 描述一次工作者失联事件：Busy 为真时优先选择忙碌实例，
// 其上的在途任务可能已产生效果，进入未知状态。
type Fault struct {
	At    time.Duration
	Class string
	Busy  bool
}

// Confirmation 是操作者确认：旧实例产生的效应已核实，
// 释放失联实例，并允许该类别的未知任务重新排队继续。
type Confirmation struct {
	At    time.Duration
	Class string
}

// ClassConfig 描述一个工作者类别的容量约束与启动延迟。
// Substitutes 是预先声明的兼容替代范围：仅当本类别容量耗尽
// 且没有在途启动时，任务才可交给这些类别的空闲实例，
// 绝不允许临时放宽亲和性来填平账面缺口。
type ClassConfig struct {
	Min          int
	Max          int
	QueueCap     int
	StartupDelay time.Duration
	Substitutes  []string
}

type Config struct {
	Classes  map[string]ClassConfig
	Tick     time.Duration
	Cooldown time.Duration
}

type workerState int

const (
	stateStarting workerState = iota
	stateIdle
	stateBusy
	stateDraining
	stateLost
)

type worker struct {
	id    int
	gen   int
	class string
	state workerState
	job   Job
}

type eventKind int

const (
	evArrival eventKind = iota
	evWorkerReady
	evComplete
	evTick
	evFault
	evConfirm
)

type event struct {
	at   time.Duration
	seq  int
	kind eventKind
	ref  int
	gen  int
	flag int
	job  Job
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
	queue       []Job
	unknown     []Job
	desired     int
	inflight    int
	lastScaleAt time.Duration
	ewmaService time.Duration
	haveService bool
	counters    metrics.Counters
}

// TaskLog 记录一次任务安排或释放的原因，保证每次调度可解释。
type TaskLog struct {
	At     time.Duration
	Action string // assign|substitute|complete|lost|requeue|release|confirm
	Class  string
	Job    int
	Worker int
	Detail string
}

// Result 是一次重放的完整产出：决策依据、任务安排/释放记录、分类别账目与总账。
type Result struct {
	Logs     []decision.Log
	TaskLogs []TaskLog
	PerClass map[string]metrics.Counters
	Total    metrics.Counters
}

type Engine struct {
	cfg      Config
	strategy decision.Strategy

	classes map[string]*classRuntime
	order   []string
	workers []*worker
	logs    []decision.Log
	taskLog []TaskLog

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
	for _, name := range e.order {
		c := e.classes[name]
		for i := 0; i < c.cfg.Min; i++ {
			id := len(e.workers) + 1
			e.workers = append(e.workers, &worker{id: id, gen: 1, class: name, state: stateStarting})
			e.scheduleReady(id, 1, c.cfg.StartupDelay)
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

func (e *Engine) logTask(at time.Duration, action, class string, jobID, workerID int, detail string) {
	e.taskLog = append(e.taskLog, TaskLog{
		At: at, Action: action, Class: class, Job: jobID, Worker: workerID, Detail: detail,
	})
}

func (e *Engine) Run(jobs []Job) (Result, error) {
	return e.RunWithEvents(jobs, nil, nil)
}

// RunWithEvents 在重放流量的同时注入工作者失联与操作者确认事件。
func (e *Engine) RunWithEvents(jobs []Job, faults []Fault, confirms []Confirmation) (Result, error) {
	for i, job := range jobs {
		if _, ok := e.classes[job.Class]; !ok {
			return Result{}, fmt.Errorf("任务类别 %q 没有对应的工作者类别", job.Class)
		}
		job.ID = i + 1
		jobs[i].ID = job.ID
		e.schedule(event{at: job.Arrival, kind: evArrival, job: job})
	}
	for _, fault := range faults {
		if _, ok := e.classes[fault.Class]; !ok {
			return Result{}, fmt.Errorf("失联事件类别 %q 没有对应的工作者类别", fault.Class)
		}
		flag := 0
		if fault.Busy {
			flag = 1
		}
		e.schedule(event{at: fault.At, kind: evFault, flag: flag, job: Job{Class: fault.Class}})
	}
	for _, confirm := range confirms {
		if _, ok := e.classes[confirm.Class]; !ok {
			return Result{}, fmt.Errorf("确认事件类别 %q 没有对应的工作者类别", confirm.Class)
		}
		e.schedule(event{at: confirm.At, kind: evConfirm, job: Job{Class: confirm.Class}})
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
		case evFault:
			e.handleFault(ev.at, ev.job.Class, ev.flag == 1)
		case evConfirm:
			e.handleConfirm(ev.at, ev.job.Class)
		}
	}

	result := Result{Logs: e.logs, TaskLogs: e.taskLog, PerClass: map[string]metrics.Counters{}}
	for _, name := range e.order {
		result.PerClass[name] = e.classes[name].counters
		result.Total.Add(e.classes[name].counters)
	}
	if err := e.checkAccounting(); err != nil {
		return result, err
	}
	return result, nil
}

func (e *Engine) handleArrival(now time.Duration, job Job) {
	c := e.classes[job.Class]
	c.counters.RecordArrival()
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
		// 容量耗尽时，仅当存在预先声明且当前空闲的兼容替代才接纳，
		// 否则明确拒绝并计数，不靠放宽亲和性填平账面缺口。
		if e.pickSubstitute(job.Class) != nil {
			c.queue = append(c.queue, job)
			c.inflight++
			e.dispatch(now, job.Class)
		} else {
			c.counters.RecordRejection()
		}
	} else {
		c.queue = append(c.queue, job)
		c.inflight++
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

// dispatch 优先把任务交给同类别的空闲工作者；仅当本类别容量已达上限
// 且没有在途启动时，才按预先声明的兼容替代范围选择替代实例。
func (e *Engine) dispatch(now time.Duration, class string) {
	c := e.classes[class]
	for len(c.queue) > 0 {
		if w := e.idleWorker(class); w != nil {
			job := c.queue[0]
			c.queue = c.queue[1:]
			e.assign(now, w, job, "assign",
				fmt.Sprintf("同类空闲实例就绪，安排任务 %d（成本 %s）", job.ID, job.Service))
			continue
		}
		sub := e.pickSubstitute(class)
		if sub == nil {
			return
		}
		job := c.queue[0]
		c.queue = c.queue[1:]
		e.assign(now, sub, job, "substitute",
			fmt.Sprintf("类别 %s 容量已达上限 %d 且无在途启动，按预先声明的兼容范围交给类别 %s 空闲实例（任务成本 %s）",
				class, c.cfg.Max, sub.class, job.Service))
	}
}

// pickSubstitute 在预先声明的兼容替代范围内寻找空闲实例。
// 本类别还能扩容、或已有在途启动即将兑现时，返回 nil：
// 替代只是容量耗尽后的兜底，不能替代正常的容量规划。
func (e *Engine) pickSubstitute(class string) *worker {
	c := e.classes[class]
	if len(c.cfg.Substitutes) == 0 {
		return nil
	}
	if e.classWorkerCount(class) < c.cfg.Max {
		return nil
	}
	if e.countStates(class, stateStarting) > 0 {
		return nil
	}
	for _, sub := range c.cfg.Substitutes {
		if w := e.idleWorker(sub); w != nil {
			return w
		}
	}
	return nil
}

func (e *Engine) assign(now time.Duration, w *worker, job Job, action, reason string) {
	w.state = stateBusy
	w.job = job
	e.logTask(now, action, job.Class, job.ID, w.id, reason)
	e.schedule(event{at: now + job.Service, kind: evComplete, ref: w.id, gen: w.gen})
}

func (e *Engine) dispatchAll(now time.Duration) {
	for _, name := range e.order {
		e.dispatch(now, name)
	}
}

func (e *Engine) handleReady(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateStarting {
		return
	}
	w.state = stateIdle
	if lost := e.countStates(w.class, stateLost); lost > 0 {
		e.logTask(now, "replace_ready", w.class, 0, w.id,
			fmt.Sprintf("接替实例就绪，但 %d 名旧实例尚未被操作者认定停止，其未知任务继续保持未知", lost))
	}
	e.dispatchAll(now)
}

func (e *Engine) handleComplete(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		// 失联实例迟到的完成事件不可信：旧实例是否停止需操作者确认。
		return
	}
	// 完成计入任务所属类别的账目：兼容替代执行不改变任务归属。
	c := e.classes[w.job.Class]
	e.logTask(now, "complete", w.job.Class, w.job.ID, w.id,
		fmt.Sprintf("任务完成，释放实例（等待 %s）", now-w.job.Arrival-w.job.Service))
	wait := now - w.job.Arrival - w.job.Service
	if wait < 0 {
		wait = 0
	}
	c.counters.RecordCompletion(wait)
	c.inflight--
	if w.state == stateDraining {
		e.removeWorker(w)
		e.dispatchAll(now)
		return
	}
	w.state = stateIdle
	w.job = Job{}
	e.dispatchAll(now)
}

// handleFault 处理工作者失联：未开始的队列任务仍可重新安排；
// 在途任务可能已产生效果，保留未知状态，不自动重投；
// 随后补充接替实例，但接替就绪也不自动认定旧实例已停止。
func (e *Engine) handleFault(now time.Duration, class string, preferBusy bool) {
	w := e.pickFaultTarget(class, preferBusy)
	if w == nil {
		e.logTask(now, "lost", class, 0, 0, "失联事件没有可作用的实例，忽略")
		return
	}
	c := e.classes[w.class]
	wasBusy := w.state == stateBusy || w.state == stateDraining
	w.state = stateLost
	if wasBusy {
		job := w.job
		w.job = Job{}
		c.unknown = append(c.unknown, job)
		c.counters.RecordUnknown()
		c.inflight--
		e.logTask(now, "lost", class, job.ID, w.id,
			fmt.Sprintf("工作者失联，在途任务 %d 可能已产生效果，保留未知状态，不自动重投；未开始的队列任务仍可重新安排", job.ID))
	} else {
		e.logTask(now, "lost", class, 0, w.id, "工作者失联，实例上没有在途任务")
	}
	// 接替流程：补充未来容量，但明确不触碰旧实例的未知账目。
	if active := e.classWorkerCount(class); active < c.desired && active < c.cfg.Max {
		id, gen := e.nextWorker()
		e.workers = append(e.workers, &worker{id: id, gen: gen, class: class, state: stateStarting})
		e.scheduleReady(id, gen, now+c.cfg.StartupDelay)
		e.logs = append(e.logs, decision.Log{
			At: now, Class: class, Action: decision.ActionReplace,
			From: active, To: active + 1,
			Reason: fmt.Sprintf("工作者 %d 失联，启动接替实例 %d 补足未来容量；旧实例在途任务保持未知，接替就绪不自动认定旧实例已停止", w.id, id),
		})
	}
	e.dispatchAll(now)
}

func (e *Engine) pickFaultTarget(class string, preferBusy bool) *worker {
	if preferBusy {
		for _, w := range e.workers {
			if w.class == class && (w.state == stateBusy || w.state == stateDraining) {
				return w
			}
		}
	}
	for _, w := range e.workers {
		if w.class == class && w.state != stateLost {
			return w
		}
	}
	return nil
}

// handleConfirm 处理操作者确认：旧实例产生的效应已核实，
// 释放失联实例，未知任务重新排队继续剩余工作。
func (e *Engine) handleConfirm(now time.Duration, class string) {
	c := e.classes[class]
	var released []int
	var kept []*worker
	for _, w := range e.workers {
		if w.class == class && w.state == stateLost {
			released = append(released, w.id)
			continue
		}
		kept = append(kept, w)
	}
	e.workers = kept
	for _, id := range released {
		e.logTask(now, "release", class, 0, id, "操作者确认旧实例效应已核实，释放失联实例")
	}
	for _, job := range c.unknown {
		c.queue = append(c.queue, job)
		c.inflight++
		c.counters.RecordUnknownResolved()
		e.logTask(now, "requeue", class, job.ID, 0,
			fmt.Sprintf("操作者确认旧效果后，任务 %d 重新排队继续剩余工作", job.ID))
	}
	c.unknown = nil
	if len(released) == 0 && c.counters.Unknown == 0 {
		e.logTask(now, "confirm", class, 0, 0, "操作者确认到达，但没有待释放的失联实例或未知任务")
	}
	e.dispatchAll(now)
}

func (e *Engine) handleTick(now time.Duration) {
	for _, name := range e.order {
		e.tickClass(now, name)
	}
}

func (e *Engine) tickClass(now time.Duration, name string) {
	c := e.classes[name]
	arrivals, completions, rejections := c.counters.RotateWindow()
	snap := metrics.Snapshot{
		At:                  now,
		Class:               name,
		Min:                 c.cfg.Min,
		Max:                 c.cfg.Max,
		Ready:               e.countStates(name, stateIdle, stateBusy),
		Starting:            e.countStates(name, stateStarting),
		Busy:                e.countStates(name, stateBusy),
		Draining:            e.countStates(name, stateDraining),
		Lost:                e.countStates(name, stateLost),
		UnknownTasks:        len(c.unknown),
		QueueLength:         len(c.queue),
		QueueCap:            c.cfg.QueueCap,
		ArrivalsInWindow:    arrivals,
		CompletionsInWindow: completions,
		RejectionsInWindow:  rejections,
		ArrivalRate:         float64(arrivals) / float64(e.cfg.Tick),
		MeanServiceTime:     c.ewmaService,
	}

	target, action, reason := e.strategy.Decide(snap)
	current := snap.Ready + snap.Starting
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
		for e.classWorkerCount(name) < target {
			id, gen := e.nextWorker()
			e.workers = append(e.workers, &worker{id: id, gen: gen, class: name, state: stateStarting})
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
	excess := e.classWorkerCount(class) - target
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
		if w.class == class && w.state == stateBusy {
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
		if w.class == class && w.state != stateLost {
			total++
		}
	}
	return total
}

func (e *Engine) nextWorker() (int, int) {
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
