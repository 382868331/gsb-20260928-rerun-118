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
}

// ClassConfig 描述一个工作者类别的容量约束与启动延迟。
type ClassConfig struct {
	Min          int
	Max          int
	QueueCap     int
	StartupDelay time.Duration
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
)

type event struct {
	at   time.Duration
	seq  int
	kind eventKind
	ref  int
	gen  int
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

func (e *Engine) Run(jobs []Job) (Result, error) {
	for _, job := range jobs {
		if _, ok := e.classes[job.Class]; !ok {
			return Result{}, fmt.Errorf("任务类别 %q 没有对应的工作者类别", job.Class)
		}
		e.schedule(event{at: job.Arrival, kind: evArrival, job: job})
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
		}
	}

	result := Result{Logs: e.logs, PerClass: map[string]metrics.Counters{}}
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
		c.counters.RecordRejection()
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

// dispatch 只把任务交给同类别的空闲工作者：适配约束是硬约束。
func (e *Engine) dispatch(now time.Duration, class string) {
	c := e.classes[class]
	for len(c.queue) > 0 {
		w := e.idleWorker(class)
		if w == nil {
			return
		}
		job := c.queue[0]
		c.queue = c.queue[1:]
		w.state = stateBusy
		w.job = job
		e.schedule(event{at: now + job.Service, kind: evComplete, ref: w.id, gen: w.gen})
	}
}

func (e *Engine) handleReady(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateStarting {
		return
	}
	w.state = stateIdle
	e.dispatch(now, w.class)
}

func (e *Engine) handleComplete(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		return
	}
	c := e.classes[w.class]
	wait := now - w.job.Arrival - w.job.Service
	if wait < 0 {
		wait = 0
	}
	c.counters.RecordCompletion(wait)
	c.inflight--
	if w.state == stateDraining {
		e.removeWorker(w)
		e.dispatch(now, w.class)
		return
	}
	w.state = stateIdle
	w.job = Job{}
	e.dispatch(now, w.class)
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
		if w.class == class {
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
		if c.counters.Completed+c.counters.Rejected != c.counters.Submitted {
			return fmt.Errorf("类别 %s 账目不平：提交=%d 完成=%d 拒绝=%d",
				name, c.counters.Submitted, c.counters.Completed, c.counters.Rejected)
		}
		if c.counters.Waited > c.counters.Completed {
			return fmt.Errorf("类别 %s 等待数异常：完成=%d 等待=%d", name, c.counters.Completed, c.counters.Waited)
		}
	}
	return nil
}
