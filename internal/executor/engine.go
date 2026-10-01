package executor

import (
	"container/heap"
	"fmt"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/metrics"
)

// Job 带类别约束：只能调度到具备对应能力的工作者上。
type Job struct {
	Class   string
	Arrival time.Duration
	Service time.Duration
}

// ClassConfig 描述一类工作者的容量边界、队列与启动延迟。
type ClassConfig struct {
	Name         string
	MinWorkers   int
	MaxWorkers   int
	QueueCap     int
	StartupDelay time.Duration
}

type Config struct {
	Classes  []ClassConfig
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
	at    time.Duration
	seq   int
	kind  eventKind
	ref   int
	gen   int
	class string
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

// classRuntime 保存单个类别的全部运行时状态。
type classRuntime struct {
	cfg      ClassConfig
	workers  []*worker
	queue    []Job
	desired  int
	inflight int

	lastScaleAt     time.Duration
	ewmaService     time.Duration
	haveServiceEWMA bool
}

type Engine struct {
	cfg      Config
	strategy decision.Strategy

	classes  []*classRuntime
	byName   map[string]*classRuntime
	counters *metrics.Counters
	logs     []decision.Log

	events eventHeap
	seq    int
}

func New(cfg Config, strategy decision.Strategy) *Engine {
	if len(cfg.Classes) == 0 {
		cfg.Classes = []ClassConfig{{Name: "default", MinWorkers: 1, MaxWorkers: 1, QueueCap: 1}}
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 3 * cfg.Tick
	}
	names := make([]string, 0, len(cfg.Classes))
	e := &Engine{cfg: cfg, strategy: strategy, byName: map[string]*classRuntime{}}
	for i := range cfg.Classes {
		cc := cfg.Classes[i]
		if cc.MinWorkers < 1 {
			cc.MinWorkers = 1
		}
		if cc.MaxWorkers < cc.MinWorkers {
			cc.MaxWorkers = cc.MinWorkers
		}
		if cc.StartupDelay <= 0 {
			cc.StartupDelay = 2 * time.Second
		}
		cr := &classRuntime{cfg: cc, desired: cc.MinWorkers}
		for id := 1; id <= cc.MinWorkers; id++ {
			cr.workers = append(cr.workers, &worker{id: id, gen: 1, state: stateStarting})
		}
		e.classes = append(e.classes, cr)
		e.byName[cc.Name] = cr
		names = append(names, cc.Name)
	}
	e.counters = metrics.NewCounters(names)
	for _, cr := range e.classes {
		for _, w := range cr.workers {
			e.scheduleReady(cr.cfg.Name, w.id, w.gen, cr.cfg.StartupDelay)
		}
	}
	return e
}

func (e *Engine) schedule(ev event) {
	e.seq++
	ev.seq = e.seq
	heap.Push(&e.events, ev)
}

func (e *Engine) scheduleReady(class string, id, gen int, at time.Duration) {
	e.schedule(event{at: at, kind: evWorkerReady, ref: id, gen: gen, class: class})
}

func (e *Engine) Run(jobs []Job) ([]decision.Log, *metrics.Counters, error) {
	for _, job := range jobs {
		e.schedule(event{at: job.Arrival, kind: evArrival, job: job})
	}
	e.schedule(event{at: e.cfg.Tick, kind: evTick})

	endAt := time.Duration(0)
	for _, job := range jobs {
		if deadline := job.Arrival + job.Service; deadline > endAt {
			endAt = deadline
		}
	}
	maxStartup := time.Duration(0)
	for _, cr := range e.classes {
		if cr.cfg.StartupDelay > maxStartup {
			maxStartup = cr.cfg.StartupDelay
		}
	}
	endAt += maxStartup + e.cfg.Cooldown + time.Second

	for e.events.Len() > 0 {
		ev := heap.Pop(&e.events).(event)
		if ev.kind == evArrival && ev.at > endAt {
			continue
		}
		switch ev.kind {
		case evArrival:
			e.handleArrival(ev.at, ev.job)
		case evWorkerReady:
			e.handleReady(ev.at, ev.class, ev.ref, ev.gen)
		case evComplete:
			e.handleComplete(ev.at, ev.class, ev.ref, ev.gen)
		case evTick:
			if ev.at <= endAt {
				e.handleTick(ev.at)
				e.schedule(event{at: ev.at + e.cfg.Tick, kind: evTick})
			}
		}
	}

	if err := e.checkAccounting(); err != nil {
		return e.logs, e.counters, err
	}
	return e.logs, e.counters, nil
}

func (e *Engine) handleArrival(now time.Duration, job Job) {
	cr, ok := e.byName[job.Class]
	if !ok {
		// 没有任何一类工作者具备该能力：立即明确拒绝。
		e.counters.RecordArrival(job.Class)
		e.counters.RecordRejection(job.Class)
		return
	}
	e.counters.RecordArrival(job.Class)
	if !cr.haveServiceEWMA {
		cr.ewmaService = job.Service
		cr.haveServiceEWMA = true
	} else {
		cr.ewmaService = (cr.ewmaService + job.Service) / 2
	}

	// 准入规则：无空闲者，且已接纳请求达到“忙碌容量 + 队列 + 在途扩容未来容量”时拒绝。
	committed := cr.countStates(stateBusy, stateDraining)
	availableCapacity := committed + cr.cfg.QueueCap + cr.countStates(stateStarting)
	if cr.countStates(stateIdle) == 0 && cr.inflight >= availableCapacity {
		e.counters.RecordRejection(job.Class)
		return
	}
	cr.queue = append(cr.queue, job)
	cr.inflight++
	e.dispatch(cr, now)
}

func (cr *classRuntime) idleWorker() *worker {
	for _, w := range cr.workers {
		if w.state == stateIdle {
			return w
		}
	}
	return nil
}

func (e *Engine) dispatch(cr *classRuntime, now time.Duration) {
	for len(cr.queue) > 0 {
		w := cr.idleWorker()
		if w == nil {
			return
		}
		job := cr.queue[0]
		cr.queue = cr.queue[1:]
		w.state = stateBusy
		w.job = job
		e.schedule(event{at: now + job.Service, kind: evComplete, ref: w.id, gen: w.gen, class: cr.cfg.Name})
	}
}

func (e *Engine) handleReady(now time.Duration, class string, id, gen int) {
	cr := e.byName[class]
	w := cr.workerByID(id, gen)
	if w == nil || w.state != stateStarting {
		return
	}
	w.state = stateIdle
	e.dispatch(cr, now)
}

func (e *Engine) handleComplete(now time.Duration, class string, id, gen int) {
	cr := e.byName[class]
	w := cr.workerByID(id, gen)
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		return
	}
	wait := now - w.job.Arrival - w.job.Service
	if wait < 0 {
		wait = 0
	}
	e.counters.RecordCompletion(class, wait)
	cr.inflight--
	if w.state == stateDraining {
		cr.removeWorker(w)
		e.dispatch(cr, now)
		return
	}
	w.state = stateIdle
	w.job = Job{}
	e.dispatch(cr, now)
}

func (e *Engine) handleTick(now time.Duration) {
	arrivals, completions, rejections := e.counters.RotateWindow()
	snap := metrics.Snapshot{At: now}
	for i, cr := range e.classes {
		snap.Classes = append(snap.Classes, metrics.ClassSnapshot{
			Name:                cr.cfg.Name,
			Ready:               cr.countStates(stateIdle, stateBusy),
			Starting:            cr.countStates(stateStarting),
			Busy:                cr.countStates(stateBusy),
			Draining:            cr.countStates(stateDraining),
			QueueLength:         len(cr.queue),
			QueueCap:            cr.cfg.QueueCap,
			ArrivalsInWindow:    arrivals[i],
			CompletionsInWindow: completions[i],
			RejectionsInWindow:  rejections[i],
			ArrivalRate:         float64(arrivals[i]) / float64(e.cfg.Tick),
			MeanServiceTime:     cr.ewmaService,
		})
	}

	for _, d := range e.strategy.Decide(snap) {
		e.applyDecision(now, d)
	}
}

func (e *Engine) applyDecision(now time.Duration, d decision.ClassDecision) {
	cr, ok := e.byName[d.Class]
	if !ok {
		return
	}
	current := cr.countStates(stateIdle, stateBusy) + cr.countStates(stateStarting)
	draining := cr.countStates(stateDraining)
	target := d.Desired
	action := d.Action
	reason := d.Reason

	if action == decision.ActionScaleDown && draining > 0 && current+draining > target {
		action = decision.ActionHold
		target = cr.desired
		reason = fmt.Sprintf("类别 %s 已有 %d 名工作者正在优雅退出，生效容量 %d，等待其完成在途工作后再决策",
			d.Class, draining, current+draining)
	}
	if action == decision.ActionScaleDown && target >= current {
		action = decision.ActionHold
		target = cr.desired
		reason = fmt.Sprintf("类别 %s 策略建议缩容到 %d，但可调度工作者已为 %d；无需重复调整",
			d.Class, target, current)
	}
	if target < cr.cfg.MinWorkers {
		target = cr.cfg.MinWorkers
	}
	if target > cr.cfg.MaxWorkers {
		target = cr.cfg.MaxWorkers
	}
	if action != decision.ActionHold && now < cr.lastScaleAt+e.cfg.Cooldown {
		action = decision.ActionHold
		reason = fmt.Sprintf("类别 %s 距上次调整 %s 不足冷却期 %s：本轮保持 %d，防止刚扩又缩",
			d.Class, now-cr.lastScaleAt, e.cfg.Cooldown, current)
		target = cr.desired
	}

	e.logs = append(e.logs, decision.Log{
		At: now, Class: d.Class, Action: action, From: current, To: cr.desired, Reason: reason,
	})

	if action == decision.ActionHold || target == cr.desired {
		return
	}

	cr.lastScaleAt = now
	e.logs[len(e.logs)-1].To = target
	if target > cr.desired {
		for len(cr.workers) < target {
			id, gen := cr.nextWorker()
			cr.workers = append(cr.workers, &worker{id: id, gen: gen, state: stateStarting})
			e.scheduleReady(cr.cfg.Name, id, gen, now+cr.cfg.StartupDelay)
		}
	} else {
		e.reconcileDown(cr, target)
	}
	cr.desired = target
}

// reconcileDown 先停空闲实例；忙碌实例标记 draining，只完成手头在途工作。
func (e *Engine) reconcileDown(cr *classRuntime, target int) {
	excess := len(cr.workers) - target
	if excess <= 0 {
		return
	}
	var toRemove []*worker
	for _, w := range cr.workers {
		if excess == 0 {
			break
		}
		if w.state == stateIdle {
			toRemove = append(toRemove, w)
			excess--
		}
	}
	for _, w := range toRemove {
		cr.removeWorker(w)
	}
	for _, w := range cr.workers {
		if excess == 0 {
			return
		}
		if w.state == stateBusy {
			w.state = stateDraining
			excess--
		}
	}
}

func (cr *classRuntime) workerByID(id, gen int) *worker {
	for _, w := range cr.workers {
		if w.id == id && w.gen == gen {
			return w
		}
	}
	return nil
}

func (cr *classRuntime) removeWorker(target *worker) {
	for i, w := range cr.workers {
		if w == target {
			cr.workers = append(cr.workers[:i], cr.workers[i+1:]...)
			return
		}
	}
}

func (cr *classRuntime) countStates(states ...workerState) int {
	total := 0
outer:
	for _, w := range cr.workers {
		for _, state := range states {
			if w.state == state {
				total++
				continue outer
			}
		}
	}
	return total
}

func (cr *classRuntime) nextWorker() (int, int) {
	maxID := 0
	byID := map[int]int{}
	for _, w := range cr.workers {
		if w.id > maxID {
			maxID = w.id
		}
		byID[w.id] = w.gen
	}
	id := maxID + 1
	return id, byID[id] + 1
}

func (e *Engine) checkAccounting() error {
	for _, cr := range e.classes {
		if cr.countStates(stateStarting, stateBusy, stateDraining) != 0 || len(cr.queue) != 0 {
			return fmt.Errorf("类别 %s 重放结束时仍有未完成工作：workers=%d queue=%d",
				cr.cfg.Name, cr.countStates(stateStarting, stateBusy, stateDraining), len(cr.queue))
		}
		cc := e.counters.Class(cr.cfg.Name)
		if cc.Completed+cc.Rejected != cc.Submitted {
			return fmt.Errorf("类别 %s 账目不平：提交=%d 完成=%d 拒绝=%d",
				cr.cfg.Name, cc.Submitted, cc.Completed, cc.Rejected)
		}
		if cc.Waited > cc.Completed {
			return fmt.Errorf("类别 %s 等待数异常：完成=%d 等待=%d", cr.cfg.Name, cc.Completed, cc.Waited)
		}
	}
	return nil
}
