package executor

import (
	"container/heap"
	"fmt"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/metrics"
)

type Job struct {
	Arrival time.Duration
	Service time.Duration
}

type Config struct {
	MinWorkers   int
	MaxWorkers   int
	QueueCap     int
	StartupDelay time.Duration
	Tick         time.Duration
	Cooldown     time.Duration
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

type Engine struct {
	cfg      Config
	strategy decision.Strategy

	workers  []*worker
	queue    []Job
	desired  int
	inflight int

	counters metrics.Counters
	logs     []decision.Log

	events          eventHeap
	seq             int
	nextDecision    time.Duration
	lastScaleAt     time.Duration
	ewmaService     time.Duration
	haveServiceEWMA bool
}

func New(cfg Config, strategy decision.Strategy) *Engine {
	if cfg.MinWorkers < 1 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers < cfg.MinWorkers {
		cfg.MaxWorkers = cfg.MinWorkers
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.StartupDelay <= 0 {
		cfg.StartupDelay = 2 * time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 3 * cfg.Tick
	}
	e := &Engine{cfg: cfg, strategy: strategy, desired: cfg.MinWorkers}
	for i := 0; i < cfg.MinWorkers; i++ {
		id := i + 1
		e.workers = append(e.workers, &worker{id: id, gen: 1, state: stateStarting})
		e.scheduleReady(id, 1, cfg.StartupDelay)
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

func (e *Engine) Run(jobs []Job) ([]decision.Log, metrics.Counters, error) {
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
	endAt += e.cfg.StartupDelay + e.cfg.Cooldown + time.Second

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

	if err := e.checkAccounting(); err != nil {
		return e.logs, e.counters, err
	}
	return e.logs, e.counters, nil
}

func (e *Engine) handleArrival(now time.Duration, job Job) {
	e.counters.RecordArrival()
	if !e.haveServiceEWMA {
		e.ewmaService = job.Service
		e.haveServiceEWMA = true
	} else {
		e.ewmaService = (e.ewmaService + job.Service) / 2
	}

	// 准入规则：无空闲者，且已接纳请求达到“忙碌容量 + 队列 + 启动中未来容量”时拒绝。
	committed := e.countStates(stateBusy, stateDraining)
	availableCapacity := committed + e.cfg.QueueCap + e.countStates(stateStarting)
	if e.countStates(stateIdle) == 0 && e.inflight >= availableCapacity {
		e.counters.RecordRejection()
	} else {
		e.queue = append(e.queue, job)
		e.inflight++
		e.dispatch(now)
	}
}

func (e *Engine) idleWorker() *worker {
	for _, w := range e.workers {
		if w.state == stateIdle {
			return w
		}
	}
	return nil
}

func (e *Engine) dispatch(now time.Duration) {
	for len(e.queue) > 0 {
		w := e.idleWorker()
		if w == nil {
			return
		}
		job := e.queue[0]
		e.queue = e.queue[1:]
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
	e.dispatch(now)
}

func (e *Engine) handleComplete(now time.Duration, id, gen int) {
	w := e.workerByID(id, gen)
	if w == nil || w.state != stateBusy && w.state != stateDraining {
		return
	}
	wait := now - w.job.Arrival - w.job.Service
	if wait < 0 {
		wait = 0
	}
	e.counters.RecordCompletion(wait)
	e.inflight--
	if w.state == stateDraining {
		e.removeWorker(w)
		e.dispatch(now)
		return
	}
	w.state = stateIdle
	w.job = Job{}
	e.dispatch(now)
}

func (e *Engine) handleTick(now time.Duration) {
	arrivals, completions, rejections := e.counters.RotateWindow()
	snap := metrics.Snapshot{
		At:                  now,
		Ready:               e.countStates(stateIdle, stateBusy),
		Starting:            e.countStates(stateStarting),
		Busy:                e.countStates(stateBusy),
		Draining:            e.countStates(stateDraining),
		QueueLength:         len(e.queue),
		QueueCap:            e.cfg.QueueCap,
		ArrivalsInWindow:    arrivals,
		CompletionsInWindow: completions,
		RejectionsInWindow:  rejections,
		ArrivalRate:         float64(arrivals) / float64(e.cfg.Tick),
		MeanServiceTime:     e.ewmaService,
	}

	target, action, reason := e.strategy.Decide(snap)
	current := snap.Ready + snap.Starting
	effectiveCapacity := current + snap.Draining
	if action == decision.ActionScaleDown && snap.Draining > 0 && effectiveCapacity > target {
		action = decision.ActionHold
		target = e.desired
		reason = fmt.Sprintf("已有 %d 名工作者正在优雅退出，当前生效容量 %d，等待其完成在途工作后再决策",
			snap.Draining, effectiveCapacity)
	}
	if action == decision.ActionScaleDown && target >= current {
		action = decision.ActionHold
		target = e.desired
		reason = fmt.Sprintf("策略建议缩容到 %d，但可调度工作者已为 %d；无需重复调整，等待在途工作自然结束",
			target, current)
	}
	if target < e.cfg.MinWorkers {
		target = e.cfg.MinWorkers
	}
	if target > e.cfg.MaxWorkers {
		target = e.cfg.MaxWorkers
	}
	if action != decision.ActionHold && now < e.lastScaleAt+e.cfg.Cooldown {
		action = decision.ActionHold
		reason = fmt.Sprintf("距上次容量调整 %s 不足冷却期 %s：本轮保持 %d，防止刚扩又缩",
			now-e.lastScaleAt, e.cfg.Cooldown, current)
		target = e.desired
	}

	e.logs = append(e.logs, decision.Log{
		At: now, Action: action, From: current, To: e.desired, Reason: reason,
	})

	if action == decision.ActionHold || target == e.desired {
		return
	}

	e.lastScaleAt = now
	e.logs[len(e.logs)-1].To = target
	if target > e.desired {
		for len(e.workers) < target {
			id, gen := e.nextWorker()
			e.workers = append(e.workers, &worker{id: id, gen: gen, state: stateStarting})
			e.scheduleReady(id, gen, now+e.cfg.StartupDelay)
		}
	} else {
		e.reconcileDown(target)
	}
	e.desired = target
}

func (e *Engine) reconcileDown(target int) {
	excess := len(e.workers) - target
	if excess <= 0 {
		return
	}
	// 空闲者立即退出；忙碌者标记 draining，只完成手头在途工作，不再领取新任务。
	var toRemove []*worker
	for _, w := range e.workers {
		if excess == 0 {
			break
		}
		if w.state == stateIdle {
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
		if w.state == stateBusy {
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

func (e *Engine) countStates(states ...workerState) int {
	total := 0
outer:
	for _, w := range e.workers {
		for _, state := range states {
			if w.state == state {
				total++
				continue outer
			}
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
	if e.countStates(stateStarting, stateBusy, stateDraining) != 0 || len(e.queue) != 0 {
		return fmt.Errorf("重放结束时仍有未完成工作：workers=%d queue=%d",
			e.countStates(stateStarting, stateBusy, stateDraining), len(e.queue))
	}
	if e.counters.Completed+e.counters.Rejected != e.counters.Submitted {
		return fmt.Errorf("账目不平：提交=%d 完成=%d 拒绝=%d",
			e.counters.Submitted, e.counters.Completed, e.counters.Rejected)
	}
	if e.counters.Waited > e.counters.Completed {
		return fmt.Errorf("等待数异常：完成=%d 等待=%d", e.counters.Completed, e.counters.Waited)
	}
	return nil
}
