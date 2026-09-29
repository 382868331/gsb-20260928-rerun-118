package controller

import "time"

const TickInterval = 100 * time.Millisecond

func tickDuration(tick int) time.Duration {
	return time.Duration(tick) * TickInterval
}

type queuedJob struct {
	job    Job
	waited int
}

type worker struct {
	job       *Job
	remaining int
	warmLeft  int
}

// Executor 执行部分：管理 worker 生命周期、排队与处理。
// warming/active/draining 中的 worker 都占用系统容量槽位。
type Executor struct {
	cap Capacity

	warming  []*worker
	active   []*worker
	draining []*worker
	queue    []*queuedJob

	nextJobID int

	// 红色区间按比例拒绝的确定性累加器（不用随机数，保证重放可复现）。
	shedAcc float64

	completedThisTick int
}

func NewExecutor(cap Capacity) *Executor {
	if cap.MinWorkers < 1 {
		cap.MinWorkers = 1
	}
	if cap.HardQueue <= 0 {
		cap.HardQueue = cap.MaxWorkers
	}
	e := &Executor{cap: cap}
	for range cap.MinWorkers {
		e.active = append(e.active, &worker{})
	}
	return e
}

// Admit 按声明规则对一个 tick 内到达的请求做接纳判定：
//  1. 硬上限 HardQueue：在途（worker 槽位 + 排队）达到上限一律拒绝，绝不藏请求；
//  2. 红色区间：占用达到 HardQueue*RedQueueRatio 后，按 RedShedRatio 比例确定性拒绝；
//  3. 其余请求全部接纳并排队。
func (e *Executor) Admit(arrivals int) (accepted, rejected int) {
	for range arrivals {
		inFlight := len(e.warming) + len(e.active) + len(e.draining) + len(e.queue)
		switch {
		case inFlight >= e.cap.HardQueue:
			rejected++
		case e.cap.RedShedRatio > 0 && float64(inFlight) >= float64(e.cap.HardQueue)*e.cap.RedQueueRatio:
			e.shedAcc += e.cap.RedShedRatio
			if e.shedAcc >= 1 {
				e.shedAcc--
				rejected++
			} else {
				e.enqueue()
				accepted++
			}
		default:
			e.enqueue()
			accepted++
		}
	}
	return accepted, rejected
}

func (e *Executor) enqueue() {
	e.nextJobID++
	e.queue = append(e.queue, &queuedJob{job: Job{ID: e.nextJobID, Work: 1}})
}

// SetTarget 是决策器驱动 worker 数量的唯一入口。
// 扩容：新 worker 进入 warming，WarmupTicks 后才可接活（启动有延迟）。
// 缩容：只抽调 active；忙的 worker 进入 draining 等在途任务完成，空闲 worker 立即下线。
func (e *Executor) SetTarget(target int) {
	if target < e.cap.MinWorkers {
		target = e.cap.MinWorkers
	}
	if target > e.cap.MaxWorkers {
		target = e.cap.MaxWorkers
	}
	serving := len(e.active) + len(e.warming)
	switch {
	case target > serving:
		for i := serving; i < target; i++ {
			e.warming = append(e.warming, &worker{warmLeft: e.cap.WarmupTicks})
		}
	case target < serving:
		// 优先撤销尚未启动的扩容；不够再抽调 active（忙的进 draining）。
		remove := serving - target
		if len(e.warming) >= remove {
			e.warming = e.warming[:len(e.warming)-remove]
			return
		}
		remove -= len(e.warming)
		e.warming = e.warming[:0]
		kept := make([]*worker, 0, len(e.active)-remove)
		for _, w := range e.active {
			if remove > 0 {
				e.draining = append(e.draining, w)
				remove--
			} else {
				kept = append(kept, w)
			}
		}
		e.active = kept
	}
}

// Process 推进一个时间单位：启动完成 -> 派单 -> 处理 -> 排空下线 -> 等待计时。
// 接纳在 Admit 中单独完成，便于观测器分别统计。
func (e *Executor) Process() int {
	e.completedThisTick = 0

	stillWarming := e.warming[:0]
	for _, w := range e.warming {
		w.warmLeft--
		if w.warmLeft <= 0 {
			e.active = append(e.active, w)
		} else {
			stillWarming = append(stillWarming, w)
		}
	}
	e.warming = stillWarming

	head := 0
	for _, w := range e.active {
		if w.job == nil && head < len(e.queue) {
			q := e.queue[head]
			head++
			w.job = &q.job
			w.remaining = q.job.Work
		}
	}
	if head > 0 {
		e.queue = e.queue[head:]
	}

	e.runWorkers(e.active)
	e.runWorkers(e.draining)

	stillDraining := e.draining[:0]
	for _, w := range e.draining {
		if w.job != nil {
			stillDraining = append(stillDraining, w)
		}
	}
	e.draining = stillDraining

	for _, q := range e.queue {
		q.waited++
	}

	return e.completedThisTick
}

func (e *Executor) runWorkers(workers []*worker) {
	for _, w := range workers {
		if w.job == nil {
			continue
		}
		w.remaining--
		if w.remaining <= 0 {
			w.job = nil
			e.completedThisTick++
		}
	}
}
