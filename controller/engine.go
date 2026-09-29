package controller

// Engine 串起观测、决策、执行三部分，按 tick 推进离散事件重放。
type Engine struct {
	exec     *Executor
	obs      *observer
	strategy Strategy
	cap      Capacity

	// DecideEveryTicks 是决策周期（控制周期可比 tick 粗）。
	DecideEveryTicks int

	Decisions []Decision
	tick      int
}

func NewEngine(cap Capacity, strategy Strategy, decideEveryTicks int) *Engine {
	if decideEveryTicks < 1 {
		decideEveryTicks = 1
	}
	return &Engine{
		exec:             NewExecutor(cap),
		obs:              newObserver(cap),
		strategy:         strategy,
		cap:              cap,
		DecideEveryTicks: decideEveryTicks,
	}
}

// Step 处理一个 tick：arrivals 为该 tick 到达的请求数。
func (g *Engine) Step(arrivals int) {
	accepted, rejected := g.exec.Admit(arrivals)
	g.obs.recordArrival(arrivals, accepted, rejected)
	completed := g.exec.Process()
	g.obs.recordCompleted(completed)
	g.obs.recordQueue(len(g.exec.queue))
	g.tick++

	if g.tick%g.DecideEveryTicks == 0 {
		snap := g.obs.snapshot(g.tick, g.exec)
		target, reasons := g.strategy.Decide(snap)
		g.exec.SetTarget(target)
		g.Decisions = append(g.Decisions, Decision{
			Tick: g.tick, At: snap.At, Current: snap.WorkersActive + snap.WorkersWarm,
			Target: target, Reasons: reasons, Observed: snap,
		})
	}
}

func (g *Engine) Snapshot() Snapshot {
	return g.obs.snapshot(g.tick, g.exec)
}

// Result 是重放结束后的核对口径：所有请求要么完成，要么被明确拒绝，
// 要么仍在系统内（排队/在途），三者之和必须等于提交总数。
type Result struct {
	Submitted      int64
	Accepted       int64
	Completed      int64
	Rejected       int64
	Pending        int64
	TotalWait      int64
	CumulativeWait int64
	Ticks          int
}

func (g *Engine) Result() Result {
	s := g.Snapshot()
	return Result{
		Submitted:      s.Submitted,
		Accepted:       s.Accepted,
		Completed:      s.Completed,
		Rejected:       s.Rejected,
		Pending:        int64(s.InFlight),
		TotalWait:      s.TotalWait,
		CumulativeWait: s.CumulativeWait,
		Ticks:          g.tick,
	}
}
