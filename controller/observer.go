package controller

// observer 观测部分：从执行器的内部状态滚动计算指标。
// 它不持有自己的“真相”，只做聚合，保证观测与执行使用同一事实来源。
type observer struct {
	cap Capacity

	submitted int64
	accepted  int64
	rejected  int64
	completed int64

	arrivalEMA float64
	doneEMA    float64
	cumWait    int64 // 每个 tick 结束时队列长度的累加（排队量的时间积分）
}

func newObserver(cap Capacity) *observer {
	return &observer{cap: cap}
}

const emaAlpha = 0.3

// recordArrival 在一个 tick 的接纳阶段调用。
func (o *observer) recordArrival(arrivals, accepted, rejected int) {
	o.submitted += int64(arrivals)
	o.accepted += int64(accepted)
	o.rejected += int64(rejected)
	if o.arrivalEMA == 0 {
		o.arrivalEMA = float64(arrivals)
	} else {
		o.arrivalEMA += emaAlpha * (float64(arrivals) - o.arrivalEMA)
	}
}

func (o *observer) recordCompleted(n int) {
	o.completed += int64(n)
	if o.doneEMA == 0 {
		o.doneEMA = float64(n)
	} else {
		o.doneEMA += emaAlpha * (float64(n) - o.doneEMA)
	}
}

// recordQueue 在每 tick 处理结束后记录当时队列长度。
func (o *observer) recordQueue(queued int) {
	o.cumWait += int64(queued)
}

// Snapshot 根据执行器当前状态生成一份可解释观测快照。
func (o *observer) snapshot(tick int, e *Executor) Snapshot {
	var busy, inFlight, totalWait int
	for _, w := range e.active {
		if w.job != nil {
			busy++
			inFlight++
		}
	}
	for _, w := range e.draining {
		if w.job != nil {
			busy++
			inFlight++
		}
	}
	for _, q := range e.queue {
		inFlight++
		totalWait += q.waited
	}
	return Snapshot{
		Tick:           tick,
		At:             tickDuration(tick),
		WorkersActive:  len(e.active),
		WorkersWarm:    len(e.warming),
		WorkersDrain:   len(e.draining),
		Busy:           busy,
		InFlight:       inFlight,
		Queued:         len(e.queue),
		Completed:      o.completed,
		Rejected:       o.rejected,
		Submitted:      o.submitted,
		Accepted:       o.accepted,
		TotalWait:      int64(totalWait),
		CumulativeWait: o.cumWait,
		ArrivalEMA:     o.arrivalEMA,
		ServiceRate:    o.doneEMA,
	}
}
