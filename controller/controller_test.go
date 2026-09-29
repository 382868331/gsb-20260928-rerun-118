package controller

import (
	"testing"
)

func testCap() Capacity {
	return Capacity{
		MinWorkers:    2,
		MaxWorkers:    6,
		WarmupTicks:   2,
		HardQueue:     10,
		RedQueueRatio: 0.8,
		RedShedRatio:  0.5,
	}
}

func TestConservationUnderOverload(t *testing.T) {
	cap := testCap()
	eng := NewEngine(cap, NewQueueStrategy(cap, DefaultQueueStrategyConfig()), 1)

	// 持续超出上限的负载，制造硬上限拒绝。
	for range 20 {
		eng.Step(20)
	}
	for eng.Snapshot().InFlight > 0 {
		eng.Step(0)
	}

	r := eng.Result()
	if r.Completed+r.Rejected+r.Pending != r.Submitted {
		t.Fatalf("请求不守恒: 提交=%d 完成=%d 拒绝=%d 残留=%d",
			r.Submitted, r.Completed, r.Rejected, r.Pending)
	}
	if r.Accepted != r.Completed+r.Pending {
		t.Fatalf("接纳口径错误: 接纳=%d 完成=%d 残留=%d", r.Accepted, r.Completed, r.Pending)
	}
	if r.Rejected == 0 {
		t.Fatal("过载场景应当出现按规则拒绝")
	}
	if r.Pending != 0 {
		t.Fatalf("排空后不应有残留, got %d", r.Pending)
	}
}

func TestHardCapRejectsImmediately(t *testing.T) {
	e := NewExecutor(testCap())
	// 初始 2 worker。一次性灌入远超硬上限的请求。
	accepted, rejected := e.Admit(50)
	if accepted+rejected != 50 {
		t.Fatalf("每个请求都必须有结论: 接纳=%d 拒绝=%d", accepted, rejected)
	}
	if got := len(e.queue) + len(e.active); got > testCap().HardQueue {
		t.Fatalf("在途超过硬上限: %d > %d", got, testCap().HardQueue)
	}
	if rejected == 0 {
		t.Fatal("达到硬上限必须拒绝，不能藏请求")
	}
}

func TestWarmupDelay(t *testing.T) {
	e := NewExecutor(testCap())
	if len(e.active) != 2 {
		t.Fatalf("初始 active 应为 2, got %d", len(e.active))
	}
	e.SetTarget(4)
	if len(e.warming) != 2 || len(e.active) != 2 {
		t.Fatalf("扩容应先进 warming: warm=%d active=%d", len(e.warming), len(e.active))
	}
	e.Process()
	if len(e.warming) != 2 {
		t.Fatalf("第 1 tick 仍在启动, warm=%d", len(e.warming))
	}
	e.Process()
	if len(e.warming) != 0 || len(e.active) != 4 {
		t.Fatalf("第 2 tick 应启动完成: warm=%d active=%d", len(e.warming), len(e.active))
	}
}

func TestDrainWaitsForInFlight(t *testing.T) {
	c := testCap()
	c.MinWorkers = 1
	e := NewExecutor(c) // 启动 4 个 worker，下限 1
	e.SetTarget(4)
	e.Process()
	e.Process() // 等启动延迟结束，4 个全部 active
	// 直接放入需要 2 tick 的在途任务，使缩容发生时 worker 仍忙。
	e.queue = append(e.queue,
		&queuedJob{job: Job{ID: 1, Work: 2}},
		&queuedJob{job: Job{ID: 2, Work: 2}},
	)
	e.Process() // 派单：2 个 worker 开始处理，还需 1 tick
	e.SetTarget(2)
	if len(e.draining) != 2 {
		t.Fatalf("忙 worker 缩容应进入 draining, got %d", len(e.draining))
	}
	// draining worker 还剩 1 tick 在途任务，必须等它完成才能下线。
	e.Process()
	if len(e.active) != 2 {
		t.Fatalf("active 应为 2, got %d", len(e.active))
	}
	if len(e.draining) != 0 {
		t.Fatalf("在途排空后 draining 应为 0, got %d", len(e.draining))
	}
}

func TestNoScaleWhileWarmingOrDraining(t *testing.T) {
	cap := testCap()
	st := NewQueueStrategy(cap, DefaultQueueStrategyConfig())
	// 有 warming worker 时，即使排队很高也必须保持目标，不能反复改。
	snap := Snapshot{Tick: 10, WorkersActive: 2, WorkersWarm: 2, Queued: 99}
	target, reasons := st.Decide(snap)
	if target != 4 {
		t.Fatalf("启动中应保持现有 serving 目标 4, got %d", target)
	}
	if len(reasons) == 0 {
		t.Fatal("每次决策必须给出依据")
	}
}

func TestEveryDecisionExplained(t *testing.T) {
	cap := testCap()
	eng := NewEngine(cap, NewQueueStrategy(cap, DefaultQueueStrategyConfig()), 2)
	for range 30 {
		eng.Step(15)
	}
	if len(eng.Decisions) == 0 {
		t.Fatal("应当产生决策记录")
	}
	for _, d := range eng.Decisions {
		if len(d.Reasons) == 0 {
			t.Fatalf("tick %d 的决策缺少依据", d.Tick)
		}
	}
}
