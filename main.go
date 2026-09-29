package main

import (
	"fmt"

	"autoscale/controller"
	"autoscale/replay"
)

func main() {
	cap := controller.Capacity{
		MinWorkers:    2,
		MaxWorkers:    8,
		WarmupTicks:   2,
		HardQueue:     24,
		RedQueueRatio: 0.75,
		RedShedRatio:  0.5,
	}

	// 突发 18/tick 持续 8 tick，随后每 2 tick 减半快速回落。
	arrivals := replay.BurstThenFalloff(1, 18, 5, 8, 2, 10)

	strategy := controller.NewQueueStrategy(cap, controller.DefaultQueueStrategyConfig())
	eng := controller.NewEngine(cap, strategy, 2)

	for _, n := range arrivals {
		eng.Step(n)
	}
	// 流量结束后继续跑到系统排空，使“完成+拒绝”可与提交总数对齐。
	for eng.Snapshot().InFlight > 0 {
		eng.Step(0)
	}

	fmt.Println("== 扩缩容决策依据（观测 -> 决策 -> 执行）==")
	for _, d := range eng.Decisions {
		s := d.Observed
		fmt.Printf("t=%-3d 目标 %d->%d | active=%d warm=%d drain=%d busy=%d queued=%d 完成=%d 拒绝=%d 到达EMA=%.2f\n",
			d.Tick, d.Current, d.Target,
			s.WorkersActive, s.WorkersWarm, s.WorkersDrain, s.Busy, s.Queued,
			s.Completed, s.Rejected, s.ArrivalEMA)
		for _, why := range d.Reasons {
			fmt.Printf("        - %s\n", why)
		}
	}

	r := eng.Result()
	fmt.Println("\n== 重放核对（突发后快速回落）==")
	fmt.Printf("提交=%d  接纳=%d  完成=%d  拒绝=%d  残留(排队/在途)=%d  模拟时长=%d tick\n",
		r.Submitted, r.Accepted, r.Completed, r.Rejected, r.Pending, r.Ticks)
	fmt.Printf("累计排队等待=%d 任务·tick（队列长度按 tick 积分；排空后在队等待=%d）\n",
		r.CumulativeWait, r.TotalWait)
	fmt.Printf("守恒检查 完成+拒绝+残留 = %d（应等于提交 %d）\n",
		r.Completed+r.Rejected+r.Pending, r.Submitted)
	fmt.Printf("接纳=完成+残留 : %d = %d+%d = %d\n",
		r.Accepted, r.Completed, r.Pending, r.Completed+r.Pending)
}
