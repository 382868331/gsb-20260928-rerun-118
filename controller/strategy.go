package controller

import (
	"fmt"
	"math"
)

// Strategy 决策策略接口。后续可增加不同策略（如固定利用率、预测式等），
// 只需实现该接口并在引擎中替换，执行与观测部分不变。
type Strategy interface {
	// Decide 根据上一周期观测决定目标 serving worker 数，必须给出人类可读理由。
	Decide(snap Snapshot) (target int, reasons []string)
}

// QueueStrategyConfig 首轮策略参数：按排队与处理能力调节。
type QueueStrategyConfig struct {
	// 高水位：Queued >= active*HighQueueRatio 视为积压，需要扩容。
	HighQueueRatio float64
	// 低水位：连续 LowTicks 个周期 Queued <= active*LowQueueRatio 且到达率低于处理能力才缩容。
	LowQueueRatio float64
	LowTicks      int
	// 扩容步长上限，避免一次拉满。
	MaxStepUp int
	// 任何一次扩缩后至少间隔 CooldownTicks 才能再次调整（含等待启动/排空完成）。
	CooldownTicks int
	// 目标按 EMA 到达率需要的 worker = ceil(ArrivalEMA * (1 + Headroom))。
	Headroom float64
}

func DefaultQueueStrategyConfig() QueueStrategyConfig {
	return QueueStrategyConfig{
		HighQueueRatio: 1.0,
		LowQueueRatio:  0.25,
		LowTicks:       3,
		MaxStepUp:      2,
		CooldownTicks:  4,
		Headroom:       0.25,
	}
}

// QueueStrategy 首轮策略。
type QueueStrategy struct {
	cfg     QueueStrategyConfig
	cap     Capacity
	lastAct int // 上一次真正调整目标的 tick
	lowRun  int // 连续低水位计数
}

func NewQueueStrategy(cap Capacity, cfg QueueStrategyConfig) *QueueStrategy {
	return &QueueStrategy{cfg: cfg, cap: cap, lastAct: -1 << 30}
}

func (s *QueueStrategy) Decide(snap Snapshot) (int, []string) {
	serving := snap.WorkersActive + snap.WorkersWarm
	// 有 worker 尚在启动或尚未排空时不做新决策，避免与在途的扩缩动作互相打架。
	if snap.WorkersWarm > 0 || snap.WorkersDrain > 0 {
		return serving, []string{fmt.Sprintf(
			"保持 %d：%d 个 worker 启动中、%d 个缩容排空中，等待上一轮动作结算",
			serving, snap.WorkersWarm, snap.WorkersDrain)}
	}
	if snap.Tick-s.lastAct < s.cfg.CooldownTicks {
		return serving, []string{fmt.Sprintf(
			"保持 %d：距上次扩缩仅 %d tick，冷却期 %d tick 未到，防止震荡",
			serving, snap.Tick-s.lastAct, s.cfg.CooldownTicks)}
	}

	highLine := float64(serving) * s.cfg.HighQueueRatio
	lowLine := math.Max(0, float64(serving)*s.cfg.LowQueueRatio)

	// 扩容判据：队列越过单 worker 高水位（排队已超出当前服务能力）。
	if float64(snap.Queued) >= highLine && serving < s.cap.MaxWorkers {
		demand := int(math.Ceil(snap.ArrivalEMA * (1 + s.cfg.Headroom)))
		target := serving + s.cfg.MaxStepUp
		if target < demand {
			target = demand
		}
		if target > s.cap.MaxWorkers {
			target = s.cap.MaxWorkers
		}
		if target <= serving {
			return serving, []string{"保持：已达 worker 上限，靠声明的拒绝规则保护系统"}
		}
		s.lastAct = snap.Tick
		s.lowRun = 0
		return target, []string{
			fmt.Sprintf("扩容 %d->%d：排队 %d >= 高水位 %.1f", serving, target, snap.Queued, highLine),
			fmt.Sprintf("到达率 EMA %.2f，含 %.0f%% 余量需 %d worker，单轮最多 +%d",
				snap.ArrivalEMA, s.cfg.Headroom*100, demand, s.cfg.MaxStepUp),
		}
	}

	// 缩容判据：连续低水位，且到达率确实低于当前处理能力，且没有在途积压。
	if float64(snap.Queued) <= lowLine &&
		snap.ArrivalEMA < float64(serving) &&
		snap.Busy < serving && serving > s.cap.MinWorkers {
		s.lowRun++
		if s.lowRun >= s.cfg.LowTicks {
			target := serving - 1
			demand := int(math.Ceil(snap.ArrivalEMA * (1 + s.cfg.Headroom)))
			if target < demand {
				target = demand
			}
			if target < s.cap.MinWorkers {
				target = s.cap.MinWorkers
			}
			if target >= serving {
				s.lowRun = 0
				return serving, []string{"保持：按到达率计算无需缩容"}
			}
			s.lastAct = snap.Tick
			s.lowRun = 0
			return target, []string{
				fmt.Sprintf("缩容 %d->%d：连续 %d tick 排队 %.0f 以下（当前 %d）",
					serving, target, s.cfg.LowTicks, lowLine, snap.Queued),
				fmt.Sprintf("到达率 EMA %.2f < 服务能力 %d，被缩 worker 的在途任务会先排空",
					snap.ArrivalEMA, serving),
			}
		}
		return serving, []string{fmt.Sprintf(
			"观察中：低水位已持续 %d/%d tick，未达缩容阈值，避免突发刚回落就缩",
			s.lowRun, s.cfg.LowTicks)}
	}

	s.lowRun = 0
	return serving, []string{fmt.Sprintf(
		"保持 %d：排队 %d 处于低水位 %.0f 与高水位 %.0f 之间（迟滞带）",
		serving, snap.Queued, lowLine, highLine)}
}
