package decision

import (
	"fmt"
	"time"

	"resource-controller/internal/metrics"
)

type Action string

const (
	ActionScaleUp   Action = "scale_up"
	ActionScaleDown Action = "scale_down"
	ActionHold      Action = "hold"
)

// Log 是一条可落盘的决策依据：时间、类别、动作、容量变化与可读原因。
type Log struct {
	At     time.Duration
	Class  string
	Action Action
	From   int
	To     int
	Reason string
}

// Strategy 按工作者类别分别决策；Snapshot 携带该类别的队列、
// 启动中、忙碌等观测以及该类别的 Min/Max 约束。
type Strategy interface {
	Name() string
	Decide(metrics.Snapshot) (desired int, action Action, reason string)
}

// QueueRateStrategy 同时看该类别队列高水位、平滑到达速率、
// 估计所需工作者数，并把仍在启动延迟中的在途扩容计入已规划容量。
type QueueRateStrategy struct {
	ScaleUpStep   int
	LowDwellTicks int
	RateAlpha     float64

	classes map[string]*queueRateState
}

type queueRateState struct {
	lowDwell        int
	ewmaArrivalRate float64
	firstSample     bool
}

func NewQueueRateStrategy() *QueueRateStrategy {
	return &QueueRateStrategy{
		ScaleUpStep:   2,
		LowDwellTicks: 2,
		RateAlpha:     0.5,
		classes:       map[string]*queueRateState{},
	}
}

func (s *QueueRateStrategy) Name() string { return "queue-rate" }

func (s *QueueRateStrategy) state(class string) *queueRateState {
	st, ok := s.classes[class]
	if !ok {
		st = &queueRateState{}
		s.classes[class] = st
	}
	return st
}

func (s *QueueRateStrategy) Decide(in metrics.Snapshot) (int, Action, string) {
	st := s.state(in.Class)
	current := in.Ready + in.Starting
	if !st.firstSample {
		st.ewmaArrivalRate = in.ArrivalRate
		st.firstSample = true
	} else {
		st.ewmaArrivalRate = s.RateAlpha*in.ArrivalRate + (1-s.RateAlpha)*st.ewmaArrivalRate
	}

	requiredByRate := int(st.ewmaArrivalRate*float64(in.MeanServiceTime) + 0.999999)
	if requiredByRate < 0 {
		requiredByRate = 0
	}
	highWatermark := in.QueueCap / 2
	pressureTarget := in.Busy + (in.QueueLength+1)/2

	switch {
	case current >= in.Max && (in.QueueLength > 0 || requiredByRate > current):
		return current, ActionHold, fmt.Sprintf(
			"类别 %s 已达上限 %d：速率需 %d 人、队列 %d/%d，新增容量不可得，继续按准入规则拒绝",
			in.Class, in.Max, requiredByRate, in.QueueLength, in.QueueCap)

	case in.Starting > 0 && (in.QueueLength >= highWatermark || requiredByRate > current):
		return current, ActionHold, fmt.Sprintf(
			"类别 %s 有 %d 名工作者仍在启动（启动有延迟），已规划容量 %d；速率需 %d 人、队列 %d/%d，等待在途扩容兑现",
			in.Class, in.Starting, current, requiredByRate, in.QueueLength, in.QueueCap)

	case in.QueueLength >= highWatermark || requiredByRate > current:
		target := requiredByRate
		if pressureTarget > target {
			target = pressureTarget
		}
		if current+s.ScaleUpStep > target {
			target = current + s.ScaleUpStep
		}
		if target > in.Max {
			target = in.Max
		}
		st.lowDwell = 0
		return target, ActionScaleUp, fmt.Sprintf(
			"类别 %s 队列 %d/%d 达到高水位 %d，或速率需 %d 人 > 当前 %d；本窗口到达 %d 完成 %d，扩容到 %d",
			in.Class, in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current,
			in.ArrivalsInWindow, in.CompletionsInWindow, target)

	case in.QueueLength == 0 && requiredByRate < current:
		st.lowDwell++
		if in.Starting > 0 {
			return current, ActionHold, fmt.Sprintf(
				"类别 %s 队列已空且速率需 %d 人，但 %d 人尚未启动完成；本轮不反向缩容，防止刚扩又缩",
				in.Class, requiredByRate, in.Starting)
		}
		if st.lowDwell < s.LowDwellTicks {
			return current, ActionHold, fmt.Sprintf(
				"类别 %s 队列已空且速率需 %d 人 < 当前 %d；低流量需连续确认 %d 轮，当前 %d 轮，避免震荡",
				in.Class, requiredByRate, current, s.LowDwellTicks, st.lowDwell)
		}
		target := requiredByRate
		if target < in.Min {
			target = in.Min
		}
		return target, ActionScaleDown, fmt.Sprintf(
			"类别 %s 连续 %d 轮队列空、平滑到达速率对应 %d 人 < 当前 %d；空闲者立即停，忙碌者完成在途工作后退出",
			in.Class, st.lowDwell, requiredByRate, current)

	default:
		st.lowDwell = 0
		return current, ActionHold, fmt.Sprintf(
			"类别 %s 队列 %d/%d 未达高水位 %d，速率需 %d 人，当前 %d 人（启动中 %d），维持容量",
			in.Class, in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current, in.Starting)
	}
}

// TotalQueueStrategy 是天真的对照策略：只看该类别的队列总长度，
// 不看到达速率，也不把仍在启动延迟中的在途扩容计入容量，
// 因此启动延迟期间容易重复扩容，队列清空后也容易立刻缩容。
type TotalQueueStrategy struct {
	ScaleUpStep   int
	LowDwellTicks int

	lowDwell map[string]int
}

func NewTotalQueueStrategy() *TotalQueueStrategy {
	return &TotalQueueStrategy{
		ScaleUpStep:   2,
		LowDwellTicks: 1,
		lowDwell:      map[string]int{},
	}
}

func (s *TotalQueueStrategy) Name() string { return "queue-only" }

func (s *TotalQueueStrategy) Decide(in metrics.Snapshot) (int, Action, string) {
	highWatermark := in.QueueCap / 2
	// 只数已就绪的工作者，刻意忽略 in.Starting 的在途扩容。
	ready := in.Ready

	switch {
	case in.QueueLength >= highWatermark:
		target := ready + s.ScaleUpStep
		if in.QueueLength > in.QueueCap*3/4 {
			target = ready + 2*s.ScaleUpStep
		}
		if target > in.Max {
			target = in.Max
		}
		if target <= ready {
			return ready, ActionHold, fmt.Sprintf(
				"类别 %s 队列 %d/%d 已达高水位，但已就绪 %d 人触及上限 %d，只能继续拒绝",
				in.Class, in.QueueLength, in.QueueCap, ready, in.Max)
		}
		s.lowDwell[in.Class] = 0
		return target, ActionScaleUp, fmt.Sprintf(
			"类别 %s 队列 %d/%d 达到高水位 %d，按已就绪 %d 人扩容到 %d（不看速率、不计在途扩容 %d 人）",
			in.Class, in.QueueLength, in.QueueCap, highWatermark, ready, target, in.Starting)

	case in.QueueLength == 0:
		s.lowDwell[in.Class]++
		if s.lowDwell[in.Class] < s.LowDwellTicks {
			return ready, ActionHold, fmt.Sprintf(
				"类别 %s 队列刚清空，等待 %d 轮确认（当前 %d 轮）",
				in.Class, s.LowDwellTicks, s.lowDwell[in.Class])
		}
		target := in.Busy
		if target < in.Min {
			target = in.Min
		}
		if target >= ready {
			return ready, ActionHold, fmt.Sprintf(
				"类别 %s 队列空但 %d 人仍忙碌，已就绪 %d 人无需调整",
				in.Class, in.Busy, ready)
		}
		return target, ActionScaleDown, fmt.Sprintf(
			"类别 %s 队列已空，只保留忙碌 %d 人与下限 %d，缩到 %d（不看速率趋势）",
			in.Class, in.Busy, in.Min, target)

	default:
		s.lowDwell[in.Class] = 0
		return ready, ActionHold, fmt.Sprintf(
			"类别 %s 队列 %d/%d 未达高水位 %d，维持已就绪 %d 人",
			in.Class, in.QueueLength, in.QueueCap, highWatermark, ready)
	}
}
