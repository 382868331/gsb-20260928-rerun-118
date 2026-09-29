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

type Log struct {
	At     time.Duration
	Action Action
	From   int
	To     int
	Reason string
}

type Strategy interface {
	Decide(metrics.Snapshot) (desired int, action Action, reason string)
}

// QueueRateStrategy 是首轮策略：同时看排队积压与到达/处理速率。
// 后续新增策略时实现 Strategy 接口即可。
type QueueRateStrategy struct {
	Min             int
	Max             int
	ScaleUpStep     int
	LowDwellTicks   int
	RateAlpha       float64
	lowDwell        int
	ewmaArrivalRate float64
	firstSample     bool
}

func NewQueueRateStrategy(min, max int) *QueueRateStrategy {
	return &QueueRateStrategy{
		Min:           min,
		Max:           max,
		ScaleUpStep:   2,
		LowDwellTicks: 2,
		RateAlpha:     0.5,
	}
}

func (s *QueueRateStrategy) Decide(in metrics.Snapshot) (int, Action, string) {
	current := in.Ready + in.Starting
	if !s.firstSample {
		s.ewmaArrivalRate = in.ArrivalRate
		s.firstSample = true
	} else {
		s.ewmaArrivalRate = s.RateAlpha*in.ArrivalRate + (1-s.RateAlpha)*s.ewmaArrivalRate
	}

	requiredByRate := int(s.ewmaArrivalRate*float64(in.MeanServiceTime) + 0.999999)
	if requiredByRate < 0 {
		requiredByRate = 0
	}
	highWatermark := in.QueueCap / 2
	pressureTarget := in.Busy + (in.QueueLength+1)/2

	switch {
	case current >= s.Max && (in.QueueLength > 0 || requiredByRate > current):
		return current, ActionHold, fmt.Sprintf(
			"已达上限 %d：速率需 %d 人、队列 %d/%d，新增容量不可得，继续按准入规则拒绝",
			s.Max, requiredByRate, in.QueueLength, in.QueueCap)

	case in.Starting > 0 && (in.QueueLength >= highWatermark || requiredByRate > current):
		return current, ActionHold, fmt.Sprintf(
			"%d 名工作者仍在启动（启动有延迟），已规划容量 %d；速率需 %d 人、队列 %d/%d，等待启动兑现",
			in.Starting, current, requiredByRate, in.QueueLength, in.QueueCap)

	case in.QueueLength >= highWatermark || requiredByRate > current:
		target := requiredByRate
		if pressureTarget > target {
			target = pressureTarget
		}
		if current+s.ScaleUpStep > target {
			target = current + s.ScaleUpStep
		}
		if target > s.Max {
			target = s.Max
		}
		s.lowDwell = 0
		return target, ActionScaleUp, fmt.Sprintf(
			"队列 %d/%d 达到高水位 %d，或速率需 %d 人 > 当前 %d；本窗口到达 %d 完成 %d，扩容到 %d",
			in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current,
			in.ArrivalsInWindow, in.CompletionsInWindow, target)

	case in.QueueLength == 0 && requiredByRate < current:
		s.lowDwell++
		if in.Starting > 0 {
			return current, ActionHold, fmt.Sprintf(
				"队列已空且速率需 %d 人，但 %d 人尚未启动完成；本轮不反向缩容，防止刚扩又缩",
				requiredByRate, in.Starting)
		}
		if s.lowDwell < s.LowDwellTicks {
			return current, ActionHold, fmt.Sprintf(
				"队列已空且速率需 %d 人 < 当前 %d；低流量需连续确认 %d 轮，当前 %d 轮，避免震荡",
				requiredByRate, current, s.LowDwellTicks, s.lowDwell)
		}
		target := requiredByRate
		if target < s.Min {
			target = s.Min
		}
		return target, ActionScaleDown, fmt.Sprintf(
			"连续 %d 轮队列空、平滑到达速率对应 %d 人 < 当前 %d；空闲者立即停，忙碌者完成在途工作后退出",
			s.lowDwell, requiredByRate, current)

	default:
		s.lowDwell = 0
		return current, ActionHold, fmt.Sprintf(
			"队列 %d/%d 未达高水位 %d，速率需 %d 人，当前 %d 人（启动中 %d），维持容量",
			in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current, in.Starting)
	}
}
