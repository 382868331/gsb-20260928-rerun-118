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

// Log 记录一次带类别与可读依据的决策，用于重放时解释行为差异。
type Log struct {
	At     time.Duration
	Class  string
	Action Action
	From   int
	To     int
	Reason string
}

// ClassDecision 是策略对单个类别的容量结论。
type ClassDecision struct {
	Class   string
	Desired int
	Action  Action
	Reason  string
}

// Strategy 可插拔：每轮观测后给出各类别的目标容量与依据。
type Strategy interface {
	Name() string
	Decide(metrics.Snapshot) []ClassDecision
}

// ClassLimit 描述一个类别的容量边界。
type ClassLimit struct {
	Name string
	Min  int
	Max  int
}

func limitOf(limits map[string]ClassLimit, name string) (min, max int) {
	l, ok := limits[name]
	if !ok {
		return 1, 1
	}
	return l.Min, l.Max
}

func limitsMap(limits []ClassLimit) map[string]ClassLimit {
	m := make(map[string]ClassLimit, len(limits))
	for _, l := range limits {
		m[l.Name] = l
	}
	return m
}

// QueueRateStrategy 分类别决策：同时看该类的排队水位、平滑到达速率、
// 估计所需工作者数，并显式扣除仍在启动延迟中的在途扩容。
type QueueRateStrategy struct {
	limits        map[string]ClassLimit
	ScaleUpStep   int
	LowDwellTicks int
	RateAlpha     float64
	states        map[string]*rateState
}

type rateState struct {
	lowDwell int
	ewmaRate float64
	sampled  bool
}

func NewQueueRateStrategy(limits []ClassLimit) *QueueRateStrategy {
	return &QueueRateStrategy{
		limits:        limitsMap(limits),
		ScaleUpStep:   2,
		LowDwellTicks: 2,
		RateAlpha:     0.5,
		states:        map[string]*rateState{},
	}
}

func (s *QueueRateStrategy) Name() string { return "queue-rate" }

func (s *QueueRateStrategy) stateFor(class string) *rateState {
	st, ok := s.states[class]
	if !ok {
		st = &rateState{}
		s.states[class] = st
	}
	return st
}

func (s *QueueRateStrategy) Decide(in metrics.Snapshot) []ClassDecision {
	decisions := make([]ClassDecision, 0, len(in.Classes))
	for _, class := range in.Classes {
		decisions = append(decisions, s.decideClass(class))
	}
	return decisions
}

func (s *QueueRateStrategy) decideClass(in metrics.ClassSnapshot) ClassDecision {
	min, max := limitOf(s.limits, in.Name)
	st := s.stateFor(in.Name)
	current := in.Ready + in.Starting

	if !st.sampled {
		st.ewmaRate = in.ArrivalRate
		st.sampled = true
	} else {
		st.ewmaRate = s.RateAlpha*in.ArrivalRate + (1-s.RateAlpha)*st.ewmaRate
	}

	requiredByRate := int(st.ewmaRate*float64(in.MeanServiceTime) + 0.999999)
	if requiredByRate < 0 {
		requiredByRate = 0
	}
	highWatermark := in.QueueCap / 2
	pressureTarget := in.Busy + (in.QueueLength+1)/2

	hold := func(reason string) ClassDecision {
		return ClassDecision{Class: in.Name, Desired: current, Action: ActionHold, Reason: reason}
	}

	switch {
	case current >= max && (in.QueueLength > 0 || requiredByRate > current):
		return hold(fmt.Sprintf(
			"类别 %s 已达上限 %d：速率需 %d 人、队列 %d/%d，新增容量不可得，继续按准入规则拒绝",
			in.Name, max, requiredByRate, in.QueueLength, in.QueueCap))

	case in.Starting > 0 && (in.QueueLength >= highWatermark || requiredByRate > current):
		return hold(fmt.Sprintf(
			"类别 %s 有 %d 名在途扩容仍在启动延迟中，已规划容量 %d；速率需 %d 人、队列 %d/%d，等待启动兑现而非重复扩容",
			in.Name, in.Starting, current, requiredByRate, in.QueueLength, in.QueueCap))

	case in.QueueLength >= highWatermark || requiredByRate > current:
		target := requiredByRate
		if pressureTarget > target {
			target = pressureTarget
		}
		if current+s.ScaleUpStep > target {
			target = current + s.ScaleUpStep
		}
		if target > max {
			target = max
		}
		st.lowDwell = 0
		return ClassDecision{Class: in.Name, Desired: target, Action: ActionScaleUp, Reason: fmt.Sprintf(
			"类别 %s 队列 %d/%d 达到高水位 %d，或速率需 %d 人 > 当前 %d（含在途 %d）；本窗口到达 %d 完成 %d，扩容到 %d",
			in.Name, in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current,
			in.Starting, in.ArrivalsInWindow, in.CompletionsInWindow, target)}

	case in.QueueLength == 0 && requiredByRate < current:
		st.lowDwell++
		if in.Starting > 0 {
			return hold(fmt.Sprintf(
				"类别 %s 队列已空且速率需 %d 人，但 %d 名在途扩容尚未就绪；本轮不反向缩容，防止刚扩又缩",
				in.Name, requiredByRate, in.Starting))
		}
		if st.lowDwell < s.LowDwellTicks {
			return hold(fmt.Sprintf(
				"类别 %s 队列已空且速率需 %d 人 < 当前 %d；低流量需连续确认 %d 轮，当前 %d 轮，避免震荡",
				in.Name, requiredByRate, current, s.LowDwellTicks, st.lowDwell))
		}
		target := requiredByRate
		if target < min {
			target = min
		}
		return ClassDecision{Class: in.Name, Desired: target, Action: ActionScaleDown, Reason: fmt.Sprintf(
			"类别 %s 连续 %d 轮队列空、平滑速率对应 %d 人 < 当前 %d；空闲者立即停，忙碌者完成在途工作后退出",
			in.Name, st.lowDwell, requiredByRate, current)}

	default:
		st.lowDwell = 0
		return hold(fmt.Sprintf(
			"类别 %s 队列 %d/%d 未达高水位 %d，速率需 %d 人，当前 %d 人（在途扩容 %d），维持容量",
			in.Name, in.QueueLength, in.QueueCap, highWatermark, requiredByRate, current, in.Starting))
	}
}

// TotalQueueStrategy 是对照策略：只看总队列长度，不区分排队类型、
// 不扣除在途扩容，也不做低流量驻留确认。用于重放同一流量时对比行为差异。
type TotalQueueStrategy struct {
	limits      map[string]ClassLimit
	ScaleUpStep int
	states      map[string]int // 各类别连续空队列轮数
}

func NewTotalQueueStrategy(limits []ClassLimit) *TotalQueueStrategy {
	return &TotalQueueStrategy{
		limits:      limitsMap(limits),
		ScaleUpStep: 2,
		states:      map[string]int{},
	}
}

func (s *TotalQueueStrategy) Name() string { return "total-queue" }

func (s *TotalQueueStrategy) Decide(in metrics.Snapshot) []ClassDecision {
	totalQueue := 0
	for _, class := range in.Classes {
		totalQueue += class.QueueLength
	}

	decisions := make([]ClassDecision, 0, len(in.Classes))
	for _, class := range in.Classes {
		min, max := limitOf(s.limits, class.Name)
		current := class.Ready + class.Starting
		switch {
		case totalQueue > 0 && class.QueueLength > 0:
			// 只看队列长度：有多少排队就补多少容量，不考虑仍在启动的在途扩容。
			target := class.Ready + class.QueueLength
			if target < current+s.ScaleUpStep {
				target = current + s.ScaleUpStep
			}
			if target > max {
				target = max
			}
			s.states[class.Name] = 0
			decisions = append(decisions, ClassDecision{Class: class.Name, Desired: target, Action: ActionScaleUp,
				Reason: fmt.Sprintf(
					"总队列 %d（该类 %d）> 0，按排队数补足容量到 %d；未扣除在途扩容 %d，可能过度扩容",
					totalQueue, class.QueueLength, target, class.Starting)})
		case totalQueue == 0 && current > min:
			s.states[class.Name]++
			if s.states[class.Name] >= 1 {
				decisions = append(decisions, ClassDecision{Class: class.Name, Desired: min, Action: ActionScaleDown,
					Reason: fmt.Sprintf(
						"总队列已空，直接缩到下限 %d；不做连续低流量确认，可能在突发间隙反复抖动", min)})
				continue
			}
			decisions = append(decisions, ClassDecision{Class: class.Name, Desired: current, Action: ActionHold,
				Reason: "总队列刚清空，观察一轮"})
		default:
			s.states[class.Name] = 0
			decisions = append(decisions, ClassDecision{Class: class.Name, Desired: current, Action: ActionHold,
				Reason: fmt.Sprintf("总队列 %d，该类队列 %d，维持 %d", totalQueue, class.QueueLength, current)})
		}
	}
	return decisions
}
