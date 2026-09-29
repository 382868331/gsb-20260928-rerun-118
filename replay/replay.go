package replay

// BurstThenFalloff 构造“突发后快速回落”的到达序列（单位：请求/tick）。
// baseline ticks 低负载 -> burstTicks 高负载突发 -> 每 falloffTicks 减半回落 -> 回到 baseline 保持 tailTicks。
func BurstThenFalloff(baseline, burst, baselineTicks, burstTicks, falloffTicks, tailTicks int) []int {
	arrivals := make([]int, 0, baselineTicks+burstTicks+falloffTicks*4+tailTicks)
	for range baselineTicks {
		arrivals = append(arrivals, baseline)
	}
	for range burstTicks {
		arrivals = append(arrivals, burst)
	}
	level := burst
	for level > baseline {
		level = (level + 1) / 2
		if level < baseline {
			level = baseline
		}
		for range falloffTicks {
			arrivals = append(arrivals, level)
		}
	}
	for range tailTicks {
		arrivals = append(arrivals, baseline)
	}
	return arrivals
}
