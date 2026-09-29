package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/executor"
)

func main() {
	min := flag.Int("min", 2, "最小工作者数")
	max := flag.Int("max", 8, "最大工作者数")
	queueCap := flag.Int("queue", 8, "有界等待队列容量")
	startup := flag.Duration("startup", 2*time.Second, "工作者启动延迟")
	tick := flag.Duration("tick", time.Second, "观测与决策周期")
	cooldown := flag.Duration("cooldown", 3*time.Second, "两次扩缩容之间的冷却期")
	seed := flag.Int64("seed", 42, "重放随机种子")
	flag.Parse()

	jobs := replayTraffic(*seed)
	strategy := decision.NewQueueRateStrategy(*min, *max)
	engine := executor.New(executor.Config{
		MinWorkers:   *min,
		MaxWorkers:   *max,
		QueueCap:     *queueCap,
		StartupDelay: *startup,
		Tick:         *tick,
		Cooldown:     *cooldown,
	}, strategy)

	logs, counters, err := engine.Run(jobs)

	fmt.Printf("重放请求总数: %d\n", len(jobs))
	for _, logEntry := range logs {
		if logEntry.Action == decision.ActionHold {
			continue
		}
		fmt.Printf("[%s] %s %d -> %d: %s\n",
			logEntry.At, logEntry.Action, logEntry.From, logEntry.To, logEntry.Reason)
	}
	fmt.Println()
	fmt.Printf("实际完成: %d\n", counters.Completed)
	fmt.Printf("明确拒绝: %d\n", counters.Rejected)
	fmt.Printf("曾等待:   %d\n", counters.Waited)
	fmt.Printf("平均等待: %s，最大等待: %s\n", counters.MeanWait(), counters.MaxWait)
	if err != nil {
		fmt.Printf("校验失败: %v\n", err)
	} else {
		fmt.Println("账目校验: 完成 + 拒绝 = 提交，且重放结束无在途/排队请求")
	}
}

// replayTraffic 生成“突发后快速回落”的确定性流量：
// 0-4s 低流量；4-8s 高强度突发；8s 后立即回落到低流量。
func replayTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	phases := []struct {
		start    time.Duration
		end      time.Duration
		rate     float64
		service  time.Duration
		jitterMS int
	}{
		{0, 4 * time.Second, 2.0, 450 * time.Millisecond, 120},
		{4 * time.Second, 8 * time.Second, 11.0, 450 * time.Millisecond, 40},
		{8 * time.Second, 16 * time.Second, 1.0, 450 * time.Millisecond, 180},
	}
	for _, phase := range phases {
		interval := time.Duration(float64(time.Second) / phase.rate)
		for at := phase.start; at < phase.end; at += interval {
			jitter := time.Duration(rng.Intn(2*phase.jitterMS+1)-phase.jitterMS) * time.Millisecond
			arrival := at + jitter
			if arrival < phase.start {
				arrival = phase.start
			}
			service := phase.service + time.Duration(rng.Intn(101)-50)*time.Millisecond
			jobs = append(jobs, executor.Job{Arrival: arrival, Service: service})
		}
	}
	return jobs
}
