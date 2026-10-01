package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"resource-controller/internal/decision"
	"resource-controller/internal/executor"
	"resource-controller/internal/metrics"
)

func main() {
	tick := flag.Duration("tick", time.Second, "观测与决策周期")
	cooldown := flag.Duration("cooldown", 3*time.Second, "同类两次扩缩容之间的冷却期")
	seed := flag.Int64("seed", 42, "重放随机种子")
	outDir := flag.String("out", "out", "决策依据保存目录")
	flag.Parse()

	classes := []executor.ClassConfig{
		{Name: "short", MinWorkers: 2, MaxWorkers: 8, QueueCap: 8, StartupDelay: 2 * time.Second},
		{Name: "long", MinWorkers: 1, MaxWorkers: 3, QueueCap: 4, StartupDelay: 4 * time.Second},
	}
	limits := []decision.ClassLimit{
		{Name: "short", Min: 2, Max: 8},
		{Name: "long", Min: 1, Max: 3},
	}
	strategies := []decision.Strategy{
		decision.NewQueueRateStrategy(limits),
		decision.NewTotalQueueStrategy(limits),
	}

	jobs := replayTraffic(*seed)
	fmt.Printf("重放请求总数: %d（同一流量、同一随机种子，逐策略重放）\n\n", len(jobs))

	type result struct {
		name     string
		counters *metrics.Counters
		err      error
	}
	var results []result
	for _, strategy := range strategies {
		engine := executor.New(executor.Config{
			Classes:  classes,
			Tick:     *tick,
			Cooldown: *cooldown,
		}, strategy)
		logs, counters, err := engine.Run(jobs)
		results = append(results, result{strategy.Name(), counters, err})

		path := filepath.Join(*outDir, fmt.Sprintf("decisions-%s.log", strategy.Name()))
		if werr := saveDecisionLog(path, logs); werr != nil {
			fmt.Printf("保存决策依据失败: %v\n", werr)
		} else {
			fmt.Printf("策略 %s 的决策依据已保存到 %s（%d 条）\n", strategy.Name(), path, len(logs))
		}
	}
	fmt.Println()

	fmt.Println("=== 策略对比（同一份流量重放） ===")
	for _, r := range results {
		fmt.Printf("策略 %s:\n", r.name)
		for _, cc := range r.counters.Classes {
			fmt.Printf("  类别 %s：提交 %d，完成 %d，拒绝 %d，曾等待 %d，平均等待 %s，最大等待 %s\n",
				cc.Name, cc.Submitted, cc.Completed, cc.Rejected, cc.Waited, cc.MeanWait(), cc.MaxWait)
		}
		fmt.Printf("  合计：提交 %d，完成 %d，拒绝 %d\n",
			r.counters.Total.Submitted, r.counters.Total.Completed, r.counters.Total.Rejected)
		if r.err != nil {
			fmt.Printf("  校验失败: %v\n", r.err)
		} else {
			fmt.Println("  账目校验: 各类别 完成 + 拒绝 = 提交，且重放结束无在途/排队请求")
		}
	}
	fmt.Println("差异解释：对照决策依据文件中的每条 Reason，可定位扩容时机、在途扩容扣除与缩容驻留的不同。")
}

func saveDecisionLog(path string, logs []decision.Log) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, entry := range logs {
		if entry.Action == decision.ActionHold {
			continue
		}
		if _, err := fmt.Fprintf(f, "[%s] 类别 %s %s %d -> %d: %s\n",
			entry.At, entry.Class, entry.Action, entry.From, entry.To, entry.Reason); err != nil {
			return err
		}
	}
	return nil
}

// replayTraffic 生成确定性混合流量，覆盖三类场景：
// 1. short 类 4-8s 的短任务突发；
// 2. long 类全程的长任务占用；
// 3. long 类 10-14s 持续超上限，触发容量达上限后的明确拒绝。
func replayTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	add := func(class string, start, end time.Duration, rate float64, service time.Duration, jitterMS int) {
		interval := time.Duration(float64(time.Second) / rate)
		for at := start; at < end; at += interval {
			jitter := time.Duration(rng.Intn(2*jitterMS+1)-jitterMS) * time.Millisecond
			arrival := at + jitter
			if arrival < start {
				arrival = start
			}
			svc := service + time.Duration(rng.Intn(101)-50)*time.Millisecond
			jobs = append(jobs, executor.Job{Class: class, Arrival: arrival, Service: svc})
		}
	}

	// 短任务突发：低流量 -> 强突发 -> 快速回落。
	add("short", 0, 4*time.Second, 2.0, 300*time.Millisecond, 120)
	add("short", 4*time.Second, 8*time.Second, 11.0, 300*time.Millisecond, 40)
	add("short", 8*time.Second, 18*time.Second, 1.0, 300*time.Millisecond, 180)

	// 长任务占用：全程稳定长任务，考验 draining 与启动延迟更长的类别。
	add("long", 0, 18*time.Second, 0.8, 2500*time.Millisecond, 200)

	// 容量达上限：long 类上限 3 人、队列 4，10-14s 的到达强度远超承载，必然产生拒绝。
	add("long", 10*time.Second, 14*time.Second, 4.0, 2500*time.Millisecond, 50)

	return jobs
}
