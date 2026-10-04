package main

import (
	"encoding/json"
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

// replayRecord 是落盘的决策依据：同一流量、同一配置下某策略的完整决策轨迹与账目。
type replayRecord struct {
	Scenario string                      `json:"scenario"`
	Strategy string                      `json:"strategy"`
	Seed     int64                       `json:"seed"`
	Jobs     int                         `json:"jobs"`
	Logs     []decision.Log              `json:"logs"`
	TaskLogs []executor.TaskLog          `json:"task_logs"`
	PerClass map[string]metrics.Counters `json:"per_class"`
	Total    metrics.Counters            `json:"total"`
}

// scenarioSpec 把流量与注入的失联/确认事件绑定为一个可重放场景。
type scenarioSpec struct {
	jobs     func(int64) []executor.Job
	faults   []executor.Fault
	confirms []executor.Confirmation
}

func main() {
	outDir := flag.String("out", "out", "决策依据输出目录")
	seed := flag.Int64("seed", 42, "重放随机种子")
	scenario := flag.String("scenario", "all", "重放场景：burst|long|cap|all")
	verbose := flag.Bool("v", false, "打印每条非 hold 决策")
	flag.Parse()

	strategies := map[string]func() decision.Strategy{
		"queue-rate": func() decision.Strategy { return decision.NewQueueRateStrategy() },
		"queue-only": func() decision.Strategy { return decision.NewTotalQueueStrategy() },
	}
	strategyOrder := []string{"queue-rate", "queue-only"}

	scenarios := map[string]scenarioSpec{
		"burst":    {jobs: burstTraffic},
		"long":     {jobs: longTraffic},
		"cap":      {jobs: capTraffic},
		"failover": {jobs: failoverTraffic, faults: failoverFaults(), confirms: failoverConfirms()},
	}
	scenarioOrder := []string{"burst", "long", "cap", "failover"}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, scenarioName := range scenarioOrder {
		if *scenario != "all" && *scenario != scenarioName {
			continue
		}
		spec := scenarios[scenarioName]
		jobs := spec.jobs(*seed)
		results := map[string]executor.Result{}
		for _, strategyName := range strategyOrder {
			engine := executor.New(classConfigs(), strategies[strategyName]())
			result, err := engine.RunWithEvents(jobs, spec.faults, spec.confirms)
			if err != nil {
				fmt.Printf("场景 %s 策略 %s 校验失败: %v\n", scenarioName, strategyName, err)
				continue
			}
			results[strategyName] = result
			record := replayRecord{
				Scenario: scenarioName, Strategy: strategyName, Seed: *seed,
				Jobs: len(jobs), Logs: result.Logs, TaskLogs: result.TaskLogs,
				PerClass: result.PerClass, Total: result.Total,
			}
			path := filepath.Join(*outDir, scenarioName+"-"+strategyName+".json")
			data, _ := json.MarshalIndent(record, "", "  ")
			if err := os.WriteFile(path, data, 0o644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if *verbose {
				printDecisions(scenarioName, strategyName, result)
			}
		}
		if len(results) == len(strategyOrder) {
			compare(scenarioName, jobs, strategyOrder, results)
		}
	}
}

// classConfigs 两类工作者：cpu 通用、gpu 专用（启动更慢、上限更低）。
// gpu 预先声明 cpu 为兼容替代范围；cpu 未声明任何替代，
// 因此 cpu 任务永远不会被交给 gpu 实例。
func classConfigs() executor.Config {
	return executor.Config{
		Classes: map[string]executor.ClassConfig{
			"cpu": {Min: 2, Max: 8, QueueCap: 8, StartupDelay: 2 * time.Second},
			"gpu": {Min: 1, Max: 3, QueueCap: 4, StartupDelay: 3 * time.Second, Substitutes: []string{"cpu"}},
		},
		Tick:     time.Second,
		Cooldown: 3 * time.Second,
	}
}

type phaseSpec struct {
	start, end time.Duration
	rate       float64
	class      string
	service    time.Duration
	jitterMS   int
}

func buildJobs(rng *rand.Rand, jobs []executor.Job, phases []phaseSpec) []executor.Job {
	for _, phase := range phases {
		interval := time.Duration(float64(time.Second) / phase.rate)
		for at := phase.start; at < phase.end; at += interval {
			jitter := time.Duration(rng.Intn(2*phase.jitterMS+1)-phase.jitterMS) * time.Millisecond
			arrival := at + jitter
			if arrival < phase.start {
				arrival = phase.start
			}
			service := phase.service + time.Duration(rng.Intn(101)-50)*time.Millisecond
			jobs = append(jobs, executor.Job{Arrival: arrival, Service: service, Class: phase.class})
		}
	}
	return jobs
}

// burstTraffic 短任务突发：0-4s 低流量，4-8s 两类同时突发短任务，
// 8-10s 短暂回落后 10-14s 二次突发，检验策略是否过早缩容。
func burstTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	jobs = buildJobs(rng, jobs, []phaseSpec{
		{0, 4 * time.Second, 2.0, "cpu", 450 * time.Millisecond, 120},
		{0, 4 * time.Second, 0.5, "gpu", 600 * time.Millisecond, 100},
		{4 * time.Second, 8 * time.Second, 11.0, "cpu", 300 * time.Millisecond, 40},
		{4 * time.Second, 8 * time.Second, 3.0, "gpu", 400 * time.Millisecond, 60},
		{8 * time.Second, 10 * time.Second, 1.0, "cpu", 450 * time.Millisecond, 180},
		{8 * time.Second, 10 * time.Second, 0.4, "gpu", 600 * time.Millisecond, 150},
		{10 * time.Second, 14 * time.Second, 9.0, "cpu", 300 * time.Millisecond, 50},
		{10 * time.Second, 14 * time.Second, 2.5, "gpu", 400 * time.Millisecond, 60},
		{14 * time.Second, 20 * time.Second, 1.0, "cpu", 450 * time.Millisecond, 180},
		{14 * time.Second, 20 * time.Second, 0.4, "gpu", 600 * time.Millisecond, 150},
	})
	return jobs
}

// longTraffic 长任务占用：一批长 gpu 任务集中到达，长期占住专用容量。
func longTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	for i := 0; i < 6; i++ {
		jobs = append(jobs, executor.Job{
			Arrival: 2*time.Second + time.Duration(i)*400*time.Millisecond,
			Service: 6*time.Second + time.Duration(rng.Intn(2000))*time.Millisecond,
			Class:   "gpu",
		})
	}
	jobs = buildJobs(rng, jobs, []phaseSpec{
		{0, 14 * time.Second, 1.5, "cpu", 400 * time.Millisecond, 150},
	})
	return jobs
}

// capTraffic 容量上限：gpu 到达速率持续超过 gpu 上限处理能力，必须明确拒绝。
func capTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	jobs = buildJobs(rng, jobs, []phaseSpec{
		{0, 12 * time.Second, 2.0, "cpu", 400 * time.Millisecond, 120},
		{time.Second, 10 * time.Second, 2.5, "gpu", 2500 * time.Millisecond, 80},
	})
	return jobs
}

// failoverTraffic 人工流量：gpu 到达持续超过类别上限（容量耗尽、明确拒绝），
// cpu 在 6 秒后流量停止触发低流量缩容，与工作者失联同时发生。
func failoverTraffic(seed int64) []executor.Job {
	rng := rand.New(rand.NewSource(seed))
	var jobs []executor.Job
	jobs = buildJobs(rng, jobs, []phaseSpec{
		{0, 6 * time.Second, 6.0, "cpu", 400 * time.Millisecond, 120},
		{time.Second, 10 * time.Second, 2.5, "gpu", 2500 * time.Millisecond, 80},
	})
	return jobs
}

// failoverFaults 在 gpu 容量耗尽期间让一名忙碌 gpu 工作者失联。
func failoverFaults() []executor.Fault {
	return []executor.Fault{
		{At: 5 * time.Second, Class: "gpu", Busy: true},
	}
}

// failoverConfirms 操作者核实旧实例效应后确认，未知任务才继续。
func failoverConfirms() []executor.Confirmation {
	return []executor.Confirmation{
		{At: 9500 * time.Millisecond, Class: "gpu"},
	}
}

func printDecisions(scenario, strategy string, result executor.Result) {
	fmt.Printf("== 场景 %s / 策略 %s ==\n", scenario, strategy)
	for _, entry := range result.Logs {
		if entry.Action == decision.ActionHold {
			continue
		}
		fmt.Printf("[%6s] %-4s %-10s %d -> %d: %s\n",
			entry.At, entry.Class, entry.Action, entry.From, entry.To, entry.Reason)
	}
	for _, entry := range result.TaskLogs {
		if entry.Action == "assign" || entry.Action == "complete" {
			continue
		}
		fmt.Printf("[%6s] %-4s %-14s 任务%d 实例%d: %s\n",
			entry.At, entry.Class, entry.Action, entry.Job, entry.Worker, entry.Detail)
	}
	fmt.Println()
}

// compare 重放相同流量后解释两个策略的实际差异。
func compare(scenario string, jobs []executor.Job, order []string, results map[string]executor.Result) {
	a, b := results[order[0]], results[order[1]]
	fmt.Printf("== 场景 %s：%d 个请求，相同流量下策略对比 ==\n", scenario, len(jobs))
	fmt.Printf("%-12s %8s %8s %8s %8s %10s %10s\n", "策略", "完成", "拒绝", "未知", "等待", "平均等待", "最大等待")
	for _, name := range order {
		t := results[name].Total
		fmt.Printf("%-12s %8d %8d %8d %8d %10s %10s\n", name, t.Completed, t.Rejected, t.Unknown, t.Waited, t.MeanWait(), t.MaxWait)
	}
	for _, class := range []string{"cpu", "gpu"} {
		ca, cb := a.PerClass[class], b.PerClass[class]
		if ca.Rejected != cb.Rejected {
			fmt.Printf("类别 %s 拒绝差：%s 拒绝 %d，%s 拒绝 %d\n",
				class, order[0], ca.Rejected, order[1], cb.Rejected)
		}
	}
	fmt.Println("差异解释：")
	for _, line := range explainDiff(order[0], a, order[1], b) {
		fmt.Println(" - " + line)
	}
	fmt.Println()
}

// explainDiff 找到两条决策轨迹的第一个分叉点，并汇总账目差异。
func explainDiff(nameA string, a executor.Result, nameB string, b executor.Result) []string {
	var lines []string
	diverged := false
	n := len(a.Logs)
	if len(b.Logs) < n {
		n = len(b.Logs)
	}
	for i := 0; i < n; i++ {
		la, lb := a.Logs[i], b.Logs[i]
		if la.Class == lb.Class && (la.Action != lb.Action || la.To != lb.To) {
			lines = append(lines, fmt.Sprintf(
				"首次分叉在 %s 类别 %s：%s 选择 %s->%d（%s）；%s 选择 %s->%d（%s）",
				la.At, la.Class, nameA, la.Action, la.To, la.Reason,
				nameB, lb.Action, lb.To, lb.Reason))
			diverged = true
			break
		}
	}
	if !diverged {
		lines = append(lines, "两策略决策轨迹完全一致")
	}
	ta, tb := a.Total, b.Total
	if ta.Rejected != tb.Rejected {
		lines = append(lines, fmt.Sprintf("拒绝差 %d：在途扩容与速率估计改变了突发期的有效容量", tb.Rejected-ta.Rejected))
	}
	if ta.MeanWait() != tb.MeanWait() {
		lines = append(lines, fmt.Sprintf("平均等待差 %s：扩容时点不同导致排队深度不同", tb.MeanWait()-ta.MeanWait()))
	}
	if ta.Rejected == tb.Rejected && ta.MeanWait() == tb.MeanWait() {
		lines = append(lines, "最终账目一致，差异仅体现在容量调整时点上")
	}
	return lines
}
