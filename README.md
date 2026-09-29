# 应用资源调节系统（首轮：按排队与处理能力调节）

在虚拟服务上用离散事件仿真（1 tick = 100ms）搭出观测、决策、执行三个部分，全部用 Go 实现。

- 观测 `controller/observer.go`：每个 tick 从执行器同一事实来源聚合排队数、在途数、
  忙碌 worker、到达率 EMA、完成率 EMA、累计完成/拒绝/等待，输出 `Snapshot`。
- 决策 `controller/strategy.go`：首轮 `QueueStrategy` 按“排队 vs 当前服务能力”决定 worker
  目标数，每次决策都带人类可读理由。策略是 `Strategy` 接口，后续可直接加新策略。
- 执行 `controller/executor.go`：worker 生命周期、FIFO 排队、按 tick 处理；`controller/engine.go`
  按控制周期把三者串起来。

## 关键语义

- 启动延迟：扩容的 worker 先进 `warming`，满 `WarmupTicks` 才能接活。
- 安全缩容：只抽调 active worker；忙的 worker 进 `draining`，在途任务完成后才下线，
  排空期间不接新活。
- 防震荡：任一扩缩后有 `CooldownTicks` 冷却；有 warming/draining 未结算时不改目标；
  缩容要求连续 `LowTicks` 个周期低水位且到达率低于服务能力；高低水位之间是迟滞带。
- 按声明规则拒绝（不藏请求）：容量在 `Capacity` 里显式声明。在途达到硬上限 `HardQueue`
  时全部拒绝；占用越过 `HardQueue*RedQueueRatio` 的红色区间后，按 `RedShedRatio` 比例
  确定性拒绝（累加器实现，无随机数，重放可复现）；每个请求要么接纳、要么计入拒绝。

## 运行与核对

```
go run ./...
go test ./...
```

`main.go` 重放“低负载 -> 突发 -> 每 2 tick 减半快速回落 -> 低负载”的流量，打印每次扩缩的
观测数据与依据，并核对：

- `提交 = 完成 + 拒绝 + 残留`（无请求被藏掉）；
- `接纳 = 完成 + 残留`；
- 输出实际完成数、拒绝数、排队/等待量。
