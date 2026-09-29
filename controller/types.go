package controller

import "time"

// Job 是一个被处理的负载单元。
type Job struct {
	ID       int
	ArriveAt time.Duration
	Work     int // 处理该任务需要的 worker-tick 数（>0）
}

// Decision 是决策器在一个控制周期输出的目标工作者数及理由。
type Decision struct {
	Tick     int
	At       time.Duration
	Current  int
	Target   int
	Reasons  []string
	Observed Snapshot
}

// Snapshot 是观测器在某一时刻对外暴露的可解释视图。
type Snapshot struct {
	Tick           int
	At             time.Duration
	WorkersActive  int // 已完成启动、可接活的 worker
	WorkersWarm    int // 正在启动、尚不可接活的 worker
	WorkersDrain   int // 缩容中、只处理在途任务的 worker
	Busy           int // 正在处理任务的 worker
	InFlight       int // 已接纳但尚未完成的任务（忙碌 + 排队）
	Queued         int // 排队等待的任务
	Completed      int64
	Rejected       int64
	Submitted      int64
	Accepted       int64
	TotalWait      int64 // 排队任务累计等待 tick（含在排任务已等待时长）
	CumulativeWait int64 // 重放期间队列长度按 tick 的积分（任务 tick）
	ArrivalEMA     float64
	ServiceRate    float64 // 近窗内每 tick 完成数（平滑值）
}

// Capacity 是系统声明的容量边界。
type Capacity struct {
	MinWorkers    int
	MaxWorkers    int
	WarmupTicks   int     // 新 worker 启动延迟
	HardQueue     int     // 硬上限：活跃 worker + warm worker + 排队总数不得超过该值
	RedQueueRatio float64 // 达到硬上限的该比例后进入红色区间，按比例拒绝
	RedShedRatio  float64 // 红色区间每个请求的拒绝概率
}
