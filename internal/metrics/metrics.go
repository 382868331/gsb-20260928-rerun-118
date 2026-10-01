package metrics

import "time"

// ClassSnapshot 是某一类工作者在一个观测周期末的状态。
type ClassSnapshot struct {
	Name string

	Ready    int // 已就绪（空闲 + 忙碌）
	Starting int // 在途扩容：已决定扩容但仍在启动延迟中
	Busy     int
	Draining int // 已标记缩容、只完成手头在途工作

	QueueLength int
	QueueCap    int

	ArrivalsInWindow    int
	CompletionsInWindow int
	RejectionsInWindow  int
	ArrivalRate         float64
	MeanServiceTime     time.Duration
}

// Snapshot 是一次决策周期内所有类别的观测。
type Snapshot struct {
	At      time.Duration
	Classes []ClassSnapshot
}

// ClassCounters 记录单个类别的完成/等待/拒绝账目。
type ClassCounters struct {
	Name      string
	Submitted int
	Completed int
	Rejected  int
	Waited    int

	TotalWait time.Duration
	MaxWait   time.Duration

	windowArrivals    int
	windowCompletions int
	windowRejections  int
}

func (c *ClassCounters) RecordArrival() {
	c.Submitted++
	c.windowArrivals++
}

func (c *ClassCounters) RecordRejection() {
	c.Rejected++
	c.windowRejections++
}

func (c *ClassCounters) RecordCompletion(wait time.Duration) {
	c.Completed++
	c.windowCompletions++
	if wait > 0 {
		c.Waited++
		c.TotalWait += wait
		if wait > c.MaxWait {
			c.MaxWait = wait
		}
	}
}

func (c *ClassCounters) RotateWindow() (arrivals, completions, rejections int) {
	arrivals = c.windowArrivals
	completions = c.windowCompletions
	rejections = c.windowRejections
	c.windowArrivals = 0
	c.windowCompletions = 0
	c.windowRejections = 0
	return
}

func (c *ClassCounters) MeanWait() time.Duration {
	if c.Completed == 0 {
		return 0
	}
	return c.TotalWait / time.Duration(c.Completed)
}

// Counters 汇总全局账目，并按类别细分。
type Counters struct {
	Total   ClassCounters
	Classes []*ClassCounters
	byName  map[string]*ClassCounters
}

func NewCounters(classNames []string) *Counters {
	c := &Counters{byName: map[string]*ClassCounters{}}
	for _, name := range classNames {
		cc := &ClassCounters{Name: name}
		c.Classes = append(c.Classes, cc)
		c.byName[name] = cc
	}
	return c
}

func (c *Counters) Class(name string) *ClassCounters {
	return c.byName[name]
}

func (c *Counters) RecordArrival(class string) {
	c.Total.RecordArrival()
	if cc, ok := c.byName[class]; ok {
		cc.RecordArrival()
	}
}

func (c *Counters) RecordRejection(class string) {
	c.Total.RecordRejection()
	if cc, ok := c.byName[class]; ok {
		cc.RecordRejection()
	}
}

func (c *Counters) RecordCompletion(class string, wait time.Duration) {
	c.Total.RecordCompletion(wait)
	if cc, ok := c.byName[class]; ok {
		cc.RecordCompletion(wait)
	}
}

// RotateWindow 轮换所有类别的窗口计数，返回与 NewCounters 顺序一致的分片。
func (c *Counters) RotateWindow() (arrivals, completions, rejections []int) {
	c.Total.RotateWindow()
	for _, cc := range c.Classes {
		a, done, r := cc.RotateWindow()
		arrivals = append(arrivals, a)
		completions = append(completions, done)
		rejections = append(rejections, r)
	}
	return
}
