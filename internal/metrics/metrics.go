package metrics

import "time"

type Snapshot struct {
	At time.Duration

	Ready       int
	Starting    int
	Busy        int
	Draining    int
	QueueLength int
	QueueCap    int

	ArrivalsInWindow    int
	CompletionsInWindow int
	RejectionsInWindow  int
	ArrivalRate         float64
	MeanServiceTime     time.Duration
}

type Counters struct {
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

func (c *Counters) RecordArrival() {
	c.Submitted++
	c.windowArrivals++
}

func (c *Counters) RecordRejection() {
	c.Rejected++
	c.windowRejections++
}

func (c *Counters) RecordCompletion(wait time.Duration) {
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

func (c *Counters) RotateWindow() (arrivals, completions, rejections int) {
	arrivals = c.windowArrivals
	completions = c.windowCompletions
	rejections = c.windowRejections
	c.windowArrivals = 0
	c.windowCompletions = 0
	c.windowRejections = 0
	return
}

func (c *Counters) MeanWait() time.Duration {
	if c.Completed == 0 {
		return 0
	}
	return c.TotalWait / time.Duration(c.Completed)
}
