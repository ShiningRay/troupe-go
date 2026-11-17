package troupe

import (
	"sync"
	"time"
)

// Clock 可注入时钟：deadline、Intermission 与虚拟时间均经由 Clock
// （DESIGN §5；Rehearsal 用虚拟时钟保证判定确定性）。
type Clock interface {
	Now() time.Time
}

// SystemClock 真实时钟（UTC）。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// VirtualClock 虚拟时钟：Rehearsal 使用固定起始时刻，测试显式推进，
// 保证 deadline/sweep 判定确定性（不读系统时间）。
type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewVirtualClock(start ...time.Time) *VirtualClock {
	if len(start) > 0 {
		return &VirtualClock{now: start[0]}
	}
	return &VirtualClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *VirtualClock) AdvanceTo(instant time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = instant
}
