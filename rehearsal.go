package troupe

import (
	"context"
	"sync"
	"time"
)

// Rehearsal 确定性测试运行时（DESIGN §4.7，对齐 troupe.cr Rehearsal 语义）：
//   - 固定 namespace：不读环境/工作目录、不落盘，任意目录与进程状态下行为一致；
//   - Memory Provider（Props + ownership fencing）；
//   - 可注入 VirtualClock（固定起始时刻，显式 Advance）；
//   - Manual 调度：Pump/PumpAll 显式驱动，消除 goroutine 竞争。
type Rehearsal struct {
	Clock     *VirtualClock
	Stage     *Stage
	Namespace string
	Store     *MemoryPropStore
	Ownership *MemoryOwnershipStore

	mu     sync.Mutex
	events []StageEvent
}

func NewRehearsal(namespace string, config ...StageConfig) *Rehearsal {
	if namespace == "" {
		namespace = "rehearsal"
	}
	clock := NewVirtualClock()
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryPropStore(ownership)
	cfg := DefaultStageConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	r := &Rehearsal{
		Clock:     clock,
		Namespace: namespace,
		Store:     store,
		Ownership: ownership,
	}
	r.Stage = NewStage(
		WithClock(clock),
		WithStore(store),
		WithOwnership(ownership),
		WithManualScheduler(),
		WithConfig(cfg),
		WithName("rehearsal:"+namespace),
	)
	r.Stage.OnEvent(func(ev StageEvent) {
		r.mu.Lock()
		r.events = append(r.events, ev)
		r.mu.Unlock()
	})
	return r
}

// Actor 在固定 namespace 下构造 ActorId。
func (r *Rehearsal) Actor(role, key string) ActorID {
	return MustActorID(r.Namespace, role, key)
}

// Call 便捷入口（等价 r.Stage.Call）。
func (r *Rehearsal) Call(ctx context.Context, role, key, method string, args any, opts ...CallOption) (*CallFuture, error) {
	return r.Stage.Call(ctx, r.Actor(role, key), method, args, opts...)
}

// Advance 推进虚拟时钟（deadline/sweep 判定随之变化）。
func (r *Rehearsal) Advance(d time.Duration) { r.Clock.Advance(d) }

// Pump 处理某 Actor 至多一个 Turn；返回是否执行。
func (r *Rehearsal) Pump(id ActorID) bool { return r.Stage.Pump(id) }

// PumpAll 处理所有 slot 直到无待处理 Turn；返回处理数量。
func (r *Rehearsal) PumpAll(maxTurns ...int) int { return r.Stage.PumpAll(maxTurns...) }

// Sweep Intermission：空闲超 IdleTimeout 的 Running slot 下线。
func (r *Rehearsal) Sweep() []ActorID { return r.Stage.Sweep() }

// Register 注册角色 manifest。
func (r *Rehearsal) Register(m manifestBase) error { return r.Stage.Register(m) }

// Events 返回已记录的指定类型事件。
func (r *Rehearsal) Events(t EventType) []StageEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []StageEvent
	for _, ev := range r.events {
		if ev.Type == t {
			out = append(out, ev)
		}
	}
	return out
}

// AllEvents 返回全部已记录事件。
func (r *Rehearsal) AllEvents() []StageEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]StageEvent{}, r.events...)
}
