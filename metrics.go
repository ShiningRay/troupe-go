package troupe

import (
	"log/slog"
	"sync"
)

// StageMetrics 结构化 lifecycle/call 计数器（对齐 troupe.cr 命名）：
// activations / activation_failures / deactivations / turns_started /
// turns_completed / turns_failed / turns_expired / save_failures，
// 以及按角色的 rejections/<role>。emit 单点更新；Snapshot 供 Director/运维读取。
type StageMetrics struct {
	mu       sync.Mutex
	counters map[string]uint64
	roles    map[string]map[string]uint64
}

func newStageMetrics() *StageMetrics {
	return &StageMetrics{
		counters: make(map[string]uint64),
		roles:    make(map[string]map[string]uint64),
	}
}

// Counter 返回指定计数器当前值（不存在为 0）。
func (m *StageMetrics) Counter(name string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

// RoleCounter 返回指定角色下指定拒绝计数当前值。
func (m *StageMetrics) RoleCounter(role, name string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.roles[role][name]
}

// Inc 原子递增。
func (m *StageMetrics) Inc(name string) {
	m.mu.Lock()
	m.counters[name]++
	m.mu.Unlock()
}

// IncRole 原子递增角色维度计数。
func (m *StageMetrics) IncRole(role, name string) {
	m.mu.Lock()
	if m.roles[role] == nil {
		m.roles[role] = make(map[string]uint64)
	}
	m.roles[role][name]++
	m.mu.Unlock()
}

// Snapshot 返回全部计数器副本。
func (m *StageMetrics) Snapshot() map[string]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]uint64, len(m.counters))
	for k, v := range m.counters {
		out[k] = v
	}
	return out
}

// RoleSnapshot 返回全部角色维度计数副本。
func (m *StageMetrics) RoleSnapshot() map[string]map[string]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]map[string]uint64, len(m.roles))
	for role, counters := range m.roles {
		cp := make(map[string]uint64, len(counters))
		for k, v := range counters {
			cp[k] = v
		}
		out[role] = cp
	}
	return out
}

// updateMetrics 由 Stage.emit 单点调用（事件 → 计数映射，对齐 troupe.cr）。
func (m *StageMetrics) update(ev StageEvent) {
	switch ev.Type {
	case EventActivated:
		m.Inc("activations")
	case EventActivationFailed:
		m.Inc("activation_failures")
	case EventDeactivated:
		m.Inc("deactivations")
	case EventTurnStarted:
		m.Inc("turns_started")
	case EventTurnEnded:
		if ev.Detail == "ok" {
			m.Inc("turns_completed")
		} else {
			m.Inc("turns_failed")
		}
	case EventTurnExpired:
		m.Inc("turns_expired")
	case EventSaveFailed:
		m.Inc("save_failures")
	case EventCallRejected:
		m.IncRole(ev.ActorID.Role, "rejections")
	}
}

// SlogSink 把结构化 StageEvent 打到 slog（logger 自定 level/handler）。
// 返回值直接交给 Stage.OnEvent；注意 slog.Logger 并发安全。
func SlogSink(logger *slog.Logger) func(StageEvent) {
	return func(ev StageEvent) {
		attrs := []slog.Attr{
			slog.String("actor", ev.ActorID.Canonical()),
		}
		if ev.Method != "" {
			attrs = append(attrs, slog.String("method", ev.Method))
		}
		if ev.Detail != "" {
			attrs = append(attrs, slog.String("detail", ev.Detail))
		}
		logger.LogAttrs(nil, slog.LevelInfo, string(ev.Type), attrs...)
	}
}
