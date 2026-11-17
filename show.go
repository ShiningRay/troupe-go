package troupe

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ---- Show realtime（M4）：在线尽力通知——Actor Audience 经完整 Turn 交付
// （白名单/串行/持久化语义不变），边界回调（非 Actor 观察者）与 Actor 订阅分离。
// 不把 Show 称为持久流：持久化诉求走 Reminder/Outbox/持久流（PLAN M5）。----

// PublishReport 发布结果（在线尽力：失败放弃不重投；离线不保证）。
type PublishReport struct {
	Delivered int // Actor Audience 经 Turn 交付成功 + 边界回调成功
	Failed    int // 失败放弃（离线/队列满/超时/回调 panic）
}

type actorAudience struct {
	stage  *Stage // 显式绑定 Stage（跨 Stage Audience）
	id     ActorID
	method string
}

type externalAudience struct {
	fn func(payload json.RawMessage)
}

// Show 单个 topic 的订阅集合。
type Show struct {
	hub       *ShowHub
	topic     string
	mu        sync.Mutex
	actors    []actorAudience
	externals []externalAudience
}

// ActorSubscribe 订阅 Actor Audience：交付经同一 Stage 管道（完整 Turn）。
func (s *Show) ActorSubscribe(id ActorID, method string) {
	s.ActorSubscribeOn(s.hub.defaultStage, id, method)
}

// ActorSubscribeOn 显式绑定 Stage 订阅（跨 Stage Audience：同一 topic 的
// Audience 可各自绑定 Stage，交付按各自订阅的 Stage 进行）。
func (s *Show) ActorSubscribeOn(stage *Stage, id ActorID, method string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actors = append(s.actors, actorAudience{stage: stage, id: id, method: method})
}

// ExternalSubscribe 边界回调订阅（非 Actor 观察者；直接通知，panic 隔离）。
func (s *Show) ExternalSubscribe(fn func(payload json.RawMessage)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.externals = append(s.externals, externalAudience{fn: fn})
}

// UnsubscribeActor 移除指定 Actor 的全部订阅。
func (s *Show) UnsubscribeActor(id ActorID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.actors[:0]
	removed := 0
	for _, a := range s.actors {
		if a.id == id {
			removed++
			continue
		}
		kept = append(kept, a)
	}
	s.actors = kept
	return removed
}

// ShowHub topic 管理与发布。
type ShowHub struct {
	defaultStage *Stage
	mu           sync.Mutex
	topics       map[string]*Show
}

func NewShowHub(stage *Stage) *ShowHub {
	return &ShowHub{defaultStage: stage, topics: make(map[string]*Show)}
}

// Show 返回 topic（不存在则创建）。
func (h *ShowHub) Show(topic string) *Show {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.topics[topic]; ok {
		return s
	}
	s := &Show{hub: h, topic: topic}
	h.topics[topic] = s
	return s
}

// Publish 在线尽力发布：Actor Audience 经完整 Turn 交付（带短 deadline），
// 边界回调直接通知（panic 隔离）；失败放弃不重投（离线不保证）。
func (h *ShowHub) Publish(topic string, payload any) PublishReport {
	return h.PublishWithTimeout(topic, payload, 2*time.Second)
}

// PublishWithTimeout 同 Publish，可指定 Actor 交付的响应期限。
func (h *ShowHub) PublishWithTimeout(topic string, payload any, timeout time.Duration) PublishReport {
	h.mu.Lock()
	s, ok := h.topics[topic]
	h.mu.Unlock()
	if !ok {
		return PublishReport{}
	}
	raw := json.RawMessage("null")
	if payload != nil {
		if rm, isRaw := payload.(json.RawMessage); isRaw {
			raw = rm
		} else if b, err := json.Marshal(payload); err == nil {
			raw = b
		}
	}

	s.mu.Lock()
	actors := append([]actorAudience{}, s.actors...)
	externals := append([]externalAudience{}, s.externals...)
	s.mu.Unlock()

	report := PublishReport{}
	ctxbg := context.Background()
	for _, a := range actors {
		// Stage 停机/排空/队列满/超时 → failed（离线不保证；背压隔离不影响其他订阅者）
		f, err := a.stage.Call(ctxbg, a.id, a.method, raw,
			WithTimeout(timeout), WithDeadline(time.Now().Add(timeout)))
		if err != nil {
			report.Failed++
			continue
		}
		if _, err := f.Await(ctxbg); err != nil {
			report.Failed++
			continue
		}
		report.Delivered++
	}
	for _, e := range externals {
		func() {
			defer func() {
				if recover() != nil {
					report.Failed++ // 回调 panic 隔离：不影响其他订阅者
				}
			}()
			e.fn(raw)
			report.Delivered++
		}()
	}
	return report
}

// Topics 返回 topic 列表（排序）。
func (h *ShowHub) Topics() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.topics))
	for t := range h.topics {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// ---- Director 运维面（M4）：多 Stage 聚合健康/指标/事件；管理写操作
// （drain/shutdown）仅经 Director API，不经 HTTP。----

// Director 聚合多 Stage 的健康与观测。
type Director struct {
	mu           sync.Mutex
	stages       map[string]*Stage
	order        []string
	recentEvents []StageEvent
	maxEvents    int
}

func NewDirector() *Director {
	return &Director{
		stages:    make(map[string]*Stage),
		maxEvents: 200, // recent_events 环形上限（内存有界）
	}
}

// AddStage 注册 Stage 并接管其事件流（生命周期事件进 recent_events 环）。
func (d *Director) AddStage(name string, stage *Stage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.stages[name]; ok {
		return fmt.Errorf("Stage %s 已注册", name)
	}
	d.stages[name] = stage
	d.order = append(d.order, name)
	prev := stage.eventHandler
	stage.OnEvent(func(ev StageEvent) {
		d.mu.Lock()
		d.recentEvents = append(d.recentEvents, ev)
		if len(d.recentEvents) > d.maxEvents {
			d.recentEvents = d.recentEvents[len(d.recentEvents)-d.maxEvents:]
		}
		d.mu.Unlock()
		if prev != nil {
			prev(ev)
		}
	})
	return nil
}

// StageNames 返回注册的 Stage 名（按注册顺序）。
func (d *Director) StageNames() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string{}, d.order...)
}

// StageHealth 单 Stage 健康快照。
type StageHealth struct {
	Name        string `json:"name"`
	Status      string `json:"status"` // running | draining | closed
	Slots       int    `json:"slots"`
	Activations int    `json:"activations"`
}

// Health 聚合健康（running/draining/closed）。
func (d *Director) Health() []StageHealth {
	d.mu.Lock()
	names := append([]string{}, d.order...)
	stages := make(map[string]*Stage, len(d.stages))
	for k, v := range d.stages {
		stages[k] = v
	}
	d.mu.Unlock()
	out := make([]StageHealth, 0, len(names))
	for _, name := range names {
		st := stages[name]
		h := StageHealth{Name: name, Status: "running"}
		switch {
		case st.Closed():
			h.Status = "closed"
		case st.Draining():
			h.Status = "draining"
		}
		h.Slots = st.SlotCount()
		h.Activations = st.ActorCount()
		out = append(out, h)
	}
	return out
}

// StageMetricsSnapshot 单 Stage 计数快照（含 activation_failures/save_failures/
// turns_expired/turns_failed 等故障与未知结果可见性）。
func (d *Director) StageMetricsSnapshot(name string) (map[string]uint64, bool) {
	d.mu.Lock()
	st, ok := d.stages[name]
	d.mu.Unlock()
	if !ok {
		return nil, false
	}
	return st.Metrics().Snapshot(), true
}

// RecentEvents 返回最近事件（环形有界；最多 max 条）。
func (d *Director) RecentEvents(max int) []StageEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	events := d.recentEvents
	if max > 0 && len(events) > max {
		events = events[len(events)-max:]
	}
	return append([]StageEvent{}, events...)
}

// DrainStage 优雅排空指定 Stage（管理操作：仅 Director API，不经 HTTP）。
func (d *Director) DrainStage(name string) error {
	d.mu.Lock()
	st, ok := d.stages[name]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("Stage %s 未注册", name)
	}
	st.BeginDrain()
	return nil
}

// ShutdownStage 停机指定 Stage（管理操作：仅 Director API）。
func (d *Director) ShutdownStage(name string) error {
	d.mu.Lock()
	st, ok := d.stages[name]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("Stage %s 未注册", name)
	}
	st.Close()
	return nil
}
