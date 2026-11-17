package troupe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EventType 结构化生命周期事件（stage.OnEvent 桥接观测；metrics 计数在 M2 接入）。
type EventType string

const (
	EventActivated        EventType = "activated"
	EventActivationFailed EventType = "activation_failed"
	EventDeactivated      EventType = "deactivated"
	EventTurnStarted      EventType = "turn_started"
	EventTurnEnded        EventType = "turn_ended"
	EventTurnExpired      EventType = "turn_expired"
	EventSaveFailed       EventType = "save_failed"
	EventCallRejected     EventType = "call_rejected"
)

// StageEvent 结构化事件；handler 在持有 slot/stage 锁的路径外执行，
// 但可能来自多个 goroutine——handler 自身需并发安全，且不得回调 Stage。
type StageEvent struct {
	Type    EventType
	ActorID ActorID
	At      time.Time
	Method  string
	Detail  string
}

// StageConfig 资源上限与默认值（DESIGN §5 背压与资源上限；M1 为单机 Stage 内存预算）。
type StageConfig struct {
	MaxQueueDepth       int           // 单 Actor CallBoard 深度上限
	MaxQueueBytes       int           // 单 Actor CallBoard 字节上限
	MaxActivations      int           // Stage 激活槽上限
	DefaultCallDeadline time.Duration // 未显式给 deadline 的默认响应期限
	QueueOverheadBytes  int           // 每请求信封固定开销（入队字节预算）
	IdleTimeout         time.Duration // >0 时 Sweep 以此判定空闲下线；0 = 不启用
	// IdempotencyRetention 幂等记录保留期（过期后同键视为新操作；默认 7 天）。
	IdempotencyRetention time.Duration
}

func DefaultStageConfig() StageConfig {
	return StageConfig{
		MaxQueueDepth:        64,
		MaxQueueBytes:        256 * 1024,
		MaxActivations:       1024,
		DefaultCallDeadline:  30 * time.Second,
		QueueOverheadBytes:   256,
		IdleTimeout:          0,
		IdempotencyRetention: 7 * 24 * time.Hour,
	}
}

// Stage 应用内运行时。显式注册 manifest；call 校验角色/方法白名单后
// 排入有界 CallBoard，激活数受 MaxActivations 限制。
type Stage struct {
	clock     Clock
	store     PropStore
	ownership OwnershipStore
	mode      schedulerMode
	cfg       StageConfig
	name      string

	mu sync.Mutex

	baseCtx    context.Context // Stage 生命周期上下文（存储操作/激活共用；Close 取消）
	cancelCtx  context.CancelFunc
	repertoire map[string]manifestBase
	slots      map[ActorID]*actorSlot
	callSeq    uint64
	closed     bool

	metrics      *StageMetrics
	eventHandler func(StageEvent)
	traceHandler func(TraceEvent)
}

type StageOption func(*Stage)

func WithClock(c Clock) StageOption { return func(s *Stage) { s.clock = c } }

func WithStore(store PropStore) StageOption { return func(s *Stage) { s.store = store } }

func WithOwnership(o OwnershipStore) StageOption { return func(s *Stage) { s.ownership = o } }

// WithConfig 覆盖 StageConfig（未设字段保持 DefaultStageConfig 的值由调用方自行合并）。
func WithConfig(cfg StageConfig) StageOption { return func(s *Stage) { s.cfg = cfg } }

func WithName(name string) StageOption { return func(s *Stage) { s.name = name } }

// WithManualScheduler 关闭消费者 goroutine，由宿主/测试显式 Pump（Rehearsal 用）。
func WithManualScheduler() StageOption { return func(s *Stage) { s.mode = schedulerManual } }

func NewStage(opts ...StageOption) *Stage {
	s := &Stage{
		clock:      SystemClock{},
		cfg:        DefaultStageConfig(),
		name:       "stage",
		mode:       schedulerLive,
		repertoire: make(map[string]manifestBase),
		slots:      make(map[ActorID]*actorSlot),
		metrics:    newStageMetrics(),
	}
	s.baseCtx, s.cancelCtx = context.WithCancel(context.Background())
	ownership := NewMemoryOwnershipStore()
	s.ownership = ownership
	s.store = NewMemoryPropStore(ownership)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Stage) Store() PropStore          { return s.store }
func (s *Stage) Ownership() OwnershipStore { return s.ownership }
func (s *Stage) Clock() Clock              { return s.clock }
func (s *Stage) Config() StageConfig       { return s.cfg }
func (s *Stage) Name() string              { return s.name }

// Metrics 返回 Stage 计数器（activations/turns_completed/turns_failed 等）。
func (s *Stage) Metrics() *StageMetrics { return s.metrics }

// OnEvent 订阅结构化生命周期事件。
func (s *Stage) OnEvent(fn func(StageEvent)) { s.eventHandler = fn }

// OnTrace 订阅 Turn 边界 Trace 事件（OTel 桥接钩子；核心零 OTel 依赖）。
func (s *Stage) OnTrace(fn func(TraceEvent)) { s.traceHandler = fn }

func (s *Stage) emit(ev StageEvent) {
	s.metrics.update(ev)
	if s.eventHandler != nil {
		s.eventHandler(ev)
	}
}

// Register 显式注册角色 manifest（重复角色 → RoleConflict；白名单/迁移链校验失败 → 错误）。
func (s *Stage) Register(m manifestBase) error {
	if err := m.validateManifest(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &CallRejectedError{Reason: RejectShutdown, Detail: "Stage " + s.name + " 已停机，拒绝注册"}
	}
	if _, ok := s.repertoire[m.roleName()]; ok {
		return fmt.Errorf("角色 %s 已注册", m.roleName())
	}
	s.repertoire[m.roleName()] = m
	return nil
}

// RegisteredRoles 返回已注册角色名。
func (s *Stage) RegisteredRoles() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	roles := make([]string, 0, len(s.repertoire))
	for role := range s.repertoire {
		roles = append(roles, role)
	}
	return roles
}

// SlotCount 观测：激活槽总数（含未 Running）。
func (s *Stage) SlotCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slots)
}

// ActorCount 观测：Running 状态激活数。
func (s *Stage) ActorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, slot := range s.slots {
		if slot.stateSnapshot() == slotRunning {
			n++
		}
	}
	return n
}

// Call 位置透明的本地调用入口（M1 全部本地；远程 transport 复用同一语义管道）。
// 校验角色/白名单/参数（排队前，零副作用）后入队；队列满/停机显式拒绝。
// 返回 CallFuture；Await(ctx) 等待结果。
func (s *Stage) Call(ctx context.Context, id ActorID, method string, args any, opts ...CallOption) (*CallFuture, error) {
	var rawArgs json.RawMessage
	if args != nil {
		if rm, ok := args.(json.RawMessage); ok {
			rawArgs = rm
		} else {
			b, err := json.Marshal(args)
			if err != nil {
				return nil, &InvalidArgumentError{Detail: "参数序列化失败：" + err.Error()}
			}
			rawArgs = b
		}
	} else {
		rawArgs = json.RawMessage("null")
	}

	var o callOptions
	for _, opt := range opts {
		opt(&o)
	}

	env, slot, err := s.prepareCall(id, method, rawArgs, o)
	if err != nil {
		return nil, err
	}
	if err := slot.enqueue(env); err != nil {
		now := s.clock.Now()
		s.emit(StageEvent{Type: EventCallRejected, ActorID: id, At: now, Method: method, Detail: err.Error()})
		return nil, err
	}
	return newCallFuture(env, s.clock, s.mode == schedulerManual), nil
}

// prepareCall 锁内完成校验与请求构造；slot.enqueue 必须在 stage 锁外调用
// （manual 模式同步路径可能阻塞，持锁会死锁全部 Stage 操作）。
func (s *Stage) prepareCall(id ActorID, method string, rawArgs json.RawMessage, o callOptions) (*callEnvelope, *actorSlot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, &CallRejectedError{Reason: RejectShutdown, Actor: id.Canonical(), Detail: "Stage " + s.name + " 已停机，拒收新 Call"}
	}
	manifest, ok := s.repertoire[id.Role]
	if !ok {
		now := s.clock.Now()
		s.emit(StageEvent{Type: EventCallRejected, ActorID: id, At: now, Method: method, Detail: RejectWhitelistUnknownRole})
		return nil, nil, &UnknownRoleError{Role: id.Role}
	}
	if !manifestTurnExists(manifest, method) {
		now := s.clock.Now()
		s.emit(StageEvent{Type: EventCallRejected, ActorID: id, At: now, Method: method, Detail: "whitelist"})
		return nil, nil, &CallRejectedError{
			Reason: RejectWhitelist, Actor: id.Canonical(),
			Detail:    "方法 " + quote(method) + " 不在角色 " + id.Role + " 的白名单（生命周期/内部方法不可 RPC）",
			Retryable: false,
		}
	}
	if err := manifest.validateArgs(method, rawArgs); err != nil {
		now := s.clock.Now()
		s.emit(StageEvent{Type: EventCallRejected, ActorID: id, At: now, Method: method, Detail: "invalid_argument"})
		return nil, nil, err
	}
	s.callSeq++
	now := s.clock.Now()
	deadline := o.deadline
	if deadline.IsZero() {
		deadline = now.Add(s.cfg.DefaultCallDeadline)
		if o.timeout > 0 {
			deadline = now.Add(o.timeout)
		}
	}
	bytes := len(method) + len(rawArgs) + s.cfg.QueueOverheadBytes
	parent := o.traceparent
	if !ValidTraceparent(parent) {
		parent = "" // 非法父上下文静默降级
	}
	env := &callEnvelope{
		callID:            s.callSeq,
		method:            method,
		args:              rawArgs,
		deadline:          deadline,
		reply:             make(chan callReply, 1),
		bytes:             bytes,
		cancel:            newCancelSignal(),
		traceparent:       DeriveTraceparent(parent), // 本 Turn 自己的 span
		parentTraceparent: parent,
	}
	slot, ok := s.slots[id]
	if !ok {
		// 激活槽上限只约束新 slot：已激活 Actor 的后续调用复用既有 slot（有界不变）。
		if len(s.slots) >= s.cfg.MaxActivations {
			return nil, nil, &CallRejectedError{
				Reason: RejectBackpressure, Retryable: true, Actor: id.Canonical(),
				Detail: fmt.Sprintf("Stage activation 数 %d 达上限 %d", len(s.slots), s.cfg.MaxActivations),
			}
		}
		slot = newActorSlot(id, manifest, s.store, s.ownership, s.clock, s.cfg, s.mode, s.emit, s.emitTrace, s.baseCtx)
		s.slots[id] = slot
	}
	return env, slot, nil
}

func manifestTurnExists(m manifestBase, method string) bool {
	for _, name := range m.turnList() {
		if name == method {
			return true
		}
	}
	return false
}

// 拒绝事件的 Detail 常量（unknown_role 与 whitelist 分开便于观测过滤）。
const RejectWhitelistUnknownRole = "unknown_role"

// Pump Manual 模式：处理某 Actor 至多一个 Turn，返回是否执行。
func (s *Stage) Pump(id ActorID) bool {
	s.mu.Lock()
	slot := s.slots[id]
	s.mu.Unlock()
	if slot == nil {
		return false
	}
	return slot.pump()
}

// PumpAll Manual 模式：处理所有 slot 直到无待处理 Turn，返回处理（执行+过期）数量。
func (s *Stage) PumpAll(maxTurns ...int) int {
	limit := 4096
	if len(maxTurns) > 0 {
		limit = maxTurns[0]
	}
	processed := 0
	for {
		progressed := false
		s.mu.Lock()
		slots := make([]*actorSlot, 0, len(s.slots))
		for _, slot := range s.slots {
			slots = append(slots, slot)
		}
		s.mu.Unlock()
		for _, slot := range slots {
			for processed < limit && slot.pump() {
				processed++
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return processed
}

// Sweep Intermission：将空闲超过 IdleTimeout 的 Running slot 下线并移除
// （下次调用重新激活）。M1 由宿主显式调用；自动巡回调度属 M2 运维增强。
func (s *Stage) Sweep() []ActorID {
	if s.cfg.IdleTimeout <= 0 {
		return nil
	}
	now := s.clock.Now()
	s.mu.Lock()
	var targets []ActorID
	for id, slot := range s.slots {
		if slot.stateSnapshot() == slotRunning && now.Sub(slot.lastActivityTime()) >= s.cfg.IdleTimeout {
			targets = append(targets, id)
		}
	}
	s.mu.Unlock()
	var passivated []ActorID
	for _, id := range targets {
		s.mu.Lock()
		slot := s.slots[id]
		s.mu.Unlock()
		if slot == nil {
			continue
		}
		slot.close("intermission")
		s.mu.Lock()
		delete(s.slots, id)
		s.mu.Unlock()
		passivated = append(passivated, id)
	}
	return passivated
}

// Close 停机：拒收新 Call/注册，清空所有队列（逐个回复 shutdown 拒绝）并下线实例。
// live 模式消费者在完成在飞 Turn 后退出（exited 通道可轮询泄漏断言）。
func (s *Stage) Close() {
	s.mu.Lock()
	s.closed = true
	slots := make([]*actorSlot, 0, len(s.slots))
	for _, slot := range s.slots {
		slots = append(slots, slot)
	}
	s.slots = make(map[ActorID]*actorSlot)
	s.mu.Unlock()
	for _, slot := range slots {
		slot.close("shutdown")
	}
}

func (s *Stage) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// consumerExited 轮询辅助：指定 Actor 的消费者 goroutine 是否已退出
// （manual 模式恒 true）。测试泄漏断言用。
func (s *Stage) consumerExited(id ActorID) bool {
	s.mu.Lock()
	slot := s.slots[id]
	s.mu.Unlock()
	if slot == nil {
		return true
	}
	return slot.exitedClosed()
}
