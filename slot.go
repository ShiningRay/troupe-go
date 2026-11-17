package troupe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type slotState int32

const (
	slotIdle slotState = iota // 未激活（初始或失效后），下次消费时重新激活
	slotActivating
	slotRunning
	slotClosed
)

type schedulerMode int32

const (
	// schedulerLive 每个激活一个消费者 goroutine，阻塞等待 CallBoard 唤醒。
	schedulerLive schedulerMode = iota
	// schedulerManual 无消费者 goroutine：由宿主/测试显式 pump（Rehearsal 确定性调度）。
	schedulerManual
)

// turnBoard 有界 CallBoard：FIFO、queue depth/bytes 双重上限、排队过期在出队时判定。
// 单消费者（strict Turn 串行）；push 满即显式拒绝（backpressure，可重试）。
type turnBoard struct {
	q     []*callEnvelope
	bytes int
}

func (b *turnBoard) push(env *callEnvelope, maxDepth, maxBytes int) error {
	if len(b.q) >= maxDepth {
		return &CallRejectedError{
			Reason:    RejectBackpressure,
			Retryable: true,
			Detail:    fmt.Sprintf("队列深度 %d 达上限 %d（method %s）", len(b.q), maxDepth, env.method),
		}
	}
	if b.bytes+env.bytes > maxBytes {
		return &CallRejectedError{
			Reason:    RejectBackpressure,
			Retryable: true,
			Detail:    fmt.Sprintf("队列字节 %d 将超上限 %d（method %s）", b.bytes+env.bytes, maxBytes, env.method),
		}
	}
	b.q = append(b.q, env)
	b.bytes += env.bytes
	return nil
}

// takeReadyLocked 弹出队首；队首已过 deadline 时逐个剔除（过期无副作用），
// 返回第一个未过期请求（不加锁版本，调用方持锁）。
func (b *turnBoard) peekExpired(now time.Time) (expired []*callEnvelope, next *callEnvelope) {
	for len(b.q) > 0 {
		head := b.q[0]
		if head.deadline.After(now) {
			next = head
			break
		}
		b.q = b.q[1:]
		b.bytes -= head.bytes
		expired = append(expired, head)
	}
	return expired, next
}

func (b *turnBoard) pop() *callEnvelope {
	if len(b.q) == 0 {
		return nil
	}
	head := b.q[0]
	b.q = b.q[1:]
	b.bytes -= head.bytes
	return head
}

func (b *turnBoard) drainLocked() []*callEnvelope {
	all := b.q
	b.q = nil
	b.bytes = 0
	return all
}

func (b *turnBoard) empty() bool { return len(b.q) == 0 }

// actorSlot 单个 virtual actor 的激活生命周期（Casting single-flight）+
// 有界 CallBoard + 单消费串行执行（strict Turn）+ Intermission/失效下线。
//
// 激活顺序（不可颠倒）：
// acquire epoch → read/create → 逐级迁移（fenced CAS 提交，失败不写）→ OnStage → running。
type actorSlot struct {
	id        ActorID
	manifest  manifestBase
	store     PropStore
	ownership OwnershipStore
	clock     Clock
	cfg       StageConfig
	live      bool
	emitFn    func(StageEvent)

	mu           sync.Mutex
	board        turnBoard
	wakeup       chan struct{}
	state        slotState
	act          *activation
	lastActivity time.Time
	executing    bool
	closed       bool
	exited       chan struct{} // live 模式：消费者 goroutine 退出时关闭（泄漏断言用）

	emitTraceFn func(TraceEvent)

	baseCtx context.Context // Stage 生命周期上下文：存储操作与激活共用，Close 后取消
}

func newActorSlot(id ActorID, m manifestBase, store PropStore, ownership OwnershipStore,
	clock Clock, cfg StageConfig, mode schedulerMode, emit func(StageEvent), emitTrace func(TraceEvent),
	baseCtx context.Context) *actorSlot {
	s := &actorSlot{
		id:           id,
		manifest:     m,
		store:        store,
		ownership:    ownership,
		clock:        clock,
		cfg:          cfg,
		live:         mode == schedulerLive,
		emitFn:       emit,
		emitTraceFn:  emitTrace,
		baseCtx:      baseCtx,
		wakeup:       make(chan struct{}, 1),
		lastActivity: clock.Now(),
		exited:       make(chan struct{}),
	}
	if s.live {
		go s.runLoop()
	}
	return s
}

func (s *actorSlot) enqueue(req *callEnvelope) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return &CallRejectedError{
			Reason: RejectShutdown,
			Actor:  s.id.Canonical(),
			Detail: "Actor 已关闭，拒收新 Call（method " + req.method + "）",
		}
	}
	err := s.board.push(req, s.cfg.MaxQueueDepth, s.cfg.MaxQueueBytes)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.lastActivity = s.clock.Now()
	s.mu.Unlock()
	s.signal()
	return nil
}

// runLoop live 模式消费者：单 goroutine 严格串行处理队列；
// close 后完成在飞 Turn 再退出（不中途打断执行占用语义）。
func (s *actorSlot) runLoop() {
	defer close(s.exited)
	for {
		<-s.wakeup
		if s.isClosed() {
			s.performDeactivate("shutdown")
			return
		}
		for {
			req := s.takeReady()
			if req == nil {
				break
			}
			s.execute(req)
			if s.isClosed() {
				s.performDeactivate("shutdown")
				return
			}
		}
	}
}

func (s *actorSlot) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// expireEnvelopes 锁外回复 queued 超时（零副作用）并产出事件/trace。
func (s *actorSlot) expireEnvelopes(expired []*callEnvelope) {
	now := s.clock.Now()
	for _, env := range expired {
		env.reply <- callReply{
			err:       &CallTimeoutError{Phase: PhaseQueued, Method: env.method},
			settledAt: now,
		}
		s.emitFn(StageEvent{Type: EventTurnExpired, ActorID: s.id, At: now, Method: env.method, Detail: PhaseQueued})
		if s.emitTraceFn != nil {
			s.emitTraceFn(TraceEvent{
				CallID: env.callID, ActorID: s.id, Method: env.method,
				OK: false, Outcome: "expired",
				Traceparent: env.traceparent, ParentTraceparent: env.parentTraceparent, At: now,
			})
		}
	}
}

// takeReady 取出下一个可执行请求；队首过期请求在锁外回复 queued 超时（零副作用）。
func (s *actorSlot) takeReady() *callEnvelope {
	s.mu.Lock()
	expired, next := s.board.peekExpired(s.clock.Now())
	if next != nil {
		s.board.pop()
		s.lastActivity = s.clock.Now()
	}
	s.mu.Unlock()
	s.expireEnvelopes(expired)
	return next
}

func (s *actorSlot) signal() {
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

// pump Manual 模式：先清队首过期请求，再执行至多一个未过期 Turn。
// 返回是否执行了 Turn；Turn 执行中重入 pump 属宿主误用，显式报错。
func (s *actorSlot) pump() bool {
	s.mu.Lock()
	if s.executing {
		s.mu.Unlock()
		panic(&InvalidActorStateError{Detail: "Turn 执行中不能重入 pump"})
	}
	if s.closed {
		s.mu.Unlock()
		return false
	}
	expired, next := s.board.peekExpired(s.clock.Now())
	if next != nil {
		s.board.pop()
		s.executing = true
		s.lastActivity = s.clock.Now()
	}
	s.mu.Unlock()
	s.expireEnvelopes(expired)
	if next == nil {
		return false
	}
	s.execute(next)
	return true
}

func (s *actorSlot) stateSnapshot() slotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *actorSlot) lastActivityTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActivity
}

func (s *actorSlot) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.executing && s.board.empty()
}

// close 关闭：停收新请求、清空队列（逐个回复 shutdown 拒绝）、下线实例并停止消费者。
// live 模式下 OffStage 由消费者 goroutine 在 settle 后执行，避免与执行中 Turn 竞争；
// manual 模式在本调用内同步下线。
func (s *actorSlot) close(reason string) {
	s.mu.Lock()
	newlyClosed := !s.closed
	s.closed = true
	drained := s.board.drainLocked()
	s.mu.Unlock()
	now := s.clock.Now()
	for _, env := range drained {
		env.reply <- callReply{
			err: &CallRejectedError{
				Reason: RejectShutdown,
				Actor:  s.id.Canonical(),
				Detail: "Stage 已停机，调用被拒绝（" + env.method + "）",
			},
			settledAt: now,
		}
	}
	if newlyClosed && !s.live {
		s.performDeactivate(reason)
	}
	s.signal()
}

// exitedClosed 供泄漏断言轮询消费者是否已退出（manual 模式恒已"退出"）。
func (s *actorSlot) exitedClosed() bool {
	if !s.live {
		return true
	}
	select {
	case <-s.exited:
		return true
	default:
		return false
	}
}

// ensureActivated single-flight：已 Running 直接复用；否则完成激活后再进入 Running。
// 激活失败统一包装为 ActivationError，槽位回到 Idle 允许后续重试。
func (s *actorSlot) ensureActivated() (activation, error) {
	s.mu.Lock()
	if s.state == slotRunning && s.act != nil {
		act := *s.act
		s.mu.Unlock()
		return act, nil
	}
	if s.closed {
		s.mu.Unlock()
		return activation{}, &InvalidActorStateError{Detail: "Actor " + s.id.Canonical() + " 正在关闭"}
	}
	s.state = slotActivating
	s.mu.Unlock()

	act, err := s.activate()
	if err != nil {
		s.mu.Lock()
		s.state = slotIdle
		s.mu.Unlock()
		var ae *ActivationError
		if !errors.As(err, &ae) {
			err = &ActivationError{Detail: "激活失败：" + err.Error(), Cause: err}
		}
		return activation{}, err
	}
	s.mu.Lock()
	s.act = &act
	s.state = slotRunning
	s.lastActivity = s.clock.Now()
	s.mu.Unlock()
	s.emitFn(StageEvent{Type: EventActivated, ActorID: s.id, At: s.clock.Now()})
	return act, nil
}

// activate 激活顺序（不可颠倒）：acquire epoch → read/create → 逐级迁移 → parse → hooks。
// 存储操作使用 Stage 生命周期上下文（Close 后快速失败，不悬挂 goroutine）。
func (s *actorSlot) activate() (activation, error) {
	epoch, err := s.ownership.Acquire(s.baseCtx, s.id)
	if err != nil {
		return activation{}, err // 租期冲突等 ownership 拒绝原样上抛（槽位回 Idle 可重试）
	}
	record, err := s.store.Read(s.baseCtx, s.id)
	if err != nil {
		return activation{}, err
	}
	if record == nil {
		initial, err := s.manifest.initialJSON()
		if err != nil {
			return activation{}, err
		}
		rec, err := s.store.Create(s.baseCtx, s.id, s.manifest.schemaVer(), initial, epoch)
		if err != nil {
			return activation{}, err
		}
		record = &rec
	} else {
		record, err = s.migrateIfNeeded(record, epoch)
		if err != nil {
			return activation{}, err
		}
	}
	props, err := s.manifest.parseFinal(record.Data)
	if err != nil {
		return activation{}, err
	}
	// 幂等 journal 激活时装载（未过期记录；过期条目按新操作处理）。
	journal := make(map[string]IdempotencyRecord)
	if js, ok := s.store.(JournalingStore); ok {
		records, err := js.LoadJournal(s.baseCtx, s.id, s.clock.Now().Unix())
		if err != nil {
			return activation{}, err
		}
		for _, r := range records {
			journal[r.Key] = r
		}
	}
	actCtx := &ActivationContext{
		store:         s.store,
		schemaVersion: s.manifest.schemaVer(),
		Epoch:         epoch,
		Revision:      record.Revision,
		clock:         s.clock,
		journal:       journal,
		retention:     s.cfg.IdempotencyRetention,
		storeCtx:      s.baseCtx,
	}
	act, err := s.manifest.buildActor(s.id, props, actCtx)
	if err != nil {
		return activation{}, err
	}
	if act.hooks != nil {
		if err := act.hooks.OnStage(); err != nil {
			return activation{}, err
		}
	}
	return act, nil
}

// migrateIfNeeded 逐级迁移：任何一步失败即 ActivationError，且不产生任何写入。
func (s *actorSlot) migrateIfNeeded(record *PropRecord, epoch uint64) (*PropRecord, error) {
	if record.SchemaVersion == s.manifest.schemaVer() {
		return record, nil
	}
	path := s.manifest.migrationPath(record.SchemaVersion)
	if path == nil {
		return nil, &ActivationError{
			Detail: fmt.Sprintf("schema %d 无法逐级迁移到 %d（缺级或降级不受支持）",
				record.SchemaVersion, s.manifest.schemaVer()),
		}
	}
	data := record.Data
	for _, step := range path {
		out, err := step.Apply(data)
		if err != nil {
			return nil, &ActivationError{Detail: fmt.Sprintf("迁移 %d→%d 失败：%v", step.From, step.To, err), Cause: err}
		}
		data = out
	}
	updated, err := s.store.Write(s.baseCtx, s.id, record.Revision, s.manifest.schemaVer(), data, epoch)
	if err != nil {
		return nil, &ActivationError{Detail: "迁移提交失败：" + err.Error(), Cause: err}
	}
	return &updated, nil
}

func (s *actorSlot) execute(req *callEnvelope) {
	act, err := s.ensureActivated()
	if err != nil {
		now := s.clock.Now()
		req.reply <- callReply{err: err, settledAt: now}
		s.emitFn(StageEvent{Type: EventActivationFailed, ActorID: s.id, At: now, Method: req.method, Detail: err.Error()})
		s.clearExecuting()
		return
	}
	started := s.clock.Now()
	s.emitFn(StageEvent{Type: EventTurnStarted, ActorID: s.id, At: started, Method: req.method})
	callCtx := &CallContext{
		CallID:      req.callID,
		ActorID:     s.id,
		Method:      req.method,
		Deadline:    req.deadline,
		clock:       s.clock,
		cancel:      req.cancel,
		Traceparent: req.traceparent,
	}
	value, err := s.manifest.invokeTurn(act, callCtx, req.method, req.args)
	settled := s.clock.Now()

	if err == nil {
		// 结果经 JSON 边界序列化（跨线/跨激活一致；原始消息仅进程内）。
		var raw json.RawMessage
		if value != nil {
			b, merr := json.Marshal(value)
			if merr != nil {
				err = fmt.Errorf("turn %s 结果序列化失败：%w", req.method, merr)
			} else {
				raw = b
			}
		} else {
			raw = json.RawMessage("null")
		}
		if err == nil {
			s.mu.Lock()
			s.lastActivity = settled
			s.executing = false
			s.mu.Unlock()
			req.reply <- callReply{value: raw, settledAt: settled}
			s.emitFn(StageEvent{Type: EventTurnEnded, ActorID: s.id, At: settled, Method: req.method, Detail: "ok"})
			if s.emitTraceFn != nil {
				s.emitTraceFn(TraceEvent{
					CallID: req.callID, ActorID: s.id, Method: req.method,
					Duration: settled.Sub(started), OK: true, Outcome: "completed",
					Traceparent: req.traceparent, ParentTraceparent: req.parentTraceparent, At: settled,
				})
			}
			return
		}
	}
	// 失败路径：保存冲突/fencing 拒绝使实例失效——丢弃未提交状态，
	// 下次调用从最后已提交快照重新激活。
	settled = s.clock.Now()
	invalidated := isInvalidating(err)
	s.mu.Lock()
	s.lastActivity = settled
	s.executing = false
	if invalidated {
		s.act = nil
		s.state = slotIdle
	}
	s.mu.Unlock()
	if invalidated {
		s.emitFn(StageEvent{Type: EventSaveFailed, ActorID: s.id, At: settled, Method: req.method, Detail: err.Error()})
	}
	req.reply <- callReply{err: err, settledAt: settled}
	s.emitFn(StageEvent{Type: EventTurnEnded, ActorID: s.id, At: settled, Method: req.method, Detail: "error"})
	if s.emitTraceFn != nil {
		s.emitTraceFn(TraceEvent{
			CallID: req.callID, ActorID: s.id, Method: req.method,
			Duration: settled.Sub(started), OK: false, Outcome: "failed",
			Traceparent: req.traceparent, ParentTraceparent: req.parentTraceparent, At: settled,
		})
	}
}

func (s *actorSlot) clearExecuting() {
	s.mu.Lock()
	s.executing = false
	s.mu.Unlock()
}

func (s *actorSlot) performDeactivate(reason string) {
	s.mu.Lock()
	var act *activation
	if s.act != nil {
		a := *s.act
		act = &a
	}
	s.act = nil
	s.state = slotClosed
	s.mu.Unlock()
	if act == nil {
		return
	}
	if act.hooks != nil {
		if err := act.hooks.OffStage(); err != nil {
			s.emitFn(StageEvent{
				Type: EventDeactivated, ActorID: s.id, At: s.clock.Now(),
				Detail: "off_stage_error:" + err.Error(),
			})
			return
		}
	}
	s.emitFn(StageEvent{Type: EventDeactivated, ActorID: s.id, At: s.clock.Now(), Detail: reason})
}

// isInvalidating 保存失败/所有权丢失使实例失效（Conflict/InvalidActorState/ownership|invalidated 拒绝）。
func isInvalidating(err error) bool {
	var ce *ConflictError
	var iase *InvalidActorStateError
	var cre *CallRejectedError
	if errors.As(err, &ce) || errors.As(err, &iase) {
		return true
	}
	if errors.As(err, &cre) {
		return cre.Reason == RejectOwnership || cre.Reason == RejectInvalidated
	}
	return false
}
