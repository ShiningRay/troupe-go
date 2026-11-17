package troupe

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ---- 持久 Reminder（M3）：稳定键、原子 claim（租期）、complete/release fencing、
// 至少一次重投（业务幂等兜底）。volatile timer 与 durable reminder 保持分离。----

// OutboxRecord 外部副作用事件（与 Props 同事务提交；派发至少一次）。
type OutboxRecord struct {
	EventID   string
	ActorID   string // canonical
	Topic     string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// ReminderRecord 持久提醒记录。
type ReminderRecord struct {
	ActorID        string // canonical
	Name           string // 稳定键 (actor_id, name)
	Method         string // 交付为普通 Turn 的方法名
	Payload        json.RawMessage
	NextDue        time.Time
	ClaimHolder    string
	ClaimExpiresAt time.Time
	StatusVersion  uint64
}

// ReminderStore 契约：claim 原子（租期守卫）；complete/release 以
// status_version + claim_holder + 租期 fencing（跨节点 Provider 用 DB 时钟）。
type ReminderStore interface {
	// Schedule upsert 稳定键提醒（重复调度覆盖 nextDue/payload；status_version 不变语义）。
	Schedule(ctx context.Context, actorID ActorID, name, method string, payload json.RawMessage, dueAt time.Time) error
	// Cancel 取消未交付提醒（稳定键删除；已交付记录不存在返回 false）。
	Cancel(ctx context.Context, actorID ActorID, name string) bool
	// ClaimDue 原子认领到期提醒：置 claim_holder=holder、租期、status_version+1。
	// 跨节点 Provider 先按归属过滤（troupe_ownership 持有者，无记录任何节点可认领）。
	ClaimDue(ctx context.Context, now time.Time, holder string, lease time.Duration) ([]ReminderRecord, error)
	// Complete 交付成功：fencing（status_version + holder + 租期内）通过则删除记录。
	Complete(ctx context.Context, actorID ActorID, name string, statusVersion uint64, holder string) (bool, error)
	// Release 交付失败：fencing 通过则释放（nextDue=now + retryIn → 至少一次重投）。
	Release(ctx context.Context, actorID ActorID, name string, statusVersion uint64, holder string, retryIn time.Duration) (bool, error)
}

// MemoryReminderStore 单机实现：锁内原子 claim/complete/release；
// 归属过滤仅在跨节点 Provider（PG）有意义——单节点全体可认领。
type MemoryReminderStore struct {
	mu        sync.Mutex
	reminders map[string]*ReminderRecord // key: actorID\x1fname
}

func NewMemoryReminderStore() *MemoryReminderStore {
	return &MemoryReminderStore{reminders: make(map[string]*ReminderRecord)}
}

func reminderKey(actorID ActorID, name string) string {
	return actorID.Canonical() + "\x1f" + name
}

func (m *MemoryReminderStore) Schedule(ctx context.Context, actorID ActorID, name, method string, payload json.RawMessage, dueAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := reminderKey(actorID, name)
	if rec, ok := m.reminders[key]; ok {
		rec.Method = method
		rec.Payload = payload
		rec.NextDue = dueAt
		return nil
	}
	m.reminders[key] = &ReminderRecord{
		ActorID: actorID.Canonical(), Name: name, Method: method,
		Payload: payload, NextDue: dueAt, StatusVersion: 0,
	}
	return nil
}

func (m *MemoryReminderStore) Cancel(ctx context.Context, actorID ActorID, name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := reminderKey(actorID, name)
	if _, ok := m.reminders[key]; !ok {
		return false
	}
	delete(m.reminders, key)
	return true
}

func (m *MemoryReminderStore) ClaimDue(ctx context.Context, now time.Time, holder string, lease time.Duration) ([]ReminderRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var claimed []ReminderRecord
	keys := make([]string, 0, len(m.reminders))
	for key := range m.reminders {
		keys = append(keys, key)
	}
	sort.Strings(keys) // 确定性顺序
	for _, key := range keys {
		rec := m.reminders[key]
		if rec.NextDue.After(now) {
			continue
		}
		// 已被其他持有者认领且租期未过 → 跳过
		if rec.ClaimHolder != "" && rec.ClaimHolder != holder && rec.ClaimExpiresAt.After(now) {
			continue
		}
		rec.ClaimHolder = holder
		rec.ClaimExpiresAt = now.Add(lease)
		rec.StatusVersion++
		claimed = append(claimed, *rec)
	}
	return claimed, nil
}

func (m *MemoryReminderStore) fencingOK(rec *ReminderRecord, statusVersion uint64, holder string, now time.Time) bool {
	return rec.StatusVersion == statusVersion && rec.ClaimHolder == holder && rec.ClaimExpiresAt.After(now)
}

func (m *MemoryReminderStore) Complete(ctx context.Context, actorID ActorID, name string, statusVersion uint64, holder string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.reminders[reminderKey(actorID, name)]
	if !ok {
		return false, nil
	}
	if !m.fencingOK(rec, statusVersion, holder, time.Now()) {
		return false, nil // fencing 拒绝：认领已被接管/过期
	}
	delete(m.reminders, reminderKey(actorID, name))
	return true, nil
}

func (m *MemoryReminderStore) Release(ctx context.Context, actorID ActorID, name string, statusVersion uint64, holder string, retryIn time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.reminders[reminderKey(actorID, name)]
	if !ok {
		return false, nil
	}
	if !m.fencingOK(rec, statusVersion, holder, time.Now()) {
		return false, nil
	}
	rec.NextDue = time.Now().Add(retryIn)
	rec.ClaimHolder = "" // 释放认领（下次到期任意实例可认领）
	rec.ClaimExpiresAt = time.Time{}
	return true, nil
}

// ReminderRunner 持久提醒交付调度：轮询认领 → 作为普通 Turn 交付（白名单/串行/
// 持久化语义不变）→ 成功 complete / 失败 release 重投（至少一次；重复由业务幂等兜底）。
type ReminderRunner struct {
	stage      *Stage
	store      ReminderStore
	instance   string
	lease      time.Duration
	interval   time.Duration
	retryDelay time.Duration // 交付失败 release 后的重投延迟
	stop       chan struct{}
	stopped    chan struct{}
	deliveries int64 // 测试观察：累计交付尝试数
}

func NewReminderRunner(stage *Stage, store ReminderStore, instance string) *ReminderRunner {
	return &ReminderRunner{
		stage:      stage,
		store:      store,
		instance:   instance,
		lease:      30 * time.Second,
		interval:   500 * time.Millisecond,
		retryDelay: 5 * time.Second,
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
}

// WithLeaseInterval 覆盖租期与轮询间隔（测试用短值）。
func (r *ReminderRunner) WithLeaseInterval(lease, interval time.Duration) *ReminderRunner {
	r.lease = lease
	r.interval = interval
	return r
}

// WithRetryDelay 覆盖交付失败后的重投延迟（至少一次；间隔内由业务幂等兜底）。
func (r *ReminderRunner) WithRetryDelay(d time.Duration) *ReminderRunner {
	r.retryDelay = d
	return r
}

// Start 启动轮询（非阻塞；Stop/ctx 取消退出）。
func (r *ReminderRunner) Start(ctx context.Context) {
	go r.run(ctx)
}

func (r *ReminderRunner) run(ctx context.Context) {
	defer close(r.stopped)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-ticker.C:
			r.PollOnce(ctx)
		}
	}
}

// PollOnce 手动驱动一轮认领+交付（Manual/Rehearsal 测试用）。
func (r *ReminderRunner) PollOnce(ctx context.Context) int {
	due, err := r.store.ClaimDue(ctx, time.Now(), r.instance, r.lease)
	if err != nil {
		return 0
	}
	delivered := 0
	for _, rec := range due {
		r.deliveries++
		aid, perr := ParseActorID(rec.ActorID)
		if perr != nil {
			_, _ = r.store.Release(ctx, MustActorIDFromString(rec.ActorID), rec.Name, rec.StatusVersion, r.instance, r.retryDelay)
			continue
		}
		f, cerr := r.stage.Call(ctx, aid, rec.Method, rec.Payload,
			WithDeadline(rec.NextDue.Add(r.lease)))
		if cerr != nil {
			_, _ = r.store.Release(ctx, aid, rec.Name, rec.StatusVersion, r.instance, r.retryDelay)
			continue
		}
		// Manual 调度（Rehearsal）：Call 后需 Pump 才有回复
		if r.stage.Manual() {
			r.stage.PumpAll()
		}
		_, werr := f.Await(ctx)
		if werr != nil {
			// 失败重投（至少一次）；重复交付由业务幂等兜底
			_, _ = r.store.Release(ctx, aid, rec.Name, rec.StatusVersion, r.instance, r.retryDelay)
			continue
		}
		ok, _ := r.store.Complete(ctx, aid, rec.Name, rec.StatusVersion, r.instance)
		if ok {
			delivered++
		}
	}
	return delivered
}

func (r *ReminderRunner) Stop() {
	close(r.stop)
	<-r.stopped
}

// MustActorIDFromString 解析 canonical 字符串（失败 panic——runner 内部不变量）。
func MustActorIDFromString(canonical string) ActorID {
	id, err := ParseActorID(canonical)
	if err != nil {
		panic(fmt.Sprintf("reminder actor id 解析失败：%v", err))
	}
	return id
}

// ---- Outbox：外部副作用与 Props 同事务提交；派发至少一次 ----

// OutboxStore 能力接口：EmitEvent + Props 同事务提交、轮询派发、at-least-once 标记。
type OutboxStore interface {
	JournalingStore
	// WriteWithOutbox Props CAS + 幂等 journal + outbox 同事务原子提交。
	WriteWithOutbox(ctx context.Context, actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord, outbox []OutboxRecord) (PropRecord, error)
	// PollOutbox 返回未投递事件（激活时装载 journal；派发器轮询）。
	PollOutbox(ctx context.Context, actorID ActorID, limit int) ([]OutboxRecord, error)
	// MarkSent 标记已投递（at-least-once：派发成功后标记；崩溃重投由接收方幂等）。
	MarkSent(ctx context.Context, actorID ActorID, eventID string) bool
}

// MemoryOutboxStore 内存实现：委托 MemoryPropStore 的统一原子提交（writeCommit）。
type MemoryOutboxStore struct {
	*MemoryPropStore
}

func NewMemoryOutboxStore(ownership OwnershipStore) *MemoryOutboxStore {
	return &MemoryOutboxStore{MemoryPropStore: NewMemoryPropStore(ownership)}
}

func (m *MemoryOutboxStore) WriteWithOutbox(ctx context.Context, actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord, outbox []OutboxRecord) (PropRecord, error) {
	return m.MemoryPropStore.writeCommit(ctx, actorID, expectedRevision, schemaVersion, data, epoch, journal, outbox)
}

func (m *MemoryOutboxStore) PollOutbox(ctx context.Context, actorID ActorID, limit int) ([]OutboxRecord, error) {
	return m.MemoryPropStore.pollOutbox(ctx, actorID, limit), nil
}

func (m *MemoryOutboxStore) MarkSent(ctx context.Context, actorID ActorID, eventID string) bool {
	return m.MemoryPropStore.markSent(ctx, actorID, eventID)
}

// OutboxDispatcher at-least-once 派发器：轮询指定 Actor 的未投递事件 →
// handler 投递（接收方幂等）→ 成功 MarkSent；崩溃/失败重投（结果未知 ≠ 丢失）。
type OutboxDispatcher struct {
	store    OutboxStore
	actors   []ActorID
	handler  func(ctx context.Context, rec OutboxRecord) error
	interval time.Duration
	stop     chan struct{}
	stopped  chan struct{}
}

func NewOutboxDispatcher(store OutboxStore, actors []ActorID, handler func(ctx context.Context, rec OutboxRecord) error) *OutboxDispatcher {
	return &OutboxDispatcher{
		store:    store,
		actors:   actors,
		handler:  handler,
		interval: 500 * time.Millisecond,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (d *OutboxDispatcher) WithInterval(interval time.Duration) *OutboxDispatcher {
	d.interval = interval
	return d
}

// Start 启动轮询（非阻塞；Stop/ctx 取消退出）。
func (d *OutboxDispatcher) Start(ctx context.Context) {
	go func() {
		defer close(d.stopped)
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-d.stop:
				return
			case <-ticker.C:
				_ = d.PollOnce(ctx)
			}
		}
	}()
}

// PollOnce 手动驱动一轮派发（Manual/Rehearsal 测试用）；返回派发成功数。
func (d *OutboxDispatcher) PollOnce(ctx context.Context) int {
	delivered := 0
	for _, aid := range d.actors {
		events, err := d.store.PollOutbox(ctx, aid, 128)
		if err != nil {
			continue
		}
		for _, ev := range events {
			if err := d.handler(ctx, ev); err != nil {
				continue // 失败不 MarkSent → 至少一次重投
			}
			if d.store.MarkSent(ctx, aid, ev.EventID) {
				delivered++
			}
		}
	}
	return delivered
}

func (d *OutboxDispatcher) Stop() {
	close(d.stop)
	<-d.stopped
}
