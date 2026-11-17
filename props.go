package troupe

import "sync"

// 错误子分类常量（跨线码表见 errors.go；这些值是 golden/errors.json
// 的结构化字段值，不可随意改动）。
const (
	// ConflictError.Kind
	ConflictExists   = "exists"
	ConflictRevision = "revision"

	// CallTimeoutError.Phase
	PhaseQueued    = "queued"    // 排队中过期：未执行、零副作用、可安全重试
	PhaseExecuting = "executing" // 执行中过期/迟到 settle：结果未知

	// CallRejectedError.Reason
	RejectBackpressure = "backpressure" // 可重试
	RejectWhitelist    = "whitelist"
	RejectShutdown     = "shutdown"
	RejectDraining     = "draining" // 可漂移转发（M3）
	RejectOwnership    = "ownership"
	RejectInvalidated  = "invalidated"
)

// PropRecord Props 存储记录。三个独立版本维度（DESIGN §7，docs/persistence 语义）：
// Revision = 乐观并发 CAS；SchemaVersion = 状态结构版本；Epoch = 最后写入者的
// 所有权 fencing token。Data 为 JSON 字符串：Memory Provider 同样走完整序列化边界，
// read 返回独立快照。
type PropRecord struct {
	Revision      uint64
	SchemaVersion int
	Data          string
	Epoch         uint64
}

// OwnershipStore 为每个 ActorId 发放单调递增 epoch（fencing token）。
type OwnershipStore interface {
	// Acquire 取得下一个 epoch（激活时调用；跨节点 Provider 的租期语义在 M3 扩展）。
	Acquire(actorID ActorID) uint64
	// Current 返回该 ActorId 当前有效 epoch。
	Current(actorID ActorID) uint64
	// Renew 同 epoch 续租（跨节点 Provider 的 lease 语义；单节点 Provider 恒持有时
	// 返回 true，epoch 已被取代返回 false）。
	Renew(actorID ActorID, epoch uint64) bool
}

// MemoryOwnershipStore 单机内存所有权：acquire 单调递增。
type MemoryOwnershipStore struct {
	mu     sync.Mutex
	epochs map[ActorID]uint64
}

func NewMemoryOwnershipStore() *MemoryOwnershipStore {
	return &MemoryOwnershipStore{epochs: make(map[ActorID]uint64)}
}

func (m *MemoryOwnershipStore) Acquire(actorID ActorID) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epochs[actorID]++
	return m.epochs[actorID]
}

func (m *MemoryOwnershipStore) Current(actorID ActorID) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epochs[actorID]
}

func (m *MemoryOwnershipStore) Renew(actorID ActorID, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epochs[actorID] == epoch
}

// IdempotencyRecord 幂等请求记录（与 Props 同事务原子提交；docs/persistence 失败语义）。
type IdempotencyRecord struct {
	Key          string
	Fingerprint  string
	OK           bool
	Value        string // OK 时为结果 JSON
	ErrorClass   string
	ErrorMessage string
	ExpiresAt    int64 // unix 秒；过期后同键视为新操作
}

// JournalingStore 幂等日志能力（可选能力接口；Memory/SQLite/PG 实现）。
// WriteJournaled 把 Props CAS 与幂等记录在同一次原子提交内落盘（失败则两者都不生效）。
type JournalingStore interface {
	PropStore
	WriteJournaled(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord) (PropRecord, error)
	// LoadJournal 返回未过期的幂等记录（激活时装载）。
	LoadJournal(actorID ActorID, nowUnix int64) ([]IdempotencyRecord, error)
}

// PropStore 契约（DESIGN §7）：
//   - Create 原子存在检查；已存在即 *ConflictError{exists}。
//   - Write 以 expectedRevision + epoch 原子 CAS；fencing 必须在 Provider 层校验，
//     epoch != current 一律拒绝（*CallRejectedError{ownership}），不允许应用层先读后写。
//     幂等记录与 Props 同一次原子提交经 JournalingStore 能力接口（M2）。
//   - CrossNodeSafe 声明 Provider 能力：Memory/SQLite/File 为 false（仅进程/单机持久，
//     不能用于跨节点恢复）；生产模式（M3）必须拒绝以此类 Provider 声称接管语义。
type PropStore interface {
	CrossNodeSafe() bool
	// Read 返回当前记录；不存在返回 (nil, nil)。
	Read(actorID ActorID) (*PropRecord, error)
	Create(actorID ActorID, schemaVersion int, data string, epoch uint64) (PropRecord, error)
	Write(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64) (PropRecord, error)
}

// MemoryPropStore Rehearsal/单机用：模拟 CAS 与 fencing；
// 真实接管语义在 M3 由共享 Provider（PostgreSQL）验证。
type MemoryPropStore struct {
	mu        sync.Mutex
	records   map[ActorID]PropRecord
	journals  map[ActorID]map[string]IdempotencyRecord
	ownership OwnershipStore
}

func NewMemoryPropStore(ownership OwnershipStore) *MemoryPropStore {
	return &MemoryPropStore{
		records:   make(map[ActorID]PropRecord),
		journals:  make(map[ActorID]map[string]IdempotencyRecord),
		ownership: ownership,
	}
}

func (m *MemoryPropStore) CrossNodeSafe() bool { return false }

func (m *MemoryPropStore) Read(actorID ActorID) (*PropRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec, ok := m.records[actorID]; ok {
		cp := rec // 独立副本：调用方修改不落入存储
		return &cp, nil
	}
	return nil, nil
}

func (m *MemoryPropStore) Create(actorID ActorID, schemaVersion int, data string, epoch uint64) (PropRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[actorID]; ok {
		return PropRecord{}, &ConflictError{
			Kind: ConflictExists,
			// 与 troupe.cr 一致的消息结构；Detail 由 Error() 拼接。
		}
	}
	if err := m.assertFencing(actorID, epoch); err != nil {
		return PropRecord{}, err
	}
	rec := PropRecord{Revision: 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}
	m.records[actorID] = rec
	return rec, nil
}

func (m *MemoryPropStore) Write(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64) (PropRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[actorID]
	if !ok {
		return PropRecord{}, &ConflictError{Kind: ConflictRevision}
	}
	// fencing 优先于 CAS：旧 epoch 一律拒绝，即使 revision 恰好匹配
	// （旧 epoch 的 create/write/migrate 必须失败）。
	if err := m.assertFencing(actorID, epoch); err != nil {
		return PropRecord{}, err
	}
	if rec.Revision != expectedRevision {
		return PropRecord{}, &ConflictError{Kind: ConflictRevision}
	}
	updated := PropRecord{Revision: rec.Revision + 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}
	m.records[actorID] = updated
	return updated, nil
}

func (m *MemoryPropStore) assertFencing(actorID ActorID, epoch uint64) error {
	current := m.ownership.Current(actorID)
	if current != epoch {
		return &CallRejectedError{
			Reason:    RejectOwnership,
			Retryable: false,
		}
	}
	return nil
}

// WriteJournaled 幂等 journal 与 Props 同锁原子提交（CAS/fencing 失败则 journal 不写）。
func (m *MemoryPropStore) WriteJournaled(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord) (PropRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[actorID]
	if !ok {
		return PropRecord{}, &ConflictError{Kind: ConflictRevision, Actor: actorID.Canonical()}
	}
	if err := m.assertFencing(actorID, epoch); err != nil {
		return PropRecord{}, err
	}
	if rec.Revision != expectedRevision {
		return PropRecord{}, &ConflictError{Kind: ConflictRevision, Actor: actorID.Canonical()}
	}
	updated := PropRecord{Revision: rec.Revision + 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}
	m.records[actorID] = updated
	jm := m.journals[actorID]
	if jm == nil {
		jm = make(map[string]IdempotencyRecord)
		m.journals[actorID] = jm
	}
	for _, entry := range journal {
		jm[entry.Key] = entry
	}
	return updated, nil
}

func (m *MemoryPropStore) LoadJournal(actorID ActorID, nowUnix int64) ([]IdempotencyRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	jm := m.journals[actorID]
	if len(jm) == 0 {
		return nil, nil
	}
	out := make([]IdempotencyRecord, 0, len(jm))
	for _, entry := range jm {
		if entry.ExpiresAt > nowUnix {
			out = append(out, entry)
		}
	}
	return out, nil
}
