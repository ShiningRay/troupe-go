package troupe

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ActivationContext 一次激活的所有权与存储视图。Revision 由 Save 原子推进。
// journal 为激活时装载的幂等记录（未过期）；outbox 为待提交外部副作用事件
// （激活时装载未派发事件，Save 与 Props 同事务提交）；storeCtx 为存储操作上下文
// （随 Stage 生命周期取消，Close 后的存储调用快速失败）。
type ActivationContext struct {
	store         PropStore
	schemaVersion int
	Epoch         uint64
	Revision      uint64
	clock         Clock
	journal       map[string]IdempotencyRecord
	outbox        map[string]OutboxRecord
	retention     time.Duration
	storeCtx      context.Context
}

// StoreCtx 返回存储操作上下文（随 Stage 生命周期取消）。
func (c *ActivationContext) StoreCtx() context.Context { return c.storeCtx }

func (c *ActivationContext) Now() time.Time { return c.clock.Now() }

// commitAtomic Props CAS + 幂等 journal + outbox 同事务原子提交
// （store 能力按需检测；失败显式报错，不做静默降级）。
func (c *ActivationContext) commitAtomic(id ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord) (PropRecord, error) {
	var outbox []OutboxRecord
	for _, ev := range c.outbox {
		outbox = append(outbox, ev)
	}
	if len(outbox) > 0 {
		os, ok := c.store.(OutboxStore)
		if !ok {
			return PropRecord{}, &InvalidActorStateError{Detail: "存在未提交外部事件，但当前 PropStore 不支持 Outbox（需实现 OutboxStore）"}
		}
		return os.WriteWithOutbox(c.storeCtx, id, expectedRevision, schemaVersion, data, epoch, journal, outbox)
	}
	js, ok := c.store.(JournalingStore)
	if !ok {
		if len(journal) > 0 {
			return PropRecord{}, &InvalidActorStateError{Detail: "当前 PropStore 不支持幂等 journal（需实现 JournalingStore）"}
		}
		return c.store.Write(c.storeCtx, id, expectedRevision, schemaVersion, data, epoch)
	}
	return js.WriteJournaled(c.storeCtx, id, expectedRevision, schemaVersion, data, epoch, journal)
}

// Props 是可恢复状态的契约标记：必须是纯数据 DTO（encoding/json 可往返），
// 禁止 func/chan/goroutine/互斥锁等运行时对象进入 Props（DESIGN §6）。
// 序列化边界由框架强制：激活从 store.Data JSON 反序列化重建实例，
// Save 序列化后 CAS 提交——调用方与 Actor 永不共享可变结构。
type Props = any

// Actor 类型化 Actor 基类：用户角色以指针嵌入
//
//	type CartActor struct { *troupe.Actor[CartProps] }
//
// 并在 manifest 工厂中构造。具体 Actor 可实现 StageHooks 获得 on/off 生命周期。
type Actor[P any] struct {
	id    ActorID
	props P
	actx  *ActivationContext
}

func NewActor[P any](id ActorID, props P, ctx *ActivationContext) *Actor[P] {
	return &Actor[P]{id: id, props: props, actx: ctx}
}

// ID 返回本 Actor 的 canonical 标识。
func (a *Actor[P]) ID() ActorID { return a.id }

// Props 返回本激活工作状态的指针（仅本 Actor 的串行 Turn 可见/可改；
// Crystal 引用语义的 Go 等价物——跨 Turn 持有指针无效化后即失效）。
func (a *Actor[P]) Props() *P { return &a.props }

// SchemaVersion 返回 manifest 目标 schema 版本。
func (a *Actor[P]) SchemaVersion() int { return a.actx.schemaVersion }

// Save 显式持久化：以 expectedRevision + epoch 原子 CAS 提交当前 Props
// （含待提交幂等记录与 outbox 事件——同事务原子提交）。
// 失败（CAS 冲突 / fencing 拒绝）会使实例失效：后续调用从最后已提交快照重新激活。
func (a *Actor[P]) Save() error {
	data, err := json.Marshal(a.props)
	if err != nil {
		return &InvalidActorStateError{Detail: "props 序列化失败：" + err.Error()}
	}
	rec, err := a.actx.commitAtomic(a.id, a.actx.Revision, a.actx.schemaVersion, string(data), a.actx.Epoch, nil)
	if err != nil {
		return err
	}
	a.actx.Revision = rec.Revision
	a.actx.outbox = make(map[string]OutboxRecord) // 事件已随 Props 落盘，交由 OutboxDispatcher 派发
	return nil
}

// EmitEvent 外部副作用 Outbox（docs/outbox 指引）：事件记入 journal，
// 下次 Save 与 Props 同原子提交；派发经 OutboxDispatcher 至少一次（接收方幂等）。
// eventID 自动生成；store 不支持 Outbox 时立即报错（fail fast，不做静默丢失）。
func (a *Actor[P]) EmitEvent(topic string, payload any) (string, error) {
	if _, ok := a.actx.store.(OutboxStore); !ok {
		return "", &InvalidActorStateError{Detail: "当前 PropStore 不支持 Outbox（需实现 OutboxStore）"}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", &InvalidActorStateError{Detail: "事件载荷序列化失败：" + err.Error()}
	}
	id := newEventID()
	if a.actx.outbox == nil {
		a.actx.outbox = make(map[string]OutboxRecord)
	}
	a.actx.outbox[id] = OutboxRecord{
		EventID: id, ActorID: a.id.Canonical(), Topic: topic,
		Payload: b, CreatedAt: a.actx.clock.Now(),
	}
	return id, nil
}

func newEventID() string {
	b := make([]byte, 16)
	if _, err := crand.Read(b); err != nil {
		panic(fmt.Sprintf("event id 生成失败：%v", err))
	}
	return hex.EncodeToString(b)
}

// OnStage / OffStage 默认空实现；具体 Actor 通过实现 StageHooks 覆盖。
// （Go 嵌入不发生虚分派，框架以类型断言在具体类型上发现钩子，见 manifest.buildActor。）
func (a *Actor[P]) OnStage() error  { return nil }
func (a *Actor[P]) OffStage() error { return nil }

func (a *Actor[P]) actorID() ActorID { return a.id }

// Idempotent 幂等执行（对齐 troupe.cr 失败语义）：
//   - 同键同指纹 → 重放首次结果（或重抛首次错误）；
//   - 同键不同指纹 → IdempotencyConflictError；
//   - fn 只修改内存 Props、不要 Save：helper 在 fn settle 后把 Props 变更 + 幂等
//     记录**一次原子提交**（WriteJournaled）；失败也记录，重放时按记录重抛；
//   - 过期（ExpiresAt）后同键视为新操作。保留期 = StageConfig.IdempotencyRetention。
func (a *Actor[P]) Idempotent(key, fingerprint string, fn func() (any, error)) (json.RawMessage, error) {
	ctx := a.actx
	now := ctx.clock.Now().Unix()
	if existing, ok := ctx.journal[key]; ok && existing.ExpiresAt > now {
		if existing.Fingerprint != fingerprint {
			return nil, &IdempotencyConflictError{
				Detail: "幂等键 " + quote(key) + " 已用于不同参数指纹（" + existing.Fingerprint + " ≠ " + fingerprint + "）",
			}
		}
		if existing.OK {
			return json.RawMessage(existing.Value), nil
		}
		return nil, errors.New("幂等重放：首次执行失败（" + existing.ErrorClass + ": " + existing.ErrorMessage + "）")
	}
	expiresAt := ctx.clock.Now().Add(ctx.retention).Unix()
	value, err := fn()
	record := IdempotencyRecord{Key: key, Fingerprint: fingerprint, ExpiresAt: expiresAt}
	var raw json.RawMessage
	if err != nil {
		record.OK = false
		record.ErrorClass = fmt.Sprintf("%T", err)
		record.ErrorMessage = err.Error()
	} else {
		b, merr := json.Marshal(value)
		if merr != nil {
			return nil, fmt.Errorf("幂等结果序列化失败：%w", merr)
		}
		record.OK = true
		record.Value = string(b)
		raw = b
	}
	// Props 变更（fn 内存修改）与幂等记录一次原子提交。
	data, serr := json.Marshal(a.props)
	if serr != nil {
		return nil, &InvalidActorStateError{Detail: "props 序列化失败：" + serr.Error()}
	}
	rec, werr := ctx.commitAtomic(a.id, ctx.Revision, ctx.schemaVersion, string(data), ctx.Epoch, []IdempotencyRecord{record})
	if werr != nil {
		return nil, werr
	}
	ctx.Revision = rec.Revision
	ctx.journal[key] = record
	if err != nil {
		return nil, err // 失败记录已提交后重抛原错误（重放时按记录重抛）
	}
	return raw, nil
}

// CancelSignal 协作取消信号（DESIGN §5 时间与取消）：仅置位标志，Turn 是否/何时
// 响应由 Actor 代码决定。框架不会强行终止 Turn；取消不改变副作用语义
// （settle 后正常回复）。
type CancelSignal struct {
	v atomic.Bool
}

func newCancelSignal() *CancelSignal { return &CancelSignal{} }

func (c *CancelSignal) Request()        { c.v.Store(true) }
func (c *CancelSignal) Requested() bool { return c.v.Load() }

// CallContext 单个 Turn 的调用上下文。Clock 可注入：Rehearsal 中 Turn 体内推进
// 虚拟时钟可确定性模拟耗时。Cancel 为协作取消信号（CallFuture#Cancel 置位，
// Turn 经 CancelRequested 观察并自行决定是否/如何收束）。Traceparent 在 M2 接入
// W3C 派生；M1 恒为空。
type CallContext struct {
	CallID      uint64
	ActorID     ActorID
	Method      string
	Deadline    time.Time
	clock       Clock
	cancel      *CancelSignal
	Traceparent string
}

func (c *CallContext) Now() time.Time { return c.clock.Now() }

// CancelRequested 返回调用方是否已请求协作取消。
func (c *CallContext) CancelRequested() bool { return c.cancel.Requested() }

func (c *CallContext) cancelSignal() *CancelSignal { return c.cancel }
