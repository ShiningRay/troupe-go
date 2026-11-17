package troupe

// M3-D：持久 Reminder（memory + runner 交付调度）与 Outbox（同事务提交 + 至少一次派发）。

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestReminderDeliversTurnAndCompletes(t *testing.T) {
	r := NewRehearsal("remind")
	registerJobManifest(t, r)
	reminders := NewMemoryReminderStore()
	runner := NewReminderRunner(r.Stage, reminders, "silo-1").WithLeaseInterval(30*time.Second, time.Hour).WithRetryDelay(200 * time.Millisecond)
	ctxbg := context.Background()

	id := r.Actor("job", "r1")
	if err := reminders.Schedule(ctxbg, id, "retry-pay", "charge", json.RawMessage(`{"amount":5}`),
		time.Now().Add(150*time.Millisecond)); err != nil {
		t.Fatalf("schedule 失败：%v", err)
	}

	// 到期前轮询：无交付（真实时钟统一；NextDue 未到）
	if n := runner.PollOnce(ctxbg); n != 0 {
		t.Fatalf("到期前交付 = %d", n)
	}
	// 到期后：认领 → 交付为普通 Turn → complete
	time.Sleep(150 * time.Millisecond)
	if n := runner.PollOnce(ctxbg); n != 1 {
		t.Fatalf("到期后交付 = %d, want 1", n)
	}
	// 记录已 complete（删除）：再轮询无交付
	if n := runner.PollOnce(ctxbg); n != 0 {
		t.Fatalf("完成后重复交付 = %d", n)
	}
}

func TestReminderFailureRedeliversAtLeastOnce(t *testing.T) {
	r := NewRehearsal("remindfail")
	_ = registerJobManifest(t, r)
	reminders := NewMemoryReminderStore()
	runner := NewReminderRunner(r.Stage, reminders, "silo-1").WithLeaseInterval(30*time.Second, time.Hour).WithRetryDelay(200 * time.Millisecond)
	ctxbg := context.Background()

	var attempts int64
	m := Define("flaky", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("charge", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		if atomic.AddInt64(&attempts, 1) == 1 {
			return false, errors.New("首次交付失败")
		}
		return true, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatal(err)
	}

	id := r.Actor("flaky", "f1")
	if err := reminders.Schedule(ctxbg, id, "pay", "charge", json.RawMessage("null"), time.Now()); err != nil {
		t.Fatal(err)
	}

	// 第一次交付失败 → release（retryIn 200ms）
	if n := runner.PollOnce(ctxbg); n != 0 {
		t.Fatalf("失败交付不应计入 = %d", n)
	}
	// release 后重投：到期后交付成功
	time.Sleep(250 * time.Millisecond)
	if n := runner.PollOnce(ctxbg); n != 1 {
		t.Fatalf("重投交付 = %d, want 1", n)
	}
	if got := atomic.LoadInt64(&attempts); got != 2 {
		t.Fatalf("fn 尝试 = %d, want 2（至少一次）", got)
	}
	// 完成后不再交付
	time.Sleep(100 * time.Millisecond)
	if n := runner.PollOnce(ctxbg); n != 0 {
		t.Fatalf("完成后重复交付 = %d", n)
	}
}

func TestReminderClaimFencingAndLeaseTakeover(t *testing.T) {
	reminders := NewMemoryReminderStore()
	ctxbg := context.Background()
	id := MustActorID("app", "job", "fence")
	if err := reminders.Schedule(ctxbg, id, "pay", "charge", json.RawMessage("null"), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}

	// silo-1 认领（status_version → 1）
	c1, err := reminders.ClaimDue(ctxbg, time.Now(), "silo-1", 30*time.Second)
	if err != nil || len(c1) != 1 {
		t.Fatalf("claim = %d, %v", len(c1), err)
	}
	rec := c1[0]

	// silo-2 在 silo-1 租期内不可重复认领
	c2, _ := reminders.ClaimDue(ctxbg, time.Now(), "silo-2", 30*time.Second)
	if len(c2) != 0 {
		t.Fatalf("租期内他实例重复认领 = %d", len(c2))
	}

	// fencing：stale status_version / 错误 holder 的 complete 拒绝
	if ok, _ := reminders.Complete(ctxbg, id, "pay", rec.StatusVersion-1, "silo-1"); ok {
		t.Fatal("stale version complete 应拒绝")
	}
	if ok, _ := reminders.Complete(ctxbg, id, "pay", rec.StatusVersion, "silo-2"); ok {
		t.Fatal("错误 holder complete 应拒绝")
	}

	// 模拟 silo-1 认领者宕机：租期过期后 silo-2 接管认领（status_version → 2）
	c3, _ := reminders.ClaimDue(ctxbg, time.Now().Add(31*time.Second), "silo-2", 30*time.Second)
	if len(c3) != 1 || c3[0].ClaimHolder != "silo-2" {
		t.Fatalf("租期过期接管 = %+v", c3)
	}
	// silo-1 的旧 complete（旧 version）被拒；silo-2 complete 成功
	if ok, _ := reminders.Complete(ctxbg, id, "pay", rec.StatusVersion, "silo-1"); ok {
		t.Fatal("被接管后旧认领 complete 应拒绝")
	}
	if ok, _ := reminders.Complete(ctxbg, id, "pay", c3[0].StatusVersion, "silo-2"); !ok {
		t.Fatal("新认领 complete 应成功")
	}
	if reminders.Cancel(ctxbg, id, "pay") {
		t.Fatal("已完成的记录不应可取消")
	}
}

func TestReminderScheduleUpsertAndCancel(t *testing.T) {
	reminders := NewMemoryReminderStore()
	ctxbg := context.Background()
	id := MustActorID("app", "job", "u1")
	if err := reminders.Schedule(ctxbg, id, "n", "m", json.RawMessage("null"), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	// 重复调度覆盖（不报 exists）
	if err := reminders.Schedule(ctxbg, id, "n", "m2", json.RawMessage(`{"a":1}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("重复调度失败：%v", err)
	}
	// 未到期不可认领
	c, _ := reminders.ClaimDue(ctxbg, time.Now(), "s1", time.Second)
	if len(c) != 0 {
		t.Fatalf("未到期被认领：%d", len(c))
	}
	if !reminders.Cancel(ctxbg, id, "n") {
		t.Fatal("取消应成功")
	}
	if reminders.Cancel(ctxbg, id, "n") {
		t.Fatal("重复取消应返回 false")
	}
}

// ---- Outbox ----

func TestOutboxAtomicCommitAndDispatch(t *testing.T) {
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryOutboxStore(ownership)
	r := NewRehearsal("outbox")
	// Rehearsal 默认 memory store（无 outbox）——注入 outbox store
	r.Stage = NewStage(WithClock(r.Clock), WithStore(store), WithOwnership(ownership),
		WithManualScheduler(), WithName("rehearsal:outbox"))

	m := Define("emit", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	var emitted []string
	m.Turn(Turn("fire", func(a *cartActor, ctx *CallContext, args struct{ Topic string }) (bool, error) {
		eid, err := a.EmitEvent(args.Topic, map[string]any{"n": len(a.Props().Items)})
		if err != nil {
			return false, err
		}
		emitted = append(emitted, eid)
		// 事件与 Props 同事务：Save 提交两者
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	m.Turn(Turn("peek", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "emit", "e1")

	// Emit 但不 Save：事件不落盘（journal 内，poll 不可见）
	fNoSave, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	_ = fNoSave
	r.PumpAll()

	// Emit + Save：同事务提交
	f, err := r.Stage.Call(ctxbg, id, "fire", map[string]any{"topic": "orders"})
	if err != nil {
		t.Fatal(err)
	}
	r.PumpAll()
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatalf("fire 失败：%v", err)
	}

	// 派发器：至少一次；handler 失败不 MarkSent → 重投
	var delivered int64
	var payloads []json.RawMessage
	dispatcher := NewOutboxDispatcher(store, []ActorID{id}, func(ctx context.Context, rec OutboxRecord) error {
		if atomic.AddInt64(&delivered, 1) == 1 {
			return errors.New("首次派发失败")
		}
		payloads = append(payloads, rec.Payload)
		return nil
	}).WithInterval(time.Hour)
	if n := dispatcher.PollOnce(ctxbg); n != 0 {
		t.Fatalf("失败派发计数 = %d", n)
	}
	if n := dispatcher.PollOnce(ctxbg); n != 1 {
		t.Fatalf("重投派发 = %d, want 1", n)
	}
	if got := atomic.LoadInt64(&delivered); got != 2 {
		t.Fatalf("handler 调用 = %d, want 2（至少一次）", got)
	}
	if len(payloads) != 1 || !json.Valid(payloads[0]) {
		t.Fatalf("载荷 = %v", payloads)
	}
	// MarkSent 后不再投递
	if n := dispatcher.PollOnce(ctxbg); n != 0 {
		t.Fatalf("已派发重复投递 = %d", n)
	}
}

func TestEmitEventWithoutOutboxStoreFailsFast(t *testing.T) {
	// 普通 MemoryPropStore（不支持 Outbox）：EmitEvent 立即报错（不静默丢失）
	r := NewRehearsal("nooutbox")
	m := Define("plain", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("fire", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		_, err := a.EmitEvent("orders", map[string]any{})
		return false, err
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	f, _ := r.Call(ctxbg, "plain", "p", "fire", nil)
	r.PumpAll()
	_, err := f.Await(ctxbg)
	var iase *InvalidActorStateError
	if !errors.As(err, &iase) {
		t.Fatalf("EmitEvent 错误 = %v, want InvalidActorStateError（fail fast）", err)
	}
}

func TestOutboxSurvivesReactivation(t *testing.T) {
	// 事件提交但派发器崩溃未 MarkSent → 失效重激活后事件仍在（journal 装载）
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryOutboxStore(ownership)
	rClock := NewVirtualClock()
	stage := NewStage(WithClock(rClock), WithStore(store), WithOwnership(ownership), WithManualScheduler())
	m := Define("persist", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("fire", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		if _, err := a.EmitEvent("orders", map[string]any{"k": 1}); err != nil {
			return false, err
		}
		return a.Save() == nil, nil
	}))
	m.Turn(Turn("peek", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "persist", "p1")

	f, _ := stage.Call(ctxbg, id, "fire", nil)
	stage.PumpAll()
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatal(err)
	}
	events, err := store.PollOutbox(ctxbg, id, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("事件 = %d, %v, want 1", len(events), err)
	}

	// 失效重激活（外部写者制造冲突）：outbox journal 重新装载，事件不丢
	rec, _ := store.Read(ctxbg, id)
	e, _ := ownership.Acquire(ctxbg, id)
	if _, err := store.Write(ctxbg, id, rec.Revision, 1, `{"items":["ext"]}`, e); err != nil {
		t.Fatal(err)
	}
	f2, _ := stage.Call(ctxbg, id, "peek", nil)
	stage.PumpAll()
	if _, err := f2.Await(ctxbg); err != nil {
		t.Fatalf("重激活失败：%v", err)
	}
	events2, _ := store.PollOutbox(ctxbg, id, 10)
	if len(events2) != 1 {
		t.Fatalf("重激活后事件丢失：%d", len(events2))
	}
}

// registerJobManifest 注册 job 角色的 charge Turn（交付目标）。
func registerJobManifest(t *testing.T, r *Rehearsal) *Manifest[cartProps, *cartActor] {
	m := Define("job", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("charge", func(a *cartActor, ctx *CallContext, _ json.RawMessage) (bool, error) {
		a.Props().Items = append(a.Props().Items, "charged")
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	return m
}
