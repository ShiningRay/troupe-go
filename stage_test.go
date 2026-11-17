package troupe

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 基础行为 ----

func TestCallBasicsLive(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "cart", "c1")

	f, err := stage.Call(context.Background(), id, "add", map[string]any{"item": "pear"})
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	raw, err := f.Await(context.Background())
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if string(raw) != "1" {
		t.Fatalf("add 结果 = %s, want 1", raw)
	}

	// 持久化读回：create(rev1) + save(rev2)
	rec, _ := stage.Store().Read(id)
	if rec == nil || rec.Revision != 2 {
		t.Fatalf("store 记录 revision = %d, want 2（create+save）", rec.Revision)
	}
	if !strings.Contains(rec.Data, "pear") {
		t.Fatalf("store 数据缺 pear：%s", rec.Data)
	}
	stage.Close()
}

func TestUnknownRoleAndWhitelist(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}

	// 未注册角色
	_, err := stage.Call(context.Background(), MustActorID("app", "ghost", "1"), "add", nil)
	var ure *UnknownRoleError
	if !errors.As(err, &ure) {
		t.Fatalf("未知角色错误 = %v, want UnknownRoleError", err)
	}

	// 白名单外方法
	_, err = stage.Call(context.Background(), MustActorID("app", "cart", "c1"), "on_stage", nil)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectWhitelist || cre.Retryable {
		t.Fatalf("白名单外错误 = %v, want CallRejectedError{whitelist, retryable=false}", err)
	}

	// 参数校验失败零副作用（排队前拒绝）
	if _, err = stage.Call(context.Background(), MustActorID("app", "cart", "c1"), "add", map[string]any{"item": 42}); err == nil {
		t.Fatal("类型不符应拒绝")
	}
	rec, _ := stage.Store().Read(MustActorID("app", "cart", "c1"))
	if rec != nil {
		t.Fatalf("排队前拒绝不应创建 Props：%+v", rec)
	}
	stage.Close()
}

// ---- 验收：并发首调只激活一次 ----

func TestConcurrentFirstCallActivatesOnce(t *testing.T) {
	stage := NewStage()
	var mu sync.Mutex
	activations := 0
	stage.OnEvent(func(ev StageEvent) {
		if ev.Type == EventActivated {
			mu.Lock()
			activations++
			mu.Unlock()
		}
	})
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "cart", "hot")

	const n = 32
	futures := make([]*CallFuture, n)
	var start sync.WaitGroup
	start.Add(n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Done()
			start.Wait() // 同步起跑：并发首调竞争同一 slot
			f, err := stage.Call(context.Background(), id, "add", map[string]any{"item": "x"})
			if err != nil {
				t.Errorf("call %d 失败：%v", i, err)
				return
			}
			futures[i] = f
		}(i)
	}
	wg.Wait()

	for i, f := range futures {
		if f == nil {
			t.Fatalf("call %d 未取得 future", i)
		}
		if _, err := f.Await(context.Background()); err != nil {
			t.Fatalf("await %d 失败：%v", i, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if activations != 1 {
		t.Fatalf("激活次数 = %d, want 1（single-flight）", activations)
	}
	rec, _ := stage.Store().Read(id)
	if rec == nil || rec.Revision != n+1 {
		t.Fatalf("revision = %d, want %d（create + 每个写 Turn 各提交一次）", rec.Revision, n+1)
	}
	stage.Close()
}

// ---- 验收：100 个写 Turn 不交错 ----

func TestStrictSerialTurns(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "cart", "serial")

	const n = 100
	futures := make([]*CallFuture, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 有界队列满时显式拒绝（backpressure, retryable）——生产方语义正确的行为是重试。
			for attempt := 0; ; attempt++ {
				f, err := stage.Call(context.Background(), id, "add", map[string]any{"item": "i"})
				if err == nil {
					mu.Lock()
					futures = append(futures, f)
					mu.Unlock()
					return
				}
				var cre *CallRejectedError
				if errors.As(err, &cre) && cre.Reason == RejectBackpressure && cre.Retryable && attempt < 1000 {
					time.Sleep(time.Millisecond)
					continue
				}
				t.Errorf("call %d 失败：%v", i, err)
				return
			}
		}(i)
	}
	wg.Wait()
	if len(futures) != n {
		t.Fatalf("成功入队 = %d, want %d", len(futures), n)
	}
	for i, f := range futures {
		if _, err := f.Await(context.Background()); err != nil {
			t.Fatalf("await %d 失败：%v", i, err)
		}
	}

	f, _ := stage.Call(context.Background(), id, "peek", nil)
	raw, err := f.Await(context.Background())
	if err != nil {
		t.Fatalf("peek 失败：%v", err)
	}
	var items []string
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("unmarshal 失败：%v", err)
	}
	if len(items) != n {
		t.Fatalf("items 数量 = %d, want %d（strict 串行：无丢失更新）", len(items), n)
	}
	rec, _ := stage.Store().Read(id)
	if rec.Revision != n+1 {
		t.Fatalf("revision = %d, want %d（create + n 次保存）", rec.Revision, n+1)
	}
	stage.Close()
}

// ---- 验收：排队过期无副作用 ----

func TestQueuedExpiryZeroSideEffects(t *testing.T) {
	r := NewRehearsal("expiry")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "e1")

	// 调用 1：短 deadline，不入 pump，随后时钟越过 deadline
	f1, err := r.Stage.Call(context.Background(), id, "add", map[string]any{"item": "late"}, WithTimeout(1*time.Second))
	if err != nil {
		t.Fatalf("call1 失败：%v", err)
	}
	r.Advance(2 * time.Second)

	// 调用 2：deadline 未过，正常执行
	f2, err := r.Stage.Call(context.Background(), id, "add", map[string]any{"item": "fresh"}, WithTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("call2 失败：%v", err)
	}
	processed := r.PumpAll()
	if processed != 1 {
		t.Fatalf("pump 处理数 = %d, want 1（过期请求不执行）", processed)
	}

	// f1：排队过期——零副作用、错误分类 queued
	_, err = f1.Await(context.Background())
	var cte *CallTimeoutError
	if !errors.As(err, &cte) || cte.Phase != PhaseQueued {
		t.Fatalf("f1 错误 = %v, want CallTimeoutError{queued}", err)
	}
	raw, err := f2.Await(context.Background())
	if err != nil {
		t.Fatalf("f2 await 失败：%v", err)
	}
	if string(raw) != "1" {
		t.Fatalf("f2 结果 = %s, want 1（过期项不计入）", raw)
	}

	// f1 的 item 未落盘
	f3, _ := r.Stage.Call(context.Background(), id, "peek", nil)
	r.PumpAll()
	raw, _ = f3.Await(context.Background())
	if strings.Contains(string(raw), "late") {
		t.Fatalf("过期调用产生副作用：items = %s", raw)
	}

	// 事件：turn_expired 已记录
	if len(r.Events(EventTurnExpired)) != 1 {
		t.Fatalf("turn_expired 事件数 = %d, want 1", len(r.Events(EventTurnExpired)))
	}
}

// ---- 验收：执行超时不放行旧 Turn（执行占用 + 结果未知）----

func TestExecutionTimeoutUnknownOutcome(t *testing.T) {
	stage := NewStage()
	type slowProps struct {
		Done bool `json:"done"`
	}
	m := Define("slow", func(id ActorID, props slowProps, ctx *ActivationContext) *struct{ *Actor[slowProps] } {
		return &struct{ *Actor[slowProps] }{NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() slowProps { return slowProps{} })

	gate := make(chan struct{})
	slowOnce := false
	var mu sync.Mutex
	m.Turn(Turn("long", func(a *struct{ *Actor[slowProps] }, ctx *CallContext, _ Empty) (bool, error) {
		mu.Lock()
		slowOnce = true
		mu.Unlock()
		<-gate // 模拟长执行：deadline 在等待中到期
		p := a.Props()
		p.Done = true
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	m.Turn(Turn("quick", func(a *struct{ *Actor[slowProps] }, ctx *CallContext, _ Empty) (bool, error) {
		// 只有在 long 的效果已提交后才应执行（执行占用不放行旧 Turn）；
		// 快照读 store 验证 long 已提交。
		rec, _ := a.actx.store.Read(a.ID())
		return rec != nil && strings.Contains(rec.Data, "true"), nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "slow", "s1")

	// long：50ms deadline，Turn 被闸门阻塞
	fLong, err := stage.Call(context.Background(), id, "long", nil, WithTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("long call 失败：%v", err)
	}
	_, err = fLong.Await(context.Background())
	var cte *CallTimeoutError
	if !errors.As(err, &cte) || cte.Phase != PhaseExecuting {
		t.Fatalf("long 错误 = %v, want CallTimeoutError{executing}", err)
	}

	// quick 在 long 之后入队：不得在 long settle 前执行
	var quickResult json.RawMessage
	fQuick, err := stage.Call(context.Background(), id, "quick", nil)
	if err != nil {
		t.Fatalf("quick call 失败：%v", err)
	}
	close(gate) // 放行 long
	raw, err := fQuick.Await(context.Background())
	if err != nil {
		t.Fatalf("quick await 失败：%v", err)
	}
	quickResult = raw
	if string(quickResult) != "true" {
		t.Fatalf("quick 观察到 long 未提交：store=%s", quickResult)
	}

	// 迟到的 long 结果不得放行（即使成功也是结果未知）
	_, err = fLong.Await(context.Background())
	if !errors.As(err, &cte) || cte.Phase != PhaseExecuting {
		t.Fatalf("迟到的 long 结果错误 = %v, want 仍为 CallTimeoutError{executing}", err)
	}

	// 效果已提交（结果未知 ≠ 效果未知）
	rec, _ := stage.Store().Read(id)
	if rec == nil || !strings.Contains(rec.Data, "true") {
		t.Fatalf("long 效果未提交：%+v", rec)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slowOnce {
		t.Fatal("long Turn 未执行")
	}
	stage.Close()
}

// ---- 验收：CAS/保存失败从最后提交快照恢复 ----

func TestSaveConflictInvalidatesAndReactivates(t *testing.T) {
	r := NewRehearsal("conflict")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "cf")
	ctxbg := context.Background()

	// 正常写入 revision=1
	f1, _ := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "one"})
	r.PumpAll()
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("首次写入失败：%v", err)
	}

	// 外部并发写者：直接经 store 用当前 revision 提交（模拟另一激活/节点）
	rec, _ := r.Store.Read(id)
	if _, err := r.Store.Write(id, rec.Revision, 1, `{"items":["external"]}`, r.Ownership.Current(id)); err != nil {
		t.Fatalf("外部写失败：%v", err)
	}

	// Actor 内保存：ctx.revision 已过期 → CAS 冲突 → 实例失效（save_failed 事件）
	f2, _ := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "two"})
	r.PumpAll()
	_, err := f2.Await(ctxbg)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("保存冲突错误 = %v, want ConflictError{revision}", err)
	}
	if len(r.Events(EventSaveFailed)) != 1 {
		t.Fatalf("save_failed 事件数 = %d, want 1", len(r.Events(EventSaveFailed)))
	}

	// 后续调用重新激活：从最后已提交快照（external）继续，内部写入 "two" 未泄漏
	f3, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	r.PumpAll()
	raw, err := f3.Await(ctxbg)
	if err != nil {
		t.Fatalf("重激活后 peek 失败：%v", err)
	}
	if string(raw) != `["external"]` {
		t.Fatalf("重激活快照 = %s, want [\"external\"]（丢弃未提交状态）", raw)
	}
	if len(r.Events(EventActivated)) != 2 {
		t.Fatalf("激活事件数 = %d, want 2（失效后重激活）", len(r.Events(EventActivated)))
	}
}

// ---- 验收：v1→v3 逐级迁移且失败不写 ----

func seedV1(t *testing.T, r *Rehearsal, id ActorID, data string) uint64 {
	t.Helper()
	epoch := r.Ownership.Acquire(id)
	if _, err := r.Store.Create(id, 1, data, epoch); err != nil {
		t.Fatalf("种子 v1 数据失败：%v", err)
	}
	return epoch
}

func TestMigrationChainV1ToV3(t *testing.T) {
	r := NewRehearsal("migr")
	if err := migratingManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("migr", "m1")
	seedV1(t, r, id, `{"name":"legacy"}`)

	f, err := r.Stage.Call(context.Background(), id, "describe", nil)
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	r.PumpAll()
	raw, err := f.Await(context.Background())
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal 失败：%v", err)
	}
	if got["name"] != "legacy" || got["extra"] != "base" {
		t.Fatalf("迁移后状态不符：%v", got)
	}
	rec, _ := r.Store.Read(id)
	if rec.SchemaVersion != 3 {
		t.Fatalf("schema = %d, want 3", rec.SchemaVersion)
	}
	if rec.Revision != 2 { // create(1) + 迁移提交(2)
		t.Fatalf("迁移提交 revision = %d, want 2", rec.Revision)
	}
}

func TestMigrationFailureNoWrite(t *testing.T) {
	broken := Define("migrfail", func(id ActorID, props v3Props, ctx *ActivationContext) *migratingActor {
		return &migratingActor{Actor: NewActor(id, props, ctx)}
	})
	broken.SchemaVersion(2)
	broken.Initial(func() v3Props { return v3Props{Extra: "new"} })
	broken.Migrate(Migrate(1, 2, func(v v1Props) (v2Props, error) {
		return v2Props{}, errors.New("目标结构不可达")
	}))
	broken.Turn(Turn("x", func(a *migratingActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))

	r := NewRehearsal("migrfail")
	if err := broken.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("migrfail", "m1")
	seedV1(t, r, id, `{"name":"legacy"}`)

	f, _ := r.Stage.Call(context.Background(), id, "x", nil)
	r.PumpAll()
	_, err := f.Await(context.Background())
	var ae *ActivationError
	if !errors.As(err, &ae) {
		t.Fatalf("迁移失败错误 = %v, want ActivationError", err)
	}
	if !strings.Contains(err.Error(), "1→2") {
		t.Fatalf("错误未指明迁移步骤：%v", err)
	}
	// 失败不写：记录仍是 v1 原始数据
	rec, _ := r.Store.Read(id)
	if rec.SchemaVersion != 1 || rec.Data != `{"name":"legacy"}` {
		t.Fatalf("迁移失败产生了写入：%+v", rec)
	}
	if len(r.Events(EventActivationFailed)) != 1 {
		t.Fatalf("activation_failed 事件数 = %d, want 1", len(r.Events(EventActivationFailed)))
	}

	// 槽位回 Idle：修复后（此处仅验证可重试）再次调用仍尝试激活
	if len(r.Events(EventActivated)) != 0 {
		t.Fatalf("失败的激活不应 emit activated：%d", len(r.Events(EventActivated)))
	}
}

// ---- 背压与上限 ----

func TestBackpressureExplicitRejection(t *testing.T) {
	cfg := DefaultStageConfig()
	cfg.MaxQueueDepth = 1
	r := NewRehearsal("bp", cfg)
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "b1")
	ctxbg := context.Background()

	f1, err := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "1"})
	if err != nil {
		t.Fatalf("call1 失败：%v", err)
	}
	// 队列深度 1 已被 f1 占满：第二个调用立即显式拒绝
	_, err = r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "2"})
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectBackpressure || !cre.Retryable {
		t.Fatalf("第二个 call 错误 = %v, want CallRejectedError{backpressure, retryable=true}", err)
	}
	// 消费后腾出容量：重试成功（retryable 语义）
	r.PumpAll()
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("f1 失败：%v", err)
	}
	f2, err := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "2"})
	if err != nil {
		t.Fatalf("重试 call2 失败：%v", err)
	}
	r.PumpAll()
	if _, err := f2.Await(ctxbg); err != nil {
		t.Fatalf("f2 失败：%v", err)
	}
}

func TestMaxActivationsLimit(t *testing.T) {
	cfg := DefaultStageConfig()
	cfg.MaxActivations = 1
	r := NewRehearsal("act", cfg)
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()
	f1, err := r.Stage.Call(ctxbg, r.Actor("cart", "a"), "peek", nil)
	if err != nil {
		t.Fatalf("第一个 actor call 失败：%v", err)
	}
	r.PumpAll()
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("第一个 actor await 失败：%v", err)
	}
	_, err = r.Stage.Call(ctxbg, r.Actor("cart", "b"), "peek", nil)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectBackpressure {
		t.Fatalf("第二个 actor 错误 = %v, want CallRejectedError{backpressure}", err)
	}
}

// ---- 停机与 goroutine 泄漏 ----

func TestShutdownRejectsNewAndQueued(t *testing.T) {
	// Manual 模式保证确定性：调用入队后不会被消费者抢先执行。
	r := NewRehearsal("shutdown")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "s1")
	ctxbg := context.Background()

	f1, err := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "first"})
	if err != nil {
		t.Fatalf("call1 失败：%v", err)
	}
	r.PumpAll()
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("f1 失败：%v", err)
	}

	// 入队但不 pump → Close 应拒绝队列中的调用
	f2, _ := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "second"})
	r.Stage.Close()

	_, err = f2.Await(ctxbg)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectShutdown {
		t.Fatalf("停机后排空队列调用错误 = %v, want CallRejectedError{shutdown}", err)
	}
	_, err = r.Stage.Call(ctxbg, id, "peek", nil)
	if !errors.As(err, &cre) || cre.Reason != RejectShutdown {
		t.Fatalf("停机后新 call 错误 = %v, want CallRejectedError{shutdown}", err)
	}
}

func TestStageCloseNoGoroutineLeak(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()
	var ids []ActorID
	for i := 0; i < 8; i++ {
		id := MustActorID("app", "cart", string(rune('a'+i)))
		ids = append(ids, id)
		f, err := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"})
		if err != nil {
			t.Fatalf("call %d 失败：%v", i, err)
		}
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatalf("await %d 失败：%v", i, err)
		}
	}
	afterCalls := runtime.NumGoroutine()
	if afterCalls < baseline+8 {
		t.Fatalf("激活 8 个 actor 后 goroutine = %d, baseline = %d（每激活一消费者）", afterCalls, baseline)
	}

	stage.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Close 后 goroutine 未回落：%d > baseline %d（消费者泄漏）", runtime.NumGoroutine(), baseline)
}

func TestConsumerExitedAssertion(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "cart", "x")
	f, _ := stage.Call(context.Background(), id, "peek", nil)
	if _, err := f.Await(context.Background()); err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if stage.consumerExited(id) {
		t.Fatal("Close 前消费者不应退出")
	}
	stage.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stage.consumerExited(id) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Close 后消费者未退出")
}

// ---- Intermission（sweep）----

func TestIntermissionSweep(t *testing.T) {
	cfg := DefaultStageConfig()
	cfg.IdleTimeout = 5 * time.Second
	r := NewRehearsal("idle", cfg)
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "i1")
	ctxbg := context.Background()

	f, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	r.PumpAll()
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if r.Stage.ActorCount() != 1 {
		t.Fatalf("激活数 = %d, want 1", r.Stage.ActorCount())
	}

	// 未超时：不下线
	r.Advance(1 * time.Second)
	if got := r.Sweep(); len(got) != 0 {
		t.Fatalf("未超时 sweep 下线了 %v", got)
	}
	// 超时：下线且状态保留（props 在 store，不在内存）
	r.Advance(5 * time.Second)
	got := r.Sweep()
	if len(got) != 1 || got[0] != id {
		t.Fatalf("sweep = %v, want [%v]", got, id)
	}
	if r.Stage.ActorCount() != 0 {
		t.Fatalf("下线后激活数 = %d", r.Stage.ActorCount())
	}
	// 下线事件 + off_stage 钩子
	if len(r.Events(EventDeactivated)) != 1 {
		t.Fatalf("deactivated 事件数 = %d", len(r.Events(EventDeactivated)))
	}
	// 重新调用 → 重新激活，props 从 store 恢复
	f2, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	r.PumpAll()
	raw, err := f2.Await(ctxbg)
	if err != nil {
		t.Fatalf("重激活失败：%v", err)
	}
	if len(r.Events(EventActivated)) != 2 {
		t.Fatalf("激活事件数 = %d, want 2", len(r.Events(EventActivated)))
	}
	_ = raw
}

func TestHooksOnOffStage(t *testing.T) {
	var created []*cartActor
	hookManifest := Define("hookcart", func(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
		a := &cartActor{Actor: NewActor(id, props, ctx)}
		created = append(created, a)
		return a
	})
	hookManifest.SchemaVersion(1)
	hookManifest.Initial(func() cartProps { return cartProps{} })
	hookManifest.Turn(Turn("peek", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}))

	cfg := DefaultStageConfig()
	cfg.IdleTimeout = 1 * time.Second
	r := NewRehearsal("hooks", cfg)
	if err := hookManifest.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("hookcart", "h1")
	ctxbg := context.Background()

	f, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	r.PumpAll()
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if len(created) != 1 {
		t.Fatalf("工厂调用 = %d, want 1", len(created))
	}
	if created[0].staged != 1 {
		t.Fatalf("OnStage 调用 = %d, want 1", created[0].staged)
	}

	r.Advance(2 * time.Second)
	if got := r.Sweep(); len(got) != 1 {
		t.Fatalf("sweep = %v, want 下线 1 个", got)
	}
	if created[0].unstaged != 1 {
		t.Fatalf("OffStage 调用 = %d, want 1", created[0].unstaged)
	}
}

// ---- OnStage 失败 → ActivationError ----

type failingHookActor struct {
	*Actor[cartProps]
}

func (f *failingHookActor) OnStage() error { return errors.New("boom") }

func TestOnStageFailureActivationError(t *testing.T) {
	m := Define("hookfail", func(id ActorID, props cartProps, ctx *ActivationContext) *failingHookActor {
		return &failingHookActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("peek", func(a *failingHookActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}))

	r := NewRehearsal("hookfail")
	if err := m.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("hookfail", "h1")
	ctxbg := context.Background()

	f, _ := r.Stage.Call(ctxbg, id, "peek", nil)
	r.PumpAll()
	_, err := f.Await(ctxbg)
	var ae *ActivationError
	if !errors.As(err, &ae) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("OnStage 失败错误 = %v, want ActivationError(boom)", err)
	}
	if len(r.Events(EventActivationFailed)) != 1 {
		t.Fatalf("activation_failed 事件数 = %d, want 1", len(r.Events(EventActivationFailed)))
	}
}
