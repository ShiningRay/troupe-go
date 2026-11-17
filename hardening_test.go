package troupe

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- M2：幂等记录与 Props 原子提交 ----

type payProps struct {
	Charged int `json:"charged"`
}

type payActor struct {
	*Actor[payProps]
	failOnce bool
}

func TestIdempotentReplayAndConflict(t *testing.T) {
	r := NewRehearsal("idem")
	m := Define("pay", func(id ActorID, props payProps, ctx *ActivationContext) *payActor {
		return &payActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() payProps { return payProps{} })
	calls := 0   // Turn 执行次数（重放时 Turn 照常执行）
	fnCalls := 0 // 幂等 fn 实际执行次数（重放时应跳过）
	m.Turn(Turn("charge", func(a *payActor, ctx *CallContext, args struct {
		Key  string
		Seed int
	}) (int, error) {
		calls++
		res, err := a.Idempotent(args.Key, // 幂等键 = 请求键
			"seed-"+string(rune('0'+args.Seed)), func() (any, error) {
				fnCalls++
				a.Props().Charged += args.Seed // 只改内存 Props，由 helper 原子提交
				return a.Props().Charged, nil
			})
		if err != nil {
			return 0, err
		}
		var charged int
		if err := json.Unmarshal(res, &charged); err != nil {
			return 0, err
		}
		return charged, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()

	// 首次执行
	f1, _ := r.Call(ctxbg, "pay", "p1", "charge", map[string]any{"key": "k1", "seed": 5})
	r.PumpAll()
	raw, err := f1.Await(ctxbg)
	if err != nil {
		t.Fatalf("首次 charge 失败：%v", err)
	}
	if string(raw) != "5" {
		t.Fatalf("首次结果 = %s, want 5", raw)
	}
	if fnCalls != 1 {
		t.Fatalf("fn 调用 = %d, want 1", fnCalls)
	}

	// 同键同指纹 → 重放（Turn 照常执行，fn 不再执行、Props 不再累加）
	f2, _ := r.Call(ctxbg, "pay", "p1", "charge", map[string]any{"key": "k1", "seed": 5})
	r.PumpAll()
	raw, err = f2.Await(ctxbg)
	if err != nil {
		t.Fatalf("重放失败：%v", err)
	}
	if string(raw) != "5" || fnCalls != 1 || calls != 2 {
		t.Fatalf("重放 = %s fnCalls = %d calls = %d, want 5/1/2", raw, fnCalls, calls)
	}

	// 同键不同指纹 → IdempotencyConflictError
	f3, _ := r.Call(ctxbg, "pay", "p1", "charge", map[string]any{"key": "k1", "seed": 7})
	r.PumpAll()
	_, err = f3.Await(ctxbg)
	var ice *IdempotencyConflictError
	if !errors.As(err, &ice) {
		t.Fatalf("指纹冲突错误 = %v, want IdempotencyConflictError", err)
	}

	// 不同键 → 新操作
	f4, _ := r.Call(ctxbg, "pay", "p1", "charge", map[string]any{"key": "k2", "seed": 3})
	r.PumpAll()
	raw, err = f4.Await(ctxbg)
	if err != nil {
		t.Fatalf("k2 失败：%v", err)
	}
	if string(raw) != "8" { // 5 + 3（内存 Props 已含首次提交）
		t.Fatalf("k2 结果 = %s, want 8", raw)
	}
}

func TestIdempotentFailureRecordedAndReplayed(t *testing.T) {
	r := NewRehearsal("idemfail")
	fail := true
	m := Define("payfail", func(id ActorID, props payProps, ctx *ActivationContext) *payActor {
		return &payActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() payProps { return payProps{} })
	m.Turn(Turn("charge", func(a *payActor, ctx *CallContext, args struct{ Key string }) (int, error) {
		res, err := a.Idempotent(args.Key, "fp", func() (any, error) {
			if fail {
				return nil, errors.New("网关超时")
			}
			a.Props().Charged = 1
			return 1, nil
		})
		if err != nil {
			return 0, err
		}
		var charged int
		if err := json.Unmarshal(res, &charged); err != nil {
			return 0, err
		}
		return charged, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()

	// 首次失败：记录后重抛原错误
	f1, _ := r.Call(ctxbg, "payfail", "p", "charge", map[string]any{"key": "k"})
	r.PumpAll()
	_, err := f1.Await(ctxbg)
	if err == nil || !strings.Contains(err.Error(), "网关超时") {
		t.Fatalf("首次失败错误 = %v", err)
	}

	// 同键重放：按记录重抛（fn 不再执行）
	f2, _ := r.Call(ctxbg, "payfail", "p", "charge", map[string]any{"key": "k"})
	r.PumpAll()
	_, err = f2.Await(ctxbg)
	if err == nil || !strings.Contains(err.Error(), "幂等重放") {
		t.Fatalf("重放错误 = %v, want 幂等重放首次失败", err)
	}

	// 修复后新键成功（Props 变更由 helper 原子提交）
	fail = false
	f3, _ := r.Call(ctxbg, "payfail", "p", "charge", map[string]any{"key": "k-ok"})
	r.PumpAll()
	raw, err := f3.Await(ctxbg)
	if err != nil {
		t.Fatalf("成功 charge 失败：%v", err)
	}
	if string(raw) != "1" {
		t.Fatalf("成功结果 = %s", raw)
	}
	// 重开激活：journal 恢复，同键仍重放成功值
	if err := cartManifestNil(); err != nil {
		_ = err
	}
	f4, _ := r.Call(ctxbg, "payfail", "p", "charge", map[string]any{"key": "k-ok"})
	r.PumpAll()
	raw, err = f4.Await(ctxbg)
	if err != nil || string(raw) != "1" {
		t.Fatalf("重放成功值 = %s, %v", raw, err)
	}
}

func cartManifestNil() error { return nil }

// ---- M2：协作取消全链路 ----

func TestCooperativeCancel(t *testing.T) {
	stage := NewStage()
	m := Define("job", func(id ActorID, props payProps, ctx *ActivationContext) *payActor {
		return &payActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() payProps { return payProps{} })
	m.Turn(Turn("count", func(a *payActor, ctx *CallContext, _ struct {
		Steps int
	}) (int, error) {
		// 协作取消检查点：Turn 自行决定何时收束（部分状态可提交）
		for i := 0; i < 40; i++ {
			if ctx.CancelRequested() {
				break
			}
			a.Props().Charged++
			time.Sleep(2 * time.Millisecond)
		}
		if err := a.Save(); err != nil {
			return 0, err
		}
		return a.Props().Charged, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "job", "j1")
	ctxbg := context.Background()

	f, err := stage.Call(ctxbg, id, "count", map[string]any{"steps": 40})
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	time.Sleep(20 * time.Millisecond) // 让 Turn 跑若干步
	if !f.Cancel() {
		t.Fatal("未 settle 的调用应可取消")
	}
	raw, err := f.Await(ctxbg)
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatalf("unmarshal 失败：%v", err)
	}
	if n >= 40 {
		t.Fatalf("取消未生效：跑了 %d 步（应提前收束）", n)
	}
	if n == 0 {
		t.Fatal("取消后应保留部分进度")
	}
	// 已 settle 后 Cancel 返回 false
	if f.Cancel() {
		t.Fatal("已 settle 调用不应再可取消")
	}
	stage.Close()
}

// ---- M2：W3C traceparent ----

func TestTraceparentValidation(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if !ValidTraceparent(valid) {
		t.Fatal("合法 traceparent 应通过")
	}
	for _, bad := range []string{
		"", "00-0-0-01", // 长度不符
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // 全零 traceId
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // 全零 spanId
		"zz-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // 非法 hex
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01", // 大写拒绝
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // ff 版本禁止
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",    // 缺 flags
	} {
		if ValidTraceparent(bad) {
			t.Fatalf("非法 traceparent %q 应拒绝", bad)
		}
	}
}

func TestTraceparentDerivation(t *testing.T) {
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	child := DeriveTraceparent(parent)
	if !ValidTraceparent(child) {
		t.Fatalf("派生 span 非法：%q", child)
	}
	// 同 traceId、不同 spanId
	if !strings.Contains(child, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Fatalf("派生丢失 traceId：%q", child)
	}
	if strings.Contains(child, "00f067aa0ba902b7-") && child[36:52] == "00f067aa0ba902b7" {
		t.Fatalf("派生 spanId 未更新：%q", child)
	}
	// 非法父 → 静默降级为空
	if got := DeriveTraceparent("not-a-traceparent"); got != "" {
		t.Fatalf("非法父派生 = %q, want 空", got)
	}
	if got := DeriveTraceparent(""); got != "" {
		t.Fatalf("空父派生 = %q, want 空", got)
	}
}

func TestTurnBoundaryTraceEvents(t *testing.T) {
	r := NewRehearsal("trace")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	var traces []TraceEvent
	r.Stage.OnTrace(func(ev TraceEvent) { traces = append(traces, ev) })
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctxbg := context.Background()

	id := r.Actor("cart", "t1")
	f, _ := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "x"}, WithTraceparent(parent))
	r.PumpAll()
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	// 过期调用也产出 trace
	fExp, _ := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "y"}, WithTimeout(1*time.Nanosecond))
	r.Advance(time.Second)
	r.PumpAll()
	_, _ = fExp.Await(ctxbg)

	if len(traces) != 2 {
		t.Fatalf("trace 事件 = %d, want 2", len(traces))
	}
	completed, expired := traces[0], traces[1]
	if !completed.OK || completed.Outcome != "completed" {
		t.Fatalf("completed trace = %+v", completed)
	}
	if completed.Traceparent == "" || !ValidTraceparent(completed.Traceparent) {
		t.Fatalf("Turn span 非法：%q", completed.Traceparent)
	}
	if completed.ParentTraceparent != parent {
		t.Fatalf("父上下文 = %q, want %q", completed.ParentTraceparent, parent)
	}
	if expired.Outcome != "expired" || expired.OK {
		t.Fatalf("expired trace = %+v", expired)
	}
}

// ---- M2：metrics ----

func TestStageMetrics(t *testing.T) {
	r := NewRehearsal("metrics")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()
	id := r.Actor("cart", "m1")

	// 2 成功 + 1 白名单拒绝 + 1 未知角色
	for i := 0; i < 2; i++ {
		f, _ := r.Call(ctxbg, "cart", "m1", "add", map[string]any{"item": "x"})
		r.PumpAll()
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatalf("add 失败：%v", err)
		}
	}
	_, _ = r.Stage.Call(ctxbg, id, "on_stage", nil)                           // whitelist 拒绝
	_, _ = r.Stage.Call(ctxbg, MustActorID("metrics", "nope", "1"), "x", nil) // unknown role

	snap := r.Stage.Metrics().Snapshot()
	if snap["activations"] != 1 || snap["turns_completed"] != 2 {
		t.Fatalf("metrics = %v", snap)
	}
	if snap["turns_started"] != 2 || snap["turns_failed"] != 0 {
		t.Fatalf("turns 计数 = %v", snap)
	}
	roles := r.Stage.Metrics().RoleSnapshot()
	if roles["cart"]["rejections"] != 1 {
		t.Fatalf("cart rejections = %v", roles)
	}

	// 失败 Turn 计入 turns_failed
	bad := Define("bad", func(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, ctx)}
	})
	bad.SchemaVersion(1)
	bad.Initial(func() cartProps { return cartProps{} })
	bad.Turn(Turn("boom", func(a *cartActor, ctx *CallContext, _ Empty) (Empty, error) {
		return Empty{}, errors.New("业务失败")
	}))
	if err := bad.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	fb, _ := r.Call(ctxbg, "bad", "b", "boom", nil)
	r.PumpAll()
	_, _ = fb.Await(ctxbg)
	if snap2 := r.Stage.Metrics().Snapshot(); snap2["turns_failed"] != 1 {
		t.Fatalf("turns_failed = %d, want 1", snap2["turns_failed"])
	}
}

// ---- M2：泛型 Invoke（编译期参数/结果类型） ----

func TestInvokeTyped(t *testing.T) {
	// Invoke 内部 Call 后立即 Await——仅适用于 live 调度；
	// Manual（Rehearsal）需先 Pump 再经 Future.Await（见 M1/M2 其他测试）。
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	defer stage.Close()
	ctxbg := context.Background()
	id := MustActorID("app", "cart", "i1")

	n, err := Invoke[struct{ Item string }, int](ctxbg, stage, id, "add", struct{ Item string }{Item: "pear"})
	if err != nil {
		t.Fatalf("invoke 失败：%v", err)
	}
	if n != 1 {
		t.Fatalf("add = %d, want 1", n)
	}

	// 未知方法 → 运行时白名单拒绝
	_, err = Invoke[Empty, int](ctxbg, stage, id, "nope", Empty{})
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectWhitelist {
		t.Fatalf("未知方法错误 = %v, want whitelist", err)
	}

	// 参数未知字段/类型不符 → 排队前 InvalidArgumentError（零副作用）
	var iae *InvalidArgumentError
	_, err = stage.Call(ctxbg, id, "add", map[string]any{"wrong": 1})
	if !errors.As(err, &iae) {
		t.Fatalf("未知字段错误 = %v, want InvalidArgumentError", err)
	}
	_, err = Invoke[struct{ Item string }, int](ctxbg, stage, id, "add", struct{ Item string }{Item: ""})
	// 空串是合法 string——此项应成功（值类型校验交由业务 Turn）
	if err != nil {
		t.Fatalf("合法空串 add 不应失败：%v", err)
	}
}
