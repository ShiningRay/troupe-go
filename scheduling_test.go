package troupe

// M4：调度模式交错反例（对齐 troupe.cr M4）——写饥饿、快照隔离、
// improv 冲突显式拒绝、quiesce 等待并发在飞。

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func schedulingManifest(t *testing.T, stage *Stage, gate chan struct{}) {
	t.Helper()
	m := Define("sched", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	// strict 写：追加一项
	m.Turn(Turn("write", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	// 长只读：阻塞在闸门上（写不饥饿验证）
	m.Turn(Turn("slowQuery", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		<-gate
		return a.Props().Items, nil
	}, ModeReadOnly))
	// 快照只读：读取已提交快照
	m.Turn(Turn("snapshot", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}, ModeReadOnly))
	// 只读禁写
	m.Turn(Turn("roWrite", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}, ModeReadOnly))
	// improv 可写副本：追加（Save 走激活时 revision CAS）
	m.Turn(Turn("improvWrite", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}, ModeImprov))
	if err := m.Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
}

func awaitSnap(t *testing.T, f *CallFuture) []string {
	t.Helper()
	raw, err := f.Await(context.Background())
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	var items []string
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("unmarshal 失败：%v", err)
	}
	return items
}

func TestReadOnlyNoWriteStarvation(t *testing.T) {
	// 写饥饿反例（troupe.cr M4）：长只读阻塞期间 strict 立即完成。
	stage := NewStage()
	gate := make(chan struct{})
	schedulingManifest(t, stage, gate)
	ctxbg := context.Background()
	id := MustActorID("app", "sched", "s1")

	// 先激活并提交一项
	n, err := Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "a"})
	if err != nil || n != 1 {
		t.Fatalf("初始化 write = %d, %v", n, err)
	}

	// 长只读并发执行（不占 strict 队列）
	fSlow, err := stage.Call(ctxbg, id, "slowQuery", nil, WithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // 确保 slowQuery 已开跑

	// strict 写在只读阻塞期间立即完成（写不饥饿）
	start := time.Now()
	n, err = Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "b"})
	strictLatency := time.Since(start)
	if err != nil || n != 2 {
		t.Fatalf("strict write during read_only = %d, %v", n, err)
	}
	if strictLatency > 2*time.Second {
		t.Fatalf("strict 写被只读饿死：延迟 %v", strictLatency)
	}

	close(gate)
	items := awaitSnap(t, fSlow)
	// 只读看到的是开跑时的已提交快照（["a"]），不含之后 strict 提交的 "b"
	if len(items) != 1 || items[0] != "a" {
		t.Fatalf("只读快照 = %v, want [a]（已提交快照隔离）", items)
	}
	stage.Close()
}

func TestReadOnlySnapshotIsolationAndNoWrite(t *testing.T) {
	stage := NewStage()
	gate := make(chan struct{})
	close(gate)
	schedulingManifest(t, stage, gate)
	ctxbg := context.Background()
	id := MustActorID("app", "sched", "s2")

	// strict 未提交状态对只读不可见：write 排队未执行（manual 模式）……
	// 用 live 语义验证：strict 已提交 ["a"] 后只读看到 ["a"]
	n, err := Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "a"})
	if n != 1 {
		t.Fatalf("write = %d, %v", n, err)
	}
	items := awaitSnap(t, mustCall(t, stage, id, "snapshot"))
	if len(items) != 1 || items[0] != "a" {
		t.Fatalf("只读快照 = %v, want [a]", items)
	}

	// 只读禁写：Save 显式拒绝（InvalidActorStateError），不产生写入
	_, err = Invoke[Empty, bool](ctxbg, stage, id, "roWrite", Empty{})
	var iase *InvalidActorStateError
	if !errors.As(err, &iase) {
		t.Fatalf("只读写错误 = %v, want InvalidActorStateError", err)
	}
	stage.Close()
}

func mustCall(t *testing.T, stage *Stage, id ActorID, method string) *CallFuture {
	t.Helper()
	f, err := stage.Call(context.Background(), id, method, nil)
	if err != nil {
		t.Fatalf("call %s 失败：%v", method, err)
	}
	return f
}

func TestImprovConflictExplicitRejection(t *testing.T) {
	// improv 并发可写副本：Save 以**Turn 开跑时快照** revision CAS——
	// 快照读取与 Save 之间被外部写者交错 → ConflictError 显式拒绝
	// （禁止隐式覆盖）；影子失败不失效主激活。
	// （Manual 模式同步执行无法交错——冲突场景需 live + gate。）
	stage := NewStage()
	shadowStarted := make(chan struct{})
	releaseShadow := make(chan struct{})
	m := Define("improv", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("write", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	m.Turn(Turn("improvWrite", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		shadowStarted <- struct{}{} // 快照已读取，通知外部写者交错
		<-releaseShadow
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}, ModeImprov))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "improv", "i1")

	n, err := Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "a"})
	if err != nil || n != 1 {
		t.Fatalf("初始化 = %d, %v", n, err)
	}

	// improv 开跑（快照已读）→ 外部写者推进 revision → 放行 → Save CAS 冲突
	f, err := stage.Call(ctxbg, id, "improvWrite", struct{ Item string }{Item: "imp"})
	if err != nil {
		t.Fatal(err)
	}
	<-shadowStarted
	rec, _ := stage.Store().Read(ctxbg, id)
	cur := stage.Ownership().Current(ctxbg, id) // 同 epoch 只推进 revision：纯 CAS 冲突
	if _, err := stage.Store().Write(ctxbg, id, rec.Revision, 1, `{"items":["ext"]}`, cur); err != nil {
		t.Fatal(err)
	}
	close(releaseShadow)
	_, err = f.Await(ctxbg)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("improv 冲突错误 = %v, want ConflictError{revision}（显式拒绝）", err)
	}

	// 影子失败不失效主激活；但外部写者已推进 store revision——主激活的下一次
	// strict Save 会按既有语义冲突失效、重激活从已提交快照（["ext"]）继续。
	n, err = Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "b"})
	if err == nil {
		t.Fatalf("外部写后主激活首个 strict Save 应冲突失效，得到 n=%d", n)
	}
	n, err = Invoke[struct{ Item string }, int](ctxbg, stage, id, "write", struct{ Item string }{Item: "b"})
	if err != nil || n != 2 {
		t.Fatalf("重激活后 write = %d, %v（从已提交快照继续）", n, err)
	}
	// improv 影子失败本身不产生 save_failed（失效仅限 strict Save 失败：
	// 上面的 1 次 save_failed 来自外部写者交错后主激活的 strict Save）
	if got := stage.Metrics().Counter("save_failures"); got != 1 {
		t.Fatalf("save_failures = %d, want 1（仅主激活 strict 冲突）", got)
	}
	stage.Close()
}

func TestQuiesceWaitsConcurrentInflight(t *testing.T) {
	// quiesce 反例（troupe.cr M4）：drain/idle 等待并发在飞，漏计即挂/漏。
	// （Manual 并发同步执行会阻塞调用方——quiesce 异步在飞用 live stage 验证。）
	ctxbg := context.Background()
	stage := NewStage()
	gate2 := make(chan struct{})
	schedulingManifest(t, stage, gate2)
	id2 := MustActorID("app", "sched", "q2")
	if _, err := Invoke[struct{ Item string }, int](ctxbg, stage, id2, "write", struct{ Item string }{Item: "a"}); err != nil {
		t.Fatal(err)
	}
	fConc, err := stage.Call(ctxbg, id2, "slowQuery", nil, WithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // 并发在飞

	stage.BeginDrain()
	if stage.Idle() {
		t.Fatal("并发在飞期间不应 idle（quiesce 漏计并发）")
	}
	if err := stage.FinishDrain(); err == nil {
		t.Fatal("并发在飞期间 finish 应拒绝")
	}
	close(gate2)
	if _, err := fConc.Await(ctxbg); err != nil {
		t.Fatalf("并发 Turn 失败：%v", err)
	}
	// 并发完成后 idle（已 draining：strict 拒新）
	if !stage.Idle() {
		t.Fatal("并发完成后应 idle")
	}
	if err := stage.FinishDrain(); err != nil {
		t.Fatalf("排空后 finish 失败：%v", err)
	}
}
