package troupe

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// ---- 验收：任意目录名下 Rehearsal 一致 ----

func TestRehearsalNamespaceIndependence(t *testing.T) {
	// 两个 Rehearsal 同 role/key：状态互不可见，同输入同输出。
	r1 := NewRehearsal("rehearsal-a")
	r2 := NewRehearsal("rehearsal-b")
	for _, r := range []*Rehearsal{r1, r2} {
		if err := cartManifest().Register(r.Stage); err != nil {
			t.Fatalf("注册失败：%v", err)
		}
	}
	ctxbg := context.Background()

	for i := 0; i < 3; i++ {
		f, err := r1.Call(ctxbg, "cart", "same", "add", map[string]any{"item": "x"})
		if err != nil {
			t.Fatalf("r1 call 失败：%v", err)
		}
		r1.PumpAll()
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatalf("r1 await 失败：%v", err)
		}
	}
	// r2 同 key 未受 r1 影响
	f2, err := r2.Call(ctxbg, "cart", "same", "peek", nil)
	if err != nil {
		t.Fatalf("r2 call 失败：%v", err)
	}
	r2.PumpAll()
	raw, err := f2.Await(ctxbg)
	if err != nil {
		t.Fatalf("r2 await 失败：%v", err)
	}
	if string(raw) != "[]" {
		t.Fatalf("r2 受 r1 污染：%s", raw)
	}
	// id 前缀各自固定 namespace
	if got := r1.Actor("cart", "same").Canonical(); got != "rehearsal-a/cart/same" {
		t.Fatalf("r1 canonical = %s", got)
	}
	if got := r2.Actor("cart", "same").Canonical(); got != "rehearsal-b/cart/same" {
		t.Fatalf("r2 canonical = %s", got)
	}
}

func TestRehearsalDeterministicPump(t *testing.T) {
	// Manual 调度确定性：pump 每次至多执行一个 Turn；PumpAll 清空；事件序列可断言。
	r := NewRehearsal("det")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()
	id := r.Actor("cart", "d")

	f1, _ := r.Call(ctxbg, "cart", "d", "add", map[string]any{"item": "1"})
	f2, _ := r.Call(ctxbg, "cart", "d", "add", map[string]any{"item": "2"})
	f3, _ := r.Call(ctxbg, "cart", "d", "peek", nil)

	if !r.Pump(id) {
		t.Fatal("第一个 pump 应执行一个 Turn")
	}
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("f1 失败：%v", err)
	}
	if f2.Settled() {
		t.Fatal("f2 不应在第二个 pump 前 settle（严格 FIFO）")
	}
	// 每次 pump 恰好一个 Turn
	if !r.Pump(id) || !r.Pump(id) {
		t.Fatal("后续 pump 应各执行一个 Turn")
	}
	raw, err := f3.Await(ctxbg)
	if err != nil {
		t.Fatalf("f3 失败：%v", err)
	}
	var items []string
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("unmarshal 失败：%v", err)
	}
	if len(items) != 2 || items[0] != "1" || items[1] != "2" {
		t.Fatalf("FIFO 顺序不符：%v", items)
	}
	// 清空后 pump 无事可做
	if r.Pump(id) || r.PumpAll() != 0 {
		t.Fatal("清空后 pump/pump_all 不应再执行")
	}
	if got := len(r.Events(EventTurnEnded)); got != 3 {
		t.Fatalf("turn_ended 事件数 = %d, want 3", got)
	}
}

func TestRehearsalAwaitEscapeOnCtxCancel(t *testing.T) {
	// Manual 模式未 pump：Await 阻塞直至 ctx 取消（逃逸口），返回 UnknownOutcome。
	r := NewRehearsal("escape")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	f, err := r.Call(ctx, "cart", "e", "add", map[string]any{"item": "x"})
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	_, err = f.Await(ctx)
	if _, ok := err.(*UnknownOutcomeError); !ok {
		t.Fatalf("ctx 取消错误 = %v, want UnknownOutcomeError", err)
	}
	// 之后 pump 正常执行，效果照常提交（放弃等待 ≠ 取消执行）
	r.PumpAll()
	rec, _ := r.Store.Read(r.Actor("cart", "e"))
	if rec == nil {
		t.Fatal("pump 后应正常激活提交")
	}
}

func TestRehearsalVirtualClockDeadline(t *testing.T) {
	// 虚拟时钟确定性判定 deadline，不依赖真实时间。
	r := NewRehearsal("vclock")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := r.Actor("cart", "v1")
	ctxbg := context.Background()

	// deadline 为虚拟时刻 t0+10s；调用后推进 20s → deadline 已过 → 排队过期
	f, err := r.Stage.Call(ctxbg, id, "add", map[string]any{"item": "x"}, WithTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	r.Advance(20 * time.Second)
	r.PumpAll()
	_, err = f.Await(ctxbg)
	if cte, ok := err.(*CallTimeoutError); !ok || cte.Phase != PhaseQueued {
		t.Fatalf("虚拟时钟过期错误 = %v, want CallTimeoutError{queued}", err)
	}
	// deadline 字段为绝对虚拟时刻
	if !f.req.deadline.Equal(r.Clock.Now().Add(-10 * time.Second)) {
		t.Fatalf("deadline = %v 未锚定虚拟时刻", f.req.deadline)
	}
}

func TestCallReplyJSONShape(t *testing.T) {
	// 结果经 JSON 边界：数字/对象/空值形态稳定（跨语言向量对齐前置）。
	r := NewRehearsal("shape")
	type shaped struct {
		OK    bool            `json:"ok"`
		Count int             `json:"count"`
		Extra json.RawMessage `json:"extra"`
	}
	m := Define("shape", func(id ActorID, props shaped, ctx *ActivationContext) *struct{ *Actor[shaped] } {
		return &struct{ *Actor[shaped] }{NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() shaped { return shaped{} })
	m.Turn(Turn("echo", func(a *struct{ *Actor[shaped] }, ctx *CallContext, _ shaped) (map[string]any, error) {
		return map[string]any{"ok": true, "count": 3, "extra": nil}, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	ctxbg := context.Background()
	f, err := r.Call(ctxbg, "shape", "s", "echo", shaped{OK: true, Count: 1, Extra: json.RawMessage(`{"a":1}`)})
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	r.PumpAll()
	raw, err := f.Await(ctxbg)
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if string(raw) != `{"count":3,"extra":null,"ok":true}` {
		// Go map 序列化键序为字典序——稳定但不承诺跨语言顺序；此处锚定本仓行为。
		t.Fatalf("结果 JSON = %s", raw)
	}
}
