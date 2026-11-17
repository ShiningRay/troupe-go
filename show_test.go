package troupe

// M4：Show realtime（Actor Audience 经完整 Turn/边界回调分离/在线尽力/
// 跨 Stage/背压隔离/离线不保证）与 Director（health/metrics/events/pprof）。

import (
	"context"
	"encoding/json"

	"net/http"
	"strings"

	"testing"
	"time"
)

func showManifest(t *testing.T, stage *Stage, gate chan struct{}) {
	t.Helper()
	m := Define("watch", func(id ActorID, props cartProps, actx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("on_event", func(a *cartActor, ctx *CallContext, args struct {
		Topic   string `json:"topic"`
		Preview string `json:"preview"`
	}) (bool, error) {
		a.Props().Items = append(a.Props().Items, args.Topic)
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	m.Turn(Turn("slow", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		<-gate
		return true, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
}

func TestShowActorAudienceViaFullTurn(t *testing.T) {
	// Actor Audience 经完整 Turn：白名单/串行/持久化语义不变。
	stage := NewStage()
	gate := make(chan struct{})
	close(gate)
	showManifest(t, stage, gate)
	ctxbg := context.Background()
	id := MustActorID("app", "watch", "w1")

	hub := NewShowHub(stage)
	show := hub.Show("events")
	show.ActorSubscribe(id, "on_event")

	report := hub.Publish("events", map[string]any{"topic": "orders", "preview": "x"})
	if report.Delivered != 1 || report.Failed != 0 {
		t.Fatalf("report = %+v, want 1/0", report)
	}
	// 交付经 Turn：持久化已发生（Props 落盘——完整 Turn 语义）
	rec, _ := stage.Store().Read(ctxbg, id)
	if rec == nil || !json.Valid([]byte(rec.Data)) || !strings.Contains(rec.Data, "orders") {
		t.Fatalf("Audience Turn 副作用未持久化：%+v", rec)
	}
	// 未订阅 topic：全零报告
	if got := hub.Publish("nope", nil); got.Delivered != 0 || got.Failed != 0 {
		t.Fatalf("未订阅 topic report = %+v", got)
	}
	stage.Close()
}

func TestShowExternalCallbackAndIsolation(t *testing.T) {
	// 边界回调（非 Actor 观察者）与 Actor 订阅分离；panic 隔离。
	stage := NewStage()
	gate := make(chan struct{})
	close(gate)
	showManifest(t, stage, gate)
	hub := NewShowHub(stage)
	show := hub.Show("mixed")

	var got1, got2 string
	show.ExternalSubscribe(func(p json.RawMessage) { got1 = string(p) })
	show.ExternalSubscribe(func(p json.RawMessage) { panic("回调 panic") }) // 隔离
	show.ExternalSubscribe(func(p json.RawMessage) { got2 = string(p) })

	report := hub.Publish("mixed", map[string]any{"ok": 1})
	if report.Delivered != 2 || report.Failed != 1 {
		t.Fatalf("report = %+v, want 2/1（panic 计 failed 不影响他人）", report)
	}
	if got1 == "" || got2 == "" {
		t.Fatalf("回调未收到：got1=%q got2=%q", got1, got2)
	}
	stage.Close()
}

func TestShowSlowSubscriberBackpressureIsolation(t *testing.T) {
	// 慢订阅者（队列满）failed 不影响其他订阅者照常收到（背压隔离）。
	cfg := DefaultStageConfig()
	cfg.MaxQueueDepth = 1
	slowStage := NewStage(WithConfig(cfg))
	gate := make(chan struct{})
	showManifest(t, slowStage, gate)
	healthyStage := NewStage()
	gateH := make(chan struct{})
	close(gateH)
	showManifest(t, healthyStage, gateH)

	hub := NewShowHub(healthyStage)
	show := hub.Show("orders")
	idSlow := MustActorID("app", "watch", "slow")
	idFast := MustActorID("app", "watch", "fast")
	// 跨 Stage 显式绑定：慢订阅绑定 slowStage
	show.ActorSubscribeOn(slowStage, idSlow, "on_event")
	show.ActorSubscribeOn(healthyStage, idFast, "on_event")

	// 慢 Stage：堵满队列（blocked turn 占用执行 + 1 queued）再发布
	_, _ = slowStage.Call(context.Background(), idSlow, "slow", nil) // 执行占用
	// 第二个调用与消费者出队存在竞态：重试直到入队成功
	queued := false
	for i := 0; i < 100; i++ {
		if _, err := slowStage.Call(context.Background(), idSlow, "slow", nil); err == nil {
			queued = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !queued {
		t.Fatal("占队失败（1s 内未能入队）")
	}
	if _, err := slowStage.Call(context.Background(), idSlow, "slow", nil); err == nil {
		t.Fatal("队列应已满")
	}

	report := hub.Publish("orders", map[string]any{"topic": "x"})
	if report.Delivered != 1 || report.Failed != 1 {
		t.Fatalf("report = %+v, want 1/1（慢订阅者 failed 隔离）", report)
	}
	close(gate)
	slowStage.Close()
	healthyStage.Close()
}

func TestShowOfflineNotGuaranteed(t *testing.T) {
	// Stage 停机/排空的订阅交付计入 failed（离线不保证）。
	stage := NewStage()
	gate := make(chan struct{})
	close(gate)
	showManifest(t, stage, gate)
	hub := NewShowHub(stage)
	id := MustActorID("app", "watch", "off")
	hub.Show("events").ActorSubscribe(id, "on_event")

	stage.BeginDrain() // 排空中：拒新（retryable）→ failed
	report := hub.Publish("events", map[string]any{"x": 1})
	if report.Failed != 1 || report.Delivered != 0 {
		t.Fatalf("排空中 report = %+v, want 0/1（离线不保证）", report)
	}
	stage.Close() // 停机后同样 failed
	report = hub.Publish("events", map[string]any{"x": 1})
	if report.Failed != 1 {
		t.Fatalf("停机后 report = %+v, want failed", report)
	}
}

func TestShowUnsubscribeAndPublishTimeout(t *testing.T) {
	stage := NewStage()
	gate := make(chan struct{})
	showManifest(t, stage, gate)
	hub := NewShowHub(stage)
	show := hub.Show("t")
	id := MustActorID("app", "watch", "u")
	show.ActorSubscribe(id, "on_event")
	if removed := show.UnsubscribeActor(id); removed != 1 {
		t.Fatalf("退订 = %d, want 1", removed)
	}
	if report := hub.Publish("t", nil); report.Delivered != 0 {
		t.Fatalf("退订后仍交付：%+v", report)
	}

	// 交付超时：慢 Turn（gate 未开）→ deadline 到期 → failed（放弃不重投）
	show2 := hub.Show("slow")
	show2.ActorSubscribe(id, "slow")
	done := make(chan PublishReport, 1)
	go func() { done <- hub.PublishWithTimeout("slow", nil, 80*time.Millisecond) }()
	select {
	case r := <-done:
		if r.Failed != 1 {
			t.Fatalf("超时交付 report = %+v, want failed", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("发布未在 deadline 后返回（阻塞而非放弃）")
	}
	close(gate)
	stage.Close()
}

// ---- Director ----

func TestDirectorHealthMetricsEvents(t *testing.T) {
	stage := NewStage(WithName("main"))
	if err := cartManifest().Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "cart", "d")
	f, _ := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"})
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatal(err)
	}
	// 一枚失败 Turn
	_, _ = stage.Call(ctxbg, id, "on_stage", nil) // whitelist 拒绝

	d := NewDirector()
	if err := d.AddStage("main", stage); err != nil {
		t.Fatalf("AddStage 失败：%v", err)
	}
	if err := d.AddStage("main", stage); err == nil {
		t.Fatal("重复注册应失败")
	}

	health := d.Health()
	if len(health) != 1 || health[0].Name != "main" || health[0].Status != "running" {
		t.Fatalf("health = %+v", health)
	}
	snap, ok := d.StageMetricsSnapshot("main")
	if !ok || snap["activations"] != 1 || snap["turns_completed"] != 1 {
		t.Fatalf("metrics = %v", snap)
	}
	// 故障可见：rejections 计数
	if snap["turns_failed"] != 0 {
		t.Fatalf("whitelist 拒绝不应计 turns_failed：%v", snap)
	}

	// drain → health 状态变化
	if err := d.DrainStage("main"); err != nil {
		t.Fatal(err)
	}
	if h := d.Health(); h[0].Status != "draining" {
		t.Fatalf("drain 后 status = %s", h[0].Status)
	}
	// events 环形有界
	for i := 0; i < 300; i++ {
		_, _ = stage.Call(ctxbg, id, "on_stage", nil) // 每次产生 call_rejected 事件
	}
	events := d.RecentEvents(-1)
	if len(events) > 200 {
		t.Fatalf("recent_events 超上限：%d", len(events))
	}
	// shutdown → closed
	if err := d.ShutdownStage("main"); err != nil {
		t.Fatal(err)
	}
	if h := d.Health(); h[0].Status != "closed" {
		t.Fatalf("shutdown 后 status = %s", h[0].Status)
	}
	if _, ok := d.StageMetricsSnapshot("ghost"); ok {
		t.Fatal("未注册 Stage 查询应返回 false")
	}
}

func TestHTTPHealthEndpoint(t *testing.T) {
	stage := NewStage(WithName("api"))
	if err := cartManifest().Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	d := NewDirector()
	if err := d.AddStage("api", stage); err != nil {
		t.Fatal(err)
	}
	// 事件订阅先于调用：Turn 事件进 recent_events 环
	id := MustActorID("app", "cart", "h")
	f, _ := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"})
	if _, err := f.Await(ctxbg); err != nil {
		t.Fatal(err)
	}
	ep := NewHTTPEndpoint(d)
	if err := ep.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("监听失败：%v", err)
	}
	defer ep.Close()
	client := &http.Client{Timeout: 3 * time.Second}

	// /health
	resp, err := client.Get("http://" + ep.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health 失败：%v", err)
	}
	var health map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(health["stages"].([]any)) != 1 {
		t.Fatalf("health stages = %v", health)
	}

	// /metrics：故障与未知结果可见（activation/turns_completed）
	resp, err = client.Get("http://" + ep.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var metrics map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	apiMetrics, ok := metrics["api"]
	if !ok || apiMetrics["activations"] != 1 {
		t.Fatalf("metrics api = %v", metrics)
	}

	// /events
	resp, err = client.Get("http://" + ep.Addr() + "/events")
	if err != nil {
		t.Fatal(err)
	}
	var events struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(events.Events) == 0 {
		t.Fatal("events 为空")
	}

	// pprof 可达
	resp, err = client.Get("http://" + ep.Addr() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("pprof 失败：%v", err)
	}
	resp.Body.Close()

	// 管理写操作不经 HTTP：HTTP 只读端点无法触发 drain/shutdown
	// （没有任何 POST 路由会改变 Stage 生命周期——结构保证）
	if h := d.Health(); h[0].Status != "running" {
		t.Fatalf("HTTP 探测不应改变状态：%s", h[0].Status)
	}
	stage.Close()
}

// 静默并发用（避免 unused import）
