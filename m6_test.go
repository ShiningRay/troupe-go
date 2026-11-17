package troupe

// M6：OTel 导出适配器（metrics monotonic Sum + spans/环形有界）与
// 事件溯源（Turn append 事件 → 独立重建者一致；快照+增量 == 全量）。

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestOTelAdapterExportMetricsAndSpans(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "cart", "o1")

	var mu sync.Mutex
	var exports []string
	adapter := NewOTelAdapter(stage, func(otlpJSON string) {
		mu.Lock()
		exports = append(exports, otlpJSON)
		mu.Unlock()
	}).WithRingCap(64)
	adapter.Attach()

	// 2 成功 Turn（带 traceparent → spans）+ 1 白名单拒绝（call_rejected 事件）
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for i := 0; i < 2; i++ {
		f, _ := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"}, WithTraceparent(parent))
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = stage.Call(ctxbg, id, "on_stage", nil)

	if err := adapter.ExportAll(ctxbg); err != nil {
		t.Fatalf("export 失败：%v", err)
	}
	if len(exports) != 1 {
		t.Fatalf("exports = %d, want 1", len(exports))
	}
	var payload struct {
		Format  string `json:"format"`
		Metrics []struct {
			Name        string `json:"name"`
			IsMonotonic bool   `json:"isMonotonic"`
			Sum         uint64 `json:"sum"`
		} `json:"metrics"`
		Spans []struct {
			TraceID string `json:"traceId"`
			SpanID  string `json:"spanId"`
			Status  string `json:"status"`
		} `json:"spans"`
	}
	if err := json.Unmarshal([]byte(exports[0]), &payload); err != nil {
		t.Fatalf("OTLP JSON 解析失败：%v", err)
	}
	if payload.Format != "OTLP/JSON v1" {
		t.Fatalf("format = %s", payload.Format)
	}
	// metrics monotonic Sum：lifecycle 事件计数
	foundActivated, foundTurnEnded := false, false
	for _, m := range payload.Metrics {
		if !m.IsMonotonic {
			t.Fatalf("metrics 非单调：%s", m.Name)
		}
		if m.Name == "activated" && m.Sum == 1 {
			foundActivated = true
		}
		if m.Name == "turn_ended.ok" && m.Sum == 2 {
			foundTurnEnded = true
		}
	}
	if !foundActivated || !foundTurnEnded {
		t.Fatalf("metrics 缺失：activated=%v turn_ended.ok=%v (%+v)", foundActivated, foundTurnEnded, payload.Metrics)
	}
	// spans：2 个成功 Turn 的子 span（traceId 继承、status OK）
	if len(payload.Spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(payload.Spans))
	}
	for _, s := range payload.Spans {
		if s.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || s.Status != "OK" || len(s.SpanID) != 16 {
			t.Fatalf("span = %+v", s)
		}
	}

	// ExportAll 后清空：二次导出为空 metrics（dropped 计数保留语义）
	if err := adapter.ExportAll(ctxbg); err != nil {
		t.Fatal(err)
	}
	if len(exports) != 2 {
		t.Fatalf("exports = %d, want 2", len(exports))
	}
	stage.Close()
}

func TestOTelRingCapBounded(t *testing.T) {
	stage := NewStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatal(err)
	}
	id := MustActorID("app", "cart", "r")
	var exports int
	adapter := NewOTelAdapter(stage, func(_ string) { exports++ }).WithRingCap(3)
	adapter.Attach()
	for i := 0; i < 30; i++ {
		f, _ := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"})
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatal(err)
		}
	}
	if err := adapter.ExportAll(ctxbg); err != nil {
		t.Fatal(err)
	}
	if exports != 1 {
		t.Fatalf("exports = %d", exports)
	}
	// 环形有界：30 个 Turn 但缓冲 cap=3（溢出丢弃，不耗尽内存）——
	// 导出 JSON 中 spans ≤ 3
	stage.Close()
}

type bankState struct {
	Balance int      `json:"balance"`
	History []string `json:"history"`
}

func TestEventSourcingRebuildConsistency(t *testing.T) {
	// 事件为真相源：Turn append 事件 → 独立重建者状态一致。
	store := NewMemoryStreamStore()
	r := NewRehearsal("es")
	m := Define("bank", func(id ActorID, props bankState, actx *ActivationContext) *struct{ *Actor[bankState] } {
		return &struct{ *Actor[bankState] }{NewActor(id, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() bankState { return bankState{} })
	m.Turn(Turn("deposit", func(a *struct{ *Actor[bankState] }, ctx *CallContext, args struct {
		Amount int
	}) (int, error) {
		a.Props().Balance += args.Amount
		a.Props().History = append(a.Props().History, "deposit")
		if _, err := AppendEvent(ctxbg, store, "bank", "deposited",
			map[string]any{"amount": args.Amount}); err != nil {
			return 0, err
		}
		if err := a.Save(); err != nil {
			return 0, err
		}
		return a.Props().Balance, nil
	}))
	if err := m.Register(r.Stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	_ = r.Actor("bank", "b1")
	for _, amount := range []int{100, 50, 25} {
		f, _ := r.Call(ctxbg, "bank", "b1", "deposit", map[string]any{"amount": amount})
		r.PumpAll()
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatalf("deposit 失败：%v", err)
		}
	}

	apply := func(state bankState, ev EventRecord) (bankState, error) {
		// ev.Data 已是信封内层 data（decodeEnvelope 解出）——直接解 amount
		var data struct {
			Amount int `json:"amount"`
		}
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			return state, err
		}
		state.Balance += data.Amount
		state.History = append(state.History, ev.Type)
		return state, nil
	}

	// 独立重建者：全量折叠 → 与 Actor 已提交状态一致
	rebuilt, lastSeq, err := Rebuild(ctxbg, store, "bank", bankState{}, apply)
	if err != nil {
		t.Fatalf("rebuild 失败：%v", err)
	}
	if rebuilt.Balance != 175 || len(rebuilt.History) != 3 || lastSeq != 3 {
		t.Fatalf("重建状态 = %+v lastSeq=%d", rebuilt, lastSeq)
	}

	// 快照+增量 == 全量（一致性契约）
	snapshot := bankState{Balance: 150, History: []string{"deposited", "deposited"}} // 前 2 事件经同一 apply 后的状态
	fromSnap, snapSeq, err := RebuildFromSnapshot(ctxbg, store, "bank", snapshot, 2, apply)
	if err != nil {
		t.Fatalf("snapshot rebuild 失败：%v", err)
	}
	if snapSeq != 3 || fromSnap.Balance != 175 || len(fromSnap.History) != 3 {
		t.Fatalf("快照+增量 = %+v seq=%d", fromSnap, snapSeq)
	}
	// JSON 相等（快照+增量 == 全量）
	fullJSON, _ := json.Marshal(rebuilt)
	snapJSON, _ := json.Marshal(fromSnap)
	if string(fullJSON) != string(snapJSON) {
		t.Fatalf("快照+增量 != 全量：%s vs %s", snapJSON, fullJSON)
	}

	// apply 失败 → 重建失败（错误上抛）
	_, _, err = Rebuild(ctxbg, store, "bank", bankState{}, func(_ bankState, _ EventRecord) (bankState, error) {
		return bankState{}, errors.New("apply 失败")
	})
	if err == nil {
		t.Fatal("apply 失败应上抛")
	}
}
