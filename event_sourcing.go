package troupe

// 事件溯源扩展点（M6）：事件为真相源——Turn append 事件 → 独立重建者状态一致；
// 快照+增量 == 全量 rebuild（一致性契约）；分布式事务不做（DESIGN 边界）。
// 事件日志复用 StreamStore（持久化；M5），append_event 为信封包装。

import (
	"context"
	"encoding/json"
	"fmt"
)

// EventRecord 事件信封。
type EventRecord struct {
	Seq   uint64          `json:"seq"`  // 流序号（StreamStore 分配）
	Type  string          `json:"type"` // 事件类型（业务定义）
	Data  json.RawMessage `json:"data"` // 事件载荷
	Topic string          `json:"topic"`
}

// AppendEvent 追加事件信封到持久流（独立重建者可读——事件为真相源）。
func AppendEvent(ctx context.Context, store StreamStore, topic, eventType string, data any) (EventRecord, error) {
	payload, err := json.Marshal(map[string]any{"type": eventType, "data": data})
	if err != nil {
		return EventRecord{}, fmt.Errorf("事件载荷序列化失败：%w", err)
	}
	seq, err := store.Append(ctx, topic, payload)
	if err != nil {
		return EventRecord{}, err
	}
	return EventRecord{Seq: seq, Type: eventType, Data: payload, Topic: topic}, nil
}

// decodeEnvelope 从流记录解出事件信封。
func decodeEnvelope(rec StreamRecord) (EventRecord, error) {
	var env struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		return EventRecord{}, fmt.Errorf("事件信封解析失败：%w", err)
	}
	return EventRecord{Seq: rec.Seq, Type: env.Type, Data: env.Data, Topic: rec.Topic}, nil
}

// Rebuild 顺序折叠事件重建状态（apply 返回错误即重建失败）。
// 每次调用唯一 consumer：重建始终读全量日志（该游标不再复用）。
func Rebuild[S any](ctx context.Context, store StreamStore, topic string,
	state S, apply func(state S, ev EventRecord) (S, error)) (S, uint64, error) {
	reader := NewStreamReader(store, topic, "\x00es-rebuild-"+newEventID()).WithBatch(256)
	var lastSeq uint64
	err := reader.PollEach(ctx, func(rec StreamRecord) error {
		ev, derr := decodeEnvelope(rec)
		if derr != nil {
			return derr
		}
		next, aerr := apply(state, ev)
		if aerr != nil {
			return aerr
		}
		state = next
		lastSeq = rec.Seq
		return nil
	})
	return state, lastSeq, err
}

// RebuildFromSnapshot 从快照起折叠增量事件；一致性契约：快照+增量 == 全量
// （调用方以同一 apply 折叠两条路径后状态相等——测试锚定）。
func RebuildFromSnapshot[S any](ctx context.Context, store StreamStore, topic string,
	snapshot S, snapshotSeq uint64, apply func(state S, ev EventRecord) (S, error)) (S, uint64, error) {
	events, err := store.Poll(ctx, topic, "\x00es-snapshot-delta", 1<<20)
	if err != nil {
		return snapshot, snapshotSeq, err
	}
	state := snapshot
	lastSeq := snapshotSeq
	for _, rec := range events {
		if rec.Seq <= snapshotSeq {
			continue
		}
		ev, derr := decodeEnvelope(rec)
		if derr != nil {
			return state, lastSeq, derr
		}
		next, aerr := apply(state, ev)
		if aerr != nil {
			return state, lastSeq, aerr
		}
		state = next
		lastSeq = rec.Seq
	}
	return state, lastSeq, nil
}
