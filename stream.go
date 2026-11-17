package troupe

// 持久流（M5）：事件日志 seq 全局单调、per-consumer 持久游标（ack 只前进——
// 回退拒绝）、不 ack 重放 at-least-once、retain_before 保留裁剪
// （慢消费者被裁事件丢失为显式代价）、replay 显式重放。
// 与 Show realtime 分层：fan-out ≠ 持久流。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// StreamRecord 流事件。
type StreamRecord struct {
	Seq       uint64 // per-topic 单调递增（保序键）
	Topic     string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// ErrCursorBackward ack 回退拒绝（游标只前进——不 ack 重放的前提）。
var ErrCursorBackward = errors.New("stream ack 回退拒绝（游标只前进）")

// StreamStore 契约：跨运行持久（PG Provider）；Memory 用于 Rehearsal/单机。
type StreamStore interface {
	// Append 追加事件，返回 per-topic 单调 seq。
	Append(ctx context.Context, topic string, payload json.RawMessage) (uint64, error)
	// Poll 返回 consumer 游标之后的事件（不 ack 下次重放——at-least-once）。
	Poll(ctx context.Context, topic, consumer string, limit int) ([]StreamRecord, error)
	// Ack 推进 consumer 游标（GREATEST 只前进；回退拒绝 ErrCursorBackward）。
	Ack(ctx context.Context, topic, consumer string, seq uint64) error
	// Cursor 返回 consumer 当前游标（无记录为 0）。
	Cursor(ctx context.Context, topic, consumer string) uint64
	// RetainBefore 裁剪 seq < before 的已确认事件；慢消费者游标低于裁剪线时
	// 其未消费事件一并丢失——显式代价（返回删除条数）。
	RetainBefore(ctx context.Context, topic string, before uint64) (int, error)
	// Topics 返回现存 topic（排序）。
	Topics(ctx context.Context) []string
}

// MemoryStreamStore 内存实现（Rehearsal/单机）。
type MemoryStreamStore struct {
	mu        sync.Mutex
	events    map[string][]StreamRecord // topic → 事件（按 seq 升序）
	cursors   map[string]uint64         // topic\x1fconsumer → 游标
	nextSeq   map[string]uint64
	createdAt func() time.Time
}

func NewMemoryStreamStore() *MemoryStreamStore {
	return &MemoryStreamStore{
		events:    make(map[string][]StreamRecord),
		cursors:   make(map[string]uint64),
		nextSeq:   make(map[string]uint64),
		createdAt: func() time.Time { return time.Now().UTC() },
	}
}

func streamCursorKey(topic, consumer string) string { return topic + "\x1f" + consumer }

func (m *MemoryStreamStore) Append(ctx context.Context, topic string, payload json.RawMessage) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSeq[topic]++
	rec := StreamRecord{Seq: m.nextSeq[topic], Topic: topic, Payload: payload, CreatedAt: m.createdAt()}
	m.events[topic] = append(m.events[topic], rec)
	return rec.Seq, nil
}

func (m *MemoryStreamStore) Poll(ctx context.Context, topic, consumer string, limit int) ([]StreamRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cursor := m.cursors[streamCursorKey(topic, consumer)]
	var out []StreamRecord
	for _, rec := range m.events[topic] {
		if rec.Seq > cursor {
			out = append(out, rec)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (m *MemoryStreamStore) Ack(ctx context.Context, topic, consumer string, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := streamCursorKey(topic, consumer)
	if seq <= m.cursors[key] {
		return fmt.Errorf("%w：%s/%s 游标 %d ≥ ack %d", ErrCursorBackward, topic, consumer, m.cursors[key], seq)
	}
	m.cursors[key] = seq
	return nil
}

func (m *MemoryStreamStore) Cursor(ctx context.Context, topic, consumer string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursors[streamCursorKey(topic, consumer)]
}

func (m *MemoryStreamStore) RetainBefore(ctx context.Context, topic string, before uint64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := m.events[topic]
	kept := events[:0]
	removed := 0
	for _, rec := range events {
		if rec.Seq < before {
			removed++
			continue
		}
		kept = append(kept, rec)
	}
	m.events[topic] = kept
	return removed, nil
}

func (m *MemoryStreamStore) Topics(ctx context.Context) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.events))
	for t := range m.events {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// StreamReader 分批拉取消费者：PollEach 按批交付，handler 失败立即停止
// （不越过失败事件——不 ack 重放 at-least-once）；成功后 ack 到批尾。
type StreamReader struct {
	store    StreamStore
	topic    string
	consumer string
	batch    int
}

func NewStreamReader(store StreamStore, topic, consumer string) *StreamReader {
	return &StreamReader{store: store, topic: topic, consumer: consumer, batch: 128}
}

// WithBatch 覆盖批大小。
func (r *StreamReader) WithBatch(n int) *StreamReader {
	if n > 0 {
		r.batch = n
	}
	return r
}

// PollEach 分批 poll → handler → ack 批尾；handler 失败立即返回错误
// （游标未越过失败事件——下次重放）。
func (r *StreamReader) PollEach(ctx context.Context, fn func(rec StreamRecord) error) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		batch, err := r.store.Poll(ctx, r.topic, r.consumer, r.batch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, rec := range batch {
			if err := fn(rec); err != nil {
				return err // 不越过失败事件：游标不推进，下次重放
			}
		}
		if err := r.store.Ack(ctx, r.topic, r.consumer, batch[len(batch)-1].Seq); err != nil {
			return err
		}
		if len(batch) < r.batch {
			return nil
		}
	}
}

// Replay 显式重放：从 from seq 起（无视游标）逐条交付（不推进游标）。
func (r *StreamReader) Replay(ctx context.Context, from uint64, fn func(rec StreamRecord) error) error {
	batch, err := r.store.Poll(ctx, r.topic, "\x00replay", r.batch*1024)
	if err != nil {
		return err
	}
	for _, rec := range batch {
		if rec.Seq < from {
			continue
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}
