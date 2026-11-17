package troupe

// M5-A：持久流反例——seq 单调、at-least-once 重放、ack 只前进、
// 保留裁剪显式代价、StreamReader 失败不越过。

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestStreamAppendPollAtLeastOnce(t *testing.T) {
	store := NewMemoryStreamStore()
	ctxbg := context.Background()

	for _, p := range []string{`{"i":1}`, `{"i":2}`, `{"i":3}`} {
		if _, err := store.Append(ctxbg, "orders", json.RawMessage(p)); err != nil {
			t.Fatalf("append 失败：%v", err)
		}
	}

	// 首次 poll：全部事件
	batch, err := store.Poll(ctxbg, "orders", "worker-1", 128)
	if err != nil || len(batch) != 3 {
		t.Fatalf("poll = %d, %v", len(batch), err)
	}
	if batch[0].Seq != 1 || batch[2].Seq != 3 {
		t.Fatalf("seq 不保序：%+v", batch)
	}

	// 不 ack → 重放（at-least-once）
	batch2, _ := store.Poll(ctxbg, "orders", "worker-1", 128)
	if len(batch2) != 3 {
		t.Fatalf("未 ack 重放 = %d, want 3", len(batch2))
	}

	// 独立 consumer 游标互不影响
	batch3, _ := store.Poll(ctxbg, "orders", "worker-2", 128)
	if len(batch3) != 3 {
		t.Fatalf("worker-2 = %d", len(batch3))
	}
}

func TestStreamAckForwardOnly(t *testing.T) {
	store := NewMemoryStreamStore()
	ctxbg := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = store.Append(ctxbg, "orders", json.RawMessage(`{}`))
	}
	if _, _ = store.Poll(ctxbg, "orders", "w", 128); false {
	}

	// ack seq=2 → 游标推进
	if err := store.Ack(ctxbg, "orders", "w", 2); err != nil {
		t.Fatalf("ack 失败：%v", err)
	}
	batch, _ := store.Poll(ctxbg, "orders", "w", 128)
	if len(batch) != 1 || batch[0].Seq != 3 {
		t.Fatalf("ack 后重放 = %d, want 仅 seq3", len(batch))
	}
	// 回退拒绝（游标只前进）
	err := store.Ack(ctxbg, "orders", "w", 1)
	if !errors.Is(err, ErrCursorBackward) {
		t.Fatalf("回退错误 = %v, want ErrCursorBackward", err)
	}
	// 同值 ack 幂等拒绝（游标 2 → ack 2 不前进）
	if err := store.Ack(ctxbg, "orders", "w", 2); !errors.Is(err, ErrCursorBackward) {
		t.Fatalf("同值 ack = %v, want ErrCursorBackward", err)
	}
	if got := store.Cursor(ctxbg, "orders", "w"); got != 2 {
		t.Fatalf("cursor = %d, want 2", got)
	}
}

func TestStreamRetainBeforeExplicitLoss(t *testing.T) {
	store := NewMemoryStreamStore()
	ctxbg := context.Background()
	for i := 0; i < 4; i++ {
		_, _ = store.Append(ctxbg, "orders", json.RawMessage(`{}`))
	}
	// 慢消费者游标停在 0，未 poll
	// 裁剪 seq < 3 → 慢消费者丢失 seq1-2（显式代价）
	removed, err := store.RetainBefore(ctxbg, "orders", 3)
	if err != nil || removed != 2 {
		t.Fatalf("retain removed = %d, %v, want 2", removed, err)
	}
	batch, _ := store.Poll(ctxbg, "orders", "slow", 128)
	if len(batch) != 2 || batch[0].Seq != 3 {
		t.Fatalf("慢消费者 poll = %d, want 仅 seq3/4（丢失为显式代价）", len(batch))
	}
	// 已 ack 消费者不受影响
	if err := store.Ack(ctxbg, "orders", "fast", 4); err != nil {
		t.Fatalf("fast ack 失败：%v", err)
	}
	batch, _ = store.Poll(ctxbg, "orders", "fast", 128)
	if len(batch) != 0 {
		t.Fatalf("fast 重放 = %d", len(batch))
	}
}

func TestStreamReaderPollEachStopsOnFailure(t *testing.T) {
	store := NewMemoryStreamStore()
	ctxbg := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := store.Append(ctxbg, "jobs", json.RawMessage(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	reader := NewStreamReader(store, "jobs", "w").WithBatch(2)

	// handler 在 seq2 失败：不越过（游标停在 1）
	failAt := uint64(2)
	processed := 0
	err := reader.PollEach(ctxbg, func(rec StreamRecord) error {
		processed++
		if rec.Seq >= failAt {
			return errors.New("处理失败")
		}
		return nil
	})
	if err == nil {
		t.Fatal("handler 失败应上抛")
	}
	if got := store.Cursor(ctxbg, "jobs", "w"); got != 0 {
		t.Fatalf("失败后 cursor = %d, want 0（不越过失败事件）", got)
	}
	if processed != 2 { // batch=2：seq1 成功 + seq2 失败（handler 调用两次）
		t.Fatalf("处理数 = %d, want 2", processed)
	}

	// 修复后重跑：全部成功（at-least-once 重放 seq1 起）
	processed = 0
	err = reader.PollEach(ctxbg, func(rec StreamRecord) error {
		processed++
		return nil
	})
	if err != nil || processed != 5 {
		t.Fatalf("重跑 = %d, %v, want 5", processed, err)
	}
	if got := store.Cursor(ctxbg, "jobs", "w"); got != 5 {
		t.Fatalf("最终 cursor = %d, want 5", got)
	}
	// 全 ack 后 poll 为空
	batch, _ := store.Poll(ctxbg, "jobs", "w", 128)
	if len(batch) != 0 {
		t.Fatalf("全 ack 后 poll = %d", len(batch))
	}
}
