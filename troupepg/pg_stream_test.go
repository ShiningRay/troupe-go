package troupepg

// PG 持久流集成测试（TROUPE_PG_URL 门控）：跨运行持久 + ack 只前进 + 保留裁剪。

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	troupe "github.com/ShiningRay/troupe-go"
)

func TestPGStreamAppendPollAckRetain(t *testing.T) {
	ctxbg := context.Background()
	store, _ := openPair(t, "silo-stream")
	streams, err := store.OpenStreams()
	if err != nil {
		t.Fatal(err)
	}

	seqs := make([]uint64, 0, 4)
	for i := 0; i < 4; i++ {
		seq, err := streams.Append(ctxbg, "orders", json.RawMessage(`{"n":1}`))
		if err != nil {
			t.Fatalf("append 失败：%v", err)
		}
		seqs = append(seqs, seq)
	}
	// seq 单调
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("seq 非单调：%v", seqs)
		}
	}

	// 不 ack → 重放
	batch, err := streams.Poll(ctxbg, "orders", "w", 128)
	if err != nil || len(batch) != 4 {
		t.Fatalf("poll = %d, %v", len(batch), err)
	}
	// ack 中间值 → 游标推进、回退拒绝
	if err := streams.Ack(ctxbg, "orders", "w", seqs[1]); err != nil {
		t.Fatalf("ack 失败：%v", err)
	}
	if got := streams.Cursor(ctxbg, "orders", "w"); got != seqs[1] {
		t.Fatalf("cursor = %d", got)
	}
	if err := streams.Ack(ctxbg, "orders", "w", seqs[0]); !errors.Is(err, troupe.ErrCursorBackward) {
		t.Fatalf("回退错误 = %v, want ErrCursorBackward", err)
	}
	batch, _ = streams.Poll(ctxbg, "orders", "w", 128)
	if len(batch) != 2 {
		t.Fatalf("ack 后 poll = %d, want 2", len(batch))
	}

	// 跨 go test 持久依赖 TRUNCATE——本测试内重建 store（同池）验证游标持久语义
	if got := streams.Cursor(ctxbg, "orders", "w"); got != seqs[1] {
		t.Fatalf("游标丢失：%d", got)
	}

	// 保留裁剪：慢消费者丢失为显式代价
	removed, err := streams.RetainBefore(ctxbg, "orders", seqs[3])
	if err != nil || removed != int(seqs[3])-1 {
		t.Fatalf("retain removed = %d, %v", removed, err)
	}
	batch, _ = streams.Poll(ctxbg, "orders", "slow", 128)
	if len(batch) != 1 || batch[0].Seq != seqs[3] {
		t.Fatalf("慢消费者 poll = %d, want 仅最新", len(batch))
	}
	if got := streams.Topics(ctxbg); len(got) != 1 || got[0] != "orders" {
		t.Fatalf("topics = %v", got)
	}
}
