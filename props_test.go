package troupe

import (
	"errors"
	"testing"
)

func TestMemoryStoreCreateReadRoundTrip(t *testing.T) {
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryPropStore(ownership)
	id := MustActorID("app", "cart", "1")
	epoch := ownership.Acquire(id)

	rec, err := store.Create(id, 1, `{"items":["a"]}`, epoch)
	if err != nil {
		t.Fatalf("Create 失败：%v", err)
	}
	if rec.Revision != 1 || rec.SchemaVersion != 1 {
		t.Fatalf("首条记录 revision/schema = %d/%d, want 1/1", rec.Revision, rec.SchemaVersion)
	}

	got, err := store.Read(id)
	if err != nil || got == nil {
		t.Fatalf("Read 失败：%v", err)
	}
	if got.Data != `{"items":["a"]}` {
		t.Fatalf("Data = %q", got.Data)
	}

	// read 隔离：修改返回副本不落入存储
	got.Data = "mutated"
	again, _ := store.Read(id)
	if again.Data != `{"items":["a"]}` {
		t.Fatalf("read 快照被外部修改污染：%q", again.Data)
	}
}

func TestMemoryStoreCreateExistsConflict(t *testing.T) {
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryPropStore(ownership)
	id := MustActorID("app", "cart", "1")
	epoch := ownership.Acquire(id)

	if _, err := store.Create(id, 1, `{}`, epoch); err != nil {
		t.Fatalf("首次 Create 失败：%v", err)
	}
	_, err := store.Create(id, 1, `{}`, epoch)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictExists {
		t.Fatalf("重复 Create 错误 = %v, want ConflictError{exists}", err)
	}
}

func TestMemoryStoreWriteCAS(t *testing.T) {
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryPropStore(ownership)
	id := MustActorID("app", "cart", "1")
	epoch := ownership.Acquire(id)
	store.Create(id, 1, `{"n":1}`, epoch)

	// 期望 revision 不符 → ConflictError{revision}
	_, err := store.Write(id, 99, 1, `{"n":2}`, epoch)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("CAS 冲突错误 = %v, want ConflictError{revision}", err)
	}

	// 正确 revision → 写入成功且 revision 前进
	rec, err := store.Write(id, 1, 1, `{"n":2}`, epoch)
	if err != nil {
		t.Fatalf("Write 失败：%v", err)
	}
	if rec.Revision != 2 {
		t.Fatalf("写入后 revision = %d, want 2", rec.Revision)
	}

	// 不存在的 actor → ConflictError{revision}（troupe.cr 语义）
	_, err = store.Write(MustActorID("app", "cart", "ghost"), 1, 1, `{}`, epoch)
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("缺记录 Write 错误 = %v, want ConflictError{revision}", err)
	}
}

func TestMemoryStoreFencingRejectsStaleEpoch(t *testing.T) {
	ownership := NewMemoryOwnershipStore()
	store := NewMemoryPropStore(ownership)
	id := MustActorID("app", "cart", "1")
	first := ownership.Acquire(id)
	store.Create(id, 1, `{}`, first)

	// 模拟接管：新 epoch 取得所有权
	second := ownership.Acquire(id)
	if second <= first {
		t.Fatalf("epoch 必须单调递增：%d → %d", first, second)
	}

	// 旧 epoch 写即使 revision 匹配也拒绝（fencing 优先于 CAS）
	_, err := store.Write(id, 1, 1, `{"stale":true}`, first)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectOwnership || cre.Retryable {
		t.Fatalf("旧 epoch 写错误 = %v, want CallRejectedError{ownership, retryable=false}", err)
	}

	// 旧 epoch create 同样拒绝（先 exists 检查后 fencing —— 与 troupe.cr 一致，
	// 已存在时返回 exists；此处验证 fencing 生效于 create 路径）
	ownership2 := NewMemoryOwnershipStore()
	store2 := NewMemoryPropStore(ownership2)
	id2 := MustActorID("app", "cart", "2")
	e1 := ownership2.Acquire(id2)
	_ = ownership2.Acquire(id2) // epoch 2 取代
	_, err = store2.Create(id2, 1, `{}`, e1)
	if !errors.As(err, &cre) || cre.Reason != RejectOwnership {
		t.Fatalf("旧 epoch create 错误 = %v, want CallRejectedError{ownership}", err)
	}

	// 新 epoch 写成功
	if _, err := store.Write(id, 1, 1, `{"fresh":true}`, second); err != nil {
		t.Fatalf("新 epoch Write 失败：%v", err)
	}
}

func TestMemoryStoreCrossNodeUnsafe(t *testing.T) {
	store := NewMemoryPropStore(NewMemoryOwnershipStore())
	if store.CrossNodeSafe() {
		t.Fatal("Memory Provider 不得声明 cross_node_safe")
	}
}

func TestOwnershipRenew(t *testing.T) {
	o := NewMemoryOwnershipStore()
	id := MustActorID("app", "cart", "1")
	e := o.Acquire(id)
	if !o.Renew(id, e) {
		t.Fatal("同 epoch renew 应成功")
	}
	if o.Renew(id, e-1) {
		t.Fatal("旧 epoch renew 应失败")
	}
}
