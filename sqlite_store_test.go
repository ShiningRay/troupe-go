package troupe

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTempSQLite(t *testing.T) (*SQLitePropStore, *SQLiteOwnershipStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "troupe.db")
	store, own, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite 失败：%v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, own, path
}

func TestSQLiteCreateWriteRead(t *testing.T) {
	store, own, _ := openTempSQLite(t)
	id := MustActorID("app", "cart", "1")
	epoch, _ := own.Acquire(ctxbg, id)

	rec, err := store.Create(ctxbg, id, 1, `{"items":["a"]}`, epoch)
	if err != nil {
		t.Fatalf("Create 失败：%v", err)
	}
	if rec.Revision != 1 {
		t.Fatalf("首条 revision = %d", rec.Revision)
	}

	rec2, err := store.Write(ctxbg, id, 1, 1, `{"items":["a","b"]}`, epoch)
	if err != nil {
		t.Fatalf("Write 失败：%v", err)
	}
	if rec2.Revision != 2 {
		t.Fatalf("写入后 revision = %d, want 2", rec2.Revision)
	}

	// CAS 冲突
	_, err = store.Write(ctxbg, id, 1, 1, `{"items":[]}`, epoch)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("CAS 错误 = %v, want ConflictError{revision}", err)
	}

	// fencing：新 epoch 取代后旧 epoch 写拒绝（即使 revision 匹配）
	second, _ := own.Acquire(ctxbg, id)
	_, err = store.Write(ctxbg, id, 2, 1, `{"stale":true}`, epoch)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectOwnership {
		t.Fatalf("旧 epoch 写错误 = %v, want ownership", err)
	}
	if _, err := store.Write(ctxbg, id, 2, 1, `{"items":["c"]}`, second); err != nil {
		t.Fatalf("新 epoch 写失败：%v", err)
	}
}

func TestSQLiteReopenPersistence(t *testing.T) {
	store, own, path := openTempSQLite(t)
	id := MustActorID("app", "cart", "reopen")
	epoch, _ := own.Acquire(ctxbg, id)
	store.Create(ctxbg, id, 1, `{"items":["persist"]}`, epoch)
	store.Write(ctxbg, id, 1, 1, `{"items":["persist","after-restart"]}`, epoch)
	store.Close()

	// 重开：进程级持久恢复
	store2, own2, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("重开失败：%v", err)
	}
	defer store2.Close()
	rec, err := store2.Read(ctxbg, id)
	if err != nil || rec == nil {
		t.Fatalf("重开后 read = %+v, %v", rec, err)
	}
	if rec.Revision != 2 || rec.Data != `{"items":["persist","after-restart"]}` {
		t.Fatalf("重开记录 = %+v", rec)
	}
	if own2.Current(ctxbg, id) != epoch {
		t.Fatalf("重开后 ownership 丢失：%d != %d", own2.Current(ctxbg, id), epoch)
	}
	if store2.CrossNodeSafe() {
		t.Fatal("SQLite 不得声明 cross_node_safe")
	}
}

func TestSQLiteJournalAtomicCommit(t *testing.T) {
	store, own, _ := openTempSQLite(t)
	id := MustActorID("app", "cart", "j")
	epoch, _ := own.Acquire(ctxbg, id)
	store.Create(ctxbg, id, 1, `{"items":[]}`, epoch)

	// 成功记录与 Props 同事务
	journal := []IdempotencyRecord{{
		Key: "pay-1", Fingerprint: "fp-a", OK: true,
		Value: `{"charged":true}`, ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}}
	rec, err := store.WriteJournaled(ctxbg, id, 1, 1, `{"items":["x"]}`, epoch, journal)
	if err != nil {
		t.Fatalf("WriteJournaled 失败：%v", err)
	}
	entries, err := store.LoadJournal(ctxbg, id, time.Now().Unix())
	if err != nil || len(entries) != 1 || entries[0].Key != "pay-1" {
		t.Fatalf("journal = %+v, %v", entries, err)
	}

	// CAS 失败 → journal 整体不生效（同事务回滚）
	_, err = store.WriteJournaled(ctxbg, id, 99, 1, `{}`, epoch, []IdempotencyRecord{{
		Key: "pay-2", Fingerprint: "fp-b", OK: true, Value: `{}`, ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}})
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictRevision {
		t.Fatalf("CAS 失败错误 = %v, want ConflictError{revision}", err)
	}
	if entries, _ = store.LoadJournal(ctxbg, id, time.Now().Unix()); len(entries) != 1 {
		t.Fatalf("失败提交的 journal 泄漏：%d 条", len(entries))
	}
	_ = rec

	// 过期条目被过滤
	expired := []IdempotencyRecord{{Key: "old", Fingerprint: "f", OK: true, Value: `{}`, ExpiresAt: 1}}
	if _, err := store.WriteJournaled(ctxbg, id, rec.Revision, 1, `{"items":["y"]}`, epoch, expired); err != nil {
		t.Fatalf("写过期条目失败：%v", err)
	}
	entries, _ = store.LoadJournal(ctxbg, id, time.Now().Unix())
	for _, e := range entries {
		if e.Key == "old" {
			t.Fatal("过期条目不应出现在 LoadJournal")
		}
	}
}

func TestSQLiteOwnershipRenew(t *testing.T) {
	store, own, _ := openTempSQLite(t)
	id := MustActorID("app", "cart", "r")
	e1, _ := own.Acquire(ctxbg, id)
	if !own.Renew(ctxbg, id, e1) {
		t.Fatal("同 epoch renew 应成功")
	}
	if own.Renew(ctxbg, id, e1-1) {
		t.Fatal("旧 epoch renew 应失败")
	}
	_ = store
}

// ---- Stage 集成：SQLite 后端保存/恢复 ----

func TestStageWithSQLitePersistenceRoundTrip(t *testing.T) {
	store, own, err := OpenSQLite(filepath.Join(t.TempDir(), "s1.db"))
	if err != nil {
		t.Fatalf("打开失败：%v", err)
	}
	defer store.Close() // Windows 文件锁：清理前必须关闭

	newStage := func() *Stage {
		return NewStage(WithStore(store), WithOwnership(own))
	}
	stage := newStage()
	if err := cartManifest().Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	id := MustActorID("app", "cart", "persist")
	ctxbg := context.Background()
	for i := 0; i < 3; i++ {
		f, err := stage.Call(ctxbg, id, "add", map[string]any{"item": "x"})
		if err != nil {
			t.Fatalf("call %d 失败：%v", i, err)
		}
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatalf("await %d 失败：%v", i, err)
		}
	}
	stage.Close()

	// 新 Stage（同一 store）：重激活恢复已提交状态
	stage2 := newStage()
	defer stage2.Close()
	if err := cartManifest().Register(stage2); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	f, err := stage2.Call(ctxbg, id, "peek", nil)
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	raw, err := f.Await(ctxbg)
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	if string(raw) != `["x","x","x"]` {
		t.Fatalf("恢复状态 = %s, want 3 个 x", raw)
	}
}
