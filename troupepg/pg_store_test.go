package troupepg

// PostgreSQL Provider 集成测试：需要真实 PG（TROUPE_PG_URL），未设置时跳过。
// CI 以 postgres service 必跑（对齐 troupe.cr M3 验收：真实 Provider 证据）。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	troupe "github.com/ShiningRay/troupego"
)

func pgURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TROUPE_PG_URL")
	if url == "" {
		t.Skip("TROUPE_PG_URL 未设置，跳过 PostgreSQL 用例")
	}
	return url
}

func openPair(t *testing.T, instance string) (*Store, *OwnershipStore) {
	t.Helper()
	store, own, err := Open(context.Background(), pgURL(t), WithInstance(instance), WithLease(30*time.Second))
	if err != nil {
		t.Fatalf("Open(%s) 失败：%v", instance, err)
	}
	// 测试隔离：清空共享库中的上次运行残留（容器跨 go test 持久）
	_, _ = store.pool.Exec(context.Background(),
		`TRUNCATE troupe_props, troupe_ownership, troupe_journal, troupe_reminders, troupe_outbox, troupe_outbox_sent`)
	t.Cleanup(store.Close)
	return store, own
}

func TestPGCreateWriteCAS(t *testing.T) {
	ctxbg := context.Background()
	store, own := openPair(t, "silo-cas")
	id := troupe.MustActorID("app", "cart", "cas")
	epoch, err := own.Acquire(ctxbg, id)
	if err != nil {
		t.Fatalf("acquire 失败：%v", err)
	}

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
	var ce *troupe.ConflictError
	if !errors.As(err, &ce) || ce.Kind != troupe.ConflictRevision {
		t.Fatalf("CAS 错误 = %v, want ConflictError{revision}", err)
	}

	// 重复 create → exists
	_, err = store.Create(ctxbg, id, 1, `{}`, epoch)
	if !errors.As(err, &ce) || ce.Kind != troupe.ConflictExists {
		t.Fatalf("重复 create 错误 = %v, want ConflictError{exists}", err)
	}

	if !store.CrossNodeSafe() {
		t.Fatal("PG Provider 必须声明 cross_node_safe")
	}
}

func TestPGLeaseTakeoverFencing(t *testing.T) {
	// 接管反例（对齐 troupe.cr M3）：跨实例租期内拒绝 → 过期后接管 →
	// 旧实例写被 fencing 拒绝（即使 revision 匹配）。
	ctxbg := context.Background()
	s1, o1 := openPair(t, "silo-1")
	s2, o2 := openPair(t, "silo-2")
	id := troupe.MustActorID("app", "cart", "takeover")

	e1, err := o1.Acquire(ctxbg, id)
	if err != nil {
		t.Fatalf("silo-1 acquire 失败：%v", err)
	}
	if _, err := s1.Create(ctxbg, id, 1, `{"items":["first"]}`, e1); err != nil {
		t.Fatalf("silo-1 create 失败：%v", err)
	}

	// 租期内 silo-2 acquire 显式拒绝
	_, err = o2.Acquire(ctxbg, id)
	var cre *troupe.CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != troupe.RejectOwnership {
		t.Fatalf("租期内接管错误 = %v, want ownership 拒绝", err)
	}

	// 租期过期 → silo-2 接管，epoch 单调 +1
	if err := o1.ExpireLease(ctxbg, id); err != nil {
		t.Fatalf("租期过期辅助失败：%v", err)
	}
	e2, err := o2.Acquire(ctxbg, id)
	if err != nil {
		t.Fatalf("silo-2 接管失败：%v", err)
	}
	if e2 != e1+1 {
		t.Fatalf("接管 epoch = %d, want %d+1（单调）", e2, e1)
	}

	// 旧实例（epoch1）写被拒绝——fencing 优先于 CAS
	_, err = s1.Write(ctxbg, id, 1, 1, `{"items":["stale"]}`, e1)
	if !errors.As(err, &cre) || cre.Reason != troupe.RejectOwnership {
		t.Fatalf("旧 epoch 写错误 = %v, want ownership 拒绝", err)
	}

	// 新实例写入成功；silo-1 renew 旧 epoch 失败
	if _, err := s2.Write(ctxbg, id, 1, 1, `{"items":["taken"]}`, e2); err != nil {
		t.Fatalf("silo-2 写失败：%v", err)
	}
	if o1.Renew(ctxbg, id, e1) {
		t.Fatal("旧 epoch renew 应失败")
	}
	if !o2.Renew(ctxbg, id, e2) {
		t.Fatal("当前 epoch renew 应成功")
	}
}

type payProps struct {
	Charged int `json:"charged"`
}

// TestPGStageTakeover 端到端：两个 Stage 实例经 PG 共享状态；旧实例保存被
// fencing 拒绝并失效；新实例从已提交快照继续（PLAN M3 验收第 2 条）。
func TestPGStageTakeover(t *testing.T) {
	ctxbg := context.Background()
	s1, o1 := openPair(t, "silo-stage-1")
	s2, o2 := openPair(t, "silo-stage-2")
	id := troupe.MustActorID("app", "order", "so1")

	newStage := func(store *Store, own *OwnershipStore) *troupe.Stage {
		st := troupe.NewStage(troupe.WithStore(store), troupe.WithOwnership(own))
		m := troupe.Define("order", func(aid troupe.ActorID, props payProps, actx *troupe.ActivationContext) *struct{ *troupe.Actor[payProps] } {
			return &struct{ *troupe.Actor[payProps] }{troupe.NewActor(aid, props, actx)}
		})
		m.SchemaVersion(1)
		m.Initial(func() payProps { return payProps{} })
		m.Turn(troupe.Turn("charge", func(a *struct{ *troupe.Actor[payProps] }, ctx *troupe.CallContext, _ troupe.Empty) (int, error) {
			a.Props().Charged++
			if err := a.Save(); err != nil {
				return 0, err
			}
			return a.Props().Charged, nil
		}))
		if err := m.Register(st); err != nil {
			t.Fatalf("注册失败：%v", err)
		}
		return st
	}

	stage1 := newStage(s1, o1)
	defer stage1.Close()
	n, err := troupe.Invoke[troupe.Empty, int](ctxbg, stage1, id, "charge", troupe.Empty{})
	if err != nil || n != 1 {
		t.Fatalf("silo-stage-1 charge = %d, %v", n, err)
	}

	// 接管：租期过期后 silo-stage-2 取得所有权并从已提交状态继续
	if err := o1.ExpireLease(ctxbg, id); err != nil {
		t.Fatal(err)
	}
	stage2 := newStage(s2, o2)
	defer stage2.Close()
	n, err = troupe.Invoke[troupe.Empty, int](ctxbg, stage2, id, "charge", troupe.Empty{})
	if err != nil || n != 2 {
		t.Fatalf("silo-stage-2 charge = %d, %v（应从 1 继续）", n, err)
	}

	// 旧实例再写：fencing 拒绝（save 失败 → 实例失效 → save_failed 计数）
	n, err = troupe.Invoke[troupe.Empty, int](ctxbg, stage1, id, "charge", troupe.Empty{})
	var cre *troupe.CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != troupe.RejectOwnership {
		t.Fatalf("旧实例保存错误 = %v, want ownership 拒绝", err)
	}
	_ = n
	if got := stage1.Metrics().Counter("save_failures"); got != 1 {
		t.Fatalf("save_failures = %d, want 1", got)
	}

	// 租期过期后旧实例重新激活：读到 silo-stage-2 已提交的状态（2），继续到 3
	if err := o2.ExpireLease(ctxbg, id); err != nil {
		t.Fatal(err)
	}
	// silo-stage-2 的激活槽已失效，等待其重新激活会因租期冲突失败——
	// 先关闭 silo-stage-2 让出租期（真实集群中该节点已下线）。
	stage2.Close()
	n, err = troupe.Invoke[troupe.Empty, int](ctxbg, stage1, id, "charge", troupe.Empty{})
	if err != nil || n != 3 {
		t.Fatalf("旧实例接管后 charge = %d, %v（应从 2 继续）", n, err)
	}
}

func TestPGJournalAtomicCommit(t *testing.T) {
	ctxbg := context.Background()
	store, own := openPair(t, "silo-journal")
	id := troupe.MustActorID("app", "cart", "j")
	epoch, _ := own.Acquire(ctxbg, id)
	if _, err := store.Create(ctxbg, id, 1, `{"items":[]}`, epoch); err != nil {
		t.Fatalf("create 失败：%v", err)
	}

	journal := []troupe.IdempotencyRecord{{
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

	// CAS 失败 → journal 整体不生效
	_, err = store.WriteJournaled(ctxbg, id, 99, 1, `{}`, epoch, []troupe.IdempotencyRecord{{
		Key: "pay-2", Fingerprint: "fp-b", OK: true, Value: `{}`, ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}})
	var ce *troupe.ConflictError
	if !errors.As(err, &ce) || ce.Kind != troupe.ConflictRevision {
		t.Fatalf("CAS 失败错误 = %v, want ConflictError{revision}", err)
	}
	if entries, _ = store.LoadJournal(ctxbg, id, time.Now().Unix()); len(entries) != 1 {
		t.Fatalf("失败提交的 journal 泄漏：%d 条", len(entries))
	}
	_ = rec
}

// TestPGMultiProcessRecovery 多进程验收：独立子进程写入后硬退出（≈kill -9），
// 父进程接管读取已提交状态。三进程（两子 + 父）满足 PLAN M3 验收
// "至少三个独立 Go 进程连接真实 PostgreSQL"。
func TestPGMultiProcessRecovery(t *testing.T) {
	if os.Getenv("TROUPE_HELPER") == "1" {
		helperMain()
		return
	}
	ctxbg := context.Background()
	url := pgURL(t)

	store, own := openPair(t, "silo-parent")
	id := troupe.MustActorID("app", "order", "mp")

	for i := 1; i <= 2; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestPGMultiProcessRecovery", "-test.v")
		cmd.Env = append(os.Environ(),
			"TROUPE_HELPER=1",
			fmt.Sprintf("TROUPE_HELPER_KEY=%d", i),
			"TROUPE_PG_URL="+url,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			// helper 以 os.Exit(1) 硬退出（模拟宕机）：退出码非 0 属预期路径
			if i == 1 && len(out) > 0 && containsHelperDone(out) {
				// continue：硬退出即目标
			} else if i == 2 && containsHelperDone(out) {
				// 同上
			} else {
				t.Fatalf("子进程 %d 失败：%v\n%s", i, err, out)
			}
		}
		// 子进程硬退出后：租期过期，父进程取得所有权验证子进程已提交状态
		if err := own.ExpireLease(ctxbg, id); err != nil {
			t.Fatal(err)
		}
	}

	stage := troupe.NewStage(troupe.WithStore(store), troupe.WithOwnership(own))
	defer stage.Close()
	m := troupe.Define("order", func(aid troupe.ActorID, props payProps, actx *troupe.ActivationContext) *struct{ *troupe.Actor[payProps] } {
		return &struct{ *troupe.Actor[payProps] }{troupe.NewActor(aid, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() payProps { return payProps{} })
	m.Turn(troupe.Turn("charge", func(a *struct{ *troupe.Actor[payProps] }, ctx *troupe.CallContext, _ troupe.Empty) (int, error) {
		a.Props().Charged++
		if err := a.Save(); err != nil {
			return 0, err
		}
		return a.Props().Charged, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	n, err := troupe.Invoke[troupe.Empty, int](ctxbg, stage, id, "charge", troupe.Empty{})
	if err != nil || n != 3 {
		t.Fatalf("父进程接管后 charge = %d, %v（两子进程各提交 1，应从 2 继续）", n, err)
	}
}

func containsHelperDone(out []byte) bool {
	return len(out) > 0 && (containsStr(string(out), "HELPER_DONE") || containsStr(string(out), "PASS"))
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// helperMain 子进程入口：写入一次后 os.Exit(1) 硬退出（无 defer 清理，模拟 kill -9）。
func helperMain() {
	ctxbg := context.Background()
	url := os.Getenv("TROUPE_PG_URL")
	store, own, err := Open(ctxbg, url, WithInstance("silo-child-"+os.Getenv("TROUPE_HELPER_KEY")), WithLease(30*time.Second))
	if err != nil {
		fmt.Fprintln(os.Stderr, "HELPER_OPEN_FAIL:", err)
		os.Exit(2)
	}
	id := troupe.MustActorID("app", "order", "mp")
	stage := troupe.NewStage(troupe.WithStore(store), troupe.WithOwnership(own))
	m := troupe.Define("order", func(aid troupe.ActorID, props payProps, actx *troupe.ActivationContext) *struct{ *troupe.Actor[payProps] } {
		return &struct{ *troupe.Actor[payProps] }{troupe.NewActor(aid, props, actx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() payProps { return payProps{} })
	m.Turn(troupe.Turn("charge", func(a *struct{ *troupe.Actor[payProps] }, ctx *troupe.CallContext, _ troupe.Empty) (int, error) {
		a.Props().Charged++
		if err := a.Save(); err != nil {
			return 0, err
		}
		return a.Props().Charged, nil
	}))
	if err := m.Register(stage); err != nil {
		fmt.Fprintln(os.Stderr, "HELPER_REGISTER_FAIL:", err)
		os.Exit(2)
	}
	n, err := troupe.Invoke[troupe.Empty, int](ctxbg, stage, id, "charge", troupe.Empty{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "HELPER_CHARGE_FAIL:", err)
		os.Exit(2)
	}
	fmt.Printf("HELPER_DONE charged=%d\n", n)
	// 硬退出：模拟 kill -9（不等待租期/优雅下线）
	os.Exit(1)
}
