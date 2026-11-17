package troupe

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ---- M3-C：Roster incarnation 单调协商 ----

func TestRosterIncarnationNegotiation(t *testing.T) {
	clk := NewVirtualClock()
	r := NewRoster(10*time.Second, clk)

	// 新成员：incarnation 1
	inc, err := r.Join("silo-1", "host-1:1")
	if err != nil || inc != 1 {
		t.Fatalf("首次 join = %d, %v", inc, err)
	}

	// 活成员重入团拒绝（StaleIncarnationError）
	_, err = r.Join("silo-1", "host-1:1")
	var sie *StaleIncarnationError
	if !errors.As(err, &sie) {
		t.Fatalf("活成员重入团错误 = %v, want StaleIncarnationError", err)
	}

	// 心跳：同 incarnation 刷新；旧 incarnation 拒绝
	if err := r.Heartbeat("silo-1", 1); err != nil {
		t.Fatalf("同 incarnation 心跳失败：%v", err)
	}
	if err := r.Heartbeat("silo-1", 0); !errors.As(err, &sie) {
		t.Fatalf("旧 incarnation 心跳错误 = %v, want StaleIncarnationError", err)
	}
	if err := r.Heartbeat("ghost", 1); !errors.As(err, &sie) {
		t.Fatalf("幽灵成员心跳错误 = %v", err)
	}

	// 判死（过期未心跳）：sweep 后判死；死亡成员心跳拒绝
	clk.Advance(11 * time.Second)
	dead := r.Sweep()
	if len(dead) != 1 || dead[0] != "silo-1" {
		t.Fatalf("sweep = %v, want [silo-1]", dead)
	}
	if err := r.Heartbeat("silo-1", 1); !errors.As(err, &sie) {
		t.Fatalf("死亡成员心跳错误 = %v, want StaleIncarnationError", err)
	}

	// 死亡成员重入团：incarnation 单调 +1（复活即新纪元）
	inc, err = r.Join("silo-1", "host-1:2")
	if err != nil || inc != 2 {
		t.Fatalf("死亡重入团 = %d, %v, want 2", inc, err)
	}
	if err := r.Heartbeat("silo-1", 2); err != nil {
		t.Fatalf("复活后心跳失败：%v", err)
	}
	if err := r.Heartbeat("silo-1", 1); !errors.As(err, &sie) {
		t.Fatal("复活后旧 incarnation 心跳应拒绝")
	}
}

func TestRosterMarkDeadIncarnationGuard(t *testing.T) {
	r := NewRoster(time.Second)
	if _, err := r.Join("silo-1", "h"); err != nil {
		t.Fatal(err)
	}
	if r.MarkDead("silo-1", 99) {
		t.Fatal("incarnation 不符的判死应拒绝")
	}
	if !r.MarkDead("silo-1", 1) {
		t.Fatal("incarnation 匹配判死应成功")
	}
	if r.MarkDead("silo-1", 1) {
		t.Fatal("已死成员重复判死应返回 false")
	}
}

// ---- M3-C：HashRing 只移动相邻区间键 ----

func TestHashRingGoldenCRCAndMovement(t *testing.T) {
	ring := NewHashRing(160)
	for _, n := range []string{"alpha", "beta", "gamma"} {
		ring.Add(n)
	}
	if got := ring.Nodes(); len(got) != 3 {
		t.Fatalf("nodes = %v", got)
	}

	// 键分布快照：1000 个键的归属
	keys := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		keys = append(keys, MustActorID("app", "order", itoa(i)).Canonical())
	}
	before := make(map[string]string, len(keys))
	for _, k := range keys {
		before[k] = ring.Node(k)
	}

	// 移除 beta：只有原属 beta 的键漂移（到后继），其余不动
	ring.Remove("beta")
	moved, unmoved := 0, 0
	for _, k := range keys {
		after := ring.Node(k)
		if after != before[k] {
			if before[k] != "beta" {
				t.Fatalf("非 beta 键 %s 被移动（%s → %s）", k, before[k], after)
			}
			moved++
		} else {
			unmoved++
		}
	}
	if moved == 0 {
		t.Fatal("beta 键应发生漂移")
	}
	_ = unmoved

	// 重新加入：beta 键回归
	ring.Add("beta")
	for _, k := range keys {
		if ring.Node(k) != before[k] {
			t.Fatalf("重新加入后键 %s 归属不稳（%s → %s）", k, before[k], ring.Node(k))
		}
	}
}

// ---- M3-C：Router 死亡节点键漂移 ----

func TestRouterDriftOnDeath(t *testing.T) {
	roster := NewRoster(time.Second)
	ring := NewHashRing(160)
	router := NewRouter(ring, roster)
	for _, n := range []string{"a", "b", "c"} {
		if _, err := roster.Join(n, n+":1"); err != nil {
			t.Fatal(err)
		}
		ring.Add(n)
	}

	keys := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		keys = append(keys, MustActorID("app", "sess", itoa(i)).Canonical())
	}
	before := make(map[string]string, len(keys))
	for _, k := range keys {
		before[k] = router.Route(k)
	}

	// b 判死 → Sync 移除 → 只有 b 的键漂移
	if !roster.MarkDead("b", 1) {
		t.Fatal("判死失败")
	}
	if removed := router.Sync(); len(removed) != 1 || removed[0] != "b" {
		t.Fatalf("sync removed = %v, want [b]", removed)
	}
	for _, k := range keys {
		after := router.Route(k)
		if before[k] == "b" {
			if after == "b" {
				t.Fatalf("死亡节点键 %s 未漂移", k)
			}
		} else if after != before[k] {
			t.Fatalf("存活节点键 %s 被移动（%s → %s）", k, before[k], after)
		}
	}

	// b 复活（incarnation 2）→ Sync 重新加入 → 归属恢复
	if _, err := roster.Join("b", "b:2"); err != nil {
		t.Fatal(err)
	}
	if removed := router.Sync(); len(removed) != 0 {
		t.Fatalf("复活后 sync removed = %v", removed)
	}
	for _, k := range keys {
		if router.Route(k) != before[k] {
			t.Fatalf("复活后键 %s 归属不稳", k)
		}
	}
}

// ---- M3-C：优雅 drain 三段式 ----

func TestGracefulDrain(t *testing.T) {
	// Manual 模式保证确定性：队列中的 Turn 不会被消费者抢先执行
	r := NewRehearsal("drain")
	stage := r.Stage
	if err := cartManifest().Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()
	id := MustActorID("app", "cart", "d")

	// 队列中留一个未执行 Turn（manual 模式确定性）
	f1, err := stage.Call(ctxbg, id, "add", map[string]any{"item": "queued"})
	if err != nil {
		t.Fatal(err)
	}

	stage.BeginDrain()
	if !stage.Draining() {
		t.Fatal("应处于排空中")
	}

	// 排空中拒新（retryable draining）
	_, err = stage.Call(ctxbg, id, "peek", nil)
	var cre *CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != RejectDraining || !cre.Retryable {
		t.Fatalf("排空中拒新错误 = %v, want draining retryable", err)
	}

	// 未完成排空：仍有排队 Turn → FinishDrain 拒绝
	ferr := stage.FinishDrain()
	if ferr == nil {
		t.Fatal("有排队 Turn 时 finish 应拒绝")
	}
	var iase *InvalidActorStateError
	if !errors.As(ferr, &iase) {
		t.Fatalf("finish 错误 = %v, want InvalidActorStateError", ferr)
	}

	// 排空队列后 finish 成功
	if !stage.Pump(id) {
		t.Fatal("pump 应执行排队的 Turn")
	}
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("排队 Turn 完成 = %v", err)
	}
	if !stage.Idle() {
		t.Fatal("队列清空后应 idle")
	}
	if err := stage.FinishDrain(); err != nil {
		t.Fatalf("finish 失败：%v", err)
	}
	if !stage.Closed() {
		t.Fatal("finish 后应停机")
	}

	// 停机后拒新（shutdown 优先级高于 draining）
	_, err = stage.Call(ctxbg, id, "peek", nil)
	if !errors.As(err, &cre) || cre.Reason != RejectShutdown {
		t.Fatalf("停机后错误 = %v, want shutdown", err)
	}
}
