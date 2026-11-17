package troupepg

// PG 持久 Reminder 与 Outbox 集成测试（TROUPE_PG_URL 门控；CI postgres service 必跑）。

import (
	"context"
	"errors"
	"testing"
	"time"

	troupe "github.com/ShiningRay/troupego"
)

func TestPGReminderOwnershipFilterAndFencing(t *testing.T) {
	ctxbg := context.Background()
	s1, o1 := openPair(t, "silo-r1")
	s2, o2 := openPair(t, "silo-r2")
	rem1 := s1.OpenReminders()
	rem2 := s2.OpenReminders()

	id := troupe.MustActorID("app", "job", "own")
	// silo-r1 取得 actor 所有权
	if _, err := o1.Acquire(ctxbg, id); err != nil {
		t.Fatal(err)
	}
	if err := rem1.Schedule(ctxbg, id, "pay", "charge", []byte(`{}`), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}

	// 归属过滤：silo-r2 不拥有该 actor → 认领不到
	c2, err := rem2.ClaimDue(ctxbg, time.Now(), "silo-r2", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range c2 {
		if rec.ActorID == id.Canonical() {
			t.Fatal("他实例不应认领非本节点拥有的 actor 提醒")
		}
	}
	// 本实例认领成功
	c1, err := rem1.ClaimDue(ctxbg, time.Now(), "silo-r1", 30*time.Second)
	if err != nil || len(c1) != 1 {
		t.Fatalf("本实例认领 = %d, %v, want 1", len(c1), err)
	}
	rec := c1[0]

	// fencing 语义：reminder fencing 是 claim 租期（status_version+holder+claim 租期），
	// 所有权转移只阻止新认领（归属过滤），不撤销在途认领（交付中效果保持）。
	// silo-r1 的在途 complete 在其 claim 租期内合法（交付中效果保持）。
	if ok, _ := rem1.Complete(ctxbg, id, "pay", rec.StatusVersion, "silo-r1"); !ok {
		t.Fatal("在途认领的 complete（租期内）应成功")
	}

	// claim 租期过期 + 所有权已转移：silo-r1 无法再认领，silo-r2 认领并完成
	_ = o1.ExpireLease(ctxbg, id)
	if _, err := o2.Acquire(ctxbg, id); err != nil {
		t.Fatal(err)
	}
	if err := rem2.Schedule(ctxbg, id, "pay", "charge", []byte(`{}`), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if ok, _ := rem1.ClaimDue(ctxbg, time.Now(), "silo-r1", 30*time.Second); len(ok) != 0 {
		t.Fatal("所有权转移后旧实例不应能认领")
	}
	c3, err := rem2.ClaimDue(ctxbg, time.Now(), "silo-r2", 30*time.Second)
	if err != nil || len(c3) != 1 {
		t.Fatalf("新实例认领 = %d, %v, want 1", len(c3), err)
	}
	ok, err := rem2.Complete(ctxbg, id, "pay", c3[0].StatusVersion, "silo-r2")
	if err != nil || !ok {
		t.Fatalf("新实例 complete = %v, %v", ok, err)
	}
}

func TestPGOutboxCommitAndDispatch(t *testing.T) {
	ctxbg := context.Background()
	store, own := openPair(t, "silo-ob")
	ob := store.Outbox()
	id := troupe.MustActorID("app", "job", "ob")
	epoch, err := own.Acquire(ctxbg, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctxbg, id, 1, `{"items":[]}`, epoch); err != nil {
		t.Fatal(err)
	}

	// outbox 与 Props 同事务提交
	rec, err := ob.WriteWithOutbox(ctxbg, id, 1, 1, `{"items":["x"]}`, epoch, nil, []troupe.OutboxRecord{{
		EventID: "evt-1", ActorID: id.Canonical(), Topic: "orders",
		Payload: []byte(`{"n":1}`), CreatedAt: time.Now(),
	}})
	if err != nil {
		t.Fatalf("WriteWithOutbox 失败：%v", err)
	}
	events, err := ob.PollOutbox(ctxbg, id, 10)
	if err != nil || len(events) != 1 || events[0].EventID != "evt-1" {
		t.Fatalf("poll = %d, %v", len(events), err)
	}

	// CAS 失败 → outbox 整体不生效
	_, err = ob.WriteWithOutbox(ctxbg, id, 99, 1, `{}`, epoch, nil, []troupe.OutboxRecord{{
		EventID: "evt-2", ActorID: id.Canonical(), Topic: "orders",
		Payload: []byte(`{}`), CreatedAt: time.Now(),
	}})
	var ce *troupe.ConflictError
	if !errors.As(err, &ce) || ce.Kind != troupe.ConflictRevision {
		t.Fatalf("CAS 失败错误 = %v, want ConflictError{revision}", err)
	}
	if events, _ = ob.PollOutbox(ctxbg, id, 10); len(events) != 1 {
		t.Fatalf("失败提交的 outbox 泄漏：%d 条", len(events))
	}
	_ = rec

	// MarkSent 后不再投递；崩溃未 MarkSent → 重投（at-least-once）
	if !ob.MarkSent(ctxbg, id, "evt-1") {
		t.Fatal("MarkSent 应成功")
	}
	if events, _ = ob.PollOutbox(ctxbg, id, 10); len(events) != 0 {
		t.Fatalf("MarkSent 后仍投递：%d", len(events))
	}
}
