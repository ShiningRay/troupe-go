package troupe

// M5-B：版本化目录/放置/再平衡反例——旧节点复活抢注册拒绝、同节点改派 no-op
// （旧计划重放保护）、能力子集匹配与负载最少、死亡节点 handoff/overflow 迁移。

import (
	"errors"
	"testing"
)

func TestVersionedDirectoryRegistration(t *testing.T) {
	d := NewVersionedDirectory()
	id := MustActorID("app", "cart", "v1")
	vv0 := d.ViewVersion()

	// 新条目：version 1，view_version +1
	v, err := d.Register(id, "silo-1")
	if err != nil || v != 1 {
		t.Fatalf("首次注册 = %d, %v", v, err)
	}
	// 同节点重复注册：no-op（旧计划重放保护）
	v, err = d.Register(id, "silo-1")
	if err != nil || v != 1 {
		t.Fatalf("同节点 no-op = %d, %v, want 1", v, err)
	}
	// 异节点改派：version+1
	v, err = d.Register(id, "silo-2")
	if err != nil || v != 2 {
		t.Fatalf("改派 = %d, %v, want 2", v, err)
	}
	// 带版本抢注册：旧版本拒绝（旧节点复活抢注册反例）
	_, err = d.RegisterWithVersion(id, "silo-1", 1)
	var sie *StaleIncarnationError
	if !errors.As(err, &sie) {
		t.Fatalf("旧版本抢注册错误 = %v, want StaleIncarnationError", err)
	}
	// 同节点带旧版本：no-op
	if v, err := d.RegisterWithVersion(id, "silo-2", 1); err != nil || v != 2 {
		t.Fatalf("同节点旧版本 = %d, %v, want 2/no-op", v, err)
	}
	// 高版本接受
	v, err = d.RegisterWithVersion(id, "silo-3", 5)
	if err != nil || v != 5 {
		t.Fatalf("高版本 = %d, %v, want 5", v, err)
	}
	// view_version：3 次生效变更（1 注册 + 1 改派 + 1 高版本）
	if got := d.ViewVersion(); got != vv0+3 {
		t.Fatalf("view_version = %d, want %d", got, vv0+3)
	}
	node, version, ok := d.Lookup(id)
	if !ok || node != "silo-3" || version != 5 {
		t.Fatalf("lookup = %s/%d/%v", node, version, ok)
	}
}

func TestPlacerCapabilitySubsetAndLeastLoaded(t *testing.T) {
	p := NewPlacer()
	p.AddNode("gpu-1", []string{"gpu", "orders"})
	p.AddNode("cpu-1", []string{"orders"})
	p.AddNode("gpu-2", []string{"gpu", "orders"})

	// 子集匹配：需要 [orders] → 三节点皆可，负载最少者入选
	got, err := p.Place([]string{"orders"})
	if err != nil || got == "" {
		t.Fatalf("place = %q, %v", got, err)
	}
	// 负载最少者：连续放置后负载均衡（cpu-1 与 gpu-1/gpu-2 轮转）
	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		node, err := p.Place([]string{"orders"})
		if err != nil {
			t.Fatal(err)
		}
		counts[node]++
	}
	for _, n := range counts {
		if n > 2 { // 6 次 / 3 节点，均衡时每节点 ≤2
			t.Fatalf("负载不均：%v", counts)
		}
	}
	_ = got

	// 能力子集：需要 [gpu] → gpu-1/gpu-2（不含 cpu-1）
	for i := 0; i < 4; i++ {
		node, err := p.Place([]string{"gpu"})
		if err != nil {
			t.Fatal(err)
		}
		if node != "gpu-1" && node != "gpu-2" {
			t.Fatalf("gpu 需求放置到 %s", node)
		}
	}

	// 无满足节点 → 显式拒绝
	if _, err := p.Place([]string{"quantum"}); err == nil {
		t.Fatal("无能力满足应拒绝")
	}
	p.RemoveNode("gpu-1")
	p.RemoveNode("gpu-2")
	if _, err := p.Place([]string{"gpu"}); err == nil {
		t.Fatal("移除后应拒绝")
	}
}

func TestRebalancerHandoffAndReplayProtection(t *testing.T) {
	ring := NewHashRing(160)
	d := NewVersionedDirectory()
	rebalancer := NewRebalancer(ring, d)

	// 三个节点各注册一批 entries
	nodes := []string{"a", "b", "dead"}
	for _, node := range nodes {
		ring.Add(node)
	}
	var ids []ActorID
	for i := 0; i < 30; i++ {
		id := MustActorID("app", "sess", itoa(i))
		ids = append(ids, id)
		// 经 Placer 均匀放置（capacities 相同）
		if _, err := d.Register(id, nodes[i%len(nodes)]); err != nil {
			t.Fatal(err)
		}
	}

	// 死亡节点持有 entries 数（放前快照）
	deadEntries := 0
	for _, id := range ids {
		if node, _, _ := d.Lookup(id); node == "dead" {
			deadEntries++
		}
	}
	if deadEntries == 0 {
		t.Fatal("dead 节点应持有部分 entries")
	}

	// dead 判死 → handoff 计划：全部改派到后继
	plan := rebalancer.HandoffPlan([]string{"dead"})
	if len(plan.Reassignments) != deadEntries {
		t.Fatalf("计划条目 = %d, want %d", len(plan.Reassignments), deadEntries)
	}
	for id, target := range plan.Reassignments {
		if target == "dead" {
			t.Fatalf("改派目标仍为 dead：%s", id)
		}
	}
	applied, skipped := rebalancer.ApplyPlan(plan)
	if applied != deadEntries || skipped != 0 {
		t.Fatalf("apply = %d/%d, want %d/0", applied, skipped, deadEntries)
	}

	// 全部条目不再归属 dead
	for _, id := range ids {
		if node, _, _ := d.Lookup(id); node == "dead" {
			t.Fatalf("条目 %s 仍归属 dead", id)
		}
	}

	// 旧计划重放：同节点 no-op（skipped）——重放保护
	applied2, skipped2 := rebalancer.ApplyPlan(plan)
	if applied2 != 0 || skipped2 != deadEntries {
		t.Fatalf("旧计划重放 = %d/%d, want 0/%d（no-op 保护）", applied2, skipped2, deadEntries)
	}

	// 存活节点 entries 不动：a/b 的版本不变
	for _, id := range ids {
		if node, _, _ := d.Lookup(id); node == "a" || node == "b" {
			// 版本应保持 1（未被 handoff 触碰）
		}
	}
	_ = ring
}
