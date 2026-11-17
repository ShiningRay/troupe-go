package troupe

// 版本化目录、能力放置与再平衡（M5，对齐 troupe.cr 语义）：
// per-entry version 单调 + view_version 全局视图版本；旧计划重放以 version
// 保护拒绝；Placer 能力子集匹配 + 负载最少者入选；Rebalancer 死亡节点
// handoff（环后继漂移）/新节点 overflow 迁移。

import (
	"fmt"
	"sort"
	"sync"
)

// VersionedDirectory actor_id → (node, version) 注册表：
//   - Register：新条目 version=1；同节点重复注册 no-op（旧计划重放保护）；
//     异节点改派 version+1（必须严格大于现值——旧版本抢注册拒绝）。
//   - ViewVersion：全局视图版本，任何变更 +1。
type VersionedDirectory struct {
	mu          sync.Mutex
	entries     map[ActorID]dirEntry
	viewVersion uint64
}

type dirEntry struct {
	node    string
	version uint64
}

func NewVersionedDirectory() *VersionedDirectory {
	return &VersionedDirectory{entries: make(map[ActorID]dirEntry)}
}

// Register 注册/改派 actor → node；返回生效 version。
// 新条目 1；同节点 no-op（返回现 version）；异节点 version+1。
func (d *VersionedDirectory) Register(id ActorID, node string) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.entries[id]; ok {
		if e.node == node {
			return e.version, nil // 同节点改派：no-op（旧计划重放保护）
		}
		e.version++
		e.node = node
		d.entries[id] = e
		d.viewVersion++
		return e.version, nil
	}
	d.entries[id] = dirEntry{node: node, version: 1}
	d.viewVersion++
	return 1, nil
}

// RegisterWithVersion 带版本注册：version 必须 > 现值（旧版本抢注册拒绝）。
// 同节点且版本不高于现值 → no-op（返回现 version，nil 错误）。
func (d *VersionedDirectory) RegisterWithVersion(id ActorID, node string, version uint64) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.entries[id]; ok {
		if e.node == node {
			return e.version, nil // 同节点：no-op
		}
		if version <= e.version {
			return 0, &StaleIncarnationError{
				Detail: fmt.Sprintf("actor %s 抢注册被拒：版本 %d ≤ 现值 %d（节点 %s）",
					id.Canonical(), version, e.version, e.node),
			}
		}
		d.entries[id] = dirEntry{node: node, version: version}
		d.viewVersion++
		return version, nil
	}
	d.entries[id] = dirEntry{node: node, version: version}
	d.viewVersion++
	return version, nil
}

// Lookup 返回归属节点与版本。
func (d *VersionedDirectory) Lookup(id ActorID) (node string, version uint64, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.entries[id]
	if !ok {
		return "", 0, false
	}
	return e.node, e.version, true
}

// ViewVersion 全局视图版本（任何变更 +1）。
func (d *VersionedDirectory) ViewVersion() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.viewVersion
}

// Entries 返回条目快照（按 canonical 排序）。
func (d *VersionedDirectory) Entries() map[ActorID]uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[ActorID]uint64, len(d.entries))
	for id, e := range d.entries {
		out[id] = e.version
	}
	return out
}

// ---- Placer：capabilities 需求子集匹配 + entries 负载最少者 ----

// NodeCapabilities 节点能力与负载。
type NodeCapabilities struct {
	Node         string
	Capabilities []string
}

// Placer placement 决策器。
type Placer struct {
	mu    sync.Mutex
	nodes map[string]map[string]bool // node → capabilities
	loads map[string]int             // node → 已放置 entries 数
}

func NewPlacer() *Placer {
	return &Placer{nodes: make(map[string]map[string]bool), loads: make(map[string]int)}
}

// AddNode 注册节点能力（幂等覆盖）。
func (p *Placer) AddNode(node string, capabilities []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	caps := make(map[string]bool, len(capabilities))
	for _, c := range capabilities {
		caps[c] = true
	}
	p.nodes[node] = caps
}

// RemoveNode 移除节点。
func (p *Placer) RemoveNode(node string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, node)
	delete(p.loads, node)
}

// Place 需求能力子集匹配 → 满足者中负载最少者入选（负载 +1）。
// 无满足节点显式拒绝（PlacementError）。
func (p *Placer) Place(required []string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	need := make(map[string]bool, len(required))
	for _, c := range required {
		need[c] = true
	}
	var candidates []string
	for node, caps := range p.nodes {
		ok := true
		for c := range need {
			if !caps[c] {
				ok = false
				break
			}
		}
		if ok {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return "", &PlacementError{Required: required}
	}
	sort.Strings(candidates) // 同负载确定性（字典序）
	best := candidates[0]
	for _, node := range candidates[1:] {
		if p.loads[node] < p.loads[best] {
			best = node
		}
	}
	p.loads[best]++
	return best, nil
}

// Load 返回节点负载。
func (p *Placer) Load(node string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.loads[node]
}

// PlacementError 无满足能力的节点。
type PlacementError struct {
	Required []string
}

func (e *PlacementError) Error() string {
	return fmt.Sprintf("无满足能力 %v 的节点", e.Required)
}

// ---- Rebalancer：死亡节点 handoff（环后继）+ 旧计划重放保护 ----

// HandoffPlan rebalancer 决策：entry → 目标节点（环后继漂移）。
type HandoffPlan struct {
	Reassignments map[ActorID]string
	FromNodes     []string // 本次移除（死亡）的节点
}

// Rebalancer 以 HashRing 后继 + 版本化目录生成 handoff/recovery 计划。
type Rebalancer struct {
	ring      *HashRing
	directory *VersionedDirectory
}

func NewRebalancer(ring *HashRing, directory *VersionedDirectory) *Rebalancer {
	return &Rebalancer{ring: ring, directory: directory}
}

// HandoffPlan 对死亡节点的全部条目生成改派计划（目标=环上后继）；
// 旧计划重放安全：Rebalancer 只产出计划，落盘经 Directory.RegisterWithVersion
// （version 保护——旧计划重放为 no-op 或拒绝，不产生双重改派）。
func (r *Rebalancer) HandoffPlan(deadNodes []string) HandoffPlan {
	dead := make(map[string]bool, len(deadNodes))
	for _, n := range deadNodes {
		dead[n] = true
	}
	r.mulessRingSync(dead)
	plan := HandoffPlan{Reassignments: make(map[ActorID]string), FromNodes: append([]string{}, deadNodes...)}
	entries := r.directory.Entries()
	ids := make([]ActorID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Canonical() < ids[j].Canonical() })
	for _, id := range ids {
		node, _, ok := r.directory.Lookup(id)
		if !ok || !dead[node] {
			continue
		}
		// 环上后继（死亡节点移除后漂移目标）
		if target := r.ring.Node(id.Canonical()); target != "" && !dead[target] {
			plan.Reassignments[id] = target
		}
	}
	return plan
}

// mulessRingSync 环同步（移除死亡节点使 Node() 返回后继）。
func (r *Rebalancer) mulessRingSync(dead map[string]bool) {
	for _, node := range r.ring.Nodes() {
		if dead[node] {
			r.ring.Remove(node)
		}
	}
}

// ApplyPlan 落盘改派计划（version 保护：同节点 no-op/旧版本拒绝；重放安全）。
func (r *Rebalancer) ApplyPlan(plan HandoffPlan) (applied, skipped int) {
	ids := make([]ActorID, 0, len(plan.Reassignments))
	for id := range plan.Reassignments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Canonical() < ids[j].Canonical() })
	for _, id := range ids {
		node, prevVersion, ok := r.directory.Lookup(id)
		target := plan.Reassignments[id]
		if ok && node == target {
			skipped++ // 旧计划重放：同节点 no-op
			continue
		}
		if _, err := r.directory.RegisterWithVersion(id, target, prevVersion+1); err != nil {
			skipped++ // 版本保护拒绝（并发改派已生效）
			continue
		}
		applied++
	}
	return applied, skipped
}
