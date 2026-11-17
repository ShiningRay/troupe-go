package troupe

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Roster 成员视图（M3）：join/heartbeat/sweep 判死；incarnation 单调协商——
// 活成员重入团（旧/同 incarnation）拒绝 StaleIncarnationError；
// 死亡成员重入团 incarnation 单调 +1（复活即新纪元）。分区期间不自欺为
// 已解决脑裂：所有权判定始终以存储租约/fencing 为准（DESIGN §8）。
type Roster struct {
	mu      sync.Mutex
	nodes   map[string]*rosterNode
	clock   Clock
	timeout time.Duration
}

type rosterNode struct {
	addr        string
	incarnation uint64
	lastSeen    time.Time
	alive       bool
}

// Member 成员快照。
type Member struct {
	Node        string
	Addr        string
	Incarnation uint64
	Alive       bool
}

func NewRoster(timeout time.Duration, clock ...Clock) *Roster {
	c := Clock(SystemClock{})
	if len(clock) > 0 {
		c = clock[0]
	}
	return &Roster{
		nodes:   make(map[string]*rosterNode),
		clock:   c,
		timeout: timeout,
	}
}

// Join 加入/重入团；返回生效 incarnation。
// 活成员重入团拒绝；死亡成员重入团 incarnation +1（复活即新纪元）。
func (r *Roster) Join(node, addr string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	if n, ok := r.nodes[node]; ok {
		if n.alive {
			return 0, &StaleIncarnationError{
				Detail: fmt.Sprintf("成员 %s（incarnation %d）已在团内，重入团拒绝", node, n.incarnation),
			}
		}
		// 死亡成员复活：incarnation 单调 +1
		n.incarnation++
		n.addr = addr
		n.alive = true
		n.lastSeen = now
		return n.incarnation, nil
	}
	r.nodes[node] = &rosterNode{addr: addr, incarnation: 1, lastSeen: now, alive: true}
	return 1, nil
}

// Heartbeat 心跳：incarnation 旧于当前值拒绝（过期消息不刷新视图）；
// 死亡成员心跳拒绝（判死后须经 Join 复活）。
func (r *Roster) Heartbeat(node string, incarnation uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[node]
	if !ok {
		return &StaleIncarnationError{Detail: fmt.Sprintf("成员 %s 不在团内，心跳拒绝", node)}
	}
	if !n.alive {
		return &StaleIncarnationError{Detail: fmt.Sprintf("成员 %s 已判死（incarnation %d），心跳拒绝（须 Join 复活）", node, n.incarnation)}
	}
	if incarnation < n.incarnation {
		return &StaleIncarnationError{
			Detail: fmt.Sprintf("成员 %s 心跳 incarnation %d 旧于当前 %d，拒绝", node, incarnation, n.incarnation),
		}
	}
	n.lastSeen = r.clock.Now()
	return nil
}

// MarkDead 以 incarnation 守卫判死（过期判死消息不生效）。
func (r *Roster) MarkDead(node string, incarnation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[node]
	if !ok || !n.alive || n.incarnation != incarnation {
		return false
	}
	n.alive = false
	return true
}

// Sweep 将超过 timeout 未心跳的活成员判死，返回本次判死名单。
func (r *Roster) Sweep() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	var dead []string
	for node, n := range r.nodes {
		if n.alive && now.Sub(n.lastSeen) >= r.timeout {
			n.alive = false
			dead = append(dead, node)
		}
	}
	sort.Strings(dead)
	return dead
}

// Members 返回成员快照（按节点名排序）。
func (r *Roster) Members() []Member {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Member, 0, len(r.nodes))
	for node, n := range r.nodes {
		out = append(out, Member{Node: node, Addr: n.addr, Incarnation: n.incarnation, Alive: n.alive})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// AliveNodes 返回存活节点名（排序）。
func (r *Roster) AliveNodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for node, n := range r.nodes {
		if n.alive {
			out = append(out, node)
		}
	}
	sort.Strings(out)
	return out
}

// ---- HashRing：vnodes + CRC32 一致性环（golden/hash.json 锚定 CRC32）----
// 节点增删只移动相邻区间键。

type ringPoint struct {
	hash uint32
	node string
}

// HashRing 一致性哈希环。
type HashRing struct {
	mu      sync.Mutex
	virtual int
	points  []ringPoint
	nodes   map[string]bool
}

func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = 160
	}
	return &HashRing{virtual: virtualNodes, nodes: make(map[string]bool)}
}

// Add 加入节点（幂等）。
func (h *HashRing) Add(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nodes[node] {
		return
	}
	h.nodes[node] = true
	for i := 0; i < h.virtual; i++ {
		vkey := fmt.Sprintf("%s#%d", node, i)
		h.points = append(h.points, ringPoint{hash: CanonicalHash(vkey), node: node})
	}
	sort.Slice(h.points, func(i, j int) bool { return h.points[i].hash < h.points[j].hash })
}

// Remove 移除节点（幂等）。
func (h *HashRing) Remove(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.nodes[node] {
		return
	}
	delete(h.nodes, node)
	points := h.points[:0]
	for _, p := range h.points {
		if p.node != node {
			points = append(points, p)
		}
	}
	h.points = points
}

// Node 返回 key（canonical 形式）归属节点；环为空返回 ""。
func (h *HashRing) Node(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.points) == 0 {
		return ""
	}
	hash := CanonicalHash(key)
	// 二分找第一个 >= hash 的点；越过末尾回绕到最小（后继语义）。
	lo, hi := 0, len(h.points)
	for lo < hi {
		mid := (lo + hi) / 2
		if h.points[mid].hash < hash {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(h.points) {
		lo = 0
	}
	return h.points[lo].node
}

// Nodes 返回环上节点（排序）。
func (h *HashRing) Nodes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.nodes))
	for n := range h.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// ---- Router：成员视图同步到环；死亡节点键漂移到后继 ----

// Router 以 Roster 存活成员重建环并路由（Sync 后 Route）。
type Router struct {
	ring   *HashRing
	roster *Roster
}

func NewRouter(ring *HashRing, roster *Roster) *Router {
	return &Router{ring: ring, roster: roster}
}

// Sync 以 roster 当前存活成员重建环（死亡节点移除 → 键漂移到后继）。
// 返回本次移除（判死）的节点列表。
func (rt *Router) Sync() []string {
	alive := make(map[string]bool, len(rt.roster.AliveNodes()))
	for _, node := range rt.roster.AliveNodes() {
		alive[node] = true
	}
	var removed []string
	for _, node := range rt.ring.Nodes() {
		if !alive[node] {
			rt.ring.Remove(node)
			removed = append(removed, node)
		}
	}
	for node := range alive {
		rt.ring.Add(node)
	}
	sort.Strings(removed)
	return removed
}

// Route 返回 key 的归属节点（死亡节点键漂移到后继；环空返回 ""）。
func (rt *Router) Route(key string) string { return rt.ring.Node(key) }
