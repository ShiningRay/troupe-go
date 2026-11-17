package troupe

// M4：Director 健康端点（stdlib net/http 零依赖）：/health /metrics /events +
// pprof。管理写操作（drain/shutdown）不经 HTTP——仅 Director API。

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"sort"
)

// HTTPEndpoint Director 的 HTTP 观测端点。
type HTTPEndpoint struct {
	director *Director
	server   *http.Server
}

// fmtListen 监听 TCP 地址。
func fmtListen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// NewHTTPEndpoint 构造端点（不监听；调用 Listen）。
func NewHTTPEndpoint(d *Director) *HTTPEndpoint {
	e := &HTTPEndpoint{director: d}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", e.handleHealth)
	mux.HandleFunc("/metrics", e.handleMetrics)
	mux.HandleFunc("/events", e.handleEvents)
	// pprof（运维面默认提供；net/http/pprof 注册到同一 mux）
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/heap", pprof.Index)
	e.server = &http.Server{Handler: mux}
	return e
}

// Listen 监听地址（":0" 系统分配；Addr() 取实际地址）。
func (e *HTTPEndpoint) Listen(addr string) error {
	l, err := fmtListen(addr)
	if err != nil {
		return err
	}
	e.server.Addr = l.Addr().String()
	go func() { _ = e.server.Serve(l) }()
	return nil
}

// Addr 返回监听地址（Listen 后有效）。
func (e *HTTPEndpoint) Addr() string { return e.server.Addr }

// Close 关闭端点。
func (e *HTTPEndpoint) Close() { _ = e.server.Close() }

func (e *HTTPEndpoint) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"stages": e.director.Health()})
}

func (e *HTTPEndpoint) handleMetrics(w http.ResponseWriter, r *http.Request) {
	out := map[string]map[string]uint64{}
	for _, name := range e.director.StageNames() {
		if snap, ok := e.director.StageMetricsSnapshot(name); ok {
			out[name] = snap
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (e *HTTPEndpoint) handleEvents(w http.ResponseWriter, r *http.Request) {
	events := e.director.RecentEvents(200)
	items := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		items = append(items, map[string]any{
			"type":   string(ev.Type),
			"actor":  ev.ActorID.Canonical(),
			"at":     ev.At.UnixMilli(),
			"method": ev.Method,
			"detail": ev.Detail,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i]["at"].(int64), items[j]["at"].(int64)
		return a < b
	})
	writeJSON(w, http.StatusOK, map[string]any{"events": items})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
