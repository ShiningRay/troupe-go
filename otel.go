package troupe

// OTel 导出适配器（M6）：OTLP JSON v1 结构，零外部依赖——
// metrics → monotonic Sum、TraceEvent → spans（traceId/spanId/status）。
// sink 回调即 collector 推送点（宿主实现网络传输，框架不做 IO）。

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// OTelAdapter 订阅 Stage 的 metrics/trace 并缓冲到环形缓冲（内存有界），
// ExportAll 将缓冲转为 OTLP JSON v1 交付 sink。
type OTelAdapter struct {
	stage     *Stage
	sink      func(otlpJSON string)
	ringCap   int
	mu        sync.Mutex
	metrics   map[string]uint64
	spans     []OTelSpan
	dropped   int
	attached  bool
	prevEvent func(StageEvent)
	prevTrace func(TraceEvent)
}

// OTelSpan span 导出结构（TraceEvent 投影）。
type OTelSpan struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	StartTime    string `json:"startTimeUnixNano"`
	EndTime      string `json:"endTimeUnixNano"`
	Status       string `json:"status"` // UNSET | OK | ERROR
}

// NewOTelAdapter 构造适配器（ringCap 缓冲上限，默认 1024）。
func NewOTelAdapter(stage *Stage, sink func(otlpJSON string)) *OTelAdapter {
	return &OTelAdapter{stage: stage, sink: sink, ringCap: 1024, metrics: make(map[string]uint64)}
}

// WithRingCap 覆盖环形缓冲上限（溢出丢弃最旧并计 dropped）。
func (a *OTelAdapter) WithRingCap(n int) *OTelAdapter {
	if n > 0 {
		a.ringCap = n
	}
	return a
}

// Attach 订阅 stage 事件与 trace（可重复调用为幂等；恢复既有 handler 链）。
func (a *OTelAdapter) Attach() {
	a.mu.Lock()
	if a.attached {
		a.mu.Unlock()
		return
	}
	a.attached = true
	a.mu.Unlock()

	prevEvent := captureHandler(a.stage)
	prevTrace := captureTraceHandler(a.stage)
	a.mu.Lock()
	a.prevEvent, a.prevTrace = prevEvent, prevTrace
	a.mu.Unlock()

	a.stage.OnEvent(func(ev StageEvent) {
		a.observeEvent(ev)
		if prevEvent != nil {
			prevEvent(ev)
		}
	})
	a.stage.OnTrace(func(te TraceEvent) {
		a.observeTrace(te)
		if prevTrace != nil {
			prevTrace(te)
		}
	})
}

func captureHandler(s *Stage) func(StageEvent)      { return s.eventHandlerSnapshot() }
func captureTraceHandler(s *Stage) func(TraceEvent) { return s.traceHandlerSnapshot() }

func (a *OTelAdapter) observeEvent(ev StageEvent) {
	a.mu.Lock()
	// lifecycle 事件聚合为计数（monotonic Sum 语义）
	name := string(ev.Type)
	if ev.Detail == "ok" {
		name += ".ok"
	} else if ev.Detail == "error" {
		name += ".error"
	}
	a.metrics[name]++
	a.mu.Unlock()
}

func (a *OTelAdapter) observeTrace(te TraceEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	span := OTelSpan{
		Name:   te.Method,
		Kind:   "SPAN_KIND_INTERNAL",
		Status: "UNSET",
	}
	if len(te.Traceparent) >= 55 {
		span.TraceID = te.Traceparent[3:35]
		span.SpanID = te.Traceparent[36:52]
	}
	if len(te.ParentTraceparent) >= 55 {
		span.ParentSpanID = te.ParentTraceparent[36:52]
	}
	start := te.At.Add(-te.Duration)
	span.StartTime = fmt.Sprintf("%d", start.UnixNano())
	span.EndTime = fmt.Sprintf("%d", te.At.UnixNano())
	switch te.Outcome {
	case "completed":
		span.Status = "OK"
	case "failed", "expired":
		span.Status = "ERROR"
	}
	a.spans = append(a.spans, span)
	if len(a.spans) > a.ringCap {
		drop := len(a.spans) - a.ringCap
		a.spans = a.spans[drop:]
		a.dropped += drop
	}
}

// otlpExport 导出载荷（OTLP/JSON v1 近似：metrics monotonic Sum + spans）。
type otlpExport struct {
	Format      string         `json:"format"` // "OTLP/JSON v1"
	GeneratedAt string         `json:"generatedAt"`
	Resource    map[string]any `json:"resource"`
	Metrics     []otlpMetric   `json:"metrics"`
	Spans       []OTelSpan     `json:"spans"`
	Dropped     int            `json:"droppedSpans,omitempty"`
}

// otlpMetric OTLP monotonic Sum 投影。
type otlpMetric struct {
	Name        string `json:"name"`
	IsMonotonic bool   `json:"isMonotonic"`
	Aggregation string `json:"aggregationTemporality"` // CUMULATIVE
	Sum         uint64 `json:"sum"`
}

// ExportAll 将缓冲转 OTLP JSON 交付 sink 并清空（dropped 计数保留）。
func (a *OTelAdapter) ExportAll(ctx context.Context) error {
	a.mu.Lock()
	names := make([]string, 0, len(a.metrics))
	for name := range a.metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	metrics := make([]otlpMetric, 0, len(names))
	for _, name := range names {
		metrics = append(metrics, otlpMetric{
			Name: name, IsMonotonic: true, Aggregation: "CUMULATIVE", Sum: a.metrics[name],
		})
	}
	spans := append([]OTelSpan{}, a.spans...)
	a.spans = nil
	dropped := a.dropped
	a.mu.Unlock()

	payload := otlpExport{
		Format:      "OTLP/JSON v1",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Resource:    map[string]any{"service.name": "troupego"},
		Metrics:     metrics,
		Spans:       spans,
	}
	if dropped > 0 {
		payload.Dropped = dropped
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if a.sink == nil {
		return nil
	}
	a.sink(string(b))
	return nil
}
