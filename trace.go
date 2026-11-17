package troupe

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// W3C Trace Context（traceparent）解析与派生（M2，对齐 troupe.cr TraceContext）：
// 格式 `00-<32hex>-<16hex>-<2hex flags>`；traceId/spanId 全零拒绝；
// 非法父上下文静默降级为空（新 trace），不报错不阻塞调用。

// ValidTraceparent 判断 traceparent 是否合法（非全零 id、可解析格式）。
func ValidTraceparent(tp string) bool {
	_, _, ok := parseTraceparent(tp)
	return ok
}

func parseTraceparent(tp string) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return "", "", false
	}
	version, trace, span := parts[0], parts[1], parts[2]
	if version != "00" && version != "01" { // 常见版本；ff 禁止
		return "", "", false
	}
	if len(trace) != 32 || len(span) != 32/2 {
		return "", "", false
	}
	if !isHex(trace) || !isHex(span) || !isHex(parts[3]) {
		return "", "", false
	}
	if allZero(trace) || allZero(span) {
		return "", "", false // 全零拒绝
	}
	return trace, span, true
}

// DeriveTraceparent 从父上下文派生本 Turn 的 span（同 traceId、新 spanId、
// flags 01）；父非法或为空 → ""（静默降级为新 trace）。
func DeriveTraceparent(parent string) string {
	traceID, _, ok := parseTraceparent(parent)
	if !ok {
		return ""
	}
	span := make([]byte, 8)
	if _, err := rand.Read(span); err != nil {
		return ""
	}
	return "00-" + traceID + "-" + hex.EncodeToString(span) + "-01"
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return len(s) > 0
}

func allZero(s string) bool {
	return strings.Trim(s, "0") == ""
}

// TraceEvent Turn 边界 trace 事件（outcome: completed|failed|expired）。
// Traceparent 为本 Turn 自己的 W3C span（Stage 由调用方传入的父上下文派生）；
// ParentTraceparent 为调用方传入并校验后的上下文（空表示新 trace）。
// 经 Stage.OnTrace 交付——核心零 OTel 依赖，OTel 导出适配在 M6。
type TraceEvent struct {
	CallID            uint64
	ActorID           ActorID
	Method            string
	Duration          time.Duration
	OK                bool
	Outcome           string
	Traceparent       string
	ParentTraceparent string
	At                time.Time
}

// emitTrace 经 Stage 分发（slot 回调）。
func (s *Stage) emitTrace(ev TraceEvent) {
	if s.traceHandler != nil {
		s.traceHandler(ev)
	}
}
