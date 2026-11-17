package transport

import (
	"encoding/json"
	"errors"
	"fmt"

	troupe "github.com/ShiningRay/troupe-go"
)

// Envelope 请求/回复/取消/认证共用信封（troupe-envelope/1，golden/envelope.json）。
// 字段全部 omitempty 按类型取用；ec 为 troupe-errors/1 稳定码（ADR 0001 提案，
// troupe.cr 当前实现为 Crystal 类名——本仓按 ADR 稳定码表达，跨语言对齐以
// golden/errors.json 码表为准）。
type Envelope struct {
	T     string          `json:"t"`               // "req" | "rep" | "cancel" | "auth"
	ID    int64           `json:"id"`              // call_id（连接内多路分发）
	Actor string          `json:"actor,omitempty"` // 请求：ActorId 规范形式
	M     string          `json:"m,omitempty"`     // 请求：方法名
	A     json.RawMessage `json:"a,omitempty"`     // 请求：参数
	DL    int64           `json:"dl,omitempty"`    // 请求：deadline（Unix 毫秒）
	TP    string          `json:"tp,omitempty"`    // 请求：调用方 W3C traceparent
	TK    string          `json:"tk,omitempty"`    // 认证：token（仅 auth 帧携带）
	OK    *bool           `json:"ok,omitempty"`    // 回复：是否成功
	V     json.RawMessage `json:"v,omitempty"`     // 回复：结果
	EC    string          `json:"ec,omitempty"`    // 回复：错误稳定码（troupe-errors/1）
	EM    string          `json:"em,omitempty"`    // 回复：错误消息
	EK    string          `json:"ek,omitempty"`    // 回复：ConflictKind / TimeoutPhase / RejectReason
	ERT   *bool           `json:"ert,omitempty"`   // 回复：CallRejectedError#Retryable
}

// ProtocolVersion 信封协议版本（auth ack 的 v 字段；版本协商升级点）。
const ProtocolVersion = "troupe-envelope/1"

// AuthAck 构造认证确认（id=0，ok=true，v=协议版本）。
func AuthAck() Envelope {
	ok := true
	return Envelope{T: "rep", ID: 0, OK: &ok, V: json.RawMessage(`"` + ProtocolVersion + `"`)}
}

// ErrorFields 服务端错误 → 信封错误字段：WireError 按稳定码 + 结构化字段过线；
// 业务异常降级为 "EXCEPTION" + message 透传（接收方按字符串处理）。
func ErrorFields(err error) (ec, em, ek string, ert *bool) {
	em = err.Error()
	var we troupe.WireError
	if errors.As(err, &we) {
		ec = we.WireCode()
	}
	switch e := err.(type) {
	case *troupe.ConflictError:
		ek = e.Kind
	case *troupe.CallTimeoutError:
		ek = e.Phase
	case *troupe.CallRejectedError:
		ek = e.Reason
		ert = &e.Retryable
	}
	if ec == "" {
		ec = "EXCEPTION"
		em = fmt.Sprintf("%T: %s", err, err.Error())
	}
	return ec, em, ek, ert
}

// ErrorFrom 信封错误字段 → 客户端重建错误（分类保持；EXCEPTION/未知码降级为普通 error）。
func ErrorFrom(ec, em, ek string, ert *bool) error {
	switch ec {
	case troupe.CodeConflict:
		kind := ek
		if kind == "" {
			kind = troupe.ConflictRevision
		}
		return &troupe.ConflictError{Kind: kind}
	case troupe.CodeCallTimeout:
		phase := ek
		if phase == "" {
			phase = troupe.PhaseExecuting
		}
		return &troupe.CallTimeoutError{Phase: phase, Method: em}
	case troupe.CodeCallRejected:
		reason := ek
		if reason == "" {
			reason = troupe.RejectBackpressure
		}
		retryable := ert != nil && *ert
		return &troupe.CallRejectedError{Reason: reason, Retryable: retryable, Detail: em}
	case troupe.CodeUnknownOutcome:
		return &troupe.UnknownOutcomeError{Detail: em}
	case troupe.CodeIdempotencyConflict:
		return &troupe.IdempotencyConflictError{Detail: em}
	case troupe.CodeUnknownRole:
		return &troupe.UnknownRoleError{Role: em}
	case troupe.CodeInvalidArgument:
		return &troupe.InvalidArgumentError{Detail: em}
	case troupe.CodeActivation:
		return &troupe.ActivationError{Detail: em}
	case troupe.CodeInvalidActorState:
		return &troupe.InvalidActorStateError{Detail: em}
	case troupe.CodeStaleIncarnation:
		return &troupe.StaleIncarnationError{Detail: em}
	default:
		// EXCEPTION 与未知码：消息保留、降级为普通 error
		if em == "" {
			return errors.New("remote error " + ec)
		}
		return errors.New("remote error " + ec + ": " + em)
	}
}
