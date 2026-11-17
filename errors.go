package troupe

// 错误分类码表（troupe-errors/1，由 golden/errors.json 向量锚定）。
//
// 跨线错误以稳定码 + 结构化字段表达（非类型名）；业务异常不映射码表，
// 降级为 message 透传（接收方按字符串处理）。AUTH/PROTOCOL 仅用于 transport
// 层（M3+）。InvalidActorIDError 为本地校验错误，不进入跨线码表。
const (
	CodeConflict            = "CONFLICT"
	CodeCallTimeout         = "CALL_TIMEOUT"
	CodeCallRejected        = "CALL_REJECTED"
	CodeUnknownOutcome      = "UNKNOWN_OUTCOME"
	CodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	CodeUnknownRole         = "UNKNOWN_ROLE"
	CodeInvalidArgument     = "INVALID_ARGUMENT"
	CodeActivation          = "ACTIVATION"
	CodeInvalidActorState   = "INVALID_ACTOR_STATE"
	CodeStaleIncarnation    = "STALE_INCARNATION"
	CodeAuth                = "AUTH"     // transport only
	CodeProtocol            = "PROTOCOL" // transport only
)

// WireError 是可跨线编码的框架错误；WireCode 返回 golden/errors.json 码表中的稳定码。
type WireError interface {
	error
	WireCode() string
}

// ConflictError 表示 CAS 冲突：Kind 为 "exists"（创建时已存在）或 "revision"（快照过期）。
type ConflictError struct {
	Kind string // "exists" | "revision"
}

func (e *ConflictError) Error() string    { return "conflict: " + e.Kind }
func (e *ConflictError) WireCode() string { return CodeConflict }

// CallTimeoutError 表示调用期限到期：Phase 为 "queued"（排队中过期，零副作用）
// 或 "executing"（执行中过期，结果未知）。
type CallTimeoutError struct {
	Phase string // "queued" | "executing"
}

func (e *CallTimeoutError) Error() string    { return "call timeout: " + e.Phase }
func (e *CallTimeoutError) WireCode() string { return CodeCallTimeout }

// CallRejectedError 表示入队/执行前显式拒绝；Reason 分类见 golden/errors.json，
// Retryable 标注调用方是否可安全重试（如 backpressure 可重试，whitelist 不可）。
type CallRejectedError struct {
	Reason    string // backpressure|whitelist|shutdown|draining|ownership|invalidated
	Retryable bool
}

func (e *CallRejectedError) Error() string    { return "call rejected: " + e.Reason }
func (e *CallRejectedError) WireCode() string { return CodeCallRejected }

// UnknownOutcomeError 表示结果未知（超时/断连后原 Turn 可能仍在执行并提交效果）；
// 重试必须依赖业务幂等。
type UnknownOutcomeError struct {
	Detail string
}

func (e *UnknownOutcomeError) Error() string    { return "unknown outcome: " + e.Detail }
func (e *UnknownOutcomeError) WireCode() string { return CodeUnknownOutcome }

// IdempotencyConflictError 表示同幂等键携带不同指纹的重复请求。
type IdempotencyConflictError struct{ Detail string }

func (e *IdempotencyConflictError) Error() string    { return "idempotency conflict: " + e.Detail }
func (e *IdempotencyConflictError) WireCode() string { return CodeIdempotencyConflict }

// UnknownRoleError 表示目标角色未注册。
type UnknownRoleError struct{ Role string }

func (e *UnknownRoleError) Error() string    { return "unknown role: " + e.Role }
func (e *UnknownRoleError) WireCode() string { return CodeUnknownRole }

// InvalidArgumentError 表示排队前参数校验失败（缺参/类型不符，零副作用）。
type InvalidArgumentError struct{ Detail string }

func (e *InvalidArgumentError) Error() string    { return "invalid argument: " + e.Detail }
func (e *InvalidArgumentError) WireCode() string { return CodeInvalidArgument }

// ActivationError 表示激活失败（存储读、迁移或所有权获取失败）。
type ActivationError struct{ Detail string }

func (e *ActivationError) Error() string    { return "activation: " + e.Detail }
func (e *ActivationError) WireCode() string { return CodeActivation }

// InvalidActorStateError 表示生命周期/状态约束被违反（如 read_only Turn 调用 Save）。
type InvalidActorStateError struct{ Detail string }

func (e *InvalidActorStateError) Error() string    { return "invalid actor state: " + e.Detail }
func (e *InvalidActorStateError) WireCode() string { return CodeInvalidActorState }

// StaleIncarnationError 表示成员视图 incarnation 协商失败（旧/同 incarnation 重入团）。
type StaleIncarnationError struct{ Detail string }

func (e *StaleIncarnationError) Error() string    { return "stale incarnation: " + e.Detail }
func (e *StaleIncarnationError) WireCode() string { return CodeStaleIncarnation }

// AuthError 表示 transport 认证失败（仅 transport 层，M3+）。
type AuthError struct{ Detail string }

func (e *AuthError) Error() string    { return "auth: " + e.Detail }
func (e *AuthError) WireCode() string { return CodeAuth }

// ProtocolError 表示帧/信封协议违规（仅 transport 层，M3+）。
type ProtocolError struct{ Detail string }

func (e *ProtocolError) Error() string    { return "protocol: " + e.Detail }
func (e *ProtocolError) WireCode() string { return CodeProtocol }

// InvalidActorIDError 是本地校验错误（空组件/控制字符/规范形式不合法），
// 不在 troupe-errors/1 跨线码表内。
type InvalidActorIDError struct{ Detail string }

func (e *InvalidActorIDError) Error() string { return "invalid actor id: " + e.Detail }

// wireErrorSamples 返回每个跨线码的一个样本实例，供 golden 契约测试断言
// 码表集合与 golden/errors.json 一致（无多码、无缺码）。
func wireErrorSamples() map[string]WireError {
	return map[string]WireError{
		CodeConflict:            &ConflictError{Kind: "revision"},
		CodeCallTimeout:         &CallTimeoutError{Phase: "executing"},
		CodeCallRejected:        &CallRejectedError{Reason: "backpressure", Retryable: true},
		CodeUnknownOutcome:      &UnknownOutcomeError{Detail: "deadline"},
		CodeIdempotencyConflict: &IdempotencyConflictError{Detail: "fingerprint"},
		CodeUnknownRole:         &UnknownRoleError{Role: "cart"},
		CodeInvalidArgument:     &InvalidArgumentError{Detail: "missing"},
		CodeActivation:          &ActivationError{Detail: "store"},
		CodeInvalidActorState:   &InvalidActorStateError{Detail: "save"},
		CodeStaleIncarnation:    &StaleIncarnationError{Detail: "roster"},
		CodeAuth:                &AuthError{Detail: "token"},
		CodeProtocol:            &ProtocolError{Detail: "frame"},
	}
}
