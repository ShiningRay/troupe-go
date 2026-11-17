package troupe

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// callEnvelope 排入 CallBoard 的一次调用请求。reply 为容量 1 的通道：
// 每个请求恰好收到一次回复（调用方放弃等待不影响投递）。
type callEnvelope struct {
	callID      uint64
	method      string
	args        json.RawMessage
	deadline    time.Time
	reply       chan callReply
	bytes       int
	cancel      *CancelSignal
	traceparent string
}

type callReply struct {
	value     json.RawMessage
	err       error
	settledAt time.Time
}

type callOptions struct {
	deadline    time.Time
	timeout     time.Duration
	traceparent string
}

// CallOption 定制单次调用：WithDeadline（绝对时刻）/ WithTimeout（相对时长）
// 同时给出时 deadline 优先；两者皆缺省用 StageConfig.DefaultCallDeadline。
type CallOption func(*callOptions)

func WithDeadline(t time.Time) CallOption { return func(o *callOptions) { o.deadline = t } }

func WithTimeout(d time.Duration) CallOption { return func(o *callOptions) { o.timeout = d } }

func WithTraceparent(tp string) CallOption { return func(o *callOptions) { o.traceparent = tp } }

// CallFuture 调用方持有的 Future。
//
// Live 模式：Await 阻塞至回复或 deadline；
// Manual 模式（Rehearsal）：pump 同步执行后回复已在通道内，Await 直接取回
// （未 pump 将阻塞直至 pump 或 ctx 取消——确定性调度由测试负责）。
// 回复通道容量 1：Future 缓存已取回复，Settled/Cancel 在 Await 之后语义仍正确。
type CallFuture struct {
	req    *callEnvelope
	clock  Clock
	manual bool

	mu   sync.Mutex
	got  *callReply
	done bool
}

func newCallFuture(req *callEnvelope, clock Clock, manual bool) *CallFuture {
	return &CallFuture{req: req, clock: clock, manual: manual}
}

// Method 返回本调用的方法名。
func (f *CallFuture) Method() string { return f.req.method }

// Settled 返回回复是否已抵达（无论成功失败）。
func (f *CallFuture) Settled() bool {
	f.tryTake()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got != nil
}

// Cancel 请求协作取消：仅置位 CancelSignal，Turn 经 CallContext.CancelRequested
// 观察并自行决定何时/如何收束（可提前 settle 提交部分状态）。不可强行终止；
// 已 settle 的调用无效果，返回 false。
func (f *CallFuture) Cancel() bool {
	if f.Settled() {
		return false
	}
	f.req.cancel.Request()
	return true
}

// Await 等待结果；错误原样返回（WireError 与业务错误）。
//
// 语义（执行占用，DESIGN §5.1）：
//   - reply 已抵达且 settle 时间在 deadline 内 → 正常结果；
//   - 从未开始执行（clock.Now >= deadline 且无回复）→ CallTimeoutError{queued}，
//     零副作用、可安全重试；
//   - 等待超过 deadline → CallTimeoutError{executing}：Turn 可能已开始执行，
//     结果未知——迟到的真实结果不会被放行（即使之后成功也按未知处理）；
//   - ctx 取消/超时 → UnknownOutcomeError：调用方放弃等待，Turn 可能仍会执行
//     并提交效果。
func (f *CallFuture) Await(ctx context.Context) (json.RawMessage, error) {
	if reply := f.tryTake(); reply != nil {
		return f.settle(reply)
	}
	now := f.clock.Now()
	if !now.Before(f.req.deadline) {
		// 从未开始执行：排队过期，无副作用、可安全重试。
		return nil, &CallTimeoutError{Phase: PhaseQueued, Method: f.req.method}
	}
	if f.manual {
		// Rehearsal：等待 pump 发回回复（测试先 pump 再 await）。
		// ctx 是唯一的逃逸口，避免确定性测试意外挂死。
		select {
		case reply := <-f.req.reply:
			f.cache(reply)
			return f.settle(&reply)
		case <-ctx.Done():
			return nil, &UnknownOutcomeError{Detail: "caller context cancelled before settle: " + ctx.Err().Error()}
		}
	}
	remaining := f.req.deadline.Sub(now)
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case reply := <-f.req.reply:
		f.cache(reply)
		return f.settle(&reply)
	case <-timer.C:
		return nil, &CallTimeoutError{Phase: PhaseExecuting, Method: f.req.method}
	case <-ctx.Done():
		return nil, &UnknownOutcomeError{Detail: "caller context cancelled before settle: " + ctx.Err().Error()}
	}
}

func (f *CallFuture) tryTake() *callReply {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.got != nil {
		return f.got
	}
	select {
	case reply := <-f.req.reply:
		f.got = &reply
		return f.got
	default:
		return nil
	}
}

func (f *CallFuture) cache(reply callReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = &reply
}

func (f *CallFuture) settle(reply *callReply) (json.RawMessage, error) {
	// 错误回复原样返回——queued 过期自身已说明未执行、无副作用，
	// 不能被迟到的 settle 时间掩盖成 executing。
	if reply.err != nil {
		return nil, reply.err
	}
	if reply.settledAt.After(f.req.deadline) {
		// Turn 实际执行成功但 settle 晚于 deadline：调用方按结果未知处理
		// （执行占用语义）。
		return nil, &CallTimeoutError{Phase: PhaseExecuting, Method: f.req.method}
	}
	return reply.value, nil
}
