package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	troupe "github.com/ShiningRay/troupe-go"
)

// TCPServer 包既有 Stage：白名单/串行/背压/超时/取消语义与本地一致
// （复用 Stage 管道，不新增语义）。连接级 in-flight 上限；首帧认证（可选配置）；
// 断开连接的响应不可达——调用方按 UnknownOutcome 处理（执行占用语义不变）。
type TCPServer struct {
	stage       *troupe.Stage
	auth        *Authenticator
	maxFrame    int
	maxInflight int

	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]*serverConn
	closed   bool
}

// ServerOption 服务端配置。
type ServerOption func(*TCPServer)

// WithServerAuth 启用首帧 token 认证（未配置则不要求认证）。
func WithServerAuth(auth *Authenticator) ServerOption { return func(s *TCPServer) { s.auth = auth } }

// WithMaxFrame 覆盖帧长上限（默认 1 MiB）。
func WithMaxFrame(n int) ServerOption { return func(s *TCPServer) { s.maxFrame = n } }

// WithMaxInflight 覆盖连接级 in-flight 上限（默认 128）。
func WithMaxInflight(n int) ServerOption { return func(s *TCPServer) { s.maxInflight = n } }

// NewTCPServer 创建服务端（未监听；调用 Listen）。
func NewTCPServer(stage *troupe.Stage, opts ...ServerOption) *TCPServer {
	s := &TCPServer{
		stage:       stage,
		maxFrame:    DefaultMaxFrame,
		maxInflight: 128,
		conns:       make(map[net.Conn]*serverConn),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Listen 监听地址（":0" 由系统分配；Addr() 取实际地址）。
func (s *TCPServer) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("transport 监听失败：%w", err)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		l.Close()
		return errors.New("transport 已关闭，拒绝监听")
	}
	s.listener = l
	s.mu.Unlock()
	go s.acceptLoop(l)
	return nil
}

// Addr 返回监听地址（Listen 后有效）。
func (s *TCPServer) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close 停止监听并断开全部连接（Stage 生命周期由宿主管理）。
func (s *TCPServer) Close() {
	s.mu.Lock()
	s.closed = true
	l := s.listener
	conns := make([]*serverConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if l != nil {
		l.Close()
	}
	for _, c := range conns {
		c.conn.Close()
	}
}

func (s *TCPServer) acceptLoop(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			continue // 瞬时 accept 错误：继续接受
		}
		sc := &serverConn{
			srv:     s,
			conn:    conn,
			pending: make(map[int64]*pendingCall),
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[conn] = sc
		s.mu.Unlock()
		go sc.serve()
	}
}

type pendingCall struct {
	env    Envelope
	reply  chan Envelope // cap 1
	future *troupe.CallFuture
}

type serverConn struct {
	srv     *TCPServer
	conn    net.Conn
	writeMu sync.Mutex
	authed  bool

	mu      sync.Mutex
	pending map[int64]*pendingCall
	closed  bool
}

func (c *serverConn) send(env Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return WriteFrame(c.conn, payload)
}

func (c *serverConn) close() {
	c.mu.Lock()
	already := c.closed
	c.closed = true
	c.mu.Unlock()
	if !already {
		c.conn.Close()
	}
	c.srv.mu.Lock()
	delete(c.srv.conns, c.conn)
	c.srv.mu.Unlock()
}

func (c *serverConn) serve() {
	defer c.close()
	for {
		payload, err := ReadFrame(c.conn, c.srv.maxFrame)
		if err != nil {
			return // EOF/协议违规：断开（在途调用结果未知）
		}
		var env Envelope
		if err := json.Unmarshal(payload, &env); err != nil {
			return // 信封不可解析：断开
		}
		switch env.T {
		case "auth":
			if !c.handleAuth(env) {
				return
			}
		case "req":
			if !c.handleReq(env) {
				return
			}
		case "cancel":
			c.handleCancel(env)
		default:
			return // "rep" 等服务端不应收到：断开
		}
	}
}

func (c *serverConn) handleAuth(env Envelope) bool {
	if c.srv.auth == nil {
		return true // 未启用认证：忽略 auth 帧
	}
	if c.authed {
		return true
	}
	if !c.srv.auth.Verify(env.TK) {
		_ = c.send(Envelope{T: "rep", ID: env.ID, EM: "auth failed", EC: troupe.CodeAuth})
		return false // 未认证断连
	}
	c.mu.Lock()
	c.authed = true
	c.mu.Unlock()
	ack := AuthAck()
	ack.ID = env.ID
	return c.send(ack) == nil
}

func (c *serverConn) handleReq(env Envelope) bool {
	// 认证门禁：启用认证时未认证即发业务信封 → 断连。
	if c.srv.auth != nil {
		c.mu.Lock()
		authed := c.authed
		c.mu.Unlock()
		if !authed {
			return false
		}
	}
	// 连接级 in-flight 上限（背压显式拒绝，可重试）。
	c.mu.Lock()
	if len(c.pending) >= c.srv.maxInflight {
		c.mu.Unlock()
		ok := false
		retryable := true
		_ = c.send(Envelope{
			T: "rep", ID: env.ID, OK: &ok,
			EC: troupe.CodeCallRejected, EM: fmt.Sprintf("连接 in-flight %d 达上限 %d", len(c.pending), c.srv.maxInflight),
			EK: troupe.RejectBackpressure, ERT: &retryable,
		})
		return true
	}
	pc := &pendingCall{env: env, reply: make(chan Envelope, 1)}
	c.pending[env.ID] = pc
	c.mu.Unlock()

	go c.serveCall(pc)
	return true
}

func (c *serverConn) serveCall(pc *pendingCall) {
	defer func() {
		c.mu.Lock()
		delete(c.pending, pc.env.ID)
		c.mu.Unlock()
	}()

	id, err := troupe.ParseActorID(pc.env.Actor)
	if err != nil {
		c.replyError(pc, err)
		return
	}
	opts := []troupe.CallOption{}
	if pc.env.DL > 0 {
		opts = append(opts, troupe.WithDeadline(time.UnixMilli(pc.env.DL)))
	}
	if pc.env.TP != "" {
		opts = append(opts, troupe.WithTraceparent(pc.env.TP))
	}
	future, err := c.srv.stage.Call(context.Background(), id, pc.env.M, pc.env.A, opts...)
	if err != nil {
		c.replyError(pc, err)
		return
	}

	// cancel 信封：注册协作取消（未来取消查这张表）。
	c.mu.Lock()
	c.pending[pc.env.ID].future = future
	c.mu.Unlock()

	raw, err := future.Await(context.Background()) // 服务端等 settle（执行占用语义）
	if err != nil {
		// 连接已断开时回复不可达（send 返回错误即忽略）——效果已提交与否按调用方 UnknownOutcome 处理。
		c.replyError(pc, err)
		return
	}
	ok := true
	_ = c.send(Envelope{T: "rep", ID: pc.env.ID, OK: &ok, V: raw})
}

func (c *serverConn) replyError(pc *pendingCall, err error) {
	ok := false
	ec, em, ek, ert := ErrorFields(err)
	env := Envelope{T: "rep", ID: pc.env.ID, OK: &ok, EC: ec, EM: em, EK: ek, ERT: ert}
	if err := c.send(env); err != nil {
		_ = err // 连接断开：调用方按 UnknownOutcome 处理
	}
}

func (c *serverConn) handleCancel(env Envelope) {
	c.mu.Lock()
	pc := c.pending[env.ID]
	var f *troupe.CallFuture
	if pc != nil {
		f = pc.future
	}
	c.mu.Unlock()
	if f != nil {
		f.Cancel() // 协作取消：Turn 经 CancelRequested 观察并自行收束
	}
}

// TCPClient 连接级多路复用客户端：call 按 id 分发回复；断连/协议违规 →
// 在途调用按 UnknownOutcomeError 处理（勿假定失败，按幂等键重试）。
type TCPClient struct {
	addr     string
	token    string
	maxFrame int
	dialTo   time.Duration

	mu       sync.Mutex
	conn     net.Conn
	writeMu  sync.Mutex
	pending  map[int64]*pendingClientCall
	nextID   int64
	closed   bool
	authAck  chan Envelope // auth ack 路由（id=0；readLoop 分发）
	waitAuth bool
}

// ClientOption 客户端配置。
type ClientOption func(*TCPClient)

// WithToken 客户端认证 token（首帧 auth）。
func WithToken(token string) ClientOption { return func(c *TCPClient) { c.token = token } }

// WithClientMaxFrame 覆盖帧长上限。
func WithClientMaxFrame(n int) ClientOption { return func(c *TCPClient) { c.maxFrame = n } }

// WithDialTimeout 覆盖拨号超时（默认 5s）。
func WithDialTimeout(d time.Duration) ClientOption { return func(c *TCPClient) { c.dialTo = d } }

// NewTCPClient 创建客户端（首次 Call 时惰性拨号）。
func NewTCPClient(addr string, opts ...ClientOption) *TCPClient {
	c := &TCPClient{
		addr:     addr,
		maxFrame: DefaultMaxFrame,
		dialTo:   5 * time.Second,
		pending:  make(map[int64]*pendingClientCall),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type pendingClientCall struct {
	reply chan Envelope // cap 1
}

func (c *TCPClient) ensureConn(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &troupe.CallRejectedError{Reason: troupe.RejectShutdown, Detail: "客户端已关闭"}
	}
	if c.conn != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	dialer := net.Dialer{Timeout: c.dialTo}
	conn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("transport 拨号失败：%w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	go c.readLoop(conn)

	if c.token != "" {
		if err := c.handshake(conn); err != nil {
			conn.Close()
			c.mu.Lock()
			c.conn = nil
			c.mu.Unlock()
			return err
		}
	}
	return nil
}

func (c *TCPClient) handshake(conn net.Conn) error {
	// auth ack 经 readLoop 按 id=0 路由到 authAck 通道（避免与 readLoop 抢读同一连接）。
	c.mu.Lock()
	c.authAck = make(chan Envelope, 1)
	c.waitAuth = true
	ackCh := c.authAck
	c.mu.Unlock()

	authEnv := Envelope{T: "auth", ID: 0, TK: c.token}
	payload, _ := json.Marshal(authEnv)
	c.writeMu.Lock()
	err := WriteFrame(conn, payload)
	c.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("auth 发送失败：%w", err)
	}
	select {
	case env := <-ackCh:
		if env.OK == nil || !*env.OK {
			return fmt.Errorf("auth 失败：%w", ErrAuth)
		}
		return nil
	case <-time.After(3 * time.Second):
		return errors.New("auth ack 超时（3s）")
	}
}

func (c *TCPClient) readLoop(conn net.Conn) {
	for {
		payload, err := ReadFrame(conn, c.maxFrame)
		if err != nil {
			c.failAll("连接断开")
			return
		}
		var env Envelope
		if err := json.Unmarshal(payload, &env); err != nil {
			c.failAll("信封不可解析")
			return
		}
		if env.ID == 0 {
			// auth ack 路由
			c.mu.Lock()
			waiting := c.waitAuth
			ackCh := c.authAck
			if waiting {
				c.waitAuth = false
			}
			c.mu.Unlock()
			if waiting && ackCh != nil {
				ackCh <- env
			}
			continue
		}
		c.mu.Lock()
		pc := c.pending[env.ID]
		c.mu.Unlock()
		if pc != nil {
			select {
			case pc.reply <- env:
			default:
			}
		}
	}
}

func (c *TCPClient) failAll(cause string) {
	c.mu.Lock()
	pending := c.pending
	c.pending = make(map[int64]*pendingClientCall)
	c.conn = nil
	c.mu.Unlock()
	for _, pc := range pending {
		env := Envelope{
			T:  "rep",
			OK: boolPtr(false),
			EC: troupe.CodeUnknownOutcome,
			EM: cause + "：在途调用结果未知，勿假定失败，按幂等键重试",
		}
		select {
		case pc.reply <- env:
		default:
		}
	}
}

func boolPtr(b bool) *bool { return &b }

// CallFuture 客户端侧 Future：Await 语义与服务端对齐
// （deadline 到期 = executing 结果未知；断连 = UnknownOutcome）。
type CallFuture struct {
	client *TCPClient
	env    Envelope
	reply  chan Envelope
}

// Await 等待结果：错误分类保持（WireError 重建）；deadline 到期（可能已执行）
// → CallTimeoutError{executing}；调用方 ctx 取消 → UnknownOutcomeError。
func (f *CallFuture) Await(ctx context.Context) (json.RawMessage, error) {
	remaining := 30 * time.Second
	if f.env.DL > 0 {
		remaining = time.Until(time.UnixMilli(f.env.DL))
		if remaining < 0 {
			remaining = 0
		}
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case env := <-f.reply:
		if env.OK != nil && *env.OK {
			return env.V, nil
		}
		return nil, ErrorFrom(env.EC, env.EM, env.EK, env.ERT)
	case <-timer.C:
		return nil, &troupe.CallTimeoutError{Phase: troupe.PhaseExecuting, Method: f.env.M}
	case <-ctx.Done():
		return nil, &troupe.UnknownOutcomeError{Detail: "caller context cancelled before settle: " + ctx.Err().Error()}
	}
}

// Cancel 发送协作取消信封（Turn 经 CancelRequested 观察并自行收束；不保证终止）。
func (f *CallFuture) Cancel() error {
	return f.client.sendCancel(f.env.ID)
}

func (c *TCPClient) sendCancel(callID int64) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errors.New("客户端未连接")
	}
	payload, _ := json.Marshal(Envelope{T: "cancel", ID: callID})
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return WriteFrame(conn, payload)
}

// Call 发起远程调用（位置透明；错误分类与本地一致）。
func (c *TCPClient) Call(ctx context.Context, actorID troupe.ActorID, method string, args any, deadline time.Time) (*CallFuture, error) {
	if err := c.ensureConn(ctx); err != nil {
		return nil, err
	}
	rawArgs := json.RawMessage("null")
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return nil, &troupe.InvalidArgumentError{Detail: "参数序列化失败：" + err.Error()}
		}
		rawArgs = b
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, &troupe.CallRejectedError{Reason: troupe.RejectShutdown, Detail: "客户端已关闭"}
	}
	c.nextID++
	id := c.nextID
	env := Envelope{
		T:     "req",
		ID:    id,
		Actor: actorID.Canonical(),
		M:     method,
		A:     rawArgs,
		DL:    deadline.UnixMilli(),
	}
	pc := &pendingClientCall{reply: make(chan Envelope, 1)}
	c.pending[id] = pc
	c.mu.Unlock()

	payload, _ := json.Marshal(env)
	c.writeMu.Lock()
	err := WriteFrame(c.conn, payload)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, &troupe.UnknownOutcomeError{Detail: "发送失败，结果未知：" + err.Error()}
	}
	return &CallFuture{client: c, env: env, reply: pc.reply}, nil
}

// Close 断开连接（在途调用经 failAll 得 UnknownOutcome）。
func (c *TCPClient) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	c.failAll("客户端已关闭")
}
