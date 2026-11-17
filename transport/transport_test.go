package transport

// TCP transport 测试：帧/golden 锚点、认证轮换、错误过线分类、
// deadline 执行占用、cancel 信封、断连 UnknownOutcome、in-flight 上限。

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	troupe "github.com/ShiningRay/troupego"
)

// ---- 帧协议与 golden 锚点 ----

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte(`{"t":"req","id":7,"actor":"app/order/1"}`)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame 失败：%v", err)
	}
	raw := append([]byte(nil), buf.Bytes()...) // 快照后再读（ReadFrame 会消费 reader）
	got, err := ReadFrame(bytes.NewReader(raw), DefaultMaxFrame)
	if err != nil {
		t.Fatalf("ReadFrame 失败：%v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("往返不一致：%s", got)
	}
	// 头部结构：magic/length/crc32 LE
	if binary.LittleEndian.Uint32(raw[0:4]) != Magic {
		t.Fatal("magic 不符")
	}
	if binary.LittleEndian.Uint32(raw[4:8]) != uint32(len(payload)) {
		t.Fatal("length 不符")
	}
}

func TestFrameAgainstGoldenEnvelope(t *testing.T) {
	// golden/envelope.json 的 sample_request 可被本帧协议承载（troupe-envelope/1 锚点）。
	data, err := os.ReadFile("../golden/envelope.json")
	if err != nil {
		t.Fatalf("读取 golden 失败：%v", err)
	}
	var fx struct {
		Protocol      string            `json:"protocol"`
		Frame         string            `json:"frame"`
		SampleRequest []json.RawMessage `json:"sample_request"`
	}
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatal(err)
	}
	if fx.Protocol != "troupe-envelope/1" {
		t.Fatalf("协议 = %s", fx.Protocol)
	}
	for _, sample := range fx.SampleRequest {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, sample); err != nil {
			t.Fatalf("golden 帧写入失败：%v", err)
		}
		got, err := ReadFrame(&buf, DefaultMaxFrame)
		if err != nil {
			t.Fatalf("golden 帧读取失败：%v", err)
		}
		if !bytes.Equal(got, sample) {
			t.Fatalf("golden 往返不一致：%s", got)
		}
		var env Envelope
		if err := json.Unmarshal(got, &env); err != nil {
			t.Fatalf("golden 信封解析失败：%v", err)
		}
		if env.T != "req" || env.ID != 7 || env.Actor != "app/order/1" || env.M != "increment" {
			t.Fatalf("golden 信封字段 = %+v", env)
		}
	}
}

func TestFrameProtocolViolations(t *testing.T) {
	// CRC 损坏
	var buf bytes.Buffer
	payload := []byte(`{"t":"req","id":1}`)
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	raw[len(raw)-1] ^= 0xFF // 破坏 payload 最后字节
	if _, err := ReadFrame(bytes.NewReader(raw), DefaultMaxFrame); !errors.Is(err, ErrProtocol) {
		t.Fatalf("CRC 损坏错误 = %v, want ErrProtocol", err)
	}

	// magic 不符
	buf.Reset()
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatal(err)
	}
	raw = buf.Bytes()
	binary.LittleEndian.PutUint32(raw[0:4], 0xDEADBEEF)
	if _, err := ReadFrame(bytes.NewReader(raw), DefaultMaxFrame); !errors.Is(err, ErrProtocol) {
		t.Fatalf("magic 错误 = %v, want ErrProtocol", err)
	}

	// 帧长超上限：只写头部即被拒（超限不分配内存）
	header := make([]byte, HeaderBytes)
	binary.LittleEndian.PutUint32(header[0:4], Magic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(DefaultMaxFrame)+1)
	binary.LittleEndian.PutUint32(header[8:12], 0)
	if _, err := ReadFrame(bytes.NewReader(header), DefaultMaxFrame); !errors.Is(err, ErrProtocol) {
		t.Fatalf("超限错误 = %v, want ErrProtocol", err)
	}
}

func TestAuthenticatorRotation(t *testing.T) {
	auth, err := NewAuthenticator("token-a")
	if err != nil {
		t.Fatal(err)
	}
	if !auth.Verify("token-a") || auth.Verify("token-b") {
		t.Fatal("初始验证不符")
	}
	if err := auth.Rotate("token-b"); err != nil {
		t.Fatal(err)
	}
	// 轮换宽限：新旧均可验证
	if !auth.Verify("token-a") || !auth.Verify("token-b") {
		t.Fatal("轮换宽限不符")
	}
	auth.RevokeOld()
	if auth.Verify("token-a") {
		t.Fatal("撤销后旧 token 应失效")
	}
	if !auth.Verify("token-b") {
		t.Fatal("新 token 应有效")
	}
	// 常量时间比较：不同长度返回 false 不 panic
	if SecureEquals("short", "a-much-longer-token") {
		t.Fatal("不同长度不应相等")
	}
}

// ---- 服务端/客户端组装 ----

type cartProps struct {
	Items []string `json:"items"`
}

func cartStage(t *testing.T) (*troupe.Stage, troupe.ActorID) {
	t.Helper()
	stage := troupe.NewStage()
	m := troupe.Define("cart", func(id troupe.ActorID, props cartProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[cartProps] } {
		return &struct{ *troupe.Actor[cartProps] }{troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(troupe.Turn("add", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}
	return stage, troupe.MustActorID("app", "cart", "t1")
}

func startServer(t *testing.T, stage *troupe.Stage, opts ...ServerOption) *TCPServer {
	t.Helper()
	srv := NewTCPServer(stage, opts...)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("监听失败：%v", err)
	}
	t.Cleanup(srv.Close)
	return srv
}

func TestE2ECallReply(t *testing.T) {
	stage, id := cartStage(t)
	srv := startServer(t, stage)
	client := NewTCPClient(srv.Addr().String())
	defer client.Close()
	ctxbg := context.Background()

	n, err := InvokeRemote[struct{ Item string }, int](ctxbg, client, id, "add", struct{ Item string }{Item: "pear"}, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("远程 add 失败：%v", err)
	}
	if n != 1 {
		t.Fatalf("add = %d, want 1", n)
	}
	n, err = InvokeRemote[struct{ Item string }, int](ctxbg, client, id, "add", struct{ Item string }{Item: "pear"}, time.Now().Add(5*time.Second))
	if err != nil || n != 2 {
		t.Fatalf("第二次 add = %d, %v", n, err)
	}
}

// InvokeRemote 泛型远程调用（结果经 JSON 反序列化）。
func InvokeRemote[Args, Res any](ctx context.Context, c *TCPClient, id troupe.ActorID, method string, args Args, deadline time.Time) (Res, error) {
	var zero Res
	f, err := c.Call(ctx, id, method, args, deadline)
	if err != nil {
		return zero, err
	}
	raw, err := f.Await(ctx)
	if err != nil {
		return zero, err
	}
	var res Res
	if err := json.Unmarshal(raw, &res); err != nil {
		return zero, err
	}
	return res, nil
}

func TestErrorClassificationOverWire(t *testing.T) {
	stage, _ := cartStage(t)
	srv := startServer(t, stage)
	client := NewTCPClient(srv.Addr().String())
	defer client.Close()
	ctxbg := context.Background()
	deadline := time.Now().Add(5 * time.Second)

	// 白名单拒绝（分类保持）
	_, err := InvokeRemote[troupe.Empty, int](ctxbg, client, troupe.MustActorID("app", "cart", "t1"), "on_stage", troupe.Empty{}, deadline)
	var cre *troupe.CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != troupe.RejectWhitelist || cre.Retryable {
		t.Fatalf("白名单错误 = %v, want whitelist 不可重试", err)
	}

	// 未知角色
	_, err = InvokeRemote[troupe.Empty, int](ctxbg, client, troupe.MustActorID("app", "ghost", "1"), "add", troupe.Empty{}, deadline)
	var ure *troupe.UnknownRoleError
	if !errors.As(err, &ure) {
		t.Fatalf("未知角色错误 = %v, want UnknownRoleError", err)
	}

	// 参数校验（排队前拒绝，零副作用）
	_, err = InvokeRemote[json.RawMessage, int](ctxbg, client, troupe.MustActorID("app", "cart", "t1"), "add", json.RawMessage(`{"wrong":1}`), deadline)
	var iae *troupe.InvalidArgumentError
	if !errors.As(err, &iae) {
		t.Fatalf("参数错误 = %v, want InvalidArgumentError", err)
	}
}

func TestDeadlineExecutingUnknownOutcomeOverWire(t *testing.T) {
	// 执行占用跨线：客户端 deadline 到期 = executing 结果未知；服务端效果照常提交。
	stage, _ := cartStage(t)
	// 用 hold 闸门控制：注册一个闸门 turn
	gate := make(chan struct{})
	var once sync.Once
	m := troupe.Define("gate", func(id troupe.ActorID, props cartProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[cartProps] } {
		return &struct{ *troupe.Actor[cartProps] }{troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(troupe.Turn("blocked", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, _ troupe.Empty) (bool, error) {
		<-gate
		a.Props().Items = append(a.Props().Items, "committed")
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	m.Turn(troupe.Turn("peek", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, _ troupe.Empty) ([]string, error) {
		return a.Props().Items, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	srv := startServer(t, stage)
	client := NewTCPClient(srv.Addr().String())
	defer client.Close()
	ctxbg := context.Background()
	id := troupe.MustActorID("app", "gate", "g1")

	f, err := client.Call(ctxbg, id, "blocked", troupe.Empty{}, time.Now().Add(60*time.Millisecond))
	if err != nil {
		t.Fatalf("call 失败：%v", err)
	}
	_, err = f.Await(ctxbg)
	var cte *troupe.CallTimeoutError
	if !errors.As(err, &cte) || cte.Phase != troupe.PhaseExecuting {
		t.Fatalf("deadline 错误 = %v, want CALL_TIMEOUT{executing}", err)
	}

	once.Do(func() { close(gate) })
	if _, err := InvokeRemote[troupe.Empty, []string](ctxbg, client, id, "peek", troupe.Empty{}, time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("peek 失败：%v", err)
	}
	// 迟到结果不放行（结果未知语义保持）
	_, err = f.Await(ctxbg)
	if !errors.As(err, &cte) || cte.Phase != troupe.PhaseExecuting {
		t.Fatalf("迟到结果错误 = %v, want 仍为 executing", err)
	}
}

func TestCancelEnvelopePropagation(t *testing.T) {
	stage, _ := cartStage(t)
	cancelSeen := make(chan bool, 1)
	m := troupe.Define("cc", func(id troupe.ActorID, props cartProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[cartProps] } {
		return &struct{ *troupe.Actor[cartProps] }{troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(troupe.Turn("count", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, _ troupe.Empty) (int, error) {
		steps := 0
		for i := 0; i < 60; i++ {
			if ctx.CancelRequested() {
				cancelSeen <- true
				break
			}
			steps++
			time.Sleep(2 * time.Millisecond)
		}
		return steps, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	srv := startServer(t, stage)
	client := NewTCPClient(srv.Addr().String())
	defer client.Close()
	ctxbg := context.Background()
	id := troupe.MustActorID("app", "cc", "c1")

	f, err := client.Call(ctxbg, id, "count", troupe.Empty{}, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := f.Cancel(); err != nil {
		t.Fatalf("cancel 失败：%v", err)
	}
	raw, err := f.Await(ctxbg)
	if err != nil {
		t.Fatalf("await 失败：%v", err)
	}
	var steps int
	if err := json.Unmarshal(raw, &steps); err != nil {
		t.Fatal(err)
	}
	if steps >= 60 {
		t.Fatalf("取消未生效：%d 步", steps)
	}
	select {
	case <-cancelSeen:
	default:
		t.Fatal("Turn 未观察到取消（但提前收束了？不一致）")
	}
}

func TestDisconnectUnknownOutcome(t *testing.T) {
	stage, _ := cartStage(t)
	gate := make(chan struct{})
	m := troupe.Define("dc", func(id troupe.ActorID, props cartProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[cartProps] } {
		return &struct{ *troupe.Actor[cartProps] }{troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(troupe.Turn("blocked", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, _ troupe.Empty) (bool, error) {
		<-gate
		a.Props().Items = append(a.Props().Items, "committed")
		if err := a.Save(); err != nil {
			return false, err
		}
		return true, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	srv := startServer(t, stage)
	client := NewTCPClient(srv.Addr().String())
	ctxbg := context.Background()
	id := troupe.MustActorID("app", "dc", "d1")

	f, err := client.Call(ctxbg, id, "blocked", troupe.Empty{}, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	client.Close() // 断连：在途调用 UnknownOutcome
	_, err = f.Await(ctxbg)
	var uoe *troupe.UnknownOutcomeError
	if !errors.As(err, &uoe) {
		t.Fatalf("断连错误 = %v, want UnknownOutcomeError", err)
	}
	// 服务端效果照常提交（结果未知 ≠ 效果未知）；提交发生在 gate 放行后——轮询等待
	close(gate)
	store := stage.Store()
	commitDeadline := time.Now().Add(2 * time.Second)
	for {
		rec, _ := store.Read(ctxbg, id)
		if rec != nil && bytes.Contains([]byte(rec.Data), []byte("committed")) {
			break
		}
		if time.Now().After(commitDeadline) {
			t.Fatalf("服务端效果未提交（2s 内）：%+v", rec)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInflightCap(t *testing.T) {
	stage, _ := cartStage(t)
	gate := make(chan struct{})
	m := troupe.Define("if", func(id troupe.ActorID, props cartProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[cartProps] } {
		return &struct{ *troupe.Actor[cartProps] }{troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(troupe.Turn("blocked", func(a *struct{ *troupe.Actor[cartProps] }, ctx *troupe.CallContext, _ troupe.Empty) (bool, error) {
		<-gate
		return true, nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	srv := startServer(t, stage, WithMaxInflight(1))
	client := NewTCPClient(srv.Addr().String())
	defer client.Close()
	ctxbg := context.Background()
	id := troupe.MustActorID("app", "if", "i1")
	deadline := time.Now().Add(10 * time.Second)

	f1, err := client.Call(ctxbg, id, "blocked", troupe.Empty{}, deadline)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := client.Call(ctxbg, id, "blocked", troupe.Empty{}, deadline)
	if err != nil {
		t.Fatal(err)
	}
	// in-flight 拒绝发生在服务端，经 rep 送达第二个 future
	_, err = f2.Await(ctxbg)
	var cre *troupe.CallRejectedError
	if !errors.As(err, &cre) || cre.Reason != troupe.RejectBackpressure || !cre.Retryable {
		t.Fatalf("in-flight 拒绝错误 = %v, want backpressure retryable", err)
	}
	close(gate)
	if _, err := f1.Await(ctxbg); err != nil {
		t.Fatalf("f1 失败：%v", err)
	}
}

func TestAuthOverWire(t *testing.T) {
	stage, _ := cartStage(t)
	auth, err := NewAuthenticator("secret-a")
	if err != nil {
		t.Fatal(err)
	}
	srv := startServer(t, stage, WithServerAuth(auth))

	// 无 token：未认证即发业务信封 → 拒绝（UnknownOutcome/断连）
	noTok := NewTCPClient(srv.Addr().String())
	defer noTok.Close()
	_, err = InvokeRemote[troupe.Empty, int](context.Background(), noTok,
		troupe.MustActorID("app", "cart", "t1"), "ping2", troupe.Empty{}, time.Now().Add(5*time.Second))
	if err == nil {
		t.Fatal("未认证调用应失败")
	}

	// 错误 token：auth 失败
	badTok := NewTCPClient(srv.Addr().String(), WithToken("wrong"))
	defer badTok.Close()
	_, err = InvokeRemote[troupe.Empty, int](context.Background(), badTok,
		troupe.MustActorID("app", "cart", "t1"), "add", troupe.Empty{}, time.Now().Add(5*time.Second))
	if err == nil || !contains(err.Error(), "auth") {
		t.Fatalf("错误 token = %v, want auth 失败", err)
	}

	// 正确 token：通过
	okCli := NewTCPClient(srv.Addr().String(), WithToken("secret-a"))
	defer okCli.Close()
	n, err := InvokeRemote[struct{ Item string }, int](context.Background(), okCli,
		troupe.MustActorID("app", "cart", "t1"), "add", struct{ Item string }{Item: "x"}, time.Now().Add(5*time.Second))
	if err != nil || n != 1 {
		t.Fatalf("认证后调用 = %d, %v", n, err)
	}

	// 轮换宽限：旧 token 仍可用；撤销后失效
	if err := auth.Rotate("secret-b"); err != nil {
		t.Fatal(err)
	}
	oldCli := NewTCPClient(srv.Addr().String(), WithToken("secret-a"))
	n, err = InvokeRemote[struct{ Item string }, int](context.Background(), oldCli,
		troupe.MustActorID("app", "cart", "t1"), "add", struct{ Item string }{Item: "y"}, time.Now().Add(5*time.Second))
	if err != nil || n != 2 {
		t.Fatalf("宽限旧 token 调用 = %d, %v", n, err)
	}
	oldCli.Close()

	auth.RevokeOld()
	revoked := NewTCPClient(srv.Addr().String(), WithToken("secret-a"))
	defer revoked.Close()
	_, err = InvokeRemote[troupe.Empty, int](context.Background(), revoked,
		troupe.MustActorID("app", "cart", "t1"), "add", troupe.Empty{}, time.Now().Add(5*time.Second))
	if err == nil {
		t.Fatal("撤销后旧 token 应失败")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
