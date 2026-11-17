package troupe

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// 五场景基线基准（对齐 troupe.cr bench/run.cr 口径；本仓自基线，
// 跨语言比较交 troupe-bench-suite 统一口径——不与其他语言异口径比较）。
// CI 冒烟：go test -bench=. -benchtime=1x -run=^$（数字无意义）；
// 正式数据：go test -bench=. -benchmem -run=^$。

func benchCart(b testing.TB) *Stage {
	b.Helper()
	cfg := DefaultStageConfig()
	cfg.MaxActivations = 1 << 20 // 基准不设激活上限（吞吐场景按 b.N 递增激活）
	stage := NewStage(WithConfig(cfg))
	m := Define("bench", func(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("add", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	m.Turn(Turn("ping", func(a *cartActor, ctx *CallContext, _ Empty) (int, error) {
		return len(a.Props().Items), nil // 只读不落盘
	}))
	m.Turn(Turn("spin", func(a *cartActor, ctx *CallContext, args struct{ Ms int }) (bool, error) {
		deadline := time.Now().Add(time.Duration(args.Ms) * time.Millisecond)
		x := 1
		for time.Now().Before(deadline) {
			x = x*31 + 7 // CPU 密集忙等
		}
		_ = x
		return true, nil
	}))
	if err := m.Register(stage); err != nil {
		b.Fatalf("注册失败：%v", err)
	}
	b.Cleanup(stage.Close)
	return stage
}

func benchAdd(b testing.TB, stage *Stage, id ActorID) {
	b.Helper()
	if _, err := Invoke[struct{ Item string }, int](context.Background(), stage, id, "add", struct{ Item string }{Item: "x"}); err != nil {
		b.Fatal(err)
	}
}

// 场景 1：激活吞吐（首调创建激活 + 初始 Props 落盘）。
func BenchmarkActivationThroughput(b *testing.B) {
	stage := benchCart(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchAdd(b, stage, MustActorID("app", "bench", "act-"+itoa(i)))
	}
}

// 场景 2：每 Actor 内存占用（KB/Actor，近似：激活 5000 个后的堆增长）。
func TestMemoryPerActor(t *testing.T) {
	cfg := DefaultStageConfig()
	cfg.MaxActivations = 8192
	stage := NewStage(WithConfig(cfg))
	defer stage.Close()
	m := Define("mem", func(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("ping", func(a *cartActor, ctx *CallContext, _ Empty) (int, error) {
		return len(a.Props().Items), nil
	}))
	if err := m.Register(stage); err != nil {
		t.Fatal(err)
	}
	ctxbg := context.Background()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const n = 5000
	for i := 0; i < n; i++ {
		f, err := stage.Call(ctxbg, MustActorID("app", "mem", itoa(i)), "ping", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Await(ctxbg); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	perActor := (after.HeapAlloc - before.HeapAlloc) / n
	t.Logf("KB/Actor ≈ %.2f（%d 激活，含 slot/队列/actor 实例）", float64(perActor)/1024, n)
	if perActor > 20*1024 {
		t.Fatalf("KB/Actor = %.1f 超出 20KB 预算", float64(perActor)/1024)
	}
}

// 场景 3a：热点调用（全部打到同一 Actor，严格串行成为瓶颈的形态）。
func BenchmarkHotspotCall(b *testing.B) {
	stage := benchCart(b)
	ctxbg := context.Background()
	id := MustActorID("app", "bench", "hot")
	benchAdd(b, stage, id)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Invoke[Empty, int](ctxbg, stage, id, "ping", Empty{}); err != nil {
			b.Fatal(err)
		}
	}
}

// 场景 3b：分散调用（N 个 Actor 各自并行，展示无交叉争用）。
func BenchmarkScatteredCall(b *testing.B) {
	stage := benchCart(b)
	ctxbg := context.Background()
	const actors = 64
	ids := make([]ActorID, actors)
	for i := range ids {
		ids[i] = MustActorID("app", "bench", "scat-"+itoa(i))
		benchAdd(b, stage, ids[i])
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := Invoke[Empty, int](ctxbg, stage, ids[i%actors], "ping", Empty{}); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// 场景 4：CPU 阻塞调度延迟（Go 抢占式调度核对）。
// spin Turn 占满一个 goroutine 时，邻居 Actor 的调用延迟不应接近 spin 全长
// （troupe.cr Fiber 协作式下 victim 延迟≈spin 全长；Go 异步抢占应显著更低）。
func TestCpuSpinNeighborLatency(t *testing.T) {
	stage := benchCart(t)
	ctxbg := context.Background()
	spinID := MustActorID("app", "bench", "spin")
	neighID := MustActorID("app", "bench", "neigh")
	benchAdd(t, stage, spinID)
	benchAdd(t, stage, neighID)

	const spinMs = 80
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = Invoke[struct{ Ms int }, bool](ctxbg, stage, spinID, "spin", struct{ Ms int }{Ms: spinMs})
	}()
	time.Sleep(5 * time.Millisecond) // 确保 spin 已开跑

	start := time.Now()
	if _, err := Invoke[Empty, int](ctxbg, stage, neighID, "ping", Empty{}); err != nil {
		t.Fatal(err)
	}
	latency := time.Since(start)
	<-done
	t.Logf("spin %dms 期间邻居调用延迟 = %v（Go 抢占式调度：不应接近 spin 全长）", spinMs, latency)
	if latency > time.Duration(spinMs)*time.Millisecond/2 {
		t.Fatalf("邻居延迟 %v 超过 spin 全长一半（调度受阻）", latency)
	}
}

// 场景 5：队列饱和（满队显式拒绝的成本与语义）。
func BenchmarkQueueSaturation(b *testing.B) {
	cfg := DefaultStageConfig()
	cfg.MaxQueueDepth = 32
	stage := NewStage(WithConfig(cfg))
	defer stage.Close()
	m := Define("sat", func(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
		return &cartActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{} })
	m.Turn(Turn("hold", func(a *cartActor, ctx *CallContext, _ Empty) (bool, error) {
		time.Sleep(1 * time.Millisecond)
		return true, nil
	}))
	if err := m.Register(stage); err != nil {
		b.Fatal(err)
	}
	id := MustActorID("app", "sat", "s1")
	ctxbg := context.Background()

	b.ResetTimer()
	rejected := 0
	for i := 0; i < b.N; i++ {
		_, err := stage.Call(ctxbg, id, "hold", nil)
		if err == nil {
			continue
		}
		var cre *CallRejectedError
		if errors.As(err, &cre) && cre.Reason == RejectBackpressure && cre.Retryable {
			rejected++ // 满队显式拒绝：预期路径
			continue
		}
		b.Fatal(err)
	}
}

// itoa 避免基准热路径 strconv 开销（简单递增转十进制）。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
