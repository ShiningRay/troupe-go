// Director CLI 演示（M4）：多 Stage 聚合健康/指标/事件端点。
// 运行：go run ./examples/director（监听 127.0.0.1:9464）。
// curl 127.0.0.1:9464/health；/metrics；/events；/debug/pprof/
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	troupe "github.com/ShiningRay/troupe-go"
)

type CounterProps struct {
	N int `json:"n"`
}

func main() {
	stageA := troupe.NewStage(troupe.WithName("orders"))
	stageB := troupe.NewStage(troupe.WithName("sessions"))
	for _, st := range []*troupe.Stage{stageA, stageB} {
		m := troupe.Define("counter", func(id troupe.ActorID, props CounterProps, ctx *troupe.ActivationContext) *struct{ *troupe.Actor[CounterProps] } {
			return &struct{ *troupe.Actor[CounterProps] }{troupe.NewActor(id, props, ctx)}
		})
		m.SchemaVersion(1)
		m.Initial(func() CounterProps { return CounterProps{} })
		m.Turn(troupe.Turn("bump", func(a *struct{ *troupe.Actor[CounterProps] }, ctx *troupe.CallContext, _ troupe.Empty) (int, error) {
			a.Props().N++
			if err := a.Save(); err != nil {
				return 0, err
			}
			return a.Props().N, nil
		}))
		if err := m.Register(st); err != nil {
			log.Fatal(err)
		}
	}

	d := troupe.NewDirector()
	if err := d.AddStage("orders", stageA); err != nil {
		log.Fatal(err)
	}
	if err := d.AddStage("sessions", stageB); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		id := troupe.MustActorID("app", "counter", fmt.Sprintf("o%d", i))
		if _, err := troupe.Invoke[troupe.Empty, int](ctx, stageA, id, "bump", troupe.Empty{}); err != nil {
			log.Fatal(err)
		}
	}

	ep := troupe.NewHTTPEndpoint(d)
	addr := "127.0.0.1:9464"
	if err := ep.Listen(addr); err != nil {
		log.Fatal(err)
	}
	fmt.Println("director 端点已监听 http://" + addr)
	fmt.Println("  GET /health   — 各 Stage 状态（running/draining/closed）")
	fmt.Println("  GET /metrics  — per-Stage 计数（activations/turns_completed/...）")
	fmt.Println("  GET /events   — 最近生命周期事件（环形 200）")
	fmt.Println("  GET /debug/pprof/ — Go 运行时剖析")
	fmt.Println("管理写操作（drain/shutdown）仅经 Director API，不经 HTTP。")
	fmt.Println("5s 后演示 DrainStage（orders），10s 后退出。")

	time.Sleep(5 * time.Second)
	if err := d.DrainStage("orders"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("orders 已进入排空（/health 观察 status=draining）")
	time.Sleep(5 * time.Second)
	_ = ctx
}
