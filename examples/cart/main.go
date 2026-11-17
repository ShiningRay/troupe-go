// 最小 Cart 示例（M1/M2 语义冒烟，CI 以 `go run ./examples/cart` 验证）。
package main

import (
	"context"
	"fmt"
	"log"

	troupe "github.com/ShiningRay/troupego"
)

type CartProps struct {
	Items []string `json:"items"`
}

type CartActor struct {
	*troupe.Actor[CartProps]
}

// OnStage/OffStage 生命周期钩子（实现 StageHooks 即被框架发现）。
func (c *CartActor) OnStage() error {
	fmt.Println("cart on stage:", c.ID())
	return nil
}

func (c *CartActor) OffStage() error {
	fmt.Println("cart off stage:", c.ID())
	return nil
}

func main() {
	// 显式注册：方法白名单 + schemaVersion + 迁移链在 Register 时校验。
	m := troupe.Define("cart", func(id troupe.ActorID, props CartProps, ctx *troupe.ActivationContext) *CartActor {
		return &CartActor{Actor: troupe.NewActor(id, props, ctx)}
	})
	m.SchemaVersion(1)
	m.Initial(func() CartProps { return CartProps{} })
	// 类型安全 Turn：参数/返回值经 JSON 边界；未知字段排队前拒绝。
	m.Turn(troupe.Turn("add", func(a *CartActor, ctx *troupe.CallContext,
		args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil { // revision CAS + epoch fencing 显式提交
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	// 只读并发 Turn：在已提交快照上执行，不占 strict 串行队列（M4）。
	m.Turn(troupe.Turn("peek", func(a *CartActor, ctx *troupe.CallContext, _ troupe.Empty) ([]string, error) {
		return a.Props().Items, nil
	}, troupe.ModeReadOnly))

	stage := troupe.NewStage(troupe.WithName("cart-demo"))
	defer stage.Close()
	if err := m.Register(stage); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	id := troupe.MustActorID("demo", "cart", "cart-1")
	for _, item := range []string{"pear", "plum"} {
		n, err := troupe.Invoke[struct{ Item string }, int](ctx, stage, id, "add",
			struct{ Item string }{Item: item})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("add %s -> %d items\n", item, n)
	}

	items, err := troupe.Invoke[troupe.Empty, []string](ctx, stage, id, "peek", troupe.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("peek:", items)
}
