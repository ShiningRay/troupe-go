package troupe

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type cartProps struct {
	Items []string `json:"items"`
}

type cartActor struct {
	*Actor[cartProps]
	staged   int // 生命周期钩子调用计数（仅测试观察）
	unstaged int
}

func (c *cartActor) OnStage() error  { c.staged++; return nil }
func (c *cartActor) OffStage() error { c.unstaged++; return nil }

func newCart(id ActorID, props cartProps, ctx *ActivationContext) *cartActor {
	return &cartActor{Actor: NewActor(id, props, ctx)}
}

func cartManifest() *Manifest[cartProps, *cartActor] {
	m := Define("cart", newCart)
	m.SchemaVersion(1)
	m.Initial(func() cartProps { return cartProps{Items: []string{}} })
	m.Turn(Turn("add", func(a *cartActor, ctx *CallContext, args struct{ Item string }) (int, error) {
		a.Props().Items = append(a.Props().Items, args.Item)
		if err := a.Save(); err != nil {
			return 0, err
		}
		return len(a.Props().Items), nil
	}))
	m.Turn(Turn("peek", func(a *cartActor, ctx *CallContext, _ Empty) ([]string, error) {
		return a.Props().Items, nil
	}))
	return m
}

func TestManifestRegistration(t *testing.T) {
	r := NewRehearsal("m1")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("合法 manifest 注册失败：%v", err)
	}
	if got := r.Stage.RegisteredRoles(); len(got) != 1 || got[0] != "cart" {
		t.Fatalf("RegisteredRoles = %v", got)
	}

	// 重复角色
	err := cartManifest().Register(r.Stage)
	if err == nil || !strings.Contains(err.Error(), "已注册") {
		t.Fatalf("重复注册错误 = %v", err)
	}

	// 保留名 turn
	bad := Define("bad", newCart)
	bad.SchemaVersion(1)
	bad.Initial(func() cartProps { return cartProps{} })
	bad.Turn(Turn("save", func(a *cartActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	err = bad.Register(r.Stage)
	if err == nil || !strings.Contains(err.Error(), "保留名") {
		t.Fatalf("保留名 turn 错误 = %v", err)
	}

	// 缺 schema_version / initial / turns
	noSchema := Define[cartProps, *cartActor]("noschema", newCart)
	noSchema.Initial(func() cartProps { return cartProps{} })
	noSchema.Turn(Turn("x", func(a *cartActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	if err := noSchema.Register(r.Stage); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("缺 schema_version 错误 = %v", err)
	}

	noInitial := Define[cartProps, *cartActor]("noinitial", newCart)
	noInitial.SchemaVersion(1)
	noInitial.Turn(Turn("x", func(a *cartActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	if err := noInitial.Register(r.Stage); err == nil || !strings.Contains(err.Error(), "initial") {
		t.Fatalf("缺 initial 错误 = %v", err)
	}

	noTurns := Define[cartProps, *cartActor]("noturns", newCart)
	noTurns.SchemaVersion(1)
	noTurns.Initial(func() cartProps { return cartProps{} })
	if err := noTurns.Register(r.Stage); err == nil || !strings.Contains(err.Error(), "turn") {
		t.Fatalf("零 turn 错误 = %v", err)
	}
}

type v1Props struct {
	Name string `json:"name"`
}
type v2Props struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}
type v3Props struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	Extra string `json:"extra"`
}

type migratingActor struct {
	*Actor[v3Props]
}

func migratingManifest() *Manifest[v3Props, *migratingActor] {
	m := Define("migr", func(id ActorID, props v3Props, ctx *ActivationContext) *migratingActor {
		return &migratingActor{Actor: NewActor(id, props, ctx)}
	})
	m.SchemaVersion(3)
	m.Initial(func() v3Props { return v3Props{Name: "new", Count: 0, Extra: "base"} })
	m.Migrate(Migrate(1, 2, func(v v1Props) (v2Props, error) {
		return v2Props{Name: v.Name, Count: 0}, nil
	}))
	m.Migrate(Migrate(2, 3, func(v v2Props) (v3Props, error) {
		return v3Props{Name: v.Name, Count: v.Count, Extra: "base"}, nil
	}))
	m.Turn(Turn("describe", func(a *migratingActor, ctx *CallContext, _ Empty) (map[string]any, error) {
		p := a.Props()
		return map[string]any{"name": p.Name, "count": p.Count, "extra": p.Extra}, nil
	}))
	return m
}

func TestManifestMigrationPathValidation(t *testing.T) {
	// 链断裂：1→2、3→4 但 schema_version=4 缺 2→3
	broken := Define[v3Props, *migratingActor]("broken", func(id ActorID, props v3Props, ctx *ActivationContext) *migratingActor {
		return &migratingActor{Actor: NewActor(id, props, ctx)}
	})
	broken.SchemaVersion(4)
	broken.Initial(func() v3Props { return v3Props{} })
	broken.Migrate(MigrationStep{From: 1, To: 2, Apply: func(s string) (string, error) { return s, nil }})
	broken.Migrate(MigrationStep{From: 3, To: 4, Apply: func(s string) (string, error) { return s, nil }})
	broken.Turn(Turn("x", func(a *migratingActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	if err := broken.validateManifest(); err == nil || !strings.Contains(err.Error(), "断裂") {
		t.Fatalf("链断裂错误 = %v", err)
	}

	// 终点不等于 schema_version
	misEnd := Define[v3Props, *migratingActor]("misend", func(id ActorID, props v3Props, ctx *ActivationContext) *migratingActor {
		return &migratingActor{Actor: NewActor(id, props, ctx)}
	})
	misEnd.SchemaVersion(5)
	misEnd.Initial(func() v3Props { return v3Props{} })
	misEnd.Migrate(MigrationStep{From: 4, To: 5, Apply: func(s string) (string, error) { return s, nil }})
	misEnd.Turn(Turn("x", func(a *migratingActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	// 缺 1..4 的起点链路——schema=5 而链终点 5==5，但起点 4 意味着从 1 不可达
	// （validate 只查连续与终点；不可达在运行时 migrationPath=nil 报错，与 troupe.cr 一致）
	if err := misEnd.validateManifest(); err != nil {
		t.Fatalf("链终点连续时应通过校验，得到：%v", err)
	}

	// 重复迁移起点
	dup := Define[v3Props, *migratingActor]("dup", func(id ActorID, props v3Props, ctx *ActivationContext) *migratingActor {
		return &migratingActor{Actor: NewActor(id, props, ctx)}
	})
	dup.SchemaVersion(3)
	dup.Initial(func() v3Props { return v3Props{} })
	dup.Migrate(MigrationStep{From: 1, To: 2, Apply: func(s string) (string, error) { return s, nil }})
	dup.Migrate(MigrationStep{From: 1, To: 2, Apply: func(s string) (string, error) { return s, nil }})
	dup.Migrate(MigrationStep{From: 2, To: 3, Apply: func(s string) (string, error) { return s, nil }})
	dup.Turn(Turn("x", func(a *migratingActor, ctx *CallContext, _ Empty) (Empty, error) { return Empty{}, nil }))
	if err := dup.validateManifest(); err == nil || !strings.Contains(err.Error(), "重复迁移起点") {
		t.Fatalf("重复起点错误 = %v", err)
	}
}

func TestMigrationPathUnreachable(t *testing.T) {
	m := migratingManifest()
	if path := m.migrationPath(1); len(path) != 2 {
		t.Fatalf("1→3 路径步数 = %d, want 2", len(path))
	}
	if path := m.migrationPath(3); len(path) != 0 {
		t.Fatalf("3→3 路径应为空, got %d", len(path))
	}
	if path := m.migrationPath(4); path != nil {
		t.Fatal("降级路径应为 nil")
	}
	if path := m.migrationPath(2); len(path) != 1 {
		t.Fatalf("2→3 路径步数 = %d, want 1", len(path))
	}
}

func TestTypedTurnJSONBoundary(t *testing.T) {
	r := NewRehearsal("m2")
	if err := cartManifest().Register(r.Stage); err != nil {
		t.Fatalf("注册失败：%v", err)
	}

	// 缺参/未知字段/类型不符 → Call 即刻拒绝（排队前，零副作用）
	_, err := r.Call(context.Background(), "cart", "c1", "add", map[string]any{"wrong": 1})
	var iae *InvalidArgumentError
	if !errors.As(err, &iae) {
		t.Fatalf("未知字段错误 = %v, want InvalidArgumentError", err)
	}
	// 类型不符同理
	if _, err = r.Call(context.Background(), "cart", "c1", "add", map[string]any{"item": 42}); !errors.As(err, &iae) {
		t.Fatalf("类型不符错误 = %v, want InvalidArgumentError", err)
	}
	f2, err := r.Call(context.Background(), "cart", "c1", "peek", nil)
	if err != nil {
		t.Fatalf("peek call 失败：%v", err)
	}
	r.PumpAll()
	raw, err := f2.Await(context.Background())
	if err != nil {
		t.Fatalf("peek 失败：%v", err)
	}
	var items []string
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("结果反序列化失败：%v", err)
	}
	if len(items) != 0 {
		t.Fatalf("类型不符的调用不应产生副作用：items = %v", items)
	}
}
