package troupe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// Empty 表示无参数/无返回值 Turn 的占位类型（JSON null 往返无副作用）。
type Empty struct{}

// isNilHandle 判定工厂返回的指针是否为 nil（typed-nil 不落入接口nil 陷阱）。
func isNilHandle[A any](h A) bool {
	v := reflect.ValueOf(h)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// reservedTurns 生命周期/内部方法不可注册为 Turn，也不可经 RPC 调用（DESIGN §4.2）。
var reservedTurns = map[string]bool{
	"on_stage": true, "off_stage": true, "save": true, "props": true, "actor_id": true,
}

// MigrationStep 逐级迁移步骤（v→v+1，禁止跨级/降级）。
// Apply 在序列化边界上执行：解析 from JSON → 变换 → 序列化 to JSON。
// 任何错误即迁移失败，不得写入。
type MigrationStep struct {
	From  int
	To    int
	Apply func(dataJSON string) (string, error)
}

// Migrate 构造带类型的迁移步骤：F/T 必须是 JSON 可往返的 DTO；
// fn 返回错误即迁移失败（激活失败且不写）。
func Migrate[From, To any](from, to int, fn func(From) (To, error)) MigrationStep {
	return MigrationStep{
		From: from,
		To:   to,
		Apply: func(data string) (string, error) {
			var f From
			if err := json.Unmarshal([]byte(data), &f); err != nil {
				return "", fmt.Errorf("迁移 %d→%d 反序列化失败：%w", from, to, err)
			}
			t, err := fn(f)
			if err != nil {
				return "", fmt.Errorf("迁移 %d→%d 变换失败：%w", from, to, err)
			}
			out, err := json.Marshal(t)
			if err != nil {
				return "", fmt.Errorf("迁移 %d→%d 序列化失败：%w", from, to, err)
			}
			return string(out), nil
		},
	}
}

// StageHooks 可选生命周期钩子：具体 Actor 类型实现本接口时，
// 激活后调用 OnStage（错误 → ActivationError），下线前调用 OffStage
// （错误记录事件仍下线）。以类型断言发现，不要求嵌入 Actor 基类以外的事。
type StageHooks interface {
	OnStage() error
	OffStage() error
}

// actorHandle 是框架内部对激活实例的最小视图（unexported 方法 + troupe 包内
// Actor 基类实现 = 密封接口：外部只能经嵌入 *Actor[P] 获得方法集）。
type actorHandle interface {
	actorID() ActorID
}

// activation 一次激活的实例与其可选钩子。
type activation struct {
	handle actorHandle
	hooks  StageHooks // nil 表示未实现 StageHooks
}

// TurnDef 经 troupe.Turn 包级泛型函数构造（Go 方法不能自带类型参数），
// 由 Manifest.Turn 登记到白名单。
type TurnDef[A any] struct {
	name     string
	invoke   func(a A, ctx *CallContext, args json.RawMessage) (any, error)
	validate func(args json.RawMessage) error
}

// Turn 构造类型安全 Turn 定义：参数经 JSON 边界反序列化（排队前 validate
// 预检，缺参/类型不符 → InvalidArgumentError，零副作用）；返回值经 JSON 边界
// 序列化后回给调用方。类型参数由 fn 签名推导。
func Turn[Args, Res any, A any](name string, fn func(A, *CallContext, Args) (Res, error)) TurnDef[A] {
	return TurnDef[A]{
		name: name,
		invoke: func(a A, ctx *CallContext, raw json.RawMessage) (any, error) {
			var args Args
			if err := decodeTurnArgs(raw, &args); err != nil {
				return nil, err
			}
			res, err := fn(a, ctx, args)
			if err != nil {
				return nil, err
			}
			return res, nil
		},
		validate: func(raw json.RawMessage) error {
			var args Args
			return decodeTurnArgs(raw, &args)
		},
	}
}

func decodeTurnArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil // 无参调用：参数取零值，不视为错误
	}
	// 未知字段显式拒绝（对齐"缺参/类型不符排队前拒绝"纪律，避免静默丢参）。
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &InvalidArgumentError{Detail: "参数解析失败：" + err.Error()}
	}
	return nil
}

// manifestBase 类型擦除后的 manifest 视图（Stage/Slot 经此驱动任意具体 manifest）。
type manifestBase interface {
	roleName() string
	schemaVer() int
	turnList() []string
	initialJSON() (string, error)
	migrationPath(fromVersion int) []MigrationStep // nil = 不可达（缺级/降级）
	parseFinal(data string) (any, error)
	buildActor(id ActorID, props any, ctx *ActivationContext) (activation, error)
	invokeTurn(act activation, ctx *CallContext, method string, raw json.RawMessage) (any, error)
	validateArgs(method string, raw json.RawMessage) error
	validateManifest() error
}

// Manifest 角色 manifest：显式注册、方法白名单、Props schemaVersion 与迁移链。
// P 为 Props DTO（JSON 可往返）；A 为具体 Actor 类型（嵌入 *Actor[P]，可选实现
// StageHooks）。
type Manifest[P any, A actorHandle] struct {
	role         string
	targetSchema int
	initial      func() P
	factory      func(id ActorID, props P, ctx *ActivationContext) A
	migrations   []MigrationStep
	turns        map[string]TurnDef[A]
	errs         []error
}

// Define 由具体 Actor 工厂构造 manifest；Turn 登记的闭包绑定该具体类型。
func Define[P any, A actorHandle](role string, factory func(id ActorID, props P, ctx *ActivationContext) A) *Manifest[P, A] {
	return &Manifest[P, A]{
		role:    role,
		factory: factory,
		turns:   make(map[string]TurnDef[A]),
	}
}

// SchemaVersion 设置目标 schema 版本（>= 1；Register 时校验）。
func (m *Manifest[P, A]) SchemaVersion(v int) *Manifest[P, A] {
	if v < 1 {
		m.errs = append(m.errs, fmt.Errorf("角色 %s 的 schema_version 必须 >= 1", m.role))
	}
	m.targetSchema = v
	return m
}

// Initial 设置初始 Props 工厂（首次激活时创建并落盘）。
func (m *Manifest[P, A]) Initial(fn func() P) *Manifest[P, A] {
	m.initial = fn
	return m
}

// Migrate 追加迁移步骤（链由 Register 时校验：起点唯一、逐级连续、终点=schemaVersion）。
func (m *Manifest[P, A]) Migrate(step MigrationStep) *Manifest[P, A] {
	m.migrations = append(m.migrations, step)
	return m
}

// Turn 登记 Turn 定义：名字与保留名或已注册方法冲突在 Register 时校验失败。
func (m *Manifest[P, A]) Turn(def TurnDef[A]) *Manifest[P, A] {
	if def.name == "" {
		m.errs = append(m.errs, fmt.Errorf("角色 %s 的 turn 名不能为空", m.role))
		return m
	}
	if reservedTurns[def.name] {
		m.errs = append(m.errs, fmt.Errorf("turn %q 与生命周期保留名冲突（角色 %s）", def.name, m.role))
		return m
	}
	if _, dup := m.turns[def.name]; dup {
		m.errs = append(m.errs, fmt.Errorf("turn %q 与已注册方法冲突（角色 %s）", def.name, m.role))
		return m
	}
	m.turns[def.name] = def
	return m
}

// Register 将 manifest 注册到 Stage（白名单/迁移链在此校验，失败返回错误）。
func (m *Manifest[P, A]) Register(stage *Stage) error { return stage.Register(m) }

// ---- manifestBase 实现（troupe 包内部）----

func (m *Manifest[P, A]) roleName() string { return m.role }
func (m *Manifest[P, A]) schemaVer() int   { return m.targetSchema }
func (m *Manifest[P, A]) turnList() []string {
	names := make([]string, 0, len(m.turns))
	for name := range m.turns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (m *Manifest[P, A]) initialJSON() (string, error) {
	if m.initial == nil {
		return "", fmt.Errorf("角色 %s 缺少 initial Props 工厂", m.role)
	}
	data, err := json.Marshal(m.initial())
	if err != nil {
		return "", fmt.Errorf("角色 %s 的 initial Props 序列化失败：%w", m.role, err)
	}
	return string(data), nil
}

func (m *Manifest[P, A]) migrationPath(fromVersion int) []MigrationStep {
	if fromVersion == m.targetSchema {
		return []MigrationStep{}
	}
	if fromVersion > m.targetSchema || fromVersion < 1 {
		return nil
	}
	steps := make([]MigrationStep, len(m.migrations))
	copy(steps, m.migrations)
	sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })
	path := []MigrationStep{}
	current := fromVersion
	for current < m.targetSchema {
		found := -1
		for i, s := range steps {
			if s.From == current {
				found = i
				break
			}
		}
		if found < 0 {
			return nil
		}
		path = append(path, steps[found])
		current = steps[found].To
	}
	return path
}

func (m *Manifest[P, A]) parseFinal(data string) (any, error) {
	var p P
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return nil, fmt.Errorf("角色 %s 的 Props 反序列化失败：%w", m.role, err)
	}
	return p, nil
}

func (m *Manifest[P, A]) buildActor(id ActorID, propsv any, ctx *ActivationContext) (activation, error) {
	props, ok := propsv.(P)
	if !ok {
		return activation{}, fmt.Errorf("角色 %s 的 Props 类型不符：期望 %T，实际 %T", m.role, props, propsv)
	}
	h := m.factory(id, props, ctx)
	if isNilHandle(h) {
		return activation{}, fmt.Errorf("角色 %s 的 Actor 工厂返回 nil", m.role)
	}
	var hooks StageHooks
	if sh, ok := any(h).(StageHooks); ok {
		hooks = sh
	}
	return activation{handle: h, hooks: hooks}, nil
}

func (m *Manifest[P, A]) invokeTurn(act activation, ctx *CallContext, method string, raw json.RawMessage) (any, error) {
	def, ok := m.turns[method]
	if !ok {
		return nil, &CallRejectedError{
			Reason:    RejectWhitelist,
			Actor:     ctx.ActorID.Canonical(),
			Detail:    "方法 " + quote(method) + " 不在角色 " + m.role + " 的白名单",
			Retryable: false,
		}
	}
	h, ok := act.handle.(A)
	if !ok {
		return nil, &InvalidActorStateError{Detail: "激活实例类型不符：期望 " + fmt.Sprintf("%T", h) + "，实际 " + fmt.Sprintf("%T", act.handle)}
	}
	return def.invoke(h, ctx, raw)
}

func (m *Manifest[P, A]) validateArgs(method string, raw json.RawMessage) error {
	def, ok := m.turns[method]
	if !ok {
		return nil // 白名单先行拒绝；此处不重复报错
	}
	if def.validate != nil {
		return def.validate(raw)
	}
	return nil
}

func (m *Manifest[P, A]) validateManifest() error {
	errs := append([]error{}, m.errs...)
	if m.targetSchema < 1 {
		errs = append(errs, fmt.Errorf("角色 %s 缺少 schema_version", m.role))
	}
	if m.initial == nil {
		errs = append(errs, fmt.Errorf("角色 %s 缺少 initial Props 工厂", m.role))
	}
	if len(m.turns) == 0 {
		errs = append(errs, fmt.Errorf("角色 %s 未注册任何 turn", m.role))
	}
	// 迁移链校验：起点唯一、逐级连续、终点必须等于 schema_version。
	froms := make(map[int]bool, len(m.migrations))
	steps := make([]MigrationStep, len(m.migrations))
	copy(steps, m.migrations)
	sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })
	for _, s := range steps {
		if froms[s.From] {
			errs = append(errs, fmt.Errorf("角色 %s 存在重复迁移起点 %d", m.role, s.From))
		}
		froms[s.From] = true
	}
	for i, s := range steps {
		if i > 0 && steps[i-1].To != s.From {
			errs = append(errs, fmt.Errorf("角色 %s 迁移链在 %d 处断裂", m.role, s.From))
		}
	}
	if len(steps) > 0 && steps[len(steps)-1].To != m.targetSchema {
		errs = append(errs, fmt.Errorf("角色 %s 迁移链终点 %d 必须等于 schema_version %d",
			m.role, steps[len(steps)-1].To, m.targetSchema))
	}
	return errors.Join(errs...)
}
