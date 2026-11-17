# Troupe.go

面向 Go 的 Virtual Actor 框架，语义对标 Orleans 10.0，沿用 Troupe 的剧团术语；
继承 [troupe.cr](https://github.com/ShiningRay/troupe-cr) 已验证的里程碑经验
（strict 串行 / 三码分离 CAS+schemaVersion+epoch fencing / 有界拒绝 /
UnknownOutcome / golden 向量对齐）。

> **当前状态：M6 生态模块完成（全里程碑 M0-M6 交付）。** 单机核心（M1）、SQLite/幂等/取消/metrics/trace/基准（M2）、PostgreSQL 跨节点接管/TCP transport/集群路由/Reminder/Outbox（M3）、交错调度模式 + Show realtime + Director（M4）、持久流 + 版本化目录/放置/再平衡（M5）、OTel 导出适配器 + 事件溯源 + 版本兼容矩阵 + 滚动升级指引（M6）。全仓用例 `-race` 全绿（root 84 / transport 11 / PG 8）。公开边界见 [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)：公共 API 仍为 alpha，跨节点语义以真实 PostgreSQL 多进程证据为准。

## 文档

- [DESIGN.md](DESIGN.md)：目标架构、Go 执行模型与核心契约、troupe.cr 经验清单。
- [PLAN.md](PLAN.md)：分阶段方案（M0–M6）、验收门禁与风险对应。
- [验证记录](docs/verification.md)：各里程碑实际命令与结果。
- [Orleans 差距](docs/orleans-gap.md)
- [多语言统一规划](https://github.com/ShiningRay/troupe)

## 验证

要求 Go >= 1.23（本地验证：go1.26.0 windows/amd64）。

```sh
gofmt -l .        # 无输出
go vet ./...
go test -race ./...
```

## 已锚定的公共语义（M0）

```go
id, err := troupe.NewActorID("app", "cart", "cart-1")
canonical := id.Canonical()          // "app/cart/cart-1"
parsed, err := troupe.ParseActorID(canonical)

troupe.CanonicalHashHex("app/order/1") // "93880cb0"（golden/hash.json）

var err2 troupe.WireError = &troupe.CallTimeoutError{Phase: troupe.PhaseExecuting}
err2.WireCode()                        // "CALL_TIMEOUT"（golden/errors.json）
```

`golden/*.json` 与 troupe.cr / troupejs / trouperb 读取同一组向量文件，
断言跨语言行为一致；向量源自 troupe.cr ADR 0001 提案（troupe-planning 采纳后
迁入统一仓）。

## 最小用法（M1，单机 Stage）

```go
type CartProps struct {
    Items []string `json:"items"`
}

type CartActor struct {
    *troupe.Actor[CartProps]
}

// 可选生命周期钩子：实现 troupe.StageHooks 即被框架发现
func (a *CartActor) OnStage() error  { return nil }
func (a *CartActor) OffStage() error { return nil }

func main() {
    m := troupe.Define("cart", func(id troupe.ActorID, props CartProps, ctx *troupe.ActivationContext) *CartActor {
        return &CartActor{Actor: troupe.NewActor(id, props, ctx)}
    })
    m.SchemaVersion(1)
    m.Initial(func() CartProps { return CartProps{} })
    // 类型安全 Turn：参数/返回值经 JSON 边界；未知字段排队前显式拒绝
    m.Turn(troupe.Turn("add", func(a *CartActor, ctx *troupe.CallContext,
        args struct{ Item string }) (int, error) {
        a.Props().Items = append(a.Props().Items, args.Item)
        if err := a.Save(); err != nil { // revision CAS + epoch fencing 显式提交
            return 0, err
        }
        return len(a.Props().Items), nil
    }))

    stage := troupe.NewStage() // M1：Memory PropStore（CrossNodeSafe=false）
    if err := m.Register(stage); err != nil {
        panic(err)
    }

    id := troupe.MustActorID("app", "cart", "cart-1")
    f, err := stage.Call(context.Background(), id, "add",
        map[string]any{"item": "pear"})
    if err != nil {
        panic(err) // UnknownRole / whitelist / backpressure 在此显式拒绝
    }
    raw, err := f.Await(context.Background())
    // err 分类：CALL_TIMEOUT{queued}=零副作用可重试；
    // CALL_TIMEOUT{executing}/UNKNOWN_OUTCOME=结果未知，重试靠业务幂等。
    _ = raw
    stage.Close()
}
```

**测试**：用 `troupe.NewRehearsal("ns")`——固定 namespace、Memory Provider、
虚拟时钟（`Advance`）、Manual 确定性调度（`Pump/PumpAll`）：

```go
r := troupe.NewRehearsal("test")
m.Register(r.Stage)
f, _ := r.Call(ctx, "cart", "c1", "add", map[string]any{"item": "x"})
r.PumpAll()
raw, err := f.Await(ctx)
```

## 实施阶段

按 [PLAN.md](PLAN.md)：M0–M6 全部交付（✅ 2026-11-17 当轮完成）：M0 golden 锚定 →
M1 单机核心 → M2 SQLite/硬化/观测 → M3 PostgreSQL/transport/集群 →
M4 调度扩展/Show → M5 流/目录/协议 → M6 生态/发布（tag v0.1.0-alpha.1）。

## 许可

MIT，见 [LICENSE](LICENSE)。
