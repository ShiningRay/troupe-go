# Troupe.go

面向 Go 的 Virtual Actor 框架，语义对标 Orleans 10.0，沿用 Troupe 的剧团术语；
继承 [troupe.cr](https://github.com/ShiningRay/troupe.cr) 已验证的里程碑经验
（strict 串行 / 三码分离 CAS+schemaVersion+epoch fencing / 有界拒绝 /
UnknownOutcome / golden 向量对齐）。

> **当前状态：M0 设计与验证基线（scaffold）。** 已实现 canonical ActorID
> （troupe-identity/1）、规范哈希（troupe-hash-crc32/1）与错误码表
> （troupe-errors/1），全部由 `golden/*.json` 契约测试锚定。Stage/Actor/Props/
> 持久化/集群均未实现——不要据此宣称单机或集群可用。

## 文档

- [DESIGN.md](DESIGN.md)：目标架构、Go 执行模型与核心契约、troupe.cr 经验清单。
- [PLAN.md](PLAN.md)：分阶段方案（M0–M6）、验收门禁与风险对应。
- [Orleans 差距](docs/orleans-gap.md)
- [多语言统一规划](https://github.com/ShiningRay/troupe-planning)

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

var err2 troupe.WireError = &troupe.CallTimeoutError{Phase: "executing"}
err2.WireCode()                        // "CALL_TIMEOUT"（golden/errors.json）
```

`golden/*.json` 与 troupe.cr / troupejs / trouperb 读取同一组向量文件，
断言跨语言行为一致；向量源自 troupe.cr ADR 0001 提案（troupe-planning 采纳后
迁入统一仓）。

## 计划中的最小用法（M1，未实现）

以下为目标形态预览，API 未冻结：

```go
type CartProps struct {
    Items []string `json:"items"`
}

type CartActor struct {
    troupe.Actor[CartProps]
}

func (a *CartActor) Add(item string) (int, error) {
    a.Props().Items = append(a.Props().Items, item)
    if err := a.Save(); err != nil { // revision CAS + epoch fencing 显式提交
        return 0, err
    }
    return len(a.Props().Items), nil
}

stage := troupe.NewStage() // M1：Memory PropStore
stage.Register(...)
result, err := stage.Call(ctx, troupe.MustActorID("app", "cart", "cart-1"),
    "add", args) // strict 串行 Turn；超时=UnknownOutcome；过载显式拒绝
```

## 实施阶段

按 [PLAN.md](PLAN.md)：M0（✅ 2026-11-17）→ M1 单机核心 → M2 SQLite/硬化/
观测 → M3 PostgreSQL/transport/集群 → M4 调度扩展/Show → M5 流/目录/协议 →
M6 生态/发布。

## 许可

MIT，见 [LICENSE](LICENSE)。
