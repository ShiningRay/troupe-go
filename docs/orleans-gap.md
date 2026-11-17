# Troupe.go 与 Orleans 10.0 的差距

> 本文描述目标差距，不表示相关能力已实现。基线口径与
> [troupe-planning Orleans 对标基线](https://github.com/ShiningRay/troupe/blob/main/docs/orleans-baseline.md)
> 一致。

| 能力域 | Orleans 基线 | Go 当前状态 | 计划入口 |
|---|---|---|---|
| Identity/lifecycle | 稳定 GrainId、activation incarnation、完整生命周期 | ✅ M0：canonical ActorID（golden 锚定）；生命周期未实现 | M1 最小核心 |
| Turn scheduling | turn-based、reentrancy/read-only/call-chain | 只有 strict goroutine/channel 方案 | M1 strict，M4 交错 |
| Timeout/cancellation | 可配置响应期限、远程取消、失败通知 | 错误码表已锚定（CALL_TIMEOUT/UNKNOWN_OUTCOME）；机制未实现 | M1/M2 |
| Props | IGrainStorage、ETag、显式写与 Provider | 只有 CAS/schemaVersion 方案 | M1 Memory，M2 SQLite，M3 PG |
| Ownership/directory | 目录注册、DHT、视图交接与恢复 | 无实现；先设计 ownership | M3 ownership，M5 directory ADR |
| Placement | compatible silos、resource-aware、rebalancing | 无实现 | M5 |
| Membership | 表维护、健康探测、suspect/dead | 无实现 | M3 基础 Roster |
| Timer/reminders | activation timer 与持久 reminder 分离 | 只有契约 | M3 共享 reminders |
| Streams | 持久队列、sequence token、拉取/订阅/平衡 | 无实现；Show 只规划实时通知 | M4 realtime，M5 persistent ADR |
| Serialization | 代码生成、版本容错 | 显式 DTO/泛型方案；golden 向量锚定 identity/hash/errors | M1/M2 |
| Transactions/extensions | 分布式参与方、恢复、activation 扩展 | 非近期目标 | M6 独立评估 |
| Observability | metrics、W3C trace、Dashboard | 只有计划（slog/metrics 接口/pprof 优势） | M2/M4/M6 |
| 测试与发布 | 多层次核心/Provider/分布式测试 | scaffold CI（fmt/vet/test -race 双平台） | 各阶段验收门禁 |

## 不照搬的部分

- 不复制 .NET class、DI 或私有 serializer；Go 使用显式 DTO/泛型/可选
  codegen 提供本地体验。
- 不把 goroutine/channel 等同于 silo 的分布式正确性（抢占式调度是性能优势，
  不是单激活或容错的来源）。
- 不要求每个 Orleans 模块都进入 Go 首版。
- 事务和事件溯源必须在共享状态、可靠消息与恢复被验证后单独设计。

## 相对 troupe.cr 的差异预期

- **优势**：抢占式调度（CPU 密集 Turn 不饿死节点）、`-race` 竞态检测、
  pprof 内建、`context.Context` 原生取消链、单二进制部署。
- **需补纪律**：Go 无值语义/不可变保护，Props 复制边界要靠显式 Clone +
  反例测试；goroutine 泄漏需生命周期断言。
- **同等约束**：不能强杀 goroutine（协作取消）；Fiber/goroutine 轻量都
  不等于分布式正确性。
