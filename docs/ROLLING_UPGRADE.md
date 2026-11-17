# 滚动升级与回滚指引（ROLLING_UPGRADE）

> 本仓 alpha 阶段：滚动升级以「存储向后兼容 + 节点逐台替换」为基线；
> 反例验证跟随 troupe.cr M6 语义（版本化目录 + fencing 是安全网）。

## 原则

1. **存储先行**：schema 迁移只增列/只增表（`troupe_*` 前缀），旧节点代码
   能在新表结构上运行后再替换节点。
2. **fencing 是安全网**：滚动期间双活写由 epoch fencing 拒绝——旧 epoch 写
   一律失败（CallRejectedError{ownership}），实例自动失效并重激活。
3. **不兼容即停机换**：跨版本 wire 协议不兼容时，不做滚动——分批停机替换
   （drain → 停机 → 升级 → 重入团）。

## 滚动步骤（同 wire 兼容窗口内）

1. 新节点入团：`Roster.Join`（死亡/新节点 incarnation 单调）→ Router.Sync
   → 新节点只承接新放置。
2. 逐台排空旧节点：`Director.DrainStage(name)`（拒新 retryable，可漂移转发）
   → `Stage.Idle()`（队列+在飞+并发清空）→ `ShutdownStage`。
3. 所有权交接：旧节点租期过期后新节点接管（PG lease，DB 时钟判定）；
   在途 reminder claim 由 claim 租期 fencing 保护。
4. 重复至全量。

## 回滚

- 存储层：迁移只增不改——回滚到旧版本代码可直接读旧表结构。
- 目录层：改派经版本保护（旧计划重放 no-op/拒绝）——回滚重放旧 handoff
  计划不会产生双重改派。
- 流层：游标只前进（ack 回退显式拒绝）——回滚消费者从其游标重放，
  at-least-once 语义不变。
- 事件溯源：快照+增量 == 全量——回滚版本以同一 apply 折叠即可重建。

## schemaVersion 迁移（Props）

- 逐级 v→v+1，禁止跨级/降级；迁移失败 = 激活失败且不写
  （旧节点读到未知高版本 schema → ActivationError → 节点不得服务该 actor，
  由新版本节点接管——升级顺序：先升级承担该 role 的节点）。

## 已知边界

- wire 协议版本协商（auth ack 版本字段）尚未实现（ADR 0001 升级点）——
  当前假设全部节点同版本部署。
- troupe_* 表结构变更尚无自动迁移版本表——升级前人工核对
  docs/COMPATIBILITY.md Provider 矩阵。
