# 版本兼容矩阵（COMPATIBILITY）

> Troupe.go 当前版本：v0.1.0-alpha.1。本仓尚处 alpha——公共 API 未冻结，
> 依赖方以 go.mod 锁定具体 commit/tag；以下承诺仅覆盖 golden 向量与已锚定语义。

## 跨语言承诺（冻结级）

| 契约 | 版本 | 状态 | 说明 |
|---|---|---|---|
| troupe-identity/1 | 1 | 锚定 | canonical ActorID（golden/identity.json，四语言同源向量） |
| troupe-hash-crc32/1 | 1 | 锚定 | CRC32 IEEE 规范哈希（golden/hash.json；进程内/同版本环） |
| troupe-errors/1 | 1 | 锚定 | 错误稳定码 + 结构化字段（golden/errors.json；本仓 transport ec 用稳定码——与 troupe.cr 类名实现存在已记录差异，待 ADR 采纳统一） |
| troupe-envelope/1 | 1 | 提案锚定 | 帧协议 + 信封字段（golden/envelope.json；ADR 0001 Proposed） |

## Go API 稳定性（alpha 级）

| API 面 | 状态 | 说明 |
|---|---|---|
| ActorID / CanonicalHash / WireError 码表常量 | 稳定 | M0 起未变，golden 锚定 |
| Stage / Manifest / Turn 注册 | 演进中 | M4 增加 mode 参数（向后兼容：默认 strict） |
| ActivationContext 字段 | 内部 | storeCtx/writable 等可能调整——Turn 体内只依赖 Props/Save/CallContext |
| PropStore / JournalingStore / OutboxStore | 演进中 | ctx 化为既成事实（M3）；后续只增不改 |
| OwnershipStore | 演进中 | M3 起 Acquire 带 ctx + error（租期冲突） |
| transport 包 | 实验 | envelope 字段冻结级，Server/Client API 实验性 |
| troupepg 包 | 实验 | 表结构 troupe_* 前缀；迁移只增列不破坏 |

## Provider 能力矩阵

| Provider | CAS+fencing | journal 同事务 | outbox 同事务 | 跨节点接管 | 流 | 提醒 |
|---|---|---|---|---|---|---|
| Memory | ✅ | ✅ | ✅ | ❌（CrossNodeSafe=false） | ✅ | ✅ |
| SQLite（modernc 纯 Go） | ✅ | ✅ | ❌（未实现） | ❌ | ❌ | ❌ |
| PostgreSQL（troupepg） | ✅（租期+单语句 fencing） | ✅ | ✅ | ✅ | ✅ | ✅ |

## 不承诺项

- 不承诺 exactly-once（超时/断连 = 结果未知；重试依赖业务幂等）。
- Memory/SQLite Provider 不承诺跨节点恢复。
- 跨语言互操作以 golden 向量 + ADR 采纳为准——不以相似 API 命名宣称互操作。
