// Package troupe 是面向 Go 的 Virtual Actor 框架（剧团隐喻），语义对标
// Microsoft Orleans 10.0，与 troupejs / trouperb / troupe.cr 共享核心语义、
// Provider 合同与 golden 测试向量（见 golden/ 目录）。
//
// 当前状态：M0 设计与验证基线。仅锚定 canonical identity（troupe-identity/1）、
// 规范哈希（troupe-hash-crc32/1）与错误码表（troupe-errors/1）；Stage/Actor/
// Props/持久化/集群均未实现，见 PLAN.md。
package troupe
