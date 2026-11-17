package troupe

import (
	"fmt"
	"hash/crc32"
)

// CanonicalHash 返回 canonical 字符串的 CRC32 IEEE 802.3 指纹
// （troupe-hash-crc32/1：poly 0xEDB88320，init/xorout 0xFFFFFFFF，
// 作用于 canonical UTF-8 字节；由 golden/hash.json 向量锚定）。
//
// 仅承诺进程内/同版本环与指纹一致性；不承诺跨语言哈希环一致性
// （各实现可用不同环算法，只需 golden 向量验证 CRC32 本身）。
func CanonicalHash(canonical string) uint32 {
	return crc32.ChecksumIEEE([]byte(canonical))
}

// CanonicalHashHex 返回 8 位小写十六进制指纹（golden/hash.json 的字段形式）。
func CanonicalHashHex(canonical string) string {
	return fmt.Sprintf("%08x", CanonicalHash(canonical))
}

// ActorHash 返回 ActorID 规范形式的 CRC32 指纹（哈希环与幂等指纹的公共基础）。
func ActorHash(id ActorID) uint32 {
	return CanonicalHash(id.Canonical())
}
