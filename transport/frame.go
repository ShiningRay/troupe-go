// Package transport 实现 TCP transport（M3，troupe-envelope/1，ADR 0001 提案）：
// 帧 `[magic "TRP1" u32 LE][length u32 LE][crc32 u32 LE][payload UTF-8 JSON]`。
// 魔数不符/帧长超上限/CRC 失败 = 协议违规：立即断开；长度上限在读 payload
// 之前校验（超限不分配内存）；断开连接上所有在途调用按 UnknownOutcome 处理。
// 信封字段与 golden/envelope.json 对齐：t/id/actor/m/a/dl/tp/tk/ok/v/ec/em/ek/ert。
package transport

import (
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

// Magic "TRP1"（u32 LE = 0x54525031）。
const Magic uint32 = 0x54525031

// HeaderBytes 帧头长度。
const HeaderBytes = 12

// DefaultMaxFrame 默认帧长上限（1 MiB）。
const DefaultMaxFrame = 1 << 20

// ErrProtocol 协议违规（魔数/长度上限/CRC）：连接断开，不跨网络传播。
var ErrProtocol = errors.New("protocol violation")

// ErrAuth 认证失败（token 无效/未认证即发业务信封）：连接断开。
var ErrAuth = errors.New("auth failed")

// ReadFrame 读一帧；io.EOF 表示连接关闭；协议违规返回 ErrProtocol（调用方须断开）。
func ReadFrame(r io.Reader, maxFrame int) ([]byte, error) {
	header := make([]byte, HeaderBytes)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(header[0:4]) != Magic {
		return nil, ErrProtocol
	}
	length := binary.LittleEndian.Uint32(header[4:8])
	if length > uint32(maxFrame) {
		// 长度上限在读 payload 之前校验：超限不分配内存。
		return nil, ErrProtocol
	}
	expectedCRC := binary.LittleEndian.Uint32(header[8:12])
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return nil, ErrProtocol
	}
	return payload, nil
}

// WriteFrame 写一帧（header + payload，单次编码）。
func WriteFrame(w io.Writer, payload []byte) error {
	header := make([]byte, HeaderBytes)
	binary.LittleEndian.PutUint32(header[0:4], Magic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(payload))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// SecureEquals 常量时间比较（避免短路时序泄漏 token 前缀）。
func SecureEquals(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Authenticator token 认证与轮换：current + grace 集合——Rotate 后旧 token
// 在 RevokeOld 之前仍可验证（轮换宽限），Revoke 后立即失效。验证为常量时间比较。
type Authenticator struct {
	mu      sync.Mutex
	current string
	grace   map[string]bool
}

func NewAuthenticator(token string) (*Authenticator, error) {
	if token == "" {
		return nil, errors.New("token 不能为空")
	}
	return &Authenticator{current: token, grace: make(map[string]bool)}, nil
}

// CurrentToken 返回当前生效 token。
func (a *Authenticator) CurrentToken() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

// Rotate 轮换：新 token 生效，旧 token 进入宽限集（相同 token 为 no-op）。
func (a *Authenticator) Rotate(newToken string) error {
	if newToken == "" {
		return errors.New("token 不能为空")
	}
	if SecureEquals(newToken, a.CurrentToken()) {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.grace[a.current] = true
	a.current = newToken
	return nil
}

// RevokeOld 撤销宽限集：轮换完成后调用，旧 token 立即失效。
func (a *Authenticator) RevokeOld() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.grace = make(map[string]bool)
}

// Verify 验证 token（当前或宽限集内均有效）。
func (a *Authenticator) Verify(token string) bool {
	if token == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if SecureEquals(token, a.current) {
		return true
	}
	for old := range a.grace {
		if SecureEquals(token, old) {
			return true
		}
	}
	return false
}
