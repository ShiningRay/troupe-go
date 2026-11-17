package troupe

import (
	"strconv"
	"strings"
)

// ActorID 是 namespace + role + key 的规范标识（troupe-identity/1，
// 与 troupe-planning 各语言实现共享语义）。
//
// 规则（由 golden/identity.json 向量锚定）：
//   - 三个组成部分都必须非空，且不含控制字符（C0 与 DEL）。
//   - 规范字符串形式为 `namespace/role/key`；组成部分内的 `%` 编码为 `%25`、
//     `/` 编码为 `%2F`，因此未转义的 `/` 只能是分隔符。
//   - Parse 严格接受规范形式。
//   - 不做 Unicode 归一化：不同码点序列视为不同 ActorID。
type ActorID struct {
	Namespace string
	Role      string
	Key       string
}

// NewActorID 校验并构造 ActorID，任一组成部分非法返回 *InvalidActorIDError。
func NewActorID(namespace, role, key string) (ActorID, error) {
	if err := validateComponent("namespace", namespace); err != nil {
		return ActorID{}, err
	}
	if err := validateComponent("role", role); err != nil {
		return ActorID{}, err
	}
	if err := validateComponent("key", key); err != nil {
		return ActorID{}, err
	}
	return ActorID{Namespace: namespace, Role: role, Key: key}, nil
}

// MustActorID 在非法输入时 panic，仅供测试与字面量常量使用。
func MustActorID(namespace, role, key string) ActorID {
	id, err := NewActorID(namespace, role, key)
	if err != nil {
		panic(err)
	}
	return id
}

// ParseActorID 解析规范形式 `namespace/role/key`（严格接受 Canonical 的输出）。
func ParseActorID(canonical string) (ActorID, error) {
	parts := strings.Split(canonical, "/")
	if len(parts) != 3 {
		return ActorID{}, &InvalidActorIDError{
			Detail: "ActorID 规范形式必须为 namespace/role/key：" + quote(canonical),
		}
	}
	return NewActorID(decodeComponent(parts[0]), decodeComponent(parts[1]), decodeComponent(parts[2]))
}

// Canonical 返回规范字符串形式。
func (id ActorID) Canonical() string {
	var b strings.Builder
	b.WriteString(encodeComponent(id.Namespace))
	b.WriteByte('/')
	b.WriteString(encodeComponent(id.Role))
	b.WriteByte('/')
	b.WriteString(encodeComponent(id.Key))
	return b.String()
}

func (id ActorID) String() string { return id.Canonical() }

func validateComponent(field, value string) error {
	if value == "" {
		return &InvalidActorIDError{Detail: "ActorID " + field + " 不能为空"}
	}
	for _, c := range value {
		if c < 0x20 || c == 0x7f {
			return &InvalidActorIDError{
				Detail: "ActorID " + field + " 包含控制字符：" + quote(value),
			}
		}
	}
	return nil
}

func encodeComponent(component string) string {
	component = strings.ReplaceAll(component, "%", "%25")
	return strings.ReplaceAll(component, "/", "%2F")
}

// decodeComponent 仅反转义规范编码；先 %2F 后 %25，保证字面量 `%2F` 不被二次解码
// （与 troupe.cr 顺序一致：连续两次 ReplaceAll）。
func decodeComponent(component string) string {
	component = strings.ReplaceAll(component, "%2F", "/")
	return strings.ReplaceAll(component, "%25", "%")
}

// quote 返回 Go 语法引用形式，错误消息里用于避免控制字符注入终端。
func quote(s string) string { return strconv.Quote(s) }
