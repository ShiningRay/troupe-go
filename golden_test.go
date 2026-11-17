package troupe

import (
	"encoding/json"
	"os"
	"testing"
)

// golden 契约测试：读取与 troupe.cr / troupejs / trouperb 相同的 golden/*.json
// 向量文件，断言本实现行为一致（troupe-planning 跨语言对齐锚点，
// 对应 troupe.cr spec/golden_fixtures_spec.cr）。

type identityFixture struct {
	Protocol string `json:"protocol"`
	Cases    []struct {
		Valid     bool   `json:"valid"`
		Namespace string `json:"namespace"`
		Role      string `json:"role"`
		Key       string `json:"key"`
		Canonical string `json:"canonical"`
		Reason    string `json:"reason"`
	} `json:"cases"`
}

func TestGoldenIdentity(t *testing.T) {
	data, err := os.ReadFile("golden/identity.json")
	if err != nil {
		t.Fatalf("读取 golden/identity.json 失败：%v", err)
	}
	var fx identityFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("解析 golden/identity.json 失败：%v", err)
	}
	if fx.Protocol != "troupe-identity/1" {
		t.Fatalf("协议版本 = %q, want troupe-identity/1", fx.Protocol)
	}
	for _, c := range fx.Cases {
		if c.Valid {
			id, err := ParseActorID(c.Canonical)
			if err != nil {
				t.Errorf("ParseActorID(%q) 应合法：%v", c.Canonical, err)
				continue
			}
			if id.Namespace != c.Namespace || id.Role != c.Role || id.Key != c.Key {
				t.Errorf("ParseActorID(%q) = %s/%s/%s, want %s/%s/%s",
					c.Canonical, id.Namespace, id.Role, id.Key, c.Namespace, c.Role, c.Key)
			}
			if got := id.Canonical(); got != c.Canonical {
				t.Errorf("Canonical() = %q, want %q", got, c.Canonical)
			}
			if built, err := NewActorID(c.Namespace, c.Role, c.Key); err != nil || built != id {
				t.Errorf("NewActorID(%q, %q, %q) 与 ParseActorID(%q) 不一致：%v",
					c.Namespace, c.Role, c.Key, c.Canonical, err)
			}
		} else {
			if _, err := ParseActorID(c.Canonical); err == nil {
				t.Errorf("ParseActorID(%q) 应拒绝（%s）", c.Canonical, c.Reason)
			}
		}
	}
}

type hashFixture struct {
	Protocol string `json:"protocol"`
	Cases    []struct {
		Key   string `json:"key"`
		CRC32 string `json:"crc32"`
	} `json:"cases"`
}

func TestGoldenHash(t *testing.T) {
	data, err := os.ReadFile("golden/hash.json")
	if err != nil {
		t.Fatalf("读取 golden/hash.json 失败：%v", err)
	}
	var fx hashFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("解析 golden/hash.json 失败：%v", err)
	}
	if fx.Protocol != "troupe-hash-crc32/1" {
		t.Fatalf("协议版本 = %q, want troupe-hash-crc32/1", fx.Protocol)
	}
	for _, c := range fx.Cases {
		if got := CanonicalHashHex(c.Key); got != c.CRC32 {
			t.Errorf("CanonicalHashHex(%q) = %s, want %s", c.Key, got, c.CRC32)
		}
	}
}

type errorsFixture struct {
	Codes []struct {
		Code              string          `json:"code"`
		Class             string          `json:"class"`
		TransportOnly     bool            `json:"transport_only"`
		RetryableByReason map[string]bool `json:"retryable_by_reason"`
	} `json:"codes"`
}

func TestGoldenErrorCodes(t *testing.T) {
	data, err := os.ReadFile("golden/errors.json")
	if err != nil {
		t.Fatalf("读取 golden/errors.json 失败：%v", err)
	}
	var fx errorsFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("解析 golden/errors.json 失败：%v", err)
	}
	golden := make(map[string]bool, len(fx.Codes))
	for _, c := range fx.Codes {
		golden[c.Code] = true
	}
	samples := wireErrorSamples()
	for code := range golden {
		if _, ok := samples[code]; !ok {
			t.Errorf("码表缺 %s 的 Go 类型", code)
		}
	}
	for code, sample := range samples {
		if !golden[code] {
			t.Errorf("Go 错误类型 %T 的 WireCode %q 不在 golden 码表内", sample, code)
		}
		if sample.WireCode() != code {
			t.Errorf("%T.WireCode() = %q, want %q", sample, sample.WireCode(), code)
		}
	}
}
