package troupe

import (
	"strings"
	"testing"
)

func TestActorIDRoundTrip(t *testing.T) {
	cases := []struct {
		namespace, role, key string
		canonical            string
	}{
		{"app", "order", "1", "app/order/1"},
		{"app", "ro/le", "a%b/c", "app/ro%2Fle/a%25b%2Fc"},
		{"ns", "role", "%2F", "ns/role/%252F"},
		{"ns", "role", "%25", "ns/role/%2525"},
	}
	for _, c := range cases {
		id, err := NewActorID(c.namespace, c.role, c.key)
		if err != nil {
			t.Fatalf("NewActorID(%q, %q, %q) unexpected error: %v", c.namespace, c.role, c.key, err)
		}
		if got := id.Canonical(); got != c.canonical {
			t.Errorf("Canonical() = %q, want %q", got, c.canonical)
		}
		parsed, err := ParseActorID(c.canonical)
		if err != nil {
			t.Fatalf("ParseActorID(%q) unexpected error: %v", c.canonical, err)
		}
		if parsed != id {
			t.Errorf("ParseActorID(%q) = %+v, want %+v", c.canonical, parsed, id)
		}
	}
}

func TestNewActorIDRejectsInvalid(t *testing.T) {
	cases := []struct {
		namespace, role, key, field string
	}{
		{"", "role", "key", "namespace"},
		{"app", "", "key", "role"},
		{"app", "role", "", "key"},
		{"app", "ro\nle", "1", "role"},
		{"app", "role", "\x00", "key"},
		{"app", "role", "\x7f", "key"},
	}
	for _, c := range cases {
		_, err := NewActorID(c.namespace, c.role, c.key)
		if err == nil {
			t.Errorf("NewActorID(%q, %q, %q) 应返回错误", c.namespace, c.role, c.key)
			continue
		}
		if _, ok := err.(*InvalidActorIDError); !ok {
			t.Errorf("NewActorID 错误类型 = %T, want *InvalidActorIDError", err)
		}
		if want := "ActorID " + c.field; !strings.Contains(err.Error(), want) {
			t.Errorf("错误消息 %q 应包含 %q", err.Error(), want)
		}
	}
}

func TestParseActorIDRejectsInvalid(t *testing.T) {
	for _, canonical := range []string{"app//1", "app/ro/le/1", "", "only-two"} {
		if _, err := ParseActorID(canonical); err == nil {
			t.Errorf("ParseActorID(%q) 应返回错误", canonical)
		}
	}
}

func TestMustActorIDPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustActorID 非法输入应 panic")
		}
	}()
	MustActorID("", "role", "key")
}
