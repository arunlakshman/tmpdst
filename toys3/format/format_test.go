package format

import (
	"bytes"
	"strings"
	"testing"
)

func TestObjectRoundTrip(t *testing.T) {
	for _, p := range [][]byte{nil, {}, []byte("x"), bytes.Repeat([]byte("ab"), 100)} {
		f := EncodeObject(p)
		if len(f) != HeaderSize+len(p) || string(f[:4]) != Magic {
			t.Fatalf("EncodeObject(%q): bad header", p)
		}
		got, ok := DecodeObject(f)
		if !ok || !bytes.Equal(got, p) {
			t.Fatalf("DecodeObject(EncodeObject(%q)) = %q, %v", p, got, ok)
		}
	}
}

func TestDecodeRejectsDamage(t *testing.T) {
	f := EncodeObject([]byte("payload"))
	for i := range f {
		bad := bytes.Clone(f)
		bad[i] ^= 1
		if _, ok := DecodeObject(bad); ok {
			t.Fatalf("bit flip at byte %d accepted", i)
		}
	}
	for n := 0; n < len(f); n++ {
		if _, ok := DecodeObject(f[:n]); ok {
			t.Fatalf("truncation to %d bytes accepted", n)
		}
	}
}

func TestEscapeKey(t *testing.T) {
	for _, c := range []struct{ key, name string }{
		{"abc-_XYZ09", "abc-_XYZ09"},
		{"b/k", "b%2Fk"},
		{".", "%2E"},
		{"..", "%2E%2E"},
		{"a b%", "a%20b%25"},
		{"\xff", "%FF"},
	} {
		if got := EscapeKey(c.key); got != c.name {
			t.Errorf("EscapeKey(%q) = %q, want %q", c.key, got, c.name)
		}
		if got, ok := UnescapeKey(c.name); !ok || got != c.key {
			t.Errorf("UnescapeKey(%q) = %q, %v", c.name, got, ok)
		}
	}
	for _, bad := range []string{"%2f", "%41", "%2", "%G0", "a/b", "%"} {
		if _, ok := UnescapeKey(bad); ok {
			t.Errorf("UnescapeKey(%q) accepted a non-canonical name", bad)
		}
	}
}

func TestValidKey(t *testing.T) {
	if ValidKey("") || !ValidKey("k") || !ValidKey(strings.Repeat("a", MaxNameLen)) ||
		ValidKey(strings.Repeat("a", MaxNameLen+1)) || ValidKey(strings.Repeat("/", MaxNameLen/3+1)) {
		t.Fatal("ValidKey bounds wrong")
	}
}

func TestPaths(t *testing.T) {
	if got := ObjPath("/d", "b/k"); got != "/d/obj/b%2Fk" {
		t.Fatalf("ObjPath = %q", got)
	}
	if got := TmpPath("/d", 0xff); got != "/d/tmp/00000000000000ff" {
		t.Fatalf("TmpPath = %q", got)
	}
}
