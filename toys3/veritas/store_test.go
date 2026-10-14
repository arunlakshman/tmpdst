package veritas

import (
	"slices"
	"strings"
	"testing"

	"manifester/toys3/control"
)

func TestStore(t *testing.T) {
	s := NewStore(4)
	long := strings.Repeat("k", 256)
	steps := []struct {
		op     control.Op
		status control.Status
		body   string
	}{
		{control.Get{Key: "a"}, control.StatusNotFound, ""},
		{control.Delete{Key: "a"}, control.StatusNotFound, ""},
		{control.Copy{Src: "a", Dst: "b"}, control.StatusNotFound, ""},
		{control.Put{Key: "a", Data: []byte("abcd")}, control.StatusOK, ""},
		{control.Put{Key: "a", Data: []byte("abcde")}, control.StatusBadRequest, ""}, // too large: no change
		{control.Put{Key: "", Data: []byte("x")}, control.StatusBadRequest, ""},
		{control.Put{Key: long, Data: []byte("x")}, control.StatusBadRequest, ""},
		{control.Get{Key: "a"}, control.StatusOK, "abcd"},
		{control.Copy{Src: "a", Dst: "a"}, control.StatusOK, ""},
		{control.Copy{Src: "a", Dst: "b"}, control.StatusOK, ""},
		{control.Copy{Src: "a", Dst: ""}, control.StatusBadRequest, ""},
		{control.Copy{Src: "zz", Dst: ""}, control.StatusBadRequest, ""}, // 400 before 404
		{control.Put{Key: "a", Data: nil}, control.StatusOK, ""},
		{control.Get{Key: "b"}, control.StatusOK, "abcd"},
		{control.Get{Key: "a"}, control.StatusOK, ""},
		{control.Delete{Key: "a"}, control.StatusOK, ""},
		{control.Delete{Key: ""}, control.StatusBadRequest, ""},
		{control.Get{Key: "a"}, control.StatusNotFound, ""},
	}
	for _, st := range steps {
		status, body := s.Apply(st.op)
		if status != st.status || string(body) != st.body {
			t.Fatalf("%#v: got %d %q, want %d %q", st.op, status, body, st.status, st.body)
		}
	}
	if got := s.Keys(); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("Keys = %q", got)
	}
}

func TestStoreNeverAliases(t *testing.T) {
	s := NewStore(0)
	in := []byte("abc")
	s.Apply(control.Put{Key: "k", Data: in})
	in[0] = 'X'
	_, body := s.Apply(control.Get{Key: "k"})
	body[1] = 'Y'
	v, _ := s.Value("k")
	v[2] = 'Z'
	if v, _ := s.Value("k"); string(v) != "abc" {
		t.Fatalf("store aliased a caller's slice: %q", v)
	}
}
