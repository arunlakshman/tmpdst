package s3store

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"manifester/internal/core"
)

func newTestStore(t *testing.T, prefix string) (*Store, *fakeS3) {
	t.Helper()
	f := newFakeS3(t)
	s, err := New(t.Context(), Config{Endpoint: f.srv.URL, Bucket: "b", Prefix: prefix, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, f
}

func wantCode(t *testing.T, err error, code core.ErrCode) {
	t.Helper()
	var se *Error
	if !errors.As(err, &se) || se.Code != code {
		t.Fatalf("want %v, got %v", code, err)
	}
}

func TestStorePutGetDeleteList(t *testing.T) {
	ctx := t.Context()
	s, f := newTestStore(t, "p/")
	for k, v := range map[string]string{"a": "0123456789", "b/c": "xyz", "b/d": ""} {
		if err := s.Put(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := f.objs["b/p/a"]; !ok {
		t.Fatalf("prefix not applied: %v", f.objs)
	}
	wantCode(t, s.Put(ctx, "", nil), core.ErrInvalid)
	if err := s.EnsureBucket(ctx); err != nil { // already owned
		t.Fatal(err)
	}

	get := func(key string, off, n int64) ([]byte, error) {
		return s.Get(ctx, core.SegmentRef{Key: key, Off: off, Len: n})
	}
	ok := []struct {
		off, n int64
		want   string
	}{{0, 0, "0123456789"}, {3, 0, "3456789"}, {2, 4, "2345"}, {10, 0, ""}}
	for _, c := range ok {
		got, err := get("a", c.off, c.n)
		if err != nil || string(got) != c.want {
			t.Errorf("get(%d,%d) = %q, %v; want %q", c.off, c.n, got, err, c.want)
		}
	}
	if got, err := get("b/d", 0, 0); err != nil || len(got) != 0 {
		t.Errorf("empty object: %q %v", got, err)
	}
	if got, err := get("b/d", 0, 0); err != nil || got == nil {
		t.Errorf("empty object should be non-nil: %v %v", got, err)
	}
	_, err := get("", 0, 0)
	wantCode(t, err, core.ErrNotFound)
	_, err = get("missing", 0, 0)
	wantCode(t, err, core.ErrNotFound)
	_, err = get("missing", 2, 3)
	wantCode(t, err, core.ErrNotFound)
	_, err = get("a", 20, 0)
	wantCode(t, err, core.ErrInvalid)
	_, err = get("a", 8, 5) // runs past the end: server clips, store notices
	wantCode(t, err, core.ErrInvalid)
	_, err = get("a", -1, 0)
	wantCode(t, err, core.ErrInvalid)
	_, err = get("missing", -1, 0)
	wantCode(t, err, core.ErrNotFound)

	keys, err := s.List(ctx, "b/")
	if err != nil || len(keys) != 2 || keys[0] != "b/c" || keys[1] != "b/d" {
		t.Errorf("list %v %v", keys, err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, s.Delete(ctx, "a"), core.ErrNotFound)
	wantCode(t, s.Delete(ctx, ""), core.ErrNotFound)
}

func TestStoreServerErrors(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name   string
		status int
		code   string
		want   core.ErrCode
		out    core.Outcome
	}{
		{"slowdown", 503, "SlowDown", core.ErrUnavailable, core.NotApplied},
		{"throttled", 429, "Throttling", core.ErrUnavailable, core.NotApplied},
		{"forbidden", 403, "AccessDenied", core.ErrIO, core.NotApplied},
		{"internal", 500, "InternalError", core.ErrIO, core.MaybeApplied},
	}
	for _, c := range cases {
		s, f := newTestStore(t, "")
		if err := s.Put(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		f.inject = func(*http.Request) (int, string) { return c.status, c.code }
		before := f.calls
		ops := map[string]struct {
			act core.IOCall
			err error
		}{}
		_, gerr := s.Get(ctx, core.SegmentRef{Key: "k"})
		_, gerr2 := s.Get(ctx, core.SegmentRef{Key: "k", Off: 1, Len: 1})
		ops["put"] = struct {
			act core.IOCall
			err error
		}{core.ObjectPut{}, s.Put(ctx, "k", []byte("v"))}
		ops["get"] = struct {
			act core.IOCall
			err error
		}{core.ObjectGet{}, gerr}
		ops["get-range"] = struct {
			act core.IOCall
			err error
		}{core.ObjectGet{}, gerr2}
		ops["delete"] = struct {
			act core.IOCall
			err error
		}{core.ObjectDelete{}, s.Delete(ctx, "k")}
		_, lerr := s.List(ctx, "")
		ops["list"] = struct {
			act core.IOCall
			err error
		}{core.ObjectList{}, lerr}
		if f.calls-before != 5 {
			t.Errorf("%s: %d requests for 5 calls, the store must not retry", c.name, f.calls-before)
		}
		for name, op := range ops {
			if op.err == nil {
				t.Errorf("%s/%s: no error", c.name, name)
				continue
			}
			code, out := Classifier{}.Classify(op.act, op.err)
			wantOut := c.out
			if name == "get" || name == "get-range" || name == "list" {
				wantOut = core.NotApplied
			}
			if code != c.want || out != wantOut {
				t.Errorf("%s/%s: %v %v, want %v %v (%v)", c.name, name, code, out, c.want, wantOut, op.err)
			}
		}
	}
}

func TestStoreTimeout(t *testing.T) {
	release := make(chan struct{})
	slow := newSlow(release)
	t.Cleanup(slow.Close)
	t.Cleanup(func() { close(release) })
	st, err := New(t.Context(), Config{Endpoint: slow.URL, Bucket: "b", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = st.Put(ctx, "k", []byte("v"))
	// A deadline hit mid-request surfaces as an SDK ResponseError with status
	// 0; the classifier treats it as ErrIO/MaybeApplied (the executor's own
	// timeout, not this path, yields ErrTimeout).
	code, out := Classifier{}.Classify(core.ObjectPut{}, err)
	if code != core.ErrIO || out != core.MaybeApplied || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout: %v %v (%v)", code, out, err)
	}
}

func TestStoreDialAndHTTPClient(t *testing.T) {
	f := newFakeS3(t)
	url := f.srv.URL
	f.srv.Close()
	s, err := New(t.Context(), Config{Endpoint: url, Bucket: "b", PathStyle: true, HTTP: &http.Client{}, Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Put(t.Context(), "k", nil)
	code, out := Classifier{}.Classify(core.ObjectPut{}, err)
	if code != core.ErrUnavailable || out != core.NotApplied {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
	if err := s.EnsureBucket(t.Context()); err == nil {
		t.Error("EnsureBucket on a dead endpoint succeeded")
	}
	if !strings.HasPrefix(UniqueBucket(), "manifester-") {
		t.Error("UniqueBucket")
	}
}
