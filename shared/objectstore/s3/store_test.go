package s3

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/manifesterio/mds/shared/objectstore"
	"github.com/manifesterio/mds/shared/objectstore/s3/fakes3"
	"github.com/manifesterio/mds/shared/status"
)

func newTestStore(t *testing.T, prefix string) (*Store, *fakes3.Server) {
	t.Helper()
	f := fakes3.New(t)
	s, err := New(t.Context(), Config{Endpoint: f.URL, Bucket: "b", Prefix: prefix, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, f
}

func wantCode(t *testing.T, err error, code status.ErrCode) {
	t.Helper()
	se, ok := errors.AsType[*Error](err)
	if !ok || se.Code != code {
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
	if _, ok := f.Objs["b/p/a"]; !ok {
		t.Fatalf("prefix not applied: %v", f.Objs)
	}
	wantCode(t, s.Put(ctx, "", nil), status.ErrInvalid)
	if err := s.EnsureBucket(ctx); err != nil { // already owned
		t.Fatal(err)
	}

	get := func(key string, off, n int64) ([]byte, error) {
		return s.Get(ctx, objectstore.SegmentRef{Key: key, Off: off, Len: n})
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
	wantCode(t, err, status.ErrNotFound)
	_, err = get("missing", 0, 0)
	wantCode(t, err, status.ErrNotFound)
	_, err = get("missing", 2, 3)
	wantCode(t, err, status.ErrNotFound)
	_, err = get("a", 20, 0)
	wantCode(t, err, status.ErrInvalid)
	_, err = get("a", 8, 5) // runs past the end: server clips, store notices
	wantCode(t, err, status.ErrInvalid)
	_, err = get("a", -1, 0)
	wantCode(t, err, status.ErrInvalid)
	_, err = get("missing", -1, 0)
	wantCode(t, err, status.ErrNotFound)

	keys, err := s.List(ctx, "b/")
	if err != nil || len(keys) != 2 || keys[0] != "b/c" || keys[1] != "b/d" {
		t.Errorf("list %v %v", keys, err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, s.Delete(ctx, "a"), status.ErrNotFound)
	wantCode(t, s.Delete(ctx, ""), status.ErrNotFound)
}

func TestStoreServerErrors(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name   string
		status int
		code   string
		want   status.ErrCode
		out    status.Outcome
	}{
		{"slowdown", 503, "SlowDown", status.ErrUnavailable, status.NotApplied},
		{"throttled", 429, "Throttling", status.ErrUnavailable, status.NotApplied},
		{"forbidden", 403, "AccessDenied", status.ErrIO, status.NotApplied},
		{"internal", 500, "InternalError", status.ErrIO, status.MaybeApplied},
	}
	for _, c := range cases {
		s, f := newTestStore(t, "")
		if err := s.Put(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		f.Inject = func(*http.Request) (int, string) { return c.status, c.code }
		before := f.Calls
		ops := map[string]struct {
			act objectstore.Call
			err error
		}{}
		_, gerr := s.Get(ctx, objectstore.SegmentRef{Key: "k"})
		_, gerr2 := s.Get(ctx, objectstore.SegmentRef{Key: "k", Off: 1, Len: 1})
		ops["put"] = struct {
			act objectstore.Call
			err error
		}{objectstore.Put{}, s.Put(ctx, "k", []byte("v"))}
		ops["get"] = struct {
			act objectstore.Call
			err error
		}{objectstore.Get{}, gerr}
		ops["get-range"] = struct {
			act objectstore.Call
			err error
		}{objectstore.Get{}, gerr2}
		ops["delete"] = struct {
			act objectstore.Call
			err error
		}{objectstore.Delete{}, s.Delete(ctx, "k")}
		_, lerr := s.List(ctx, "")
		ops["list"] = struct {
			act objectstore.Call
			err error
		}{objectstore.List{}, lerr}
		if f.Calls-before != 5 {
			t.Errorf("%s: %d requests for 5 calls, the store must not retry", c.name, f.Calls-before)
		}
		for name, operation := range ops {
			if operation.err == nil {
				t.Errorf("%s/%s: no error", c.name, name)
				continue
			}
			code, out := Classifier{}.Classify(operation.act, operation.err)
			wantOut := c.out
			if name == "get" || name == "get-range" || name == "list" {
				wantOut = status.NotApplied
			}
			if code != c.want || out != wantOut {
				t.Errorf("%s/%s: %v %v, want %v %v (%v)", c.name, name, code, out, c.want, wantOut, operation.err)
			}
		}
	}
}

func TestStoreTimeout(t *testing.T) {
	release := make(chan struct{})
	slow := fakes3.NewSlow(release)
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
	code, out := Classifier{}.Classify(objectstore.Put{}, err)
	if code != status.ErrIO || out != status.MaybeApplied || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout: %v %v (%v)", code, out, err)
	}
}

func TestStoreDialAndHTTPClient(t *testing.T) {
	f := fakes3.New(t)
	url := f.URL
	f.Close()
	s, err := New(t.Context(), Config{Endpoint: url, Bucket: "b", PathStyle: true, HTTP: &http.Client{}, Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Put(t.Context(), "k", nil)
	code, out := Classifier{}.Classify(objectstore.Put{}, err)
	if code != status.ErrUnavailable || out != status.NotApplied {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
	if err := s.EnsureBucket(t.Context()); err == nil {
		t.Error("EnsureBucket on a dead endpoint succeeded")
	}
	if !strings.HasPrefix(UniqueBucket(), "mds-") {
		t.Error("UniqueBucket")
	}
}
