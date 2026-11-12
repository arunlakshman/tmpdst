//go:build integration

package s3store_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/manifesterio/mds/shared/objectstore"
	s3store "github.com/manifesterio/mds/shared/objectstore/s3"
	"github.com/manifesterio/mds/shared/status"
)

func wantCode(t *testing.T, err error, code status.ErrCode) {
	t.Helper()
	se, ok := errors.AsType[*s3store.Error](err)
	if !ok || se.Code != code {
		t.Fatalf("want %v, got %v", code, err)
	}
}

func TestStorePutGetDeleteList(t *testing.T) {
	ctx := t.Context()
	s, _, info := newTestStore(t, "p/")
	for k, v := range map[string]string{"a": "0123456789", "b/c": "xyz", "b/d": ""} {
		if err := s.Put(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := info.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &info.Bucket, Key: aws.String("p/a")}); err != nil {
		t.Fatalf("prefix not applied: %v", err)
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
		s, ft, _ := newTestStore(t, "")
		if err := s.Put(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		ft.set(func(r *http.Request) (*http.Response, error) { return s3ErrorResponse(r, c.status, c.code), nil })
		before := ft.calls.Load()
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
		if ft.calls.Load()-before != 5 {
			t.Errorf("%s: %d requests for 5 calls, the store must not retry", c.name, ft.calls.Load()-before)
		}
		for name, operation := range ops {
			if operation.err == nil {
				t.Errorf("%s/%s: no error", c.name, name)
				continue
			}
			code, out := s3store.Classifier{}.Classify(operation.act, operation.err)
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
	st, ft, _ := newTestStore(t, "")
	// The request never gets an answer: it waits for its context.
	ft.set(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := st.Put(ctx, "k", []byte("v"))
	// A deadline hit mid-request surfaces as an SDK ResponseError with status
	// 0; the classifier treats it as ErrIO/MaybeApplied (the executor's own
	// timeout, not this path, yields ErrTimeout).
	code, out := s3store.Classifier{}.Classify(objectstore.Put{}, err)
	if code != status.ErrIO || out != status.MaybeApplied || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout: %v %v (%v)", code, out, err)
	}
}
