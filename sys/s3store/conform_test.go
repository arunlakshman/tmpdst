package s3store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"manifester/core"
	"manifester/sys"
	"manifester/veritas"
	"manifester/veritas/veritastest"
	"manifester/veritas/vsys"
)

// Conformance C3: the same random operations against the store under test and
// a fresh veritas.ObjectStore, every result compared.

type pair struct {
	impl sys.ObjectStore
	spec *veritas.ObjectStore
}

func codeOf(err error) core.ErrCode {
	var se *Error
	var ve *vsys.Error
	switch {
	case err == nil:
		return core.OK
	case errors.As(err, &se):
		return se.Code
	case errors.As(err, &ve):
		return ve.Code
	}
	return core.ErrIO
}

func step(p pair, a core.IOAction) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got, want string
	switch a := a.(type) {
	case core.ObjPut:
		got = codeOf(p.impl.Put(ctx, a.Key, a.Data)).String()
		want = p.spec.Exec(a).(core.ObjPutDone).Err.String()
	case core.ObjGet:
		d, err := p.impl.Get(ctx, a.Ref)
		got = fmt.Sprintf("%v %q", codeOf(err), d)
		w := p.spec.Exec(a).(core.ObjGetDone)
		want = fmt.Sprintf("%v %q", w.Err, w.Data)
		if w.Err != core.OK {
			want = fmt.Sprintf("%v %q", w.Err, []byte(nil))
		}
	case core.ObjDelete:
		got = codeOf(p.impl.Delete(ctx, a.Key)).String()
		want = p.spec.Exec(a).(core.ObjDeleteDone).Err.String()
	case core.ObjList:
		k, err := p.impl.List(ctx, a.Prefix)
		got = fmt.Sprintf("%v %q", codeOf(err), k)
		w := p.spec.Exec(a).(core.ObjListDone)
		want = fmt.Sprintf("%v %q", w.Err, w.Keys)
	}
	if got != want {
		return fmt.Errorf("impl %s, spec %s", got, want)
	}
	return nil
}

func check(newImpl func() sys.ObjectStore) veritastest.Check[pair, core.IOAction] {
	keys := []string{"", "a", "a/b", "a/c", "b"}
	prefixes := []string{"", "a", "a/", "b", "z"}
	return veritastest.Check[pair, core.IOAction]{
		Name: "core.IOAction",
		New:  func() pair { return pair{newImpl(), veritas.NewObjectStore()} },
		Gen: func(r *veritastest.Rand, i int) core.IOAction {
			req, key := core.ReqID(i+1), keys[r.Intn(len(keys))]
			switch r.Intn(7) {
			case 0, 1, 2:
				data := make([]byte, r.Intn(5))
				for j := range data {
					data[j] = byte('a' + r.Intn(26))
				}
				return core.ObjPut{Req: req, Key: key, Data: data}
			case 3, 4:
				return core.ObjGet{Req: req, Ref: core.SegmentRef{Key: key, Off: int64(r.Intn(7) - 1), Len: int64(r.Intn(7) - 1)}}
			case 5:
				return core.ObjDelete{Req: req, Key: key}
			}
			return core.ObjList{Req: req, Prefix: prefixes[r.Intn(len(prefixes))]}
		},
		Step:          step,
		Format:        func(a core.IOAction) string { return fmt.Sprintf("%#v", a) },
		Ops:           60,
		MaxShrinkRuns: 200,
	}
}

func run(t *testing.T, newImpl func() sys.ObjectStore, seeds int) {
	t.Helper()
	veritastest.Run(t, check(newImpl), veritastest.Options{Seeds: seeds, Seed: -1})
}

func TestConformanceMem(t *testing.T) {
	run(t, func() sys.ObjectStore { return vsys.NewObjectStore() }, 100)
}

// TestConformanceS3 runs against a real S3 endpoint (SeaweedFS) when
// MANIFESTER_S3_ENDPOINT is set; every sequence gets its own key prefix.
func TestConformanceS3(t *testing.T) {
	ep := os.Getenv("MANIFESTER_S3_ENDPOINT")
	if ep == "" {
		t.Skip("MANIFESTER_S3_ENDPOINT not set")
	}
	base := UniqueBucket()
	n := 0
	run(t, func() sys.ObjectStore {
		s, err := New(t.Context(), Config{Endpoint: ep, Bucket: base, Prefix: fmt.Sprintf("c3-%d/", n), PathStyle: true})
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			if err := s.EnsureBucket(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		n++
		return s
	}, 50)
}
