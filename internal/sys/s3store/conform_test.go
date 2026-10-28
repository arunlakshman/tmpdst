package s3store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"manifester/internal/core"
	"manifester/internal/sys"
	"manifester/internal/verify/veritas/veritastest/observation"
	"manifester/internal/verify/veritas/veritastest/plantedbug"
	"manifester/internal/verify/veritas/veritastest/runcontrol"
	"manifester/internal/verify/veritas/veritastest/suites"
	"manifester/internal/verify/veritas/vsys"
)

// Conformance C3: the shared object store suite runs on the S3 adapter and on
// the in-process fake. Both are compared with veritas.ObjectStore.

// codeOf turns an error of the adapter or of the fake into a core.ErrCode.
// Any other error counts as ErrIO, as the broker would see it.
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

// storeSubject makes a sys.ObjectStore usable as a suite subject. Neither the
// fake nor real S3 can give the hash of the spec, so StateHash says no.
type storeSubject struct{ store sys.ObjectStore }

func (s storeSubject) Do(op core.IOCall) (observation.ObjectResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch op := op.(type) {
	case core.ObjectPut:
		return writeResult(s.store.Put(ctx, op.Key, op.Data)), nil
	case core.ObjectGet:
		data, err := s.store.Get(ctx, op.Ref)
		if err != nil {
			return observation.ObjectResult{Code: codeOf(err)}, nil
		}
		return observation.ObjectResult{Data: data}, nil
	case core.ObjectDelete:
		return writeResult(s.store.Delete(ctx, op.Key)), nil
	case core.ObjectList:
		keys, err := s.store.List(ctx, op.Prefix)
		if err != nil {
			return observation.ObjectResult{Code: codeOf(err)}, nil
		}
		return observation.ObjectResult{Keys: keys}, nil
	}
	return observation.ObjectResult{}, fmt.Errorf("the operation type %T is not an object store operation", op)
}

// writeResult follows the spec: a write that succeeds is Applied, and a write
// that fails is NotApplied.
func writeResult(err error) observation.ObjectResult {
	if err != nil {
		return observation.ObjectResult{Code: codeOf(err), Outcome: core.NotApplied}
	}
	return observation.ObjectResult{Code: core.OK, Outcome: core.Applied}
}

func (storeSubject) StateHash() (uint64, bool) { return 0, false }

func newFake() suites.ObjectSubject { return storeSubject{vsys.NewObjectStore()} }

func runProfiles(t *testing.T, newSubject func() suites.ObjectSubject, defaultSeeds int) {
	for _, profile := range suites.AllObjectProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ObjectStore(t, profile, newSubject, runcontrol.Options(t, defaultSeeds))
		})
	}
}

func TestConformanceMem(t *testing.T) { runProfiles(t, newFake, 100) }

// newFakeS3Subject returns the S3 adapter on the in-process fake S3 server.
// It runs without a network, so it always runs.
func newFakeS3Subject(t *testing.T) func() suites.ObjectSubject {
	return func() suites.ObjectSubject {
		f := newFakeS3(t)
		s, err := New(t.Context(), Config{Endpoint: f.srv.URL, Bucket: "b", Prefix: "c3/", PathStyle: true})
		if err != nil {
			t.Fatal(err)
		}
		return storeSubject{s}
	}
}

func TestConformanceFake(t *testing.T) { runProfiles(t, newFakeS3Subject(t), 8) }

// TestConformanceS3 runs against a real S3 endpoint (SeaweedFS) only when
// MANIFESTER_S3_ENDPOINT is set. Every sequence gets its own key prefix.
func TestConformanceS3(t *testing.T) {
	ep := os.Getenv("MANIFESTER_S3_ENDPOINT")
	if ep == "" {
		t.Skip("The environment variable MANIFESTER_S3_ENDPOINT is not set, so the test of the real S3 endpoint is skipped.")
	}
	base := UniqueBucket()
	n := 0
	runProfiles(t, func() suites.ObjectSubject {
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
		return storeSubject{s}
	}, 50)
}

// TestPlantedBugsCaught proves that the suite fails when the adapter on the
// fake S3 server has a known bug, so a pass of TestConformanceFake means
// something.
func TestPlantedBugsCaught(t *testing.T) {
	plantedbug.RequireAllObjectBugsCaught(t, newFakeS3Subject(t), 100)
}

// TestPlantedBugsCaughtMem does the same for the in-process fake store.
func TestPlantedBugsCaughtMem(t *testing.T) { plantedbug.RequireAllObjectBugsCaught(t, newFake, 100) }
