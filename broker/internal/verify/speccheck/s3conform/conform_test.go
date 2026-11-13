package s3conform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/injectedbug"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specsystem"
	"github.com/manifesterio/mds/shared/objectstore"
	s3store "github.com/manifesterio/mds/shared/objectstore/s3"
	"github.com/manifesterio/mds/shared/objectstore/s3/fakes3"
)

// Conformance C3: the shared object store suite runs on the S3 adapter and on
// the in-process fake. Both are compared with objectstore.Spec.

// codeOf turns an error of the adapter or of the fake into a core.ErrCode.
// Any other error counts as ErrIO, as the broker would see it.
func codeOf(err error) core.ErrCode {
	if err == nil {
		return core.OK
	}
	if se, ok := errors.AsType[*s3store.Error](err); ok {
		return se.Code
	}
	if ve, ok := errors.AsType[*specsystem.Error](err); ok {
		return ve.Code
	}
	return core.ErrIO
}

// storeSubject makes a objectstore.Store usable as a suite subject. Neither the
// fake nor real S3 can give the hash of the spec, so it has no StateHash method.
type storeSubject struct{ store objectstore.Store }

func (s storeSubject) Apply(operation objectstore.Call) (conformance.ObjectResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch call := operation.(type) {
	case objectstore.Put:
		return writeResult(s.store.Put(ctx, call.Key, call.Data)), nil
	case objectstore.Get:
		data, err := s.store.Get(ctx, call.Ref)
		if err != nil {
			return conformance.ObjectResult{Code: codeOf(err)}, nil
		}
		return conformance.ObjectResult{Data: data}, nil
	case objectstore.Delete:
		return writeResult(s.store.Delete(ctx, call.Key)), nil
	case objectstore.List:
		keys, err := s.store.List(ctx, call.Prefix)
		if err != nil {
			return conformance.ObjectResult{Code: codeOf(err)}, nil
		}
		return conformance.ObjectResult{Keys: keys}, nil
	}
	return conformance.ObjectResult{}, fmt.Errorf("the operation type %T is not an object store operation", operation)
}

// writeResult follows the spec: a write that succeeds is Applied, and a write
// that fails is NotApplied.
func writeResult(err error) conformance.ObjectResult {
	if err != nil {
		return conformance.ObjectResult{Code: codeOf(err), Outcome: core.NotApplied}
	}
	return conformance.ObjectResult{Code: core.OK, Outcome: core.Applied}
}

func newFake() conformance.ObjectSubject { return storeSubject{specsystem.NewObjectStore()} }

// runProfiles runs the object store suite on every profile. checks is the
// number of checks (random lists of steps) for each profile.
func runProfiles(t *testing.T, newSubject func() conformance.ObjectSubject, checks int) {
	for _, profile := range conformance.InputProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			l := conformance.ObjectStoreLockstep(profile, newSubject)
			l.Checks = checks
			l.Check(t)
		})
	}
}

func TestConformanceMem(t *testing.T) { runProfiles(t, newFake, 100) }

// newFakeS3Subject returns the S3 adapter on the in-process fake S3 server.
// It runs without a network, so it always runs.
func newFakeS3Subject(t *testing.T) func() conformance.ObjectSubject {
	return func() conformance.ObjectSubject {
		f := fakes3.New(t)
		s, err := s3store.New(t.Context(), s3store.Config{Endpoint: f.URL, Bucket: "b", Prefix: "c3/", PathStyle: true})
		if err != nil {
			t.Fatal(err)
		}
		return storeSubject{s}
	}
}

func TestConformanceFake(t *testing.T) { runProfiles(t, newFakeS3Subject(t), 8) }

// TestConformanceS3 runs against a real S3 endpoint (SeaweedFS) only when
// MDS_S3_ENDPOINT is set. Every sequence gets its own key prefix.
func TestConformanceS3(t *testing.T) {
	ep := os.Getenv("MDS_S3_ENDPOINT")
	if ep == "" {
		t.Skip("The environment variable MDS_S3_ENDPOINT is not set, so the test of the real S3 endpoint is skipped.")
	}
	base := s3store.UniqueBucket()
	n := 0
	runProfiles(t, func() conformance.ObjectSubject {
		s, err := s3store.New(t.Context(), s3store.Config{Endpoint: ep, Bucket: base, Prefix: fmt.Sprintf("c3-%d/", n), PathStyle: true})
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
	injectedbug.ObjectBugsCaught(t, newFakeS3Subject(t), 100)
}

// TestPlantedBugsCaughtMem does the same for the in-process fake store.
func TestPlantedBugsCaughtMem(t *testing.T) {
	injectedbug.ObjectBugsCaught(t, newFake, 100)
}
