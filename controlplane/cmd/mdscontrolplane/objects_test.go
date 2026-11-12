package main

import (
	"context"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/system"
	"github.com/manifesterio/mds/controlplane/internal/system/s3store"
)

// missingObjectStore answers every Get like a real S3 bucket with no such key.
type missingObjectStore struct{ system.ObjectStore }

func (missingObjectStore) Get(context.Context, runtime.SegmentRef) ([]byte, error) {
	return nil, &s3store.Error{Code: runtime.ErrNotFound}
}

// A missing checkpoint must look like "not found" to the executor. If it looks
// like a read failure, the machine retries forever and a new bucket never boots.
func TestMissingCheckpointIsNotFound(t *testing.T) {
	objects := sysObjects{missingObjectStore{}}
	_, err := objects.Get(t.Context(), runtime.SegmentRef{Key: "checkpoint/00000000000000000001"})
	code, _ := executor.Classify(runtime.ObjectGet{}, err)
	if code != runtime.ErrNotFound {
		t.Fatalf("missing object classified as %v, want ErrNotFound", code)
	}
}
