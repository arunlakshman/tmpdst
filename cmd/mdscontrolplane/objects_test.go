package main

import (
	"context"
	"testing"

	"mds/internal/controlplane/executor"
	"mds/internal/core"
	"mds/internal/system"
	"mds/internal/system/s3store"
)

// missingObjectStore answers every Get like a real S3 bucket with no such key.
type missingObjectStore struct{ system.ObjectStore }

func (missingObjectStore) Get(context.Context, core.SegmentRef) ([]byte, error) {
	return nil, &s3store.Error{Code: core.ErrNotFound}
}

// A missing checkpoint must look like "not found" to the executor. If it looks
// like a read failure, the machine retries forever and a new bucket never boots.
func TestMissingCheckpointIsNotFound(t *testing.T) {
	objects := sysObjects{missingObjectStore{}}
	_, err := objects.Get(t.Context(), core.SegmentRef{Key: "checkpoint/latest"})
	code, _ := executor.Classify(core.ObjectGet{}, err)
	if code != core.ErrNotFound {
		t.Fatalf("missing object classified as %v, want ErrNotFound", code)
	}
}
