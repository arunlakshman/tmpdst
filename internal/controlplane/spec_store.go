package controlplane

import (
	"context"

	"github.com/manifesterio/mds/internal/core"
)

// Store is the contract for the four segment and topic calls: CommitSegment,
// Lookup, DescribeTopics and CreateTopic. Each method must act the same as
// speccheck.ControlPlane, op for op, and be safe for concurrent use.
//
// A business error is a non-OK core.ErrCode with a nil error. The error result
// is for infrastructure failures only.
//
// These types implement Store in this repo:
//   - memorystore.Store: an in-memory store. Only tests use it.
//   - stateupdater.Store: an adapter over stateupdater with a local slot
//     counter. Only tests use it.
//
// No non-test code takes a Store as a parameter, stores one, or calls one.
type Store interface {
	CommitSegment(ctx context.Context, operation core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error)
	Lookup(ctx context.Context, operation core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error)
	DescribeTopics(ctx context.Context, operation core.DescribeTopics) (core.DescribeTopicsResp, error)
	CreateTopic(ctx context.Context, operation core.CreateTopic) (core.ErrCode, error)
}
