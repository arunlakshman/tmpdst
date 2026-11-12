// Package system is the production seam: narrow interfaces over real I/O that the
// production Executor calls from goroutines. SDK and HTTP types never leak past
// an adapter. The simulator does NOT implement these; it implements simulator.Engine
// over the same Action/Event vocabulary (docs/lessons.md, section 0).
package system

import (
	"context"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// ObjectStore is the S3 adapter.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// ControlPlane is the HTTP client adapter for the control plane.
type ControlPlane interface {
	Call(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneResponse, error)
}

// ErrorClassifier is the single place SDK/HTTP/net errors become plain data
// (lesson 1). Getting Outcome right here is what makes acks honest.
type ErrorClassifier interface {
	// Classify reduces err to plain data. It logs err; the machine only sees the
	// code. The Outcome depends on which operation failed, not only on err.
	Classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome)
}
