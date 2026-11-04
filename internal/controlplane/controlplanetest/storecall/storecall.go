// Package storecall gives tests a typed view of a store's single Exec method.
package storecall

import (
	"context"

	"github.com/manifesterio/mds/internal/core"
)

// Executor is anything with the single Exec method of the test stores.
type Executor = core.OperationRunner

// Call runs an operation that has a response and a business code.
func Call[R any](ctx context.Context, s Executor, operation core.ControlPlaneOperation) (R, core.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return core.ResponseAs[R](d), d.Err, err
}

// Code runs an operation that has only a business code.
func Code(ctx context.Context, s Executor, operation core.ControlPlaneOperation) (core.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return d.Err, err
}

// Read runs an operation that has a response and no business code.
func Read[R any](ctx context.Context, s Executor, operation core.ControlPlaneOperation) (R, error) {
	d, err := s.Exec(ctx, operation)
	return core.ResponseAs[R](d), err
}
