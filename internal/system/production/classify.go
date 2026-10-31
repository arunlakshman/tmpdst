package production

import (
	"errors"
	"log/slog"

	"mds/internal/core"
	"mds/internal/system"
	"mds/internal/verify/speccheck/specsystem"
)

// Classifier routes by action: ControlPlaneCall to CP, Obj* to Objects, anything else
// is a bug that classifies as ErrIO, MaybeApplied.
type Classifier struct{ Objects, CP system.ErrorClassifier }

var _ system.ErrorClassifier = Classifier{}

// Classify maps an IO error to an error code and outcome.
func (c Classifier) Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	switch a.(type) {
	case core.ControlPlaneCall:
		return c.CP.Classify(a, err)
	case core.ObjectPut, core.ObjectGet, core.ObjectDelete, core.ObjectList:
		return c.Objects.Classify(a, err)
	}
	slog.Error("classify: unexpected action", "action", a, "err", err)
	return core.ErrIO, core.MaybeApplied
}

// MemoryErrorClassifier classifies the in-memory object store (specsystem), which fails only
// with truthful spec errors.
type MemoryErrorClassifier struct{}

// Classify maps an in-memory store error to an error code and outcome.
func (MemoryErrorClassifier) Classify(_ core.IOCall, err error) (core.ErrCode, core.Outcome) {
	if ve, ok := errors.AsType[*specsystem.Error](err); ok {
		return ve.Code, core.NotApplied
	}
	return core.ErrIO, core.MaybeApplied
}
