package prod

import (
	"errors"
	"log/slog"

	"manifester/internal/core"
	"manifester/internal/sys"
	"manifester/internal/verify/veritas/vsys"
)

// Classifier routes by action: CPCall to CP, Obj* to Objects, anything else
// is a bug that classifies as ErrIO, MaybeApplied.
type Classifier struct{ Objects, CP sys.Classifier }

var _ sys.Classifier = Classifier{}

// Classify maps an IO error to an error code and outcome.
func (c Classifier) Classify(a core.IOAction, err error) (core.ErrCode, core.Outcome) {
	switch a.(type) {
	case core.CPCall:
		return c.CP.Classify(a, err)
	case core.ObjPut, core.ObjGet, core.ObjDelete, core.ObjList:
		return c.Objects.Classify(a, err)
	}
	slog.Error("classify: unexpected action", "action", a, "err", err)
	return core.ErrIO, core.MaybeApplied
}

// MemClassifier classifies the in-memory object store (vsys), which fails only
// with truthful spec errors.
type MemClassifier struct{}

// Classify maps an in-memory store error to an error code and outcome.
func (MemClassifier) Classify(_ core.IOAction, err error) (core.ErrCode, core.Outcome) {
	if ve, ok := errors.AsType[*vsys.Error](err); ok {
		return ve.Code, core.NotApplied
	}
	return core.ErrIO, core.MaybeApplied
}
