package production

import (
	"log/slog"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/controlplane/client"
	"github.com/manifesterio/mds/controlplane/contract"
)

// The control plane client (controlplane/client) imports no broker package. These checks
// show at compile time that it still fits the broker interfaces.
var (
	_ system.ControlPlane    = (*client.Client)(nil)
	_ system.ErrorClassifier = ControlPlaneClassifier{}
)

// ControlPlaneClassifier is the system.ErrorClassifier for ControlPlaneCall (lesson 1).
// The rules are in client.Classify. This type only takes the operation out of the action.
// An action that is not a ControlPlaneCall counts as a write.
type ControlPlaneClassifier struct{}

// Classify maps a failed control plane call to its error code and outcome.
func (ControlPlaneClassifier) Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	var op contract.Operation
	if call, ok := a.(core.ControlPlaneCall); ok {
		op = call.Op
	}
	code, out := client.Classify(op, err)
	slog.Debug("cp call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}
