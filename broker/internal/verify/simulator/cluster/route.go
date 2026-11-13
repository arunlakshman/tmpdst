package cluster

import (
	"fmt"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakenetwork"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// services are the addresses of the three fake services. Its route method is the
// route function of every node (brokers and workloads): one exhaustive type
// switch over core.Action. A new action without a case is a compile error (the
// gochecksumtype linter) and not a failure of one seed.
type services struct {
	world        *kernel.World
	objects      kernel.ServiceAddr[core.ObjectCall, core.EventData]
	controlPlane kernel.ServiceAddr[core.ControlPlaneCall, core.EventData]
	network      kernel.ServiceAddr[fakenetwork.Call, core.EventData]
}

func (s services) route(r *kernel.Router[core.EventData], out core.Action) {
	switch x := out.(type) {
	case core.RecordFact:
		r.Fact(x.Fact)
	case core.SetTimer:
		r.SetTimer(x.ID, max(x.After, 0), core.TimerFired{ID: x.ID, Gen: x.Gen})
	case core.CancelTimer:
		r.CancelTimer(x.ID)
	case core.StopForRestart:
		r.End(machine.Trip, x.Cause)
	case core.ShutdownDone:
		r.End(machine.Exit, "exit")
	case core.ObjectCall:
		kernel.Call(r, s.objects, x)
	case core.ControlPlaneCall:
		kernel.Call(r, s.controlPlane, x)
	case core.Connect:
		kernel.Call(r, s.network, fakenetwork.Call(fakenetwork.Dial{Connect: x}))
	case core.Send:
		kernel.Call(r, s.network, fakenetwork.Call(fakenetwork.Write{Send: x}))
	case core.CloseConn:
		kernel.Call(r, s.network, fakenetwork.Call(fakenetwork.Close{CloseConn: x}))
	case core.PauseReads:
		kernel.Call(r, s.network, fakenetwork.Call(fakenetwork.Pause{PauseReads: x}))
	case core.ResumeReads:
		kernel.Call(r, s.network, fakenetwork.Call(fakenetwork.Resume{ResumeReads: x}))
	case core.ControlPlaneReply:
		// The reply goes to a control plane replica. This run has none.
		s.world.Fail(kernel.FailProtocol, "unrouted_action", fmt.Sprintf("%T has no service in this run", x))
	}
}
