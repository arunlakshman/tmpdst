package system

import (
	"fmt"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
)

// Host is what the route function of one broker process needs from outside.
type Host struct {
	// Exec runs the I/O actions. It must not block.
	Exec Executor
	// Connections gets CloseConn, PauseReads and ResumeReads.
	Connections Connections
	// Facts, if set, gets the facts of RecordFact (tracing).
	Facts func(machine.Fact)
}

// Loop is the loop type of the broker: the input of the machine is core.EventData.
type Loop = live.Loop[core.EventData]

// Route returns the route function for live.Config. It runs on the loop goroutine, in
// the order of the outputs of one Step. Every case of core.Action is listed, so a
// new action without a case is easy to see. It returns an error for an action that a
// production process does not know (ControlPlaneReply is for the simulator only: the
// production control plane client answers a call in the same ControlPlaneDone).
//
// The inbox class of an input that the loop delivers itself (a timer that fired) is
// core.InboxKindOf. Use it as live.Config.Class.
func Route(h Host) func(l *Loop, out core.Action) error {
	return func(l *Loop, out core.Action) error {
		switch a := out.(type) {
		case core.Send:
			h.Exec.Execute(a)
		case core.Connect:
			h.Exec.Execute(a)
		case core.ObjectCall:
			h.Exec.Execute(a)
		case core.ControlPlaneCall:
			h.Exec.Execute(a)
		case core.RecordFact:
			if h.Facts != nil {
				h.Facts(a.Fact)
			}
		case core.SetTimer:
			l.SetTimer(a.ID, a.After, core.TimerFired{ID: a.ID, Gen: a.Gen})
		case core.CancelTimer:
			l.CancelTimer(a.ID)
		case core.CloseConn:
			_ = h.Connections.Close(a.Conn, a.Abort)
		case core.PauseReads:
			h.Connections.PauseReads(a.Conn)
		case core.ResumeReads:
			h.Connections.ResumeReads(a.Conn)
		case core.StopForRestart:
			l.End(machine.Trip, a.Cause)
		case core.ShutdownDone:
			l.End(machine.Exit, "")
		case core.ControlPlaneReply:
			return fmt.Errorf("system: unknown action %T", a)
		default:
			return fmt.Errorf("system: unknown action %T", a)
		}
		return nil
	}
}
