package replica

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// readWaiter is one lookup or describe that waits for a fresh view.
// A read is answered only after this node has applied the log tail that was
// read AFTER the read arrived (docs/log-cp-service.md, "Lookup / Describe").
// Without it a standby could answer from an old fold and show a consumer an
// end offset below what was already acked.
type readWaiter struct {
	request machine.RequestID
	from    machine.NodeInstance
	read    func() (runtime.ControlPlaneDone, error)
	round   uint64              // first tail check that started after the read arrived
	goal    runtime.LogPosition // tail seen by that check; valid when haveGoal
	have    bool
}

// queueRead holds a read until a tail check that starts after now has been applied.
func (m *Replica) queueRead(request machine.RequestID, from machine.NodeInstance, read func() (runtime.ControlPlaneDone, error)) {
	m.readWaiters = append(m.readWaiters, readWaiter{request: request, from: from, read: read, round: m.tailsStarted + 1})
}

// noteTail gives the waiters covered by tail check `round` their goal slot.
func (m *Replica) noteTail(round uint64, tail runtime.LogPosition) {
	for i := range m.readWaiters {
		w := &m.readWaiters[i]
		if !w.have && w.round <= round {
			w.goal, w.have = tail, true
		}
	}
}

// serveReads answers every waiter whose goal slot is applied.
func (m *Replica) serveReads() []runtime.Action {
	if len(m.readWaiters) == 0 || !m.ready {
		return nil
	}
	var acts []runtime.Action
	keep := m.readWaiters[:0]
	for _, w := range m.readWaiters {
		if !w.have || m.applied < w.goal {
			keep = append(keep, w)
			continue
		}
		acts = append(acts, runtime.ControlPlaneReply{To: w.from, Done: m.readDone(w.request, w.read)})
	}
	m.readWaiters = keep
	return acts
}

// refuseReads answers every waiting read with Unavailable so the bridge can
// try the other replica at once.
func (m *Replica) refuseReads() []runtime.Action {
	var acts []runtime.Action
	for _, w := range m.readWaiters {
		acts = append(acts, runtime.ControlPlaneReply{To: w.from, Done: runtime.ControlPlaneDone{Req: w.request, Err: runtime.ErrUnavailable}})
	}
	m.readWaiters = nil
	return acts
}

func (m *Replica) readDone(request machine.RequestID, read func() (runtime.ControlPlaneDone, error)) runtime.ControlPlaneDone {
	d, err := read()
	if err != nil {
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrIO}
	}
	d.Req = request
	return d
}
