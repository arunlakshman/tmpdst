package replica

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// onControlPlaneDeliver handles one simulator-bridge ControlPlaneDeliver: reads wait for a fresh tail and answer from the fold when
// Ready; commits and creates go through Offer and ControlPlaneReply after apply.
func (m *Replica) onControlPlaneDeliver(o runtime.ControlPlaneDeliver) []runtime.Action {
	operation := o.Op
	switch m.meta.Mode(operation) {
	case metadb.Logged:
		cmd, _ := metadb.EntryOf(operation)
		return m.withReplies(m.Offer(ClientOperation{Req: o.Req, From: o.From, Cmd: cmd}))
	case metadb.ReadOnly:
		return m.offerRead(o, func() (runtime.ControlPlaneDone, error) { return m.meta.Read(operation) })
	default:
		return []runtime.Action{runtime.ControlPlaneReply{To: o.From, Done: runtime.ControlPlaneDone{Req: o.Req, Err: runtime.ErrInvalid}}}
	}
}

// onLocalRead answers a read from the fold of this node at once, if the node
// is Ready. It does not wait for a fresh tail, so it is as fast as a local
// lookup and can be a little behind the log. A node that is not Ready refuses
// at once so the caller can try another node.
func (m *Replica) onLocalRead(o runtime.LocalRead) []runtime.Action {
	reply := func(d runtime.ControlPlaneDone) []runtime.Action {
		d.Req = o.Req
		return []runtime.Action{runtime.ControlPlaneReply{To: o.From, Done: d}}
	}
	if !m.Ready() {
		return reply(runtime.ControlPlaneDone{Err: runtime.ErrUnavailable})
	}
	if m.meta.Mode(o.Op) != metadb.ReadOnly {
		return reply(runtime.ControlPlaneDone{Err: runtime.ErrInvalid})
	}
	d, err := m.meta.Read(o.Op)
	if err != nil {
		return reply(runtime.ControlPlaneDone{Err: runtime.ErrIO, Outcome: runtime.NotApplied})
	}
	return reply(d)
}

// offerRead queues a read behind a fresh tail check. A node that is not Ready
// refuses at once so the bridge tries the other replica.
func (m *Replica) offerRead(o runtime.ControlPlaneDeliver, read func() (runtime.ControlPlaneDone, error)) []runtime.Action {
	if !m.Ready() {
		return []runtime.Action{runtime.ControlPlaneReply{To: o.From, Done: runtime.ControlPlaneDone{Req: o.Req, Err: runtime.ErrUnavailable}}}
	}
	m.queueRead(o.Req, o.From, read)
	return nil
}

// withReplies turns answers that have a broker From into ControlPlaneReply actions.
// Unit-test Offers without From stay in m.answers (the tests read them with TakeAnswers).
func (m *Replica) withReplies(acts []runtime.Action) []runtime.Action {
	var kept []ReplyToClient
	for _, a := range m.answers {
		if a.From.Node == "" {
			kept = append(kept, a)
			continue
		}
		acts = append(acts, runtime.ControlPlaneReply{To: a.From, Done: a.Done})
	}
	m.answers = kept
	return acts
}
