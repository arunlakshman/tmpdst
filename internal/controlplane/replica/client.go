package replica

import (
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// onControlPlaneDeliver handles one simulator-bridge ControlPlaneDeliver: reads wait for a fresh tail and answer from the fold when
// Ready; commits and creates go through Offer and ControlPlaneReply after apply.
func (m *Replica) onControlPlaneDeliver(o core.ControlPlaneDeliver) []core.Action {
	operation := o.Op
	switch m.meta.Mode(operation) {
	case module.Logged:
		cmd, _ := metadata.EntryOf(operation)
		return m.withReplies(m.Offer(ClientOperation{Req: o.Req, From: o.From, Cmd: cmd}))
	case module.ReadOnly:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.meta.Read(operation) })
	default:
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: core.ControlPlaneDone{Req: o.Req, Err: core.ErrInvalid}}}
	}
}

// offerRead queues a read behind a fresh tail check. A node that is not Ready
// refuses at once so the bridge tries the other replica.
func (m *Replica) offerRead(o core.ControlPlaneDeliver, read func() (core.ControlPlaneDone, error)) []core.Action {
	if !m.Ready() {
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: core.ControlPlaneDone{Req: o.Req, Err: core.ErrUnavailable}}}
	}
	m.queueRead(o.Req, o.From, read)
	return nil
}

// withReplies turns answers that have a broker From into ControlPlaneReply actions.
// Unit-test Offers without From stay in m.answers for TakeAnswers.
func (m *Replica) withReplies(acts []core.Action) []core.Action {
	var kept []ReplyToClient
	for _, a := range m.answers {
		if a.From.Node == "" {
			kept = append(kept, a)
			continue
		}
		acts = append(acts, core.ControlPlaneReply{To: a.From, Done: a.Done})
	}
	m.answers = kept
	return acts
}
