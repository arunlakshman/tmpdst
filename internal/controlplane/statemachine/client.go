package statemachine

import (
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

// onCPOffer handles one sim-bridge ControlPlaneDeliver: reads answer from the fold when
// Ready; commits and creates go through Offer and ControlPlaneReply after apply.
func (m *Machine) onCPOffer(o core.ControlPlaneDeliver) []core.Action {
	switch op := o.Op.(type) {
	case core.CommitSegment:
		return m.withReplies(m.Offer(ClientOperation{
			Req: o.Req, From: o.From,
			Cmd: stateupdater.Entry{Kind: stateupdater.KindCommit, Commit: op},
		}))
	case core.FindSegments:
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: m.readFold(o.Req, func() (core.ControlPlaneDone, error) {
			return m.fold.Lookup(op)
		})}}
	case core.DescribeTopics:
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: m.readFold(o.Req, func() (core.ControlPlaneDone, error) {
			return m.fold.Describe(op)
		})}}
	default:
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: core.ControlPlaneDone{Req: o.Req, Err: core.ErrInvalid}}}
	}
}

func (m *Machine) readFold(req core.RequestID, fn func() (core.ControlPlaneDone, error)) core.ControlPlaneDone {
	if !m.Ready() {
		return core.ControlPlaneDone{Req: req, Err: core.ErrUnavailable}
	}
	d, err := fn()
	if err != nil {
		return core.ControlPlaneDone{Req: req, Err: core.ErrIO}
	}
	d.Req = req
	return d
}

// withReplies turns answers that have a broker From into ControlPlaneReply actions.
// Unit-test Offers without From stay in m.answers for TakeAnswers.
func (m *Machine) withReplies(acts []core.Action) []core.Action {
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
