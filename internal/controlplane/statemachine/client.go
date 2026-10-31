package statemachine

import (
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// onControlPlaneDeliver handles one simulator-bridge ControlPlaneDeliver: reads wait for a fresh tail and answer from the fold when
// Ready; commits and creates go through Offer and ControlPlaneReply after apply.
func (m *Machine) onControlPlaneDeliver(o core.ControlPlaneDeliver) []core.Action {
	switch op := o.Op.(type) {
	case core.CommitSegment:
		return m.withReplies(m.Offer(ClientOperation{
			Req: o.Req, From: o.From,
			Cmd: stateupdater.Entry{Kind: stateupdater.KindCommit, Commit: op},
		}))
	case core.FindSegments:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.fold.Lookup(op) })
	case core.DescribeTopics:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.fold.Describe(op) })
	case core.CreateTopic:
		return m.withReplies(m.Offer(ClientOperation{
			Req: o.Req, From: o.From,
			Cmd: stateupdater.CreateEntry(op),
		}))
	case core.OffsetFetch:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.fold.OffsetFetch(op) })
	case core.DescribeGroup:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.fold.DescribeGroup(op) })
	case core.ListGroups:
		return m.offerRead(o, func() (core.ControlPlaneDone, error) { return m.fold.ListGroups(op) })
	case core.GroupInstall, core.GroupAssign, core.GroupRemove, core.OffsetCommit:
		cmd, _ := stateupdater.GroupEntry(op)
		return m.withReplies(m.Offer(ClientOperation{Req: o.Req, From: o.From, Cmd: cmd}))
	default:
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: core.ControlPlaneDone{Req: o.Req, Err: core.ErrInvalid}}}
	}
}

// offerRead queues a read behind a fresh tail check. A node that is not Ready
// refuses at once so the bridge tries the other replica.
func (m *Machine) offerRead(o core.ControlPlaneDeliver, read func() (core.ControlPlaneDone, error)) []core.Action {
	if !m.Ready() {
		return []core.Action{core.ControlPlaneReply{To: o.From, Done: core.ControlPlaneDone{Req: o.Req, Err: core.ErrUnavailable}}}
	}
	m.queueRead(o.Req, o.From, read)
	return nil
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
