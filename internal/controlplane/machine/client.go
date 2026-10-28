package machine

import (
	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
)

// onCPOffer handles one sim-bridge CPOffer: reads answer from the fold when
// Ready; commits and creates go through Offer and CPReply after apply.
func (m *Machine) onCPOffer(o core.CPOffer) []core.Action {
	switch op := o.Op.(type) {
	case core.CommitSegment:
		return m.withReplies(m.Offer(ClientOp{
			Req: o.Req, From: o.From,
			Cmd: applier.Entry{Kind: applier.KindCommit, Commit: op},
		}))
	case core.Lookup:
		return []core.Action{core.CPReply{To: o.From, Done: m.readFold(o.Req, func() (core.CPDone, error) {
			return m.fold.Lookup(op)
		})}}
	case core.DescribeTopics:
		return []core.Action{core.CPReply{To: o.From, Done: m.readFold(o.Req, func() (core.CPDone, error) {
			return m.fold.Describe(op)
		})}}
	default:
		return []core.Action{core.CPReply{To: o.From, Done: core.CPDone{Req: o.Req, Err: core.ErrInvalid}}}
	}
}

func (m *Machine) readFold(req core.ReqID, fn func() (core.CPDone, error)) core.CPDone {
	if !m.Ready() {
		return core.CPDone{Req: req, Err: core.ErrUnavailable}
	}
	d, err := fn()
	if err != nil {
		return core.CPDone{Req: req, Err: core.ErrIO}
	}
	d.Req = req
	return d
}

// withReplies turns answers that have a broker From into CPReply actions.
// Unit-test Offers without From stay in m.answers for TakeAnswers.
func (m *Machine) withReplies(acts []core.Action) []core.Action {
	var kept []Answer
	for _, a := range m.answers {
		if a.From.Node == "" {
			kept = append(kept, a)
			continue
		}
		acts = append(acts, core.CPReply{To: a.From, Done: a.Done})
	}
	m.answers = kept
	return acts
}
