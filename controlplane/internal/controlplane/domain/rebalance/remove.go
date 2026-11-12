package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// leave handles a LeaveGroup call (rule 26): one code per listed member, and
// the known ones leave in one logged GroupRemove.
func (m *Machine) leave(s *serving, in Leave) {
	codes := make([]runtime.ErrCode, len(in.Members))
	g := s.groups[in.Group]
	var gone []string
	for i, id := range in.Members {
		switch {
		case g == nil || g.find(id) == nil:
			codes[i] = runtime.ErrUnknownMember
		case !slices.Contains(gone, id):
			gone = append(gone, id)
		}
	}
	if len(gone) == 0 {
		m.reply(in.Call, runtime.OK, LeaveAnswer{Errs: codes})
		return
	}
	m.remove(s, g, gone, removeWrite{cause: causeLeave, call: in.Call, codes: codes})
}

// remove removes the members ids of g. The removal is logged first, unless
// none of them is in the log: a new member was never installed, so there is
// nothing to write (docs/rebalance.md section 13).
func (m *Machine) remove(s *serving, g *group, ids []string, k removeWrite) {
	k.ids = ids
	inLog := slices.ContainsFunc(ids, func(id string) bool { return !g.find(id).isNew })
	if !inLog {
		m.removed(s, g, k, false)
		return
	}
	reason := reasonExpire
	if k.cause == causeLeave {
		reason = reasonLeave
	}
	m.startWrite(s, g, contract.GroupRemove{Group: g.name, Members: ids, TimeMs: m.wallMs(), Reason: reason}, k)
}

// removed drops the members from memory after the removal is logged, then
// goes on (rule 27): at the round end the round ends with the rest.
func (m *Machine) removed(s *serving, g *group, k removeWrite, dropped bool) {
	for _, id := range k.ids {
		m.dropMember(s, g, id)
	}
	switch {
	case len(g.members) == 0:
		m.toEmpty(g)
	case k.cause == causeRoundEnd:
		m.finishRound(s, g)
	default:
		m.afterRemove(s, g)
	}
	if k.cause == causeLeave {
		if dropped {
			delete(s.where, k.call)
		} else {
			m.reply(k.call, runtime.OK, LeaveAnswer{Errs: k.codes})
		}
	}
}

// afterRemove is rule 27 for a group that still has members: an open round
// may now be done, and Stable or Syncing opens a round.
func (m *Machine) afterRemove(s *serving, g *group) {
	if _, ok := g.phase.(*joining); ok {
		m.maybeComplete(s, g, false)
		return
	}
	m.openRound(g, false)
}

// removeRefused handles a GroupRemove the log refused with ErrTooLarge or
// ErrInvalid: nothing was written and the members stay.
func (m *Machine) removeRefused(s *serving, g *group, w *write, k removeWrite, code runtime.ErrCode) {
	switch k.cause {
	case causeLeave:
		for i := range k.codes {
			if k.codes[i] == runtime.OK {
				k.codes[i] = code
			}
		}
		m.answerCaller(s, w, k.call, runtime.OK, LeaveAnswer{Errs: k.codes})
	case causeRoundEnd:
		m.answerParked(g, g.phase.(*joining).joins, code, nil)
	case causeSession:
		if mm := g.find(k.ids[0]); mm != nil {
			m.touch(mm)
		}
	case causeSyncDeadline:
		m.arm(&g.round, machine.Duration(g.longest()))
	}
}

// dropMember removes a member from memory. Its parked call gets ErrUnknownMember.
func (m *Machine) dropMember(s *serving, g *group, id string) {
	mm := g.find(id)
	if mm == nil {
		return
	}
	if call, ok := g.parked(mm); ok {
		g.unpark(mm)
		m.reply(call, runtime.ErrUnknownMember, nil)
	}
	if st, ok := g.phase.(*stable); ok {
		delete(st.assign, id)
	}
	m.stop(&mm.session)
	delete(s.owners, mm.session.id)
	g.delete(id)
}

// toEmpty moves a group with no member to Empty. The generation is kept (rule 13).
func (m *Machine) toEmpty(g *group) {
	g.phase = empty{}
	g.leader = ""
	m.stop(&g.round)
	m.stop(&g.delay)
}
