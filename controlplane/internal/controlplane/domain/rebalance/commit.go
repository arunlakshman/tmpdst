package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// commit is an offset commit whose write is in flight. It does not wait in
// a group queue (rule 31).
type commit struct {
	call  CallID
	req   runtime.RequestID
	timer timer
}

// commit handles an OffsetCommit call (rule 31): Kafka's checks on memory
// first, then a write. The log checks again when it applies the write.
func (m *Machine) commit(s *serving, in OffsetCommit) {
	op := in.Op
	if g := s.groups[op.Group]; g != nil && (op.Generation >= 0 || len(g.members) > 0) {
		_, mm, code := check(s, op.Group, op.Member, op.Generation)
		if _, ok := g.phase.(*syncing); ok && code == runtime.OK {
			code = runtime.ErrRebalanceInProgress
		}
		if code != runtime.OK {
			m.reply(in.Call, code, nil)
			return
		}
		m.touch(mm) // the group is PreparingRebalance or Stable here
	}
	op.TimeMs = m.wallMs()
	c := &commit{call: in.Call, req: m.submit(op), timer: m.newTimer()}
	s.reqs[c.req] = c
	s.owners[c.timer.id] = timerRef{kind: timerCommit, commit: c}
	m.arm(&c.timer, m.cfg.WriteTimeout)
	s.commits = append(s.commits, c)
}

// commitDone answers a commit. A failed write is ErrUnavailable and does not
// end the term; a code of the log's rules goes to the caller as it is.
func (m *Machine) commitDone(s *serving, c *commit, done runtime.ControlPlaneDone) {
	m.endCommit(s, c)
	switch {
	case done.Err == runtime.OK:
		m.reply(c.call, runtime.OK, CommitAnswer{OffsetCommitResp: runtime.ResponseAs[contract.OffsetCommitResp](done)})
	case logRule(done.Err):
		m.reply(c.call, done.Err, nil)
	default:
		m.reply(c.call, runtime.ErrUnavailable, nil)
	}
}

// commitTimeout answers a commit whose write timer fired. Its late answer is ignored.
func (m *Machine) commitTimeout(s *serving, c *commit, f runtime.TimerFired) {
	if !c.timer.fired(f) {
		return
	}
	delete(s.reqs, c.req)
	m.endCommit(s, c)
	m.reply(c.call, runtime.ErrUnavailable, nil)
}

// abandonCommit forgets the commit of call, if there is one. Its write may still apply.
func (m *Machine) abandonCommit(s *serving, call CallID) bool {
	i := slices.IndexFunc(s.commits, func(c *commit) bool { return c.call == call })
	if i < 0 {
		return false
	}
	c := s.commits[i]
	delete(s.reqs, c.req)
	m.endCommit(s, c)
	return true
}

func (m *Machine) endCommit(s *serving, c *commit) {
	m.stop(&c.timer)
	delete(s.owners, c.timer.id)
	s.commits = slices.DeleteFunc(s.commits, func(o *commit) bool { return o == c })
}

// logRule reports whether code is an answer of the log's rules, not a failure of the write.
func logRule(code runtime.ErrCode) bool {
	switch code {
	case runtime.ErrInvalid, runtime.ErrTooLarge, runtime.ErrUnknownMember, runtime.ErrIllegalGeneration,
		runtime.ErrUnknownPartition, runtime.ErrRebalanceInProgress:
		return true
	}
	return false
}
