package rebalance

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sim"
)

// timer is one timer slot. Its id stays the same; each arm raises gen, so a
// TimerFired of an older arm (already in the inbox when the timer was set
// again or cancelled) is ignored (section 7).
type timer struct {
	id  machine.TimerID
	gen machine.TimerVersion
	on  bool
}

// The owners of a timer.
type timerKind uint8

const (
	timerSession timerKind = iota + 1 // a member's session (or new member timeout)
	timerRound                        // a group's round end or sync deadline
	timerDelay                        // a group's initial delay
	timerWrite                        // a group's in-flight write
	timerCommit                       // an offset commit's write
)

// timerRef says what a timer id belongs to.
type timerRef struct {
	kind   timerKind
	group  string
	member string
	commit *commit
}

func (m *Machine) newTimer() timer {
	m.nextTimer++
	return timer{id: m.nextTimer}
}

// arm sets t to fire after d. An armed t is replaced.
func (m *Machine) arm(t *timer, d machine.Duration) {
	if m.maybe(SiteTimerLate) {
		d += machine.Duration(m.cx.Rand().Between(int64(machine.Millisecond), int64(lateMax)))
	}
	if t.on && m.maybe(SiteStaleTimer) {
		// The old arm fires anyway, as in live when it fired just before it was replaced.
		m.send(runtime.TimerFired{ID: t.id, Gen: t.gen})
	}
	t.gen++
	t.on = true
	m.out = append(m.out, sim.SetTimer{ID: uint64(t.id), After: timeNs(d), Body: runtime.TimerFired{ID: t.id, Gen: t.gen}})
}

// stop cancels t if it is armed.
func (m *Machine) stop(t *timer) {
	if !t.on {
		return
	}
	t.on = false
	m.out = append(m.out, sim.CancelTimer{ID: uint64(t.id)})
}

// fired reports whether f is the current arm of t, and marks t as not armed.
func (t *timer) fired(f runtime.TimerFired) bool {
	if !t.on || t.gen != f.Gen || t.id != f.ID {
		return false
	}
	t.on = false
	return true
}

// touch starts the session of mm again (rule 29). A new member keeps its new
// member timer (rule 30).
func (m *Machine) touch(mm *member) {
	if !mm.isNew {
		m.arm(&mm.session, ms(mm.sessionMs))
	}
}

// timerFired routes a timer. The timers of a busy group wait in its queue.
func (m *Machine) timerFired(s *serving, f runtime.TimerFired) {
	ref, ok := s.owners[f.ID]
	if !ok {
		return
	}
	switch ref.kind {
	case timerWrite:
		g := s.groups[ref.group]
		if w := g.write; w != nil && w.timer.fired(f) {
			// The write may still change the log: the term ends (rules 36, 38).
			g.write = nil
			delete(s.owners, f.ID)
			m.breakTerm(s, w)
		}
	case timerCommit:
		m.commitTimeout(s, ref.commit, f)
	default:
		g := s.groups[ref.group]
		if g.busy() {
			g.queue = append(g.queue, f)
			return
		}
		m.groupTimer(s, f)
	}
}

// groupTimer runs a session, round or delay timer on a group that is not busy.
func (m *Machine) groupTimer(s *serving, f runtime.TimerFired) {
	ref, ok := s.owners[f.ID]
	if !ok {
		return // the member is gone
	}
	g := s.groups[ref.group]
	switch ref.kind {
	case timerSession:
		if mm := g.find(ref.member); mm != nil && mm.session.fired(f) {
			m.sessionEnded(s, g, mm)
		}
	case timerRound:
		if g.round.fired(f) {
			m.roundTimer(s, g)
		}
	case timerDelay:
		if jn, ok := g.phase.(*joining); ok && g.delay.fired(f) {
			jn.delay = false
			m.maybeComplete(s, g, false)
		}
	}
}

// sessionEnded removes a member whose session timer fired (rule 28), unless
// it is an existing member with a parked join or sync (rule 30). Its timer
// starts again when that call ends.
func (m *Machine) sessionEnded(s *serving, g *group, mm *member) {
	if _, waits := g.parked(mm); waits && !mm.isNew {
		return
	}
	m.remove(s, g, []string{mm.id}, removeWrite{cause: causeSession})
}

// roundTimer is the round end in Joining (rule 12) or the sync deadline in
// Syncing and Stable (rule 24).
func (m *Machine) roundTimer(s *serving, g *group) {
	if _, ok := g.phase.(*joining); ok {
		m.maybeComplete(s, g, true)
		return
	}
	late := g.ids(func(mm *member) bool { return !mm.synced })
	if len(late) > 0 {
		m.remove(s, g, late, removeWrite{cause: causeSyncDeadline})
	}
}

// stopAll cancels every timer of g.
func (m *Machine) stopAll(g *group) {
	m.stop(&g.round)
	m.stop(&g.delay)
	if g.write != nil {
		m.stop(&g.write.timer)
	}
	for _, mm := range g.members {
		m.stop(&mm.session)
	}
}
