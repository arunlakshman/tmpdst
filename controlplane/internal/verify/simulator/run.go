package simulator

import (
	"fmt"
	"github.com/manifesterio/mds/dst/check"
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/dst/machine"
)

const (
	// maxSteps stops a run that does not end. A healthy run needs far fewer.
	maxSteps = 3_000_000
	// minQuiet is the least virtual time of the quiet tail. maxQuiet is the most:
	// after that the replicas should have converged, and if not it is a failure.
	minQuiet = 3 * second
	maxQuiet = 40 * second
)

func replicaFactory(idx int, f Faults, bug Bug) replica.Factory {
	return replica.New(replicaConfig(idx, f, bug))
}

// Run does one run of the simulation and judges it. The same Config always gives
// the same Result, including Hash.
func Run(cfg Config) (Result, error) {
	s, err := newSim(cfg)
	if err != nil {
		return Result{}, err
	}
	s.boot()
	s.faultPhase()
	s.quietPhase()
	s.finalPhase()
	s.finish()
	return s.result(), nil
}

// boot starts the nodes a few milliseconds apart, the clients, and the node faults.
func (s *sim) boot() {
	for _, n := range s.nodes {
		s.after(machine.Duration(s.rand.Between(0, int64(30*machine.Millisecond))), "boot", n.boot)
	}
	s.clients.start()
	if s.consumers != nil {
		s.consumers.start()
	}
	s.startNemesis()
}

// runUntil takes events until stop says so, the queue is empty or the run is too long.
func (s *sim) runUntil(stop func() bool) {
	s.runner.RunUntil(func() bool {
		if stop() {
			return true
		}
		if s.steps() >= maxSteps {
			s.violation(runEnds, "more than %d steps", maxSteps)
			return true
		}
		return false
	})
}

// faultPhase runs the workload with the faults on.
func (s *sim) faultPhase() {
	s.runUntil(func() bool { return s.steps() >= s.cfg.Steps })
}

// quietPhase stops all faults, heals the cluster, and lets it settle: every node
// up, ready, and at the end of the log, every client done, and no silent
// member left in the consumer groups. The consumers keep going in the quiet
// tail, so they are not waited for here.
func (s *sim) quietPhase() {
	s.tracef("quiet tail begins")
	s.quiet, s.faults = true, Faults{}
	s.env.setQuiet(true)
	for _, n := range s.nodes {
		n.cut = false
		n.resume()
		if !n.up {
			n.boot()
		}
	}
	if s.consumers != nil {
		s.consumers.watch()
	}
	start := s.now()
	s.runUntil(func() bool {
		waited := machine.Duration(s.now() - start)
		return waited >= maxQuiet || (waited >= minQuiet && !s.clients.busy() && s.converged() && s.groupsSettled())
	})
	s.stats.QuietMs = (s.now() - start) / int64(machine.Millisecond)
	s.verdict(s.converged(), liveReplicasConverge, "after %dms of quiet the replicas did not reach the end of the log together: %s", s.stats.QuietMs, s.describeNodes())
	if s.consumers != nil {
		s.oracle.checkGroupsSettled(s.consumers)
	}
}

// converged reports that every node is up and ready, and has applied the whole log.
func (s *sim) converged() bool {
	nodes := make([]check.Node, len(s.nodes))
	for i, n := range s.nodes {
		if r := n.replica(); n.up && r != nil {
			nodes[i] = check.Node{Up: true, Ready: r.Ready(), Applied: check.Slot(r.Applied())}
		}
	}
	return check.Converged(s.store, nodes)
}

func (s *sim) describeNodes() string {
	end, _ := s.store.logEnd()
	var b strings.Builder
	fmt.Fprintf(&b, "log has %d slots;", end)
	for _, n := range s.nodes {
		r := n.replica()
		if !n.up || r == nil {
			fmt.Fprintf(&b, " %s down;", n.name())
			continue
		}
		fmt.Fprintf(&b, " %s ready=%v applied=%d epoch=%d;", n.name(), r.Ready(), r.Applied(), r.Epoch())
	}
	return b.String()
}

// groupsWriting reports that a command of a rebalance machine still waits
// for its replica.
func (s *sim) groupsWriting() bool {
	return slices.ContainsFunc(s.nodes, func(n *simNode) bool { return n.groups.writing() })
}

// groupsSettled reports that no silent member is left in the consumer groups
// (always true in a run with no consumer groups).
func (s *sim) groupsSettled() bool { return s.consumers == nil || s.consumers.settled() }

// finalPhase reads all the state from every node. With consumer groups, the
// consumers stop first, and the machines are frozen: a machine writes on its
// own timers, and the final reads need a log that no longer changes.
func (s *sim) finalPhase() {
	if s.consumers != nil {
		s.consumers.stop()
		s.runUntil(func() bool { return !s.consumers.busy() })
		for _, n := range s.nodes {
			n.groups.freeze()
		}
		start := s.now()
		s.runUntil(func() bool {
			return machine.Duration(s.now()-start) >= maxQuiet || (!s.groupsWriting() && s.converged())
		})
	}
	s.clients.startFinal()
	s.runUntil(func() bool { return !s.clients.busy() })
}

// finish runs the checks that need the whole run.
func (s *sim) finish() {
	end, _ := s.store.logEnd()
	s.stats.LogSlots = int(end)
	s.checkSameState()
	s.oracle.verify()
}

// checkSameState requires that every live node has the final state. A node may
// be a few heartbeat slots behind; nothing else may be missing. Two nodes at the
// same slot have the same state: checkNode already checks that on every step.
func (s *sim) checkSameState() {
	for _, n := range s.nodes {
		if r := n.replica(); n.up && r != nil {
			s.verdict(s.store.quietAfter(r.Applied()), liveReplicasConverge, "%s is at slot %d and misses commands of the log", n.name(), r.Applied())
		}
	}
}

func (s *sim) result() Result {
	r := Result{
		Seed: s.cfg.Seed, Scenario: s.cfg.Scenario, Hash: s.finalHash(), Steps: s.steps(),
		EndMs: s.now() / int64(machine.Millisecond), Failures: s.failures, Stats: s.stats, Coverage: s.env.log.Coverage(),
		Sites: s.env.inj.Sites(),
	}
	if len(s.failures) > 0 {
		r.Tail = s.tail()
	}
	return r
}

// finalHash adds the end of the run to the trace hash, so two runs that only
// differ in what the oracle saw also differ in the hash.
func (s *sim) finalHash() uint64 {
	h := s.hash
	h.Uint64(uint64(s.steps()))
	h.Uint64(uint64(s.stats.Acked))
	h.Uint64(uint64(s.stats.LogSlots))
	h.Uint64(s.rand.Calls())
	return h.Sum()
}

// Report is a text for a person: the result, the failures with the command to
// reproduce them, and the last steps of a failed run.
func Report(r Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "scenario=%s seed=%d steps=%d virtual=%dms hash=%016x\n", r.Scenario, r.Seed, r.Steps, r.EndMs, r.Hash)
	st := r.Stats
	fmt.Fprintf(&b, "ops=%d acked=%d rejected=%d gave-up=%d retries=%d reads=%d log-slots=%d\n", st.Ops, st.Acked, st.Rejected, st.GaveUp, st.Retries, st.ReadsOK, st.LogSlots)
	fmt.Fprintf(&b, "boots=%d crashes=%d trips=%d pauses=%d partitions=%d failed-calls=%d lost-replies=%d cut-calls=%d quiet=%dms\n",
		st.Boots, st.Crashes, st.Trips, st.Pauses, st.Partitions, st.FailedCalls, st.LostReplies, st.CutCalls, st.QuietMs)
	if st.GroupCalls > 0 {
		fmt.Fprintf(&b, "group-calls=%d group-ok=%d\n", st.GroupCalls, st.GroupOK)
	}
	if r.OK() {
		b.WriteString("PASS")
		return b.String()
	}
	for _, f := range r.Failures {
		fmt.Fprintf(&b, "FAIL seed=%d scenario=%s step=%d t=%dms: %s: %s", r.Seed, r.Scenario, f.Step, f.TimeMs, f.Property, f.Detail)
		if f.Repeats > 0 {
			fmt.Fprintf(&b, " (and %d more)", f.Repeats)
		}
		b.WriteByte('\n')
	}
	b.WriteString("last steps:\n")
	for _, l := range r.Tail {
		b.WriteString("  " + l + "\n")
	}
	b.WriteString("reproduce: " + r.Reproduce())
	return b.String()
}
