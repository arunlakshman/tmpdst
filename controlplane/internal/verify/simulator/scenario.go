package simulator

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Faults are the probabilities and rates of one scenario. A zero value means no
// faults. In the quiet tail all of them are off.
type Faults struct {
	// Per call to the log or the object store.
	Fail    float64 // error reply, nothing applied
	Timeout float64 // timeout reply, nothing applied
	Lost    float64 // writes only: applied, reply lost (a timeout, maybe applied)
	Slow    float64 // extra delay on the way there or back
	Corrupt float64 // object reads only: one byte of the data is changed

	// Mean gap between node faults. Zero switches the fault off.
	CrashEvery     runtime.Duration // kill a node, restart it later with a fresh boot number
	PauseEvery     runtime.Duration // stop a node for a while
	PartitionEvery runtime.Duration // cut a node off from the log and the object store
	TargetLeader   bool             // crash and partition aim at the node that holds the writer epoch

	// Cooperative faults inside the replica (replica.Config StallProb and CrashProb).
	SoftFault float64
}

// Scenario is a named set of faults, and the number of consumers (clients of
// the consumer groups, consumers.go) that run in it.
type Scenario struct {
	Name      string
	Detail    string
	Faults    Faults
	Consumers int
}

const second = runtime.Second

func allScenarios() []Scenario {
	return []Scenario{
		{Name: "basic", Detail: "no faults, only random delays and reordering", Consumers: 5},
		{Name: "log-faults", Detail: "log and object store calls fail, time out, are slow, or apply with a lost reply",
			Faults: Faults{Fail: 0.04, Timeout: 0.02, Lost: 0.04, Slow: 0.05, Corrupt: 0.1}, Consumers: 6},
		{Name: "crash-restart", Detail: "nodes crash and restart with a fresh boot number; nodes pause; replicas crash by themselves",
			Faults: Faults{CrashEvery: 3 * second, PauseEvery: 4 * second, SoftFault: 0.05, Fail: 0.005, Slow: 0.02}, Consumers: 6},
		{Name: "leader-fencing", Detail: "the node that is the writer is cut off from the log, another node claims the epoch",
			Faults: Faults{PartitionEvery: 5 * second, TargetLeader: true, PauseEvery: 8 * second, Slow: 0.02}, Consumers: 6},
		{Name: "chaos", Detail: "all of the above together",
			Faults: Faults{Fail: 0.03, Timeout: 0.02, Lost: 0.03, Slow: 0.05, Corrupt: 0.1, CrashEvery: 4 * second, PauseEvery: 5 * second,
				PartitionEvery: 6 * second, TargetLeader: true, SoftFault: 0.03}, Consumers: 6},
	}
}

// ScenarioNames returns the names, in order.
func ScenarioNames() []string {
	all := allScenarios()
	names := make([]string, len(all))
	for i, sc := range all {
		names[i] = sc.Name
	}
	return names
}

// ScenarioByName finds a scenario. The empty name is "basic".
func ScenarioByName(name string) (Scenario, bool) {
	if name == "" {
		name = "basic"
	}
	for _, sc := range allScenarios() {
		if sc.Name == name {
			return sc, true
		}
	}
	return Scenario{}, false
}

// replicaConfig is the replica config of node idx. It has the shape of
// machineConfig in cmd/mdscontrolplane, with the times scaled down so that a run
// is short in virtual time: catch-up every 50 ms, a heartbeat every 250 ms, a
// takeover after 1.5 s of silence, and a checkpoint with a log trim every second.
func replicaConfig(idx int, f Faults, bug Bug) replica.Config {
	cfg := replica.Config{
		CatchUpEvery:       50 * runtime.Millisecond,
		Writer:             nodeWriter(idx),
		Rank:               idx,
		Heartbeat:          250 * runtime.Millisecond,
		TakeoverAfter:      1500 * runtime.Millisecond,
		TakeoverRankStep:   500 * runtime.Millisecond,
		CheckpointEvery:    1 * second,
		CheckpointMinSlots: 4,
		CheckpointPrefix:   "checkpoint/",
		KeepSlots:          8,
		MaxCmdsPerSlot:     4,
	}
	if f.SoftFault > 0 {
		cfg.StallProb, cfg.CrashProb = f.SoftFault, f.SoftFault
		cfg.StallDur = [2]runtime.Duration{100 * runtime.Millisecond, 800 * runtime.Millisecond}
		cfg.DownDur = [2]runtime.Duration{100 * runtime.Millisecond, 800 * runtime.Millisecond}
	}
	return cfg
}

// gap draws a random gap around mean.
func (s *sim) gap(mean runtime.Duration) runtime.Duration {
	return runtime.Duration(s.rand.Between(int64(mean)/2, int64(mean)*3/2))
}

// startNemesis begins the node faults that the scenario asks for.
func (s *sim) startNemesis() {
	f := s.faults
	s.every(f.CrashEvery, "nemesis-crash", s.crashOne)
	s.every(f.PauseEvery, "nemesis-pause", s.pauseOne)
	s.every(f.PartitionEvery, "nemesis-partition", s.partitionOne)
}

// every runs fn about every mean, until the quiet tail starts.
func (s *sim) every(mean runtime.Duration, label string, fn func()) {
	if mean <= 0 {
		return
	}
	var tick func()
	tick = func() {
		if s.quiet {
			return
		}
		fn()
		s.after(s.gap(mean), label, tick)
	}
	s.after(s.gap(mean), label, tick)
}

// victim picks the node for a fault: the writer if the scenario aims at it and
// there is one, otherwise a random node.
func (s *sim) victim() *simNode {
	if s.faults.TargetLeader {
		for _, n := range s.nodes {
			if r := n.replica(); n.up && r != nil && r.Epoch() > 0 {
				return n
			}
		}
	}
	return s.nodes[s.rand.Intn(len(s.nodes))]
}

func (s *sim) crashOne() {
	n := s.victim()
	if !n.up {
		return
	}
	n.crash("nemesis", runtime.Duration(s.rand.Between(int64(200*runtime.Millisecond), int64(3*second))))
}

func (s *sim) pauseOne() {
	n := s.victim()
	if !n.up || n.paused {
		return
	}
	n.pause()
	s.tracef("%s paused", n.name())
	s.after(runtime.Duration(s.rand.Between(int64(100*runtime.Millisecond), int64(2*second))), "resume", n.resume)
}

func (s *sim) partitionOne() {
	n := s.victim()
	if n.cut {
		return
	}
	n.cut = true
	s.stats.Partitions++
	s.tracef("%s is cut off from storage", n.name())
	s.after(runtime.Duration(s.rand.Between(int64(2*second), int64(5*second))), "heal", func() { n.cut = false })
}
