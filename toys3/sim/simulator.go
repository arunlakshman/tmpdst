package sim

import "manifester/toys3/core"

// Simulator is one run in progress. It implements View for invariants.
// Everything is owned by the single goroutine that calls Run.
type Simulator struct {
	cfg  Config
	rng  *Rand
	now  core.Time
	step int
	q    Queue

	fs     *FakeFS
	server core.Machine // nil while crashed
	inc    Incarnation  // current (or last) incarnation; 0 before first boot
	alive  bool
	quiet  bool

	workload *Workload
	oracle   *Oracle
	facts    []TimedFact

	trace uint64 // running trace hash
	res   Result
}

// New prepares a run: seeds the Rand, builds the FakeFS with the obj/ and tmp/
// directories of cfg.Server.Dir, the workload and the oracle, and schedules
// the first boot, the clients' first wakeups and the end of Chaos.
func New(cfg Config) *Simulator {
	panic("toys3/sim: New unimplemented (phase 2)")
}

// Run executes the main loop (see the package doc) until the queue drains or
// MaxSteps is hit, then runs AtEnd invariants and returns the Result.
func (s *Simulator) Run() Result {
	panic("toys3/sim: Simulator.Run unimplemented (phase 2)")
}

// View implementation.

func (s *Simulator) Step() int          { return s.step }
func (s *Simulator) Now() core.Time     { return s.now }
func (s *Simulator) Alive() bool        { return s.alive }
func (s *Simulator) Inc() Incarnation   { return s.inc }
func (s *Simulator) Quiet() bool        { return s.quiet }
func (s *Simulator) FS() *FakeFS        { return s.fs }
func (s *Simulator) History() *History  { return s.oracle.History() }
func (s *Simulator) Facts() []TimedFact { return s.facts }

var _ View = (*Simulator)(nil)

// env is the core.Env handed to each server incarnation. It draws from the
// run's single Rand, so the server's random choices replay with the seed.
type env struct {
	rng     *Rand
	buggify bool // false during QuietTail
}

func (e *env) Uint64() uint64       { return e.rng.Uint64() }
func (e *env) Intn(n int) int       { return e.rng.Intn(n) }
func (e *env) Maybe(core.Site) bool { return e.buggify && e.rng.Chance(0.05) }

var _ core.Env = (*env)(nil)
