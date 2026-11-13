package simulator

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// env is the machine.Env of a run: the random stream, the fault
// points, and the assertions. Assertions never panic: a false Always is
// recorded as a failure and the run goes on.
type env struct {
	rand *rng.Rand
	inj  *fault.Injector
	log  *assert.Log
}

func newEnv(rand *rng.Rand, fail func(property, detail string)) *env {
	return &env{
		rand: rand,
		inj:  fault.New(rand),
		log:  assert.New(fail),
	}
}

var _ runtime.Environment = (*env)(nil)

func (e *env) setQuiet(q bool) { e.inj.SetQuiet(q) }

// Uint64 implements machine.Rand.
func (e *env) Uint64() uint64 { return e.rand.Uint64() }

// Intn implements machine.Rand.
func (e *env) Intn(n int) int { return e.rand.Intn(n) }

// Between implements machine.Rand.
func (e *env) Between(lo, hi int64) int64 { return e.rand.Between(lo, hi) }

// Chance implements machine.Rand.
func (e *env) Chance(p float64) bool { return e.rand.Chance(p) }

// NewID implements machine.Rand.
func (e *env) NewID() machine.ID {
	var id machine.ID
	e.rand.Bytes(id[:])
	return id
}

// Maybe implements machine.Faults.
func (e *env) Maybe(site fault.NamedSite) bool {
	return e.inj.Maybe(fault.Site(site.Name()))
}

// MaybeP implements machine.Faults.
func (e *env) MaybeP(site fault.NamedSite, p float64) bool {
	return e.inj.MaybeP(fault.Site(site.Name()), p)
}

// Fault implements machine.Faults.
func (e *env) Fault(site fault.NamedSite, p float64) bool {
	return e.inj.Fault(fault.Site(site.Name()), p)
}

// Always implements machine.Assertions.
func (e *env) Always(cond bool, p assert.Always, details ...assert.Pair) {
	e.log.Always(cond, assert.Property(p.Name()), details...)
}

// AlwaysOrUnreachable implements machine.Assertions.
func (e *env) AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, details ...assert.Pair) {
	e.log.AlwaysOrUnreachable(cond, assert.Property(p.Name()), details...)
}

// Sometimes implements machine.Assertions.
func (e *env) Sometimes(cond bool, p assert.Sometimes) {
	e.log.Sometimes(cond, assert.Property(p.Name()))
}

// Reachable implements machine.Assertions.
func (e *env) Reachable(p assert.Reachable) {
	e.log.Reachable(assert.Property(p.Name()))
}

// Unreachable implements machine.Assertions.
func (e *env) Unreachable(p assert.Unreachable, details ...assert.Pair) {
	e.log.Unreachable(assert.Property(p.Name()), details...)
}
