package simulator

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// env is the runtime.Environment of a run: the random stream, the fault
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

// Uint64 implements runtime.Rand.
func (e *env) Uint64() uint64 { return e.rand.Uint64() }

// Intn implements runtime.Rand.
func (e *env) Intn(n int) int { return e.rand.Intn(n) }

// NewID implements runtime.Rand.
func (e *env) NewID() machine.ID {
	var id machine.ID
	e.rand.Bytes(id[:])
	return id
}

// Maybe implements runtime.FaultInjector.
func (e *env) Maybe(site runtime.FaultPointID) bool {
	return e.inj.Maybe(fault.Site(site))
}

// MaybeP implements runtime.FaultInjector.
func (e *env) MaybeP(site runtime.FaultPointID, p float64) bool {
	return e.inj.MaybeP(fault.Site(site), p)
}

// Fault implements runtime.FaultInjector.
func (e *env) Fault(site runtime.FaultPointID, p float64) bool {
	return e.inj.Fault(fault.Site(site), p)
}

func toPairs(details []runtime.ContextPair) []assert.Pair {
	if len(details) == 0 {
		return nil
	}
	out := make([]assert.Pair, len(details))
	for i, d := range details {
		out[i] = assert.Pair{Key: d.Key, Value: d.Value}
	}
	return out
}

// Always implements runtime.Assertions.
func (e *env) Always(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	e.log.Always(cond, assert.Property(p), toPairs(details)...)
}

// AlwaysOrUnreachable implements runtime.Assertions.
func (e *env) AlwaysOrUnreachable(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	e.log.AlwaysOrUnreachable(cond, assert.Property(p), toPairs(details)...)
}

// Sometimes implements runtime.Assertions.
func (e *env) Sometimes(cond bool, p runtime.PropertyID) {
	e.log.Sometimes(cond, assert.Property(p))
}

// Reachable implements runtime.Assertions.
func (e *env) Reachable(p runtime.PropertyID) {
	e.log.Reachable(assert.Property(p))
}

// Unreachable implements runtime.Assertions.
func (e *env) Unreachable(p runtime.PropertyID, details ...runtime.ContextPair) {
	e.log.Unreachable(assert.Property(p), toPairs(details)...)
}
