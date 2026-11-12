package server

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
)

// productionEnvironment is a minimal runtime.Environment for production mdscontrolplane (no buggify).
type productionEnvironment struct{ n uint64 }

func (e *productionEnvironment) Uint64() uint64 {
	e.n++
	return e.n
}
func (e *productionEnvironment) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}
func (e *productionEnvironment) NewID() machine.ID {
	var id machine.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (productionEnvironment) Maybe(fault.NamedSite) bool                                           { return false }
func (productionEnvironment) MaybeP(fault.NamedSite, float64) bool                                 { return false }
func (productionEnvironment) Fault(fault.NamedSite, float64) bool                                  { return false }
func (productionEnvironment) Always(bool, assert.Always, ...assert.Pair)                           {}
func (productionEnvironment) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (productionEnvironment) Sometimes(bool, assert.Sometimes)                                     {}
func (productionEnvironment) Reachable(assert.Reachable)                                           {}
func (productionEnvironment) Unreachable(assert.Unreachable, ...assert.Pair)                       {}
