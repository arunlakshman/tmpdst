package server

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
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
func (e *productionEnvironment) NewID() runtime.ID {
	var id runtime.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (productionEnvironment) Maybe(runtime.FaultPointID) bool                                      { return false }
func (productionEnvironment) MaybeP(runtime.FaultPointID, float64) bool                            { return false }
func (productionEnvironment) Fault(runtime.FaultPointID, float64) bool                             { return false }
func (productionEnvironment) Always(bool, runtime.PropertyID, ...runtime.ContextPair)              {}
func (productionEnvironment) AlwaysOrUnreachable(bool, runtime.PropertyID, ...runtime.ContextPair) {}
func (productionEnvironment) Sometimes(bool, runtime.PropertyID)                                   {}
func (productionEnvironment) Reachable(runtime.PropertyID)                                         {}
func (productionEnvironment) Unreachable(runtime.PropertyID, ...runtime.ContextPair)               {}
