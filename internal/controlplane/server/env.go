package server

import (
	"time"

	"mds/internal/core"
)

type realClock struct{}

func (realClock) Mono() core.NodeTime {
	return core.NodeTime(time.Now().UnixNano())
}
func (realClock) Wall() core.RealTime {
	return core.RealTime(time.Now().UnixNano())
}

// productionEnvironment is a minimal core.Environment for production mdscontrolplane (no buggify).
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
func (e *productionEnvironment) NewID() core.ID {
	var id core.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (productionEnvironment) Maybe(core.FaultPointID) bool                                   { return false }
func (productionEnvironment) MaybeP(core.FaultPointID, float64) bool                         { return false }
func (productionEnvironment) Fault(core.FaultPointID, float64) bool                          { return false }
func (productionEnvironment) Always(bool, core.PropertyID, ...core.ContextPair)              {}
func (productionEnvironment) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (productionEnvironment) Sometimes(bool, core.PropertyID)                                {}
func (productionEnvironment) Reachable(core.PropertyID)                                      {}
func (productionEnvironment) Unreachable(core.PropertyID, ...core.ContextPair)               {}
