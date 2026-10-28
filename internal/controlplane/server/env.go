package server

import (
	"time"

	"manifester/internal/core"
)

type wallClock struct{}

func (wallClock) Mono() core.NodeTime {
	return core.NodeTime(time.Now().UnixNano())
}
func (wallClock) Wall() core.RealTime {
	return core.RealTime(time.Now().UnixNano())
}

// prodEnv is a minimal core.Environment for production manicp (no buggify).
type prodEnv struct{ n uint64 }

func (e *prodEnv) Uint64() uint64 {
	e.n++
	return e.n
}
func (e *prodEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}
func (e *prodEnv) NewID() core.ID {
	var id core.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (prodEnv) Maybe(core.FaultPointID) bool                                   { return false }
func (prodEnv) MaybeP(core.FaultPointID, float64) bool                         { return false }
func (prodEnv) Fault(core.FaultPointID, float64) bool                          { return false }
func (prodEnv) Always(bool, core.PropertyID, ...core.ContextPair)              {}
func (prodEnv) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (prodEnv) Sometimes(bool, core.PropertyID)                                {}
func (prodEnv) Reachable(core.PropertyID)                                      {}
func (prodEnv) Unreachable(core.PropertyID, ...core.ContextPair)               {}
