package server

import (
	"time"

	"manifester/internal/core"
)

type wallClock struct{}

func (wallClock) Mono() core.Time {
	return core.Time(time.Now().UnixNano())
}
func (wallClock) Wall() core.WallTime {
	return core.WallTime(time.Now().UnixNano())
}

// prodEnv is a minimal core.Env for production manicp (no buggify).
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
func (prodEnv) Maybe(core.Site) bool                                { return false }
func (prodEnv) MaybeP(core.Site, float64) bool                      { return false }
func (prodEnv) Fault(core.Site, float64) bool                       { return false }
func (prodEnv) Always(bool, core.Prop, ...core.Detail)              {}
func (prodEnv) AlwaysOrUnreachable(bool, core.Prop, ...core.Detail) {}
func (prodEnv) Sometimes(bool, core.Prop)                           {}
func (prodEnv) Reachable(core.Prop)                                 {}
func (prodEnv) Unreachable(core.Prop, ...core.Detail)               {}
