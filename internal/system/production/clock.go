// Package production is the production runtime around system.Run: clock, env, executor,
// TCP connections and the wiring shared by cmd/mdsbroker and the integration
// tests. It is nondeterministic by design and is not in the determinism gate.
package production

import (
	"time"

	"mds/internal/core"
	"mds/internal/system"
)

// Clock is the process clock: Mono counts from the clock's creation using
// Go's monotonic reading, Wall is UTC.
type Clock struct{ start time.Time }

var _ system.Clock = Clock{}

// NewClock returns a Clock anchored at the current time.
func NewClock() Clock { return Clock{start: time.Now()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Clock) Mono() core.NodeTime { return core.NodeTime(time.Since(c.start)) }

// Wall returns the current wall-clock time in nanoseconds.
func (c Clock) Wall() core.RealTime { return core.RealTime(time.Now().UnixNano()) }
