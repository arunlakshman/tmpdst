package server

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
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
func (e *productionEnvironment) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}
func (e *productionEnvironment) Chance(p float64) bool {
	return p > 0 && float64(e.Uint64()%1000) < p*1000
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

// randomEnvironment is the machine.Env of the rebalance machine in production. Its random
// numbers come from a stream that the operating system seeds, because the machine makes the
// ids of new group members from them, and two processes must not make the same ids. No
// buggify site fires and no assertion is recorded.
type randomEnvironment struct {
	productionEnvironment
	rand *rng.Rand
}

// newRandomEnvironment seeds the stream from the operating system.
func newRandomEnvironment() (*randomEnvironment, error) {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("controlplane/server: random seed: %w", err)
	}
	return &randomEnvironment{rand: rng.New(binary.LittleEndian.Uint64(b[:]) | 1)}, nil
}

func (e *randomEnvironment) Uint64() uint64             { return e.rand.Uint64() }
func (e *randomEnvironment) Intn(n int) int             { return e.rand.Intn(n) }
func (e *randomEnvironment) Between(lo, hi int64) int64 { return e.rand.Between(lo, hi) }
func (e *randomEnvironment) Chance(p float64) bool      { return e.rand.Chance(p) }
func (e *randomEnvironment) NewID() machine.ID {
	var id machine.ID
	e.rand.Bytes(id[:])
	return id
}
