package production

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
)

// Env is the production core.Environment: crypto-seeded randomness, inert buggify and
// assertions that log violations and carry on.
type Env struct {
	mu  sync.Mutex
	rnd *rand.ChaCha8
	log *slog.Logger
}

var (
	_ core.Environment = (*Env)(nil)
	_ machine.Env      = (*Env)(nil)
)

// NewEnv returns a production Env that logs to log.
func NewEnv(log *slog.Logger) (*Env, error) {
	var seed [32]byte
	if _, err := crand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("prod.NewEnv: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Env{rnd: rand.NewChaCha8(seed), log: log}, nil
}

// Uint64 returns a random 64-bit value.
func (e *Env) Uint64() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rnd.Uint64()
}

// Intn returns a random int in [0, n). It returns 0 when n <= 0 (the contract of
// machine.Rand).
func (e *Env) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}

// Between returns a random value in [lo, hi]: both ends are included. It returns lo
// when hi <= lo.
func (e *Env) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	span := uint64(hi-lo) + 1 // zero when the range is every int64 value
	if span == 0 {
		return int64(e.Uint64())
	}
	return lo + int64(e.Uint64()%span)
}

// Chance is true with probability p. It draws nothing when p <= 0.
func (e *Env) Chance(p float64) bool {
	if p <= 0 {
		return false
	}
	return float64(e.Uint64()>>11)/(1<<53) < p
}

// NewID returns a random ID.
func (e *Env) NewID() machine.ID {
	var id machine.ID
	binary.LittleEndian.PutUint64(id[:8], e.Uint64())
	binary.LittleEndian.PutUint64(id[8:], e.Uint64())
	return id
}

// Maybe never takes the optional branch in production.
func (*Env) Maybe(fault.NamedSite) bool { return false }

// MaybeP never takes the optional branch in production.
func (*Env) MaybeP(fault.NamedSite, float64) bool { return false }

// Fault never injects a fault in production.
func (*Env) Fault(fault.NamedSite, float64) bool { return false }

// Sometimes is a no-op in production.
func (*Env) Sometimes(bool, assert.Sometimes) {}

// Reachable is a no-op in production.
func (*Env) Reachable(assert.Reachable) {}

// Always reports a violation of p when cond is false.
func (e *Env) Always(cond bool, p assert.Always, d ...assert.Pair) {
	if !cond {
		e.violated("always", p.Name(), d)
	}
}

// AlwaysOrUnreachable reports a violation of p when cond is false.
func (e *Env) AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	if !cond {
		e.violated("always_or_unreachable", p.Name(), d)
	}
}

// Unreachable reports a violation of p.
func (e *Env) Unreachable(p assert.Unreachable, d ...assert.Pair) {
	e.violated("unreachable", p.Name(), d)
}

func (e *Env) violated(kind, name string, d []assert.Pair) {
	args := []any{"kind", kind, "prop", name}
	for _, x := range d {
		args = append(args, x.Key, x.Value)
	}
	e.log.Error("assertion violated", args...)
}
