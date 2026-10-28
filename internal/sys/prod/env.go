package prod

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"manifester/internal/core"
)

// Env is the production core.Environment: crypto-seeded randomness, inert buggify and
// assertions that log violations and carry on.
type Env struct {
	mu  sync.Mutex
	rnd *rand.ChaCha8
	log *slog.Logger
}

var _ core.Environment = (*Env)(nil)

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

// Intn returns a random int in [0, n).
func (e *Env) Intn(n int) int {
	return int(e.Uint64() % uint64(n))
}

// NewID returns a random ID.
func (e *Env) NewID() core.ID {
	var id core.ID
	binary.LittleEndian.PutUint64(id[:8], e.Uint64())
	binary.LittleEndian.PutUint64(id[8:], e.Uint64())
	return id
}

// Maybe never takes the optional branch in production.
func (*Env) Maybe(core.FaultPointID) bool { return false }

// MaybeP never takes the optional branch in production.
func (*Env) MaybeP(core.FaultPointID, float64) bool { return false }

// Fault never injects a fault in production.
func (*Env) Fault(core.FaultPointID, float64) bool { return false }

// Sometimes is a no-op in production.
func (*Env) Sometimes(bool, core.PropertyID) {}

// Reachable is a no-op in production.
func (*Env) Reachable(core.PropertyID) {}

// Always reports a violation of p when cond is false.
func (e *Env) Always(cond bool, p core.PropertyID, d ...core.ContextPair) {
	if !cond {
		e.violated("always", p, d)
	}
}

// AlwaysOrUnreachable reports a violation of p when cond is false.
func (e *Env) AlwaysOrUnreachable(cond bool, p core.PropertyID, d ...core.ContextPair) {
	if !cond {
		e.violated("always_or_unreachable", p, d)
	}
}

// Unreachable reports a violation of p.
func (e *Env) Unreachable(p core.PropertyID, d ...core.ContextPair) { e.violated("unreachable", p, d) }

func (e *Env) violated(kind string, p core.PropertyID, d []core.ContextPair) {
	args := []any{"kind", kind, "prop", string(p)}
	for _, x := range d {
		args = append(args, x.Key, x.Value)
	}
	e.log.Error("assertion violated", args...)
}
