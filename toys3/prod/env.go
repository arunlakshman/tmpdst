package prod

import (
	"math/rand/v2"

	"manifester/toys3/core"
)

// Env is the production core.Env: real randomness, buggify always off.
type Env struct{}

func (Env) Uint64() uint64       { return rand.Uint64() }
func (Env) Intn(n int) int       { return rand.IntN(n) }
func (Env) Maybe(core.Site) bool { return false }

var _ core.Env = Env{}
