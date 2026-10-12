package prod

import (
	"math/rand/v2"

	"manifester/toys3/control"
)

// Env is the production control.Env: real randomness, buggify always off.
type Env struct{}

func (Env) Uint64() uint64          { return rand.Uint64() }
func (Env) Intn(n int) int          { return rand.IntN(n) }
func (Env) Maybe(control.Site) bool { return false }

var _ control.Env = Env{}
