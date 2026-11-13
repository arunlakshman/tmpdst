package runtime

import "github.com/manifesterio/mds/dst/machine"

// Environment is injected into every machine at construction. It is the only way
// nondeterminism enters the machines: randomness, buggify sites and assertions. Time
// is NOT here: it arrives with each Step (machine.Now).
//
// It is the dst type machine.Env. A site or an assertion is a declared value
// (dst/fault NewSite, dst/assert NewAlways and so on), never a string.
type Environment = machine.Env
