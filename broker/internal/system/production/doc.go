// Package production is the production runtime around dst/live: env, executor,
// TCP connections and the wiring shared by cmd/mdsbroker and the integration
// tests. The clock is shared/clock (Real). It is nondeterministic by design and is
// not in the determinism gate.
package production
