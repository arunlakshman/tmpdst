// Package production is the production runtime around system.Run: env, executor,
// TCP connections and the wiring shared by cmd/mdsbroker and the integration
// tests. The clock is system/realclock. It is nondeterministic by design and is
// not in the determinism gate.
package production
