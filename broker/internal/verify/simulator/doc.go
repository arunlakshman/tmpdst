// Package simulator declares the deterministic simulator's contracts: one goroutine,
// one counted RNG, one global queue, engines for every external system, and the
// Process / Workload / SafetyRule roles (docs/lessons.md). The kernel implements
// them: NewRand, NewScheduler, NewTimeline, NewNodeInbox, NewCostModel,
// NewRunner and RunSeed. Engines live in sim/engines/....
//
// Lanes at or above 1<<62 are reserved for the kernel. Engines never construct
// a Lane: they obtain each one from FakeServiceContext.Lane(key), with keys named
// "<engine>/<whatever>". The kernel allocates lanes sequentially in first-use
// order, so distinct keys never collide and one key keeps one lane per seed.
package simulator
