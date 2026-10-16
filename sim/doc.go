// Package sim declares the deterministic simulator's contracts: one goroutine,
// one counted RNG, one global queue, engines for every external system, and the
// Process / Workload / Invariant roles (docs/lessons.md). The kernel implements
// them: NewRand, NewScheduler, NewTimeline, NewNodeInbox, NewCostModel,
// NewRunner and RunSeed. Engines live in sim/engines/....
//
// Lanes at or above 1<<62 are reserved for the kernel; engines pick lanes below.
package sim
