// Package sim is the deterministic simulator for toys3: a software-in-the-loop
// testbench that runs the real controller (package control) against a model
// of the plant, on one goroutine, with one seeded RNG, one priority queue and
// a virtual clock. It drives client traffic and checks every reply.
//
// The package holds four bounded contexts, grouped by file:
//
//	testbench     simulator.go queue.go rand.go config.go hash.go trace.go log.go
//	plant model   plantmodel.go (PlantModel, Disturbances, ActuationPlan)
//	stimulus      workload.go value.go (self-describing values, shared with verification)
//	verification  oracle.go history.go invariant.go (monitors read the plant only
//	              through the GroundTruth interface)
//
// The same Config (and so the same seed) always produces the same run, event
// for event, and the same Result.TraceHash. A failing seed is a complete,
// replayable bug report.
//
// Main loop, on one goroutine:
//
//	e := queue.Pop(); clock = e.At
//	if e is addressed to a dead incarnation: drop it
//	switch e.Kind:
//	  deliver  → actions := server.Step(Event{clock, e.Event}); trace ← fold(StateHash)
//	             for a in actions:
//	               FS*   → plan := plant.Plan(a); schedule fsLand at clock+plan.DeadTime
//	               Reply → oracle.Return; workload.Complete; schedule clientWake
//	               Emit  → record telemetry     Trip → crash now
//	             during Chaos: crash now with probability CrashProb
//	  fsLand   → body := plant.Land(a, plan); schedule deliver(body) at clock+plan.FeedbackDelay
//	             during Chaos: crash now with probability CrashProb
//	  clientWake → req := workload.Issue; oracle.Invoke; schedule deliver(Request)
//	  crash    → plant.Crash(); queue.DropIncarnation; oracle.Crash; schedule restart
//	  restart  → inc++; server = Factory(...); schedule deliver(Started)
//	run invariants (per-step checks); the first failure stops the run
//
// A crash is a power loss (a disturbance): the PlantModel resolves volatile
// state, and every queued entry of the dead incarnation is dropped.
// Actuations that have not landed never happen, and undelivered feedback is
// lost. A Trip ends the incarnation the same way. Clients do not issue
// requests while the server is down, and they pause at every QuiesceEvery
// until all requests have replied.
//
// Like packages control and format, this package is checked by tools/detcheck.
package sim
