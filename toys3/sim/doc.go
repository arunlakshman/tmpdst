// Package sim is the deterministic simulator for toys3: one goroutine, one
// seeded RNG, one priority queue, a virtual clock, an adversarial fake
// filesystem, a client workload, and an oracle that checks every reply.
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
//	               FS*   → plan := fs.Plan(a); schedule fsLand at clock+plan.DeadTime
//	               Reply → oracle.Return; workload.Complete; schedule clientWake
//	               Emit  → record telemetry     Trip → crash now
//	             during Chaos: crash now with probability CrashProb
//	  fsLand   → body := fs.Land(a, plan); schedule deliver(body) at clock+plan.FeedbackDelay
//	             during Chaos: crash now with probability CrashProb
//	  clientWake → req := workload.Issue; oracle.Invoke; schedule deliver(Request)
//	  crash    → fs.Crash(); queue.DropIncarnation; oracle.Crash; schedule restart
//	  restart  → inc++; server = Factory(...); schedule deliver(Started)
//	run invariants (per-step checks); the first failure stops the run
//
// A crash is a power loss: the PlantModel resolves volatile state, and every
// queued entry of the dead incarnation is dropped. FS actions that have not
// landed never happen, and undelivered completions are lost. A Trip is the
// same kind of crash. Clients do not issue requests while the server is down,
// and they pause at every QuiesceEvery until all requests have replied.
//
// Like packages control and format, this package is checked by tools/detcheck.
package sim
