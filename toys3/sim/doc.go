// Package sim is the deterministic simulator for toys3: one goroutine, one
// seeded RNG, one priority queue, a virtual clock, an adversarial fake
// filesystem, a client workload, and an oracle that checks every reply.
//
// The same Config (and so the same seed) always produces the same run, event
// for event, and the same Result.TraceHash. A failing seed is a complete,
// replayable bug report.
//
// Main loop (phase 2), on one goroutine:
//
//	e := queue.Pop(); clock = e.At
//	if e is addressed to a dead incarnation: drop it
//	switch e.Kind:
//	  deliver  → actions := server.Step(Event{clock, e.Event}); trace ← fold(StateHash)
//	             for a in actions:
//	               FS*   → plan := fs.Plan(a); schedule fsLand at clock+plan.Land
//	               Reply → oracle.Return; workload.Complete; schedule clientWake
//	               Emit  → record fact          Halt → crash now
//	  fsLand   → body := fs.Land(a, plan); schedule deliver(body) at clock+plan.Reply
//	  clientWake → req := workload.Issue; oracle.Invoke; schedule deliver(Request)
//	  crash    → fs.Crash(); queue.DropIncarnation; oracle.Crash; schedule restart
//	  restart  → inc++; server = Factory(...); schedule deliver(Started)
//	run invariants (per-step checks)
//
// Like package core, this package is checked by tools/detcheck.
package sim
