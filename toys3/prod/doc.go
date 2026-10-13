// Package prod is the production runtime for toys3: the thin shell that does
// the real I/O the controller (package control) only asks for. It contains no
// business logic: OSExecutor is the real storage adapter (actuations in,
// feedback out) and Handler is the anti-corruption layer from S3 REST to
// control.Op.
//
//	HTTP handlers ──Request──┐                   ┌──► goroutine: os.WriteFile ... ──┐
//	                         ▼                   │                                  │
//	                    ┌─────────┐  Step()  ┌───┴──────┐                           │
//	                    │  inbox  │ ───────► │ Executor │ ──► Reply → waiting handler
//	                    │ (chan)  │          └──────────┘                           │
//	                    └─────────┘ ◄────────── FS...Done completions ──────────────┘
//
// One goroutine (Loop.Run) owns the control.Controller and calls Step one event
// at a time: the event loop is the control loop. Actuations (filesystem
// actions) run concurrently in Executor goroutines and their feedback comes
// back through the inbox. This package may import os, net/http, time, etc.; it
// is deliberately not checked by detcheck and not run in simulation.
package prod
