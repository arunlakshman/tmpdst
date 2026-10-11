// Package prod is the production runtime for toys3: the thin shell that does
// the real I/O the core only asks for. It contains no business logic.
//
//	HTTP handlers ──Request──┐                   ┌──► goroutine: os.WriteFile ... ──┐
//	                         ▼                   │                                  │
//	                    ┌─────────┐  Step()  ┌───┴──────┐                           │
//	                    │  inbox  │ ───────► │ Executor │ ──► Reply → waiting handler
//	                    │ (chan)  │          └──────────┘                           │
//	                    └─────────┘ ◄────────── FS...Done completions ──────────────┘
//
// One goroutine (Loop.Run) owns the core.Server and calls Step one event at a
// time; filesystem actions run concurrently in Executor goroutines and come
// back through the inbox. This package may import os, net/http, time, etc.; it
// is deliberately not checked by detcheck and not run in simulation.
package prod
