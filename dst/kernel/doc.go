// Package kernel is the simulation world of dst v2: nodes, services, typed
// addresses, routers, timers, lanes, inboxes, phases, incarnations, crash,
// restart, stop, pause, attrition, the cost model, the fact timeline and the
// trace hash.
//
// # A run
//
//	w := kernel.New(kernel.Config{Seed: seed})
//	store := kernel.AddService(w, newStore())        // a ServiceAddr[Req, In]
//	node := kernel.Spawn(w, kernel.NodeSpec[In, Out]{ // an Addr[In]
//		Name:  "n1",
//		New:   newMachine,
//		Route: func(r *kernel.Router[In], out Out) { ... },
//	})
//	kernel.Inject(w, node, 0, firstInput)
//	w.RunUntil(nil)
//	res := w.Finish()
//
// Everything in a run comes from one seed: one random stream (rng.Rand), one
// event queue ordered by (time, seeded tie, order of adding), and one trace
// hash. The same seed gives the same run, with the same hash.
//
// # Typed routing
//
// A machine returns plain output values. The product gives each node a route
// function: one exhaustive type switch over the output sum type (checked by
// the gochecksumtype linter). Each case calls a typed kernel function: Send,
// SendOn, Call, or a method of the Router (SetTimer, CancelTimer, SendSelf,
// Fact, End). An Addr[In] is made only by Spawn, so sending a message of the
// wrong type to a node does not compile.
//
// # Incarnations
//
// An incarnation is one boot of a node. A crash ends it; nothing in memory
// survives. Timers, SendSelf messages and service answers belong to one
// incarnation (a Ref) and are dropped when it ends. Messages sent to an Addr go
// to whatever incarnation is up when they arrive.
//
// # Type erasure
//
// The kernel keeps nodes, services and queue entries of many types in one
// list. The only file that hides their types is erase.go. No value of type any
// exists in this package (erase_test.go checks it).
package kernel
