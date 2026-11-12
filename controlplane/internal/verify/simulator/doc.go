// Package simulator is a deterministic simulation test (DST) harness for the
// replicated control plane. One run is one seed. A run builds a few replicas
// with the same constructor that production uses (replica.New), puts them on a
// virtual clock, gives them a fake shared log and a fake object store, and
// drives them with random client work and random faults. The same seed gives
// the same run, bit for bit, which the trace hash proves.
//
// Rules of a run:
//   - one goroutine, no real time, no real IO, no map iteration that decides
//     behavior;
//   - one seeded random stream (see Rand). Every choice comes from it;
//   - time moves only when the scheduler takes the next event from the queue.
//
// The oracle is the control-plane spec (spec.go is the only file that imports
// it, from internal/spec). See
// docs/simulation.md for what is checked and what is not.
package simulator
