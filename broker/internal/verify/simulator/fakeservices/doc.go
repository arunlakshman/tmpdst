// Package fakeservices holds the simulated outside systems of the broker
// simulation: the object store and the control plane (here), and the network
// (package fakenetwork).
//
// The object store and the control plane have no state or fault logic of their
// own. Each one is its spec (shared/objectstore.Spec, controlplane/spec through
// specbridge) with dst/check/faulty around it. Faulty adds the faults: slow
// reply, error with nothing applied, timeout, applied but reply lost, and outage
// window. This package adds only what is specific to the broker: how a
// core.ObjectCall or core.ControlPlaneCall maps to a spec op, how an error is
// written as a core value, the bit flip in a Get answer, and the commit size
// limit of the control plane.
//
// Because the service is the spec plus faults, there is no check that compares
// the service with the spec (the old check C2 was deleted in T1.15). The fault
// kinds are tested in dst/check/faulty, and the mapping in this package.
package fakeservices
