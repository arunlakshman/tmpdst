// Package simulation is the broker simulation on dst/kernel (dst v2).
//
// It is built side by side with the old tree internal/verify/simulator. The old
// tree and its golden hashes do not change until the old tree is deleted (plan
// task T1.12d). Then this tree is renamed back to "simulator".
//
// This step holds only the fake services (package fakeservices): the object store,
// the control plane, the log control plane bridge and the network.
package simulation
