// Package simulation is the broker simulation on dst/kernel (dst v2).
//
// The old tree (internal/verify/simulator) was deleted in plan task T1.12d. This
// tree is named "simulation" until the rename step of that task.
//
// It holds the fake services (fakeservices), the cluster host (cluster), the history
// and the truth views, and the two scenarios: kafka and echo. The command
// cmd/mdssimulator runs both through dst/campaign and imports only this tree.
package simulation
