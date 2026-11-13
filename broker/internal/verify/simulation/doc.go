// Package simulation is the broker simulation on dst/kernel (dst v2).
//
// It is built side by side with the old tree internal/verify/simulator. The old
// tree and its golden hashes do not change until the old tree is deleted (plan
// task T1.12d). Then this tree is renamed back to "simulator".
//
// It holds the fake services (fakeservices), the cluster host (cluster), the history
// and the truth views, and the two scenarios: kafka and echo. The command
// cmd/mdssimulator runs both through dst/campaign and imports only this tree.
package simulation
