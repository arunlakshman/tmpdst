package kafka

import (
	"manifester/core"
	"manifester/sim"
)

// Test-only hooks for the C4 conformance check (c4_test.go, package
// kafka_test, which also imports veritas; this package never does).

// NewOracle returns a fresh oracle invariant for o.
func NewOracle(o Opts) sim.Invariant { return newOracle(o) }

// OracleLog is the log inv (from NewOracle) reconstructed for tp.
func OracleLog(inv sim.Invariant, tp core.TopicPartition) []string {
	o := inv.(*oracle)
	return o.logs[o.idx[tp]].vals
}
