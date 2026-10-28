package kafka

import (
	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

// Test-only hooks for the C4 conformance check (c4_test.go, package
// kafka_test, which also imports veritas; this package never does).

// NewOracle returns a fresh oracle invariant for o.
func NewOracle(o Opts) sim.SafetyRule { return newOracle(o) }

// OracleLog is the log inv (from NewOracle) reconstructed for tp.
func OracleLog(inv sim.SafetyRule, tp core.TopicPartition) []string {
	o := inv.(*oracle)
	return o.logs[o.idx[tp]].vals
}
