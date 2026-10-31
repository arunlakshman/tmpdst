package kafka

import (
	"mds/internal/core"
	"mds/internal/verify/simulator"
)

// Test-only hooks for the C4 conformance check (c4_test.go, package
// kafka_test, which also imports speccheck; this package never does).

// NewOracle returns a fresh oracle invariant for o.
func NewOracle(o Opts) simulator.SafetyRule { return newOracle(o) }

// OracleLog is the log inv (from NewOracle) reconstructed for tp.
func OracleLog(inv simulator.SafetyRule, tp core.TopicPartition) []string {
	o := inv.(*expectedLogChecker)
	return o.logs[o.idx[tp]].vals
}
