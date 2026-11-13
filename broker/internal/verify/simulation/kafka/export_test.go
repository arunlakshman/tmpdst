package kafka

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/truth"
	"github.com/manifesterio/mds/dst/kernel"
)

// Test-only hooks for the C4 conformance check (c4_test.go, package
// kafka_test, which also imports speccheck; this package never does).

// NewOracle returns a fresh oracle invariant for o.
func NewOracle(o Opts, h history.Reader[Op, Res], objs truth.ObjectStore, cp truth.ControlPlane) kernel.Rule {
	return newOracle(o, h, objs, cp)
}

// OracleLog is the log inv (from NewOracle) reconstructed for tp.
func OracleLog(inv kernel.Rule, topicPartition core.TopicPartition) []string {
	o := inv.(*expectedLogChecker)
	return o.logs[o.idx[topicPartition]].vals
}
