// Package group is the consumer group context of the control plane: the group
// headers, their members with the leader's assignment, and the committed
// offsets. It keeps its state in the ordered key-value store under the keys G,
// M and O (tables.go).
//
// The context has four commands, GroupInstall, GroupAssign, GroupRemove and
// OffsetCommit, and three reads, OffsetFetch, DescribeGroup and ListGroups.
// metadb lists them as the rows of the group schema. A command runs in three
// steps, in one transaction that metadb opens: Decide (decide.go) reads the
// state before the command through a View and returns events and an answer,
// and writes nothing; Apply (tables.go) writes the events; the answer goes
// back to the caller. The log holds the commands. The events are made on every
// replica when the command is applied, and are never stored.
//
// Each write is checked before it writes anything. A group command over
// 256 KiB is ErrTooLarge; the size is counted from the fields, so it is an
// upper bound of the stored bytes. The rules are in internal/spec (groups.go),
// which is the reference model.
//
// Nothing here knows about topics. An offset commit asks how many partitions a
// topic has, through the Partitions function that metadb gives to
// CommitOffsets at wiring.
package group

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The group commands, in the shape of a metadb schema row: each one applies
// one logged operation at its slot. The slot is not used: group state does not
// depend on where in the log a command sits.

// Install applies a GroupInstall.
func Install(transaction engine.Txn, _ int64, operation contract.GroupInstall) runtime.ControlPlaneDone {
	events, code := DecideInstall(txnView{transaction: transaction}, operation)
	applyAll(transaction, events)
	return groupAnswer(code)
}

// Assign applies a GroupAssign.
func Assign(transaction engine.Txn, _ int64, operation contract.GroupAssign) runtime.ControlPlaneDone {
	events, code := DecideAssign(txnView{transaction: transaction}, operation)
	applyAll(transaction, events)
	return groupAnswer(code)
}

// Remove applies a GroupRemove.
func Remove(transaction engine.Txn, _ int64, operation contract.GroupRemove) runtime.ControlPlaneDone {
	events, code := DecideRemove(txnView{transaction: transaction}, operation)
	applyAll(transaction, events)
	return groupAnswer(code)
}

// CommitOffsets returns the command that applies an OffsetCommit. It asks
// partitions how many partitions a topic has, to check that a partition exists.
func CommitOffsets(partitions Partitions) func(engine.Txn, int64, contract.OffsetCommit) runtime.ControlPlaneDone {
	return func(transaction engine.Txn, _ int64, operation contract.OffsetCommit) runtime.ControlPlaneDone {
		events, errs, code := DecideCommitOffsets(txnView{transaction: transaction, partitions: partitions}, operation)
		applyAll(transaction, events)
		if code != runtime.OK {
			return runtime.ControlPlaneDone{Err: code}
		}
		done := runtime.ControlPlaneDone{Resp: contract.OffsetCommitResp{Errs: errs}}
		if len(events) > 0 {
			done.Outcome = runtime.Applied
		}
		return done
	}
}

func applyAll(transaction engine.Txn, events []Event) {
	for _, e := range events {
		Apply(transaction, e)
	}
}

// groupAnswer is the answer to a group write that has no response.
func groupAnswer(code runtime.ErrCode) runtime.ControlPlaneDone {
	if code != runtime.OK {
		return runtime.ControlPlaneDone{Err: code}
	}
	return runtime.ControlPlaneDone{Outcome: runtime.Applied}
}
