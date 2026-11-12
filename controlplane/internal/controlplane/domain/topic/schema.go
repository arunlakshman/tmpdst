// Package topic is the topics and segments context of the control plane: the
// topics and their partition counts, the segment commits that give out
// offsets, the idempotency window of the segment keys, and the lookups of
// runs. It keeps its state in the ordered key-value store under the keys T,
// E, R, K and S (tables.go).
//
// The context has two commands, CommitSegment and CreateTopic, and two reads,
// FindSegments and DescribeTopics. metadb lists them as the rows of the topic
// schema. A command runs in three steps, in one transaction that metadb
// opens: Decide (decide.go) reads the state before the command through a View
// and returns events and an answer, and writes nothing; Apply (tables.go)
// writes the events; the answer goes back to the caller. The log holds the
// commands. The events are made on every replica when the command is applied,
// and are never stored.
//
// Idempotency window (decision 48): a K record is kept until Trim drops it,
// that is for as long as the log keeps the command. A replay inside the window
// gets the original ranges (rebuilt from the replayed request and the stored
// bases) or ErrConflict when the request digest differs. After Trim the key is
// forgotten and the same key commits again as a new segment.
//
// Nothing here knows about consumer groups. The group context asks this one
// how many partitions a topic has, through PartitionCount.
package topic

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// CommitSegment applies a CommitSegment that sits at log slot slot. A key that
// is already in the window gets its original answer, or ErrConflict if the
// request differs.
func CommitSegment(transaction engine.Txn, slot int64, operation contract.CommitSegment) runtime.ControlPlaneDone {
	events, assigned, code := DecideCommit(txnView{transaction}, slot, operation)
	applyAll(transaction, events)
	return commitAnswer(assigned, code)
}

// CreateTopic applies a CreateTopic. The slot is not used. The check and the
// write are in one transaction, so the answer depends only on the state
// before this command.
func CreateTopic(transaction engine.Txn, _ int64, operation contract.CreateTopic) runtime.ControlPlaneDone {
	events, code := DecideCreate(txnView{transaction}, operation)
	applyAll(transaction, events)
	return runtime.ControlPlaneDone{Err: code}
}

// Known answers operation from the idempotency window when its key is already
// there. ok is false when the key is unknown (the caller must append).
func Known(transaction engine.Txn, operation contract.CommitSegment) (runtime.ControlPlaneDone, bool) {
	rec, ok := txnView{transaction}.Commit(operation.SegmentKey)
	if !ok {
		return runtime.ControlPlaneDone{}, false
	}
	return commitAnswer(replay(rec, checksum(operation.Batches), operation)), true
}

func applyAll(transaction engine.Txn, events []Event) {
	for _, e := range events {
		Apply(transaction, e)
	}
}

// commitAnswer is the answer to a CommitSegment: the ranges, or the code.
func commitAnswer(assigned []contract.BatchRange, code runtime.ErrCode) runtime.ControlPlaneDone {
	if code != runtime.OK {
		return runtime.ControlPlaneDone{Err: code}
	}
	return runtime.ControlPlaneDone{Resp: contract.CommitSegmentResp{Assigned: assigned}, Outcome: runtime.Applied}
}
