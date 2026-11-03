package storage

import (
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// The log entry kinds of this module. The numbers are part of the log format.
const (
	// KindGroupInstall is a GroupInstall.
	KindGroupInstall module.Kind = 3
	// KindGroupAssign is a GroupAssign.
	KindGroupAssign module.Kind = 4
	// KindGroupRemove is a GroupRemove.
	KindGroupRemove module.Kind = 5
	// KindOffsetCommit is an OffsetCommit.
	KindOffsetCommit module.Kind = 6
)

// Kinds lists the log entry kinds of the module.
func (Module) Kinds() []module.Kind {
	return []module.Kind{KindGroupInstall, KindGroupAssign, KindGroupRemove, KindOffsetCommit}
}

// KindOf is the kind of a group write.
func (Module) KindOf(operation core.ControlPlaneOperation) (module.Kind, bool) {
	switch operation.(type) {
	case core.GroupInstall:
		return KindGroupInstall, true
	case core.GroupAssign:
		return KindGroupAssign, true
	case core.GroupRemove:
		return KindGroupRemove, true
	case core.OffsetCommit:
		return KindOffsetCommit, true
	}
	return 0, false
}

// Encode writes the payload of a group write.
func (Module) Encode(w *kv.Writer, _ module.Kind, operation core.ControlPlaneOperation) {
	switch x := operation.(type) {
	case core.GroupInstall:
		encodeInstall(w, x)
	case core.GroupAssign:
		encodeAssign(w, x)
	case core.GroupRemove:
		encodeRemove(w, x)
	case core.OffsetCommit:
		encodeOffsetCommit(w, x)
	}
}

// Decode reads the payload of an entry of kind.
func (Module) Decode(r *kv.Reader, kind module.Kind) core.ControlPlaneOperation {
	switch kind {
	case KindGroupInstall:
		return decodeInstall(r)
	case KindGroupAssign:
		return decodeAssign(r)
	case KindGroupRemove:
		return decodeRemove(r)
	case KindOffsetCommit:
		return decodeOffsetCommit(r)
	}
	r.Fail()
	return nil
}

func encodeInstall(w *kv.Writer, operation core.GroupInstall) {
	w.String(operation.Group)
	w.String(operation.ProtocolType)
	w.String(operation.Protocol)
	w.Varint(int64(operation.Generation))
	w.String(operation.Leader)
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		w.String(m.MemberID)
		w.String(m.ClientID)
		w.String(m.ClientHost)
		w.String(m.GroupInstanceID)
		w.Varint(int64(m.SessionTimeoutMs))
		w.Varint(int64(m.RebalanceTimeoutMs))
		w.String(string(m.Metadata))
	}
}

func decodeInstall(r *kv.Reader) core.GroupInstall {
	var operation core.GroupInstall
	operation.Group = r.String()
	operation.ProtocolType = r.String()
	operation.Protocol = r.String()
	operation.Generation = int32(r.Varint())
	operation.Leader = r.String()
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var m core.GroupMember
		m.MemberID = r.String()
		m.ClientID = r.String()
		m.ClientHost = r.String()
		m.GroupInstanceID = r.String()
		m.SessionTimeoutMs = int32(r.Varint())
		m.RebalanceTimeoutMs = int32(r.Varint())
		m.Metadata = r.Bytes()
		operation.Members = append(operation.Members, m)
	}
	return operation
}

func encodeAssign(w *kv.Writer, operation core.GroupAssign) {
	w.String(operation.Group)
	w.Varint(int64(operation.Generation))
	w.String(operation.Leader)
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Assignments)))
	for _, a := range operation.Assignments {
		w.String(a.Member)
		w.String(string(a.Bytes))
	}
}

func decodeAssign(r *kv.Reader) core.GroupAssign {
	var operation core.GroupAssign
	operation.Group = r.String()
	operation.Generation = int32(r.Varint())
	operation.Leader = r.String()
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var a core.GroupAssignment
		a.Member = r.String()
		a.Bytes = r.Bytes()
		operation.Assignments = append(operation.Assignments, a)
	}
	return operation
}

func encodeRemove(w *kv.Writer, operation core.GroupRemove) {
	w.String(operation.Group)
	w.Varint(operation.TimeMs)
	w.String(operation.Reason)
	w.Uvarint(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		w.String(m)
	}
}

func decodeRemove(r *kv.Reader) core.GroupRemove {
	var operation core.GroupRemove
	operation.Group = r.String()
	operation.TimeMs = r.Varint()
	operation.Reason = r.String()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		operation.Members = append(operation.Members, r.String())
	}
	return operation
}

func encodeOffsetCommit(w *kv.Writer, operation core.OffsetCommit) {
	w.String(operation.Group)
	w.String(operation.Member)
	w.Varint(int64(operation.Generation))
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Offsets)))
	for _, o := range operation.Offsets {
		w.String(o.TopicPartition.Topic)
		w.Varint(int64(o.TopicPartition.Partition))
		w.Varint(o.Offset)
		w.Varint(int64(o.LeaderEpoch))
		w.String(o.Metadata)
	}
}

func decodeOffsetCommit(r *kv.Reader) core.OffsetCommit {
	var operation core.OffsetCommit
	operation.Group = r.String()
	operation.Member = r.String()
	operation.Generation = int32(r.Varint())
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var o core.PartitionOffset
		o.TopicPartition.Topic = r.String()
		o.TopicPartition.Partition = int32(r.Varint())
		o.Offset = r.Varint()
		o.LeaderEpoch = int32(r.Varint())
		o.Metadata = r.String()
		operation.Offsets = append(operation.Offsets, o)
	}
	return operation
}
