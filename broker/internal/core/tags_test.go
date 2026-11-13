package core

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// These tests keep the tag numbers and the Fold methods safe. Every case has one sample
// value, with every field set. The sample lists are the ones of the old tag tests.

var (
	sampleInstance = machine.NodeInstance{Node: "n1", Inc: machine.BootNumber{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	sampleRef      = SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	sampleTP       = TopicPartition{Topic: "t", Partition: -2}
	sampleRange    = contract.BatchRange{TopicPartition: sampleTP, Base: 5, Count: 3, Loc: sampleRef}
	samplePO       = contract.PartitionOffset{TopicPartition: sampleTP, Offset: 9, LeaderEpoch: -1, Metadata: "m"}
	sampleMember   = contract.GroupMember{MemberID: "m1", ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 10, RebalanceTimeoutMs: -20, Metadata: []byte("md")}
	sampleInfo     = contract.GroupMemberInfo{GroupMember: sampleMember, JoinedTimeMs: 77, Assignment: []byte("as")}
	sampleAssign   = []contract.GroupAssignment{{Member: "m1", Bytes: []byte("x")}, {Member: "m2", Bytes: nil}}
	sampleDescribe = contract.DescribeGroupResp{
		Exists: true, Generation: 4, ProtocolType: "consumer", Protocol: "range", Leader: "m1",
		AssignedGeneration: 3, State: "Stable", Members: []contract.GroupMemberInfo{sampleInfo},
	}
)

// sampleEvents has one value of every EventData case, with every field set.
func sampleEvents() map[string]EventData {
	return map[string]EventData{
		"NodeStarted": NodeStarted{Self: sampleInstance}, "StopRequest": StopRequest{Deadline: -5},
		"ConnOpened": ConnOpened{Conn: 3, Peer: "p"}, "BytesReceived": BytesReceived{Conn: 3, Data: []byte("abc")},
		"ConnClosed": ConnClosed{Conn: 4, Abort: true}, "SendDone": SendDone{Req: 5, Conn: 6, Err: ErrIO},
		"ConnectDone": ConnectDone{Req: 5, Conn: 6, Err: ErrConnClosed}, "TimerFired": TimerFired{ID: 7, Gen: 8},
		"ObjectPutDone":    ObjectDone{Result: objectstore.PutDone{Req: 9, Key: "k", Err: ErrTimeout, Outcome: MaybeApplied}},
		"ObjectGetDone":    ObjectDone{Result: objectstore.GetDone{Req: 9, Data: []byte("d"), Err: ErrNotFound}},
		"ObjectDeleteDone": ObjectDone{Result: objectstore.DeleteDone{Req: 9, Err: ErrIO, Outcome: Applied}},
		"ObjectListDone":   ObjectDone{Result: objectstore.ListDone{Req: 9, Keys: []string{"a", "b"}, Err: ErrUnavailable}},
		"ControlPlaneDone": ControlPlaneDone{Req: 9, Resp: contract.GroupSyncResp{Assignment: []byte("z")}, Err: OK, Outcome: Applied},
		"ControlPlaneDeliver": ControlPlaneDeliver{
			From: sampleInstance, Req: 11, Op: contract.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
		},
		"LiveBrokers": LiveBrokers{Brokers: []BrokerAddr{{NodeID: 1, Host: "h", Port: 9092}, {NodeID: -1}}},
	}
}

// sampleActions has one value of every Action case, with every field set.
func sampleActions() map[string]Action {
	return map[string]Action{
		"SetTimer": SetTimer{ID: 1, Gen: 2, After: 3 * machine.Second}, "CancelTimer": CancelTimer{ID: 1},
		"Send": Send{Req: 1, Conn: 2, Data: []byte("s")}, "CloseConn": CloseConn{Conn: 2, Abort: true},
		"PauseReads": PauseReads{Conn: 2}, "ResumeReads": ResumeReads{Conn: 3},
		"Connect":   Connect{Req: 1, Addr: "a:1"},
		"ObjectPut": ObjectCall{Call: objectstore.Put{Req: 1, Key: "k", Data: []byte("v")}}, "ObjectGet": ObjectCall{Call: objectstore.Get{Req: 1, Ref: sampleRef}},
		"ObjectDelete": ObjectCall{Call: objectstore.Delete{Req: 1, Key: "k"}}, "ObjectList": ObjectCall{Call: objectstore.List{Req: 1, Prefix: "p/"}},
		"ControlPlaneCall": ControlPlaneCall{Req: 1, Op: contract.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3}, Timeout: machine.Second},
		"ControlPlaneReply": ControlPlaneReply{
			To: sampleInstance, Done: ControlPlaneDone{Req: 2, Resp: contract.ListGroupsResp{}, Err: OK, Outcome: Applied},
		},
		"RecordFact": RecordFact{Fact: machine.Fact{Name: "f"}}, "StopForRestart": StopForRestart{Cause: "why"},
		"ShutdownDone": ShutdownDone{},
	}
}

// sampleOperations has one value of every control plane operation case, with every field set.
// CreateTopic has two values, because its tag depends on FailIfExists.
func sampleOperations() map[string]contract.Operation {
	return map[string]contract.Operation{
		"CommitSegment":        contract.CommitSegment{SegmentKey: "s", Batches: []contract.PendingBatch{{TopicPartition: sampleTP, Records: 4, Loc: sampleRef}}},
		"FindSegments":         contract.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3},
		"DescribeTopics":       contract.DescribeTopics{Topics: []string{"a", "b"}},
		"CreateTopic":          contract.CreateTopic{Name: "t", Partitions: 3},
		"CreateTopicExclusive": contract.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
		"GroupInstall": contract.GroupInstall{
			Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 2, Leader: "m1", TimeMs: 9,
			Members: []contract.GroupMember{sampleMember},
		},
		"GroupAssign":   contract.GroupAssign{Group: "g", Generation: 2, Leader: "m1", TimeMs: 9, Assignments: sampleAssign},
		"GroupRemove":   contract.GroupRemove{Group: "g", Members: []string{"m1", "m2"}, TimeMs: 9, Reason: "leave"},
		"OffsetCommit":  contract.OffsetCommit{Group: "g", Member: "m1", Generation: -1, TimeMs: 9, Offsets: []contract.PartitionOffset{samplePO}},
		"OffsetFetch":   contract.OffsetFetch{Group: "g", Partitions: []TopicPartition{sampleTP}},
		"DescribeGroup": contract.DescribeGroup{Group: "g"}, "ListGroups": contract.ListGroups{},
		"GroupJoin": contract.GroupJoin{
			Group: "g", MemberID: "m1", GroupInstanceID: "i", ClientID: "c", ClientHost: "h", SessionTimeoutMs: 10,
			RebalanceTimeoutMs: 20, ProtocolType: "consumer", Protocols: []contract.GroupProtocol{{Name: "range", Metadata: []byte("q")}},
		},
		"GroupSync":      contract.GroupSync{Group: "g", MemberID: "m1", Generation: 3, Assignments: sampleAssign},
		"GroupHeartbeat": contract.GroupHeartbeat{Group: "g", MemberID: "m1", Generation: 3},
		"GroupLeave":     contract.GroupLeave{Group: "g", Members: []string{"m1"}},
		"DescribeGroups": contract.DescribeGroups{Groups: []string{"g", "h"}},
	}
}

// sampleResponses has one value of every control plane response case, with every field set.
func sampleResponses() map[string]contract.Response {
	return map[string]contract.Response{
		"CommitSegmentResp":  contract.CommitSegmentResp{Assigned: []contract.BatchRange{sampleRange}},
		"FindSegmentsResp":   contract.FindSegmentsResp{Ranges: []contract.BatchRange{sampleRange}, End: 12},
		"DescribeTopicsResp": contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}},
		"OffsetCommitResp":   contract.OffsetCommitResp{Errs: []ErrCode{OK, ErrUnknownPartition}},
		"OffsetFetchResp":    contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{samplePO}},
		"DescribeGroupResp":  sampleDescribe,
		"ListGroupsResp":     contract.ListGroupsResp{Groups: []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}},
		"GroupJoinResp": contract.GroupJoinResp{
			Generation: 3, Protocol: "range", Leader: "m1", MemberID: "m2",
			Members: []contract.GroupJoinMember{{MemberID: "m1", GroupInstanceID: "i", Metadata: []byte("md")}},
		},
		"GroupSyncResp":      contract.GroupSyncResp{Assignment: []byte("as")},
		"GroupLeaveResp":     contract.GroupLeaveResp{Errs: []ErrCode{OK, ErrUnknownMember}},
		"DescribeGroupsResp": contract.DescribeGroupsResp{Groups: []contract.GroupDescription{{Name: "g", DescribeGroupResp: sampleDescribe}}},
	}
}

// sampleFieldValues has one value of every field value case. They live in dst/machine and
// have no Tag method: Fold writes the tag.
func sampleFieldValues() map[string]machine.FieldValue {
	return map[string]machine.FieldValue{
		"Int64Value": machine.Int64Value(-4), "Uint64Value": machine.Uint64Value(4), "StringValue": machine.StringValue("s"),
		"Bool": machine.Bool(true), "Bytes": machine.Bytes("by"),
	}
}

// foldOf folds one value into a fresh hash and returns the hash.
func foldOf(f func(h *trace.Hash)) trace.Hash {
	h := trace.New()
	f(&h)
	return h
}

// checkFold checks that Fold writes something, and writes the same bytes every time.
func checkFold(t *testing.T, name string, f func(h *trace.Hash)) {
	t.Helper()
	a, b := foldOf(f), foldOf(f)
	if a == trace.New() {
		t.Errorf("%s: Fold wrote nothing", name)
	}
	if a != b {
		t.Errorf("%s: Fold is not deterministic", name)
	}
}

// Every tag of an event, action, operation or response is in its range, and no two values
// share a tag. A value without a tag does not compile.
func TestTagsAreInRangeAndUnique(t *testing.T) {
	seen := map[uint64]string{}
	check := func(kind, name string, tag, lo, hi uint64) {
		t.Helper()
		if tag < lo || tag > hi {
			t.Errorf("%s %s: tag %d is outside %d..%d", kind, name, tag, lo, hi)
		}
		if prev, dup := seen[tag]; dup {
			t.Errorf("%s %s and %s have the same tag %d", kind, name, prev, tag)
		}
		seen[tag] = kind + " " + name
	}
	for n, v := range sampleEvents() {
		check("event", n, v.Tag(), 1, 99)
	}
	for n, v := range sampleActions() {
		check("action", n, v.Tag(), 101, 199)
	}
	for n, v := range sampleOperations() {
		check("operation", n, v.Tag(), 201, 299)
	}
	for n, v := range sampleResponses() {
		check("response", n, v.Tag(), 301, 399)
	}
}

// Fold of every case writes bytes, the same bytes each time, and two cases never fold alike.
func TestFoldWritesDistinctDeterministicBytes(t *testing.T) {
	hashes := map[trace.Hash]string{}
	add := func(name string, f func(h *trace.Hash)) {
		t.Helper()
		checkFold(t, name, f)
		h := foldOf(f)
		if prev, dup := hashes[h]; dup {
			t.Errorf("%s and %s fold alike", name, prev)
		}
		hashes[h] = name
	}
	for n, v := range sampleEvents() {
		add("event "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleActions() {
		add("action "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleOperations() {
		add("operation "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleResponses() {
		add("response "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleFieldValues() {
		add("field "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	// A value with a nil operation or response shares the tag of its type, so these are only
	// folded here, not in the tag check above.
	add("event ControlPlaneDone nil response", func(h *trace.Hash) {
		ControlPlaneDone{Req: 3, Err: ErrTimeout, Outcome: MaybeApplied}.Fold(h)
	})
	add("event ControlPlaneDeliver nil operation", func(h *trace.Hash) {
		ControlPlaneDeliver{From: sampleInstance, Req: 3}.Fold(h)
	})
	add("action ControlPlaneCall nil operation", func(h *trace.Hash) {
		ControlPlaneCall{Req: 4}.Fold(h)
	})
	add("action ControlPlaneReply nil response", func(h *trace.Hash) {
		ControlPlaneReply{To: sampleInstance, Done: ControlPlaneDone{Req: 4, Err: ErrIO}}.Fold(h)
	})
	fact := machine.Fact{Name: "fact", Fields: []machine.Field{{Key: "a", Value: machine.Int64Value(-1)}}}
	add("fact", func(h *trace.Hash) { fact.Fold(h) })
}
