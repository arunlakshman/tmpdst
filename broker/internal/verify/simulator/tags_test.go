package simulator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/machine"
)

// scanCore maps each marker method (isEvent, isAction, ...) to the core types that
// have it, so a new type that this test does not list fails the test.
func scanCore(t *testing.T) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	names, err := filepath.Glob("../../core/*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, fname := range names {
		if strings.HasSuffix(fname, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, fname, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) != 1 || !strings.HasPrefix(fd.Name.Name, "is") {
				continue
			}
			if id, ok := fd.Recv.List[0].Type.(*ast.Ident); ok {
				out[fd.Name.Name] = append(out[fd.Name.Name], id.Name)
			}
		}
	}
	for k := range out {
		slices.Sort(out[k])
	}
	return out
}

func names[T any](m map[string]T) []string {
	return slices.Sorted(maps.Keys(m))
}

func eq(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

var (
	sampleInstance = machine.NodeInstance{Node: "n1", Inc: machine.BootNumber{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	sampleRef      = core.SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	sampleTP       = core.TopicPartition{Topic: "t", Partition: -2}
	sampleRange    = core.BatchRange{TopicPartition: sampleTP, Base: 5, Count: 3, Loc: sampleRef}
	samplePO       = core.PartitionOffset{TopicPartition: sampleTP, Offset: 9, LeaderEpoch: -1, Metadata: "m"}
	sampleMember   = core.GroupMember{MemberID: "m1", ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 10, RebalanceTimeoutMs: -20, Metadata: []byte("md")}
	sampleInfo     = core.GroupMemberInfo{GroupMember: sampleMember, JoinedTimeMs: 77, Assignment: []byte("as")}
	sampleAssign   = []core.GroupAssignment{{Member: "m1", Bytes: []byte("x")}, {Member: "m2", Bytes: nil}}
	sampleDescribe = core.DescribeGroupResp{
		Exists: true, Generation: 4, ProtocolType: "consumer", Protocol: "range", Leader: "m1",
		AssignedGeneration: 3, State: "Stable", Members: []core.GroupMemberInfo{sampleInfo},
	}
)

// sampleEvents has one value of every EventData case, with every field set.
func sampleEvents() map[string]core.EventData {
	return map[string]core.EventData{
		"NodeStarted": core.NodeStarted{Self: sampleInstance}, "StopRequest": core.StopRequest{Deadline: -5},
		"ConnOpened": core.ConnOpened{Conn: 3, Peer: "p"}, "BytesReceived": core.BytesReceived{Conn: 3, Data: []byte("abc")},
		"ConnClosed": core.ConnClosed{Conn: 4, Abort: true}, "SendDone": core.SendDone{Req: 5, Conn: 6, Err: core.ErrIO},
		"ConnectDone": core.ConnectDone{Req: 5, Conn: 6, Err: core.ErrConnClosed}, "TimerFired": core.TimerFired{ID: 7, Gen: 8},
		"ObjectPutDone":    core.ObjectPutDone{Req: 9, Key: "k", Err: core.ErrTimeout, Outcome: core.MaybeApplied},
		"ObjectGetDone":    core.ObjectGetDone{Req: 9, Data: []byte("d"), Err: core.ErrNotFound},
		"ObjectDeleteDone": core.ObjectDeleteDone{Req: 9, Err: core.ErrIO, Outcome: core.Applied},
		"ObjectListDone":   core.ObjectListDone{Req: 9, Keys: []string{"a", "b"}, Err: core.ErrUnavailable},
		"ControlPlaneDone": core.ControlPlaneDone{Req: 9, Resp: core.GroupSyncResp{Assignment: []byte("z")}, Err: core.OK, Outcome: core.Applied},
		"ControlPlaneDeliver": core.ControlPlaneDeliver{
			From: sampleInstance, Req: 11, Op: core.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
		},
		"LiveBrokers": core.LiveBrokers{Brokers: []core.BrokerAddr{{NodeID: 1, Host: "h", Port: 9092}, {NodeID: -1}}},
	}
}

// sampleActions has one value of every Action case, with every field set.
func sampleActions() map[string]core.Action {
	return map[string]core.Action{
		"SetTimer": core.SetTimer{ID: 1, Gen: 2, After: 3 * machine.Second}, "CancelTimer": core.CancelTimer{ID: 1},
		"Send": core.Send{Req: 1, Conn: 2, Data: []byte("s")}, "CloseConn": core.CloseConn{Conn: 2, Abort: true},
		"PauseReads": core.PauseReads{Conn: 2}, "ResumeReads": core.ResumeReads{Conn: 3},
		"Connect":   core.Connect{Req: 1, Addr: "a:1"},
		"ObjectPut": core.ObjectPut{Req: 1, Key: "k", Data: []byte("v")}, "ObjectGet": core.ObjectGet{Req: 1, Ref: sampleRef},
		"ObjectDelete": core.ObjectDelete{Req: 1, Key: "k"}, "ObjectList": core.ObjectList{Req: 1, Prefix: "p/"},
		"ControlPlaneCall": core.ControlPlaneCall{Req: 1, Op: core.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3}, Timeout: machine.Second},
		"ControlPlaneReply": core.ControlPlaneReply{
			To: sampleInstance, Done: core.ControlPlaneDone{Req: 2, Resp: core.ListGroupsResp{}, Err: core.OK, Outcome: core.Applied},
		},
		"RecordFact": core.RecordFact{Fact: machine.Fact{Name: "f"}}, "StopForRestart": core.StopForRestart{Cause: "why"},
		"ShutdownDone": core.ShutdownDone{},
	}
}

// sampleOperations has one value of every ControlPlaneOperation case, with every field set.
// CreateTopic has two values, because its tag depends on FailIfExists.
func sampleOperations() map[string]core.ControlPlaneOperation {
	return map[string]core.ControlPlaneOperation{
		"CommitSegment":        core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{{TopicPartition: sampleTP, Records: 4, Loc: sampleRef}}},
		"FindSegments":         core.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3},
		"DescribeTopics":       core.DescribeTopics{Topics: []string{"a", "b"}},
		"CreateTopic":          core.CreateTopic{Name: "t", Partitions: 3},
		"CreateTopicExclusive": core.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
		"GroupInstall": core.GroupInstall{
			Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 2, Leader: "m1", TimeMs: 9,
			Members: []core.GroupMember{sampleMember},
		},
		"GroupAssign":   core.GroupAssign{Group: "g", Generation: 2, Leader: "m1", TimeMs: 9, Assignments: sampleAssign},
		"GroupRemove":   core.GroupRemove{Group: "g", Members: []string{"m1", "m2"}, TimeMs: 9, Reason: "leave"},
		"OffsetCommit":  core.OffsetCommit{Group: "g", Member: "m1", Generation: -1, TimeMs: 9, Offsets: []core.PartitionOffset{samplePO}},
		"OffsetFetch":   core.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{sampleTP}},
		"DescribeGroup": core.DescribeGroup{Group: "g"}, "ListGroups": core.ListGroups{},
		"GroupJoin": core.GroupJoin{
			Group: "g", MemberID: "m1", GroupInstanceID: "i", ClientID: "c", ClientHost: "h", SessionTimeoutMs: 10,
			RebalanceTimeoutMs: 20, ProtocolType: "consumer", Protocols: []core.GroupProtocol{{Name: "range", Metadata: []byte("q")}},
		},
		"GroupSync":      core.GroupSync{Group: "g", MemberID: "m1", Generation: 3, Assignments: sampleAssign},
		"GroupHeartbeat": core.GroupHeartbeat{Group: "g", MemberID: "m1", Generation: 3},
		"GroupLeave":     core.GroupLeave{Group: "g", Members: []string{"m1"}},
		"DescribeGroups": core.DescribeGroups{Groups: []string{"g", "h"}},
	}
}

// sampleResponses has one value of every ControlPlaneResponse case, with every field set.
func sampleResponses() map[string]core.ControlPlaneResponse {
	return map[string]core.ControlPlaneResponse{
		"CommitSegmentResp":  core.CommitSegmentResp{Assigned: []core.BatchRange{sampleRange}},
		"FindSegmentsResp":   core.FindSegmentsResp{Ranges: []core.BatchRange{sampleRange}, End: 12},
		"DescribeTopicsResp": core.DescribeTopicsResp{Topics: []core.TopicInfo{{Name: "t", Partitions: 3}}},
		"OffsetCommitResp":   core.OffsetCommitResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownPartition}},
		"OffsetFetchResp":    core.OffsetFetchResp{Offsets: []core.PartitionOffset{samplePO}},
		"DescribeGroupResp":  sampleDescribe,
		"ListGroupsResp":     core.ListGroupsResp{Groups: []core.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}},
		"GroupJoinResp": core.GroupJoinResp{
			Generation: 3, Protocol: "range", Leader: "m1", MemberID: "m2",
			Members: []core.GroupJoinMember{{MemberID: "m1", GroupInstanceID: "i", Metadata: []byte("md")}},
		},
		"GroupSyncResp":      core.GroupSyncResp{Assignment: []byte("as")},
		"GroupLeaveResp":     core.GroupLeaveResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownMember}},
		"DescribeGroupsResp": core.DescribeGroupsResp{Groups: []core.GroupDescription{{Name: "g", DescribeGroupResp: sampleDescribe}}},
	}
}

// sampleFieldValues has one value of every field value case. They live in dst/machine.
func sampleFieldValues() map[string]machine.FieldValue {
	return map[string]machine.FieldValue{
		"Int64Value": machine.Int64Value(-4), "Uint64Value": machine.Uint64Value(4), "StringValue": machine.StringValue("s"),
		"Bool": machine.Bool(true), "Bytes": machine.Bytes("by"),
	}
}

// The test lists every case of the sums. If core gets a new case that is not here, the test
// fails. A case without Tag and Fold does not compile at all.
func TestEveryCoreTypeHasATag(t *testing.T) {
	events, actions, ops, resps := sampleEvents(), sampleActions(), sampleOperations(), sampleResponses()
	// CreateTopicExclusive is the second value of CreateTopic.
	opNames := slices.DeleteFunc(names(ops), func(n string) bool { return n == "CreateTopicExclusive" })
	src := scanCore(t)
	for marker, have := range map[string][]string{
		"isEvent": names(events), "isAction": names(actions), "isControlPlaneOperation": opNames,
		"isControlPlaneResponse": names(resps),
	} {
		if !eq(src[marker], have) {
			t.Errorf("%s: core declares %v, tag test lists %v", marker, src[marker], have)
		}
	}

	// Every tag is in its range, and no two cases share a tag.
	seen := map[uint64]string{}
	check := func(kind, name string, tag, lo, hi uint64) {
		if tag < lo || tag > hi {
			t.Errorf("%s %s: tag %d is outside %d..%d", kind, name, tag, lo, hi)
		}
		if prev, dup := seen[tag]; dup {
			t.Errorf("%s %s and %s have the same tag %d", kind, name, prev, tag)
		}
		seen[tag] = kind + " " + name
	}
	for n, v := range events {
		check("event", n, v.Tag(), 1, 99)
	}
	for n, v := range actions {
		check("action", n, v.Tag(), 101, 199)
	}
	for n, v := range ops {
		check("operation", n, v.Tag(), 201, 299)
	}
	for n, v := range resps {
		check("response", n, v.Tag(), 301, 399)
	}

	// Equal bytes are not enough: two values must also hash differently.
	hashes := map[uint64]string{}
	add := func(name string, fold func(h *stateHasher)) {
		h := newStateHasher()
		fold(&h)
		if prev, dup := hashes[h.Sum()]; dup {
			t.Errorf("%s and %s hash alike", name, prev)
		}
		hashes[h.Sum()] = name
	}
	for n, v := range events {
		add("event "+n, func(h *stateHasher) { h.event(v) })
	}
	for n, v := range actions {
		add("action "+n, func(h *stateHasher) { h.action(v) })
	}
	for n, v := range ops {
		add("operation "+n, func(h *stateHasher) { v.Fold(&h.Hash) })
	}
	for n, v := range resps {
		add("response "+n, func(h *stateHasher) { v.Fold(&h.Hash) })
	}
	for n, v := range sampleFieldValues() {
		add("field "+n, func(h *stateHasher) { v.Fold(&h.Hash) })
	}
}

// Every core.EventData must also classify, or the inbox would panic mid-run.
func TestTaggedEventsClassify(t *testing.T) {
	for _, b := range []core.EventData{core.NodeStarted{}, core.StopRequest{}, core.ConnOpened{}, core.ConnectDone{}, core.ControlPlaneDone{}} {
		_ = core.InboxKindOf(b)
	}
}
