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
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// scanCore maps each marker method (isEvent, isAction, ...) to the types that have it, so a
// new type that this test does not list fails the test. It reads broker core, and for the
// control plane operations and responses (isOperation, isResponse) package contract.
func scanCore(t *testing.T) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	names, err := filepath.Glob("../../core/*.go")
	if err != nil {
		t.Fatal(err)
	}
	more, err := filepath.Glob("../../../../controlplane/contract/*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(more) == 0 {
		t.Fatal("no files found in controlplane/contract")
	}
	names = append(names, more...)
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

// caseNames lists the names of the Go types that the samples stand for, once each. The four
// object store results are samples of the one wrapper type core.ObjectDone, and the four
// object store calls are samples of core.ObjectCall.
func caseNames[T any](m map[string]T) []string {
	set := map[string]bool{}
	for n := range m {
		switch n {
		case "ObjectPutDone", "ObjectGetDone", "ObjectDeleteDone", "ObjectListDone":
			n = "ObjectDone"
		case "ObjectPut", "ObjectGet", "ObjectDelete", "ObjectList":
			n = "ObjectCall"
		}
		set[n] = true
	}
	return slices.Sorted(maps.Keys(set))
}

func eq(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

var (
	sampleInstance = machine.NodeInstance{Node: "n1", Inc: machine.BootNumber{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	sampleRef      = core.SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	sampleTP       = core.TopicPartition{Topic: "t", Partition: -2}
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
func sampleEvents() map[string]core.EventData {
	return map[string]core.EventData{
		"NodeStarted": core.NodeStarted{Self: sampleInstance}, "StopRequest": core.StopRequest{Deadline: -5},
		"ConnOpened": core.ConnOpened{Conn: 3, Peer: "p"}, "BytesReceived": core.BytesReceived{Conn: 3, Data: []byte("abc")},
		"ConnClosed": core.ConnClosed{Conn: 4, Abort: true}, "SendDone": core.SendDone{Req: 5, Conn: 6, Err: core.ErrIO},
		"ConnectDone": core.ConnectDone{Req: 5, Conn: 6, Err: core.ErrConnClosed}, "TimerFired": core.TimerFired{ID: 7, Gen: 8},
		"ObjectPutDone":    core.ObjectDone{Result: objectstore.PutDone{Req: 9, Key: "k", Err: core.ErrTimeout, Outcome: core.MaybeApplied}},
		"ObjectGetDone":    core.ObjectDone{Result: objectstore.GetDone{Req: 9, Data: []byte("d"), Err: core.ErrNotFound}},
		"ObjectDeleteDone": core.ObjectDone{Result: objectstore.DeleteDone{Req: 9, Err: core.ErrIO, Outcome: core.Applied}},
		"ObjectListDone":   core.ObjectDone{Result: objectstore.ListDone{Req: 9, Keys: []string{"a", "b"}, Err: core.ErrUnavailable}},
		"ControlPlaneDone": core.ControlPlaneDone{Req: 9, Resp: contract.GroupSyncResp{Assignment: []byte("z")}, Err: core.OK, Outcome: core.Applied},
		"ControlPlaneDeliver": core.ControlPlaneDeliver{
			From: sampleInstance, Req: 11, Op: contract.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
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
		"ObjectPut": core.ObjectCall{Call: objectstore.Put{Req: 1, Key: "k", Data: []byte("v")}}, "ObjectGet": core.ObjectCall{Call: objectstore.Get{Req: 1, Ref: sampleRef}},
		"ObjectDelete": core.ObjectCall{Call: objectstore.Delete{Req: 1, Key: "k"}}, "ObjectList": core.ObjectCall{Call: objectstore.List{Req: 1, Prefix: "p/"}},
		"ControlPlaneCall": core.ControlPlaneCall{Req: 1, Op: contract.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3}, Timeout: machine.Second},
		"ControlPlaneReply": core.ControlPlaneReply{
			To: sampleInstance, Done: core.ControlPlaneDone{Req: 2, Resp: contract.ListGroupsResp{}, Err: core.OK, Outcome: core.Applied},
		},
		"RecordFact": core.RecordFact{Fact: machine.Fact{Name: "f"}}, "StopForRestart": core.StopForRestart{Cause: "why"},
		"ShutdownDone": core.ShutdownDone{},
	}
}

// sampleOperations has one value of every ControlPlaneOperation case, with every field set.
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
		"OffsetFetch":   contract.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{sampleTP}},
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

// sampleResponses has one value of every ControlPlaneResponse case, with every field set.
func sampleResponses() map[string]contract.Response {
	return map[string]contract.Response{
		"CommitSegmentResp":  contract.CommitSegmentResp{Assigned: []contract.BatchRange{sampleRange}},
		"FindSegmentsResp":   contract.FindSegmentsResp{Ranges: []contract.BatchRange{sampleRange}, End: 12},
		"DescribeTopicsResp": contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}},
		"OffsetCommitResp":   contract.OffsetCommitResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownPartition}},
		"OffsetFetchResp":    contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{samplePO}},
		"DescribeGroupResp":  sampleDescribe,
		"ListGroupsResp":     contract.ListGroupsResp{Groups: []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}},
		"GroupJoinResp": contract.GroupJoinResp{
			Generation: 3, Protocol: "range", Leader: "m1", MemberID: "m2",
			Members: []contract.GroupJoinMember{{MemberID: "m1", GroupInstanceID: "i", Metadata: []byte("md")}},
		},
		"GroupSyncResp":      contract.GroupSyncResp{Assignment: []byte("as")},
		"GroupLeaveResp":     contract.GroupLeaveResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownMember}},
		"DescribeGroupsResp": contract.DescribeGroupsResp{Groups: []contract.GroupDescription{{Name: "g", DescribeGroupResp: sampleDescribe}}},
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
		"isEvent": caseNames(events), "isAction": caseNames(actions), "isOperation": opNames,
		"isResponse": names(resps),
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
