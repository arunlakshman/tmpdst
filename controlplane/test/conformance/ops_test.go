package conformance

import (
	"bytes"
	"slices"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The operations that rapid draws, from simple to complex: reads, commit and
// create topic, group writes, then background operations. The shrinker moves
// the drawn index toward 0, so a failure shrinks to the simplest operations
// that still show it.
//
// Names, keys and members come from small sets, so that operations hit the
// same topic, key or group again and again. Without that, a replay, a conflict
// or a group with members would almost never happen. Besides that, only these
// draws lean on purpose. Each is a small number that shrinks to the plain case.
//   - About 1 name in 10 is empty, and a few numbers are rarely out of range,
//     to reach the ErrInvalid rules.
//   - A commit is a replay of an earlier commit, or its key with other records
//     (a conflict), 2 times in 3.
//   - Group install, assign and offset commit mostly use the generation and
//     the leader that the model says is current. A random generation is almost
//     always ErrIllegalGeneration, so a group would never get members.
//   - FindSegments and each fresh commit are followed by finds with MaxBytes
//     at the edges of the stop rule, and a large group assign is sent at the
//     edge of the size limit.
//
// Each op lists in want the answers that Run must see, as "label/code".

type op struct {
	name string
	once bool // not safe to send twice: never drawn for a target with Inject
	want []string
	run  func(w *world)
}

var (
	topicNames  = []string{"a", "b", "c"}
	groupNames  = []string{"g1", "g2"}
	memberNames = []string{"m1", "m2", "m3"}
	segmentKeys = []string{"k0", "k1", "k2", "k3"}
)

// ops returns the operations for target tg, in order.
func ops(tg Target) []op {
	list := []op{
		// FindSegments of one partition, then again at each MaxBytes edge.
		{name: "find", want: []string{"find/OK", "find/ErrInvalid", "find.edge/OK"}, run: find},
		// DescribeTopics of some names, or all.
		{name: "describeTopics", want: []string{"describeTopics/OK"}, run: func(w *world) {
			names := rapid.SliceOfN(rapid.SampledFrom(append(topicNames, "zz")), 0, 2).Draw(w.t, "names")
			w.do("describeTopics", contract.DescribeTopics{Topics: names})
		}},
		// OffsetFetch of some partitions, or all.
		{name: "offsetFetch", run: func(w *world) {
			ps := rapid.SliceOfN(rapid.Custom(drawPartition), 0, 3).Draw(w.t, "partitions")
			w.do("offsetFetch", contract.OffsetFetch{Group: pick(w.t, "group", groupNames), Partitions: ps})
		}},
		{name: "describeGroup", want: []string{"describeGroup/OK"}, run: func(w *world) {
			w.do("describeGroup", contract.DescribeGroup{Group: pick(w.t, "group", groupNames)})
		}},
		{name: "listGroups", run: func(w *world) { w.do("listGroups", contract.ListGroups{}) }},
		// CommitSegment: fresh, a replay, or a conflict.
		{name: "commit", want: []string{"commit/OK", "commit/ErrInvalid", "commit.replay/OK", "commit.conflict/ErrConflict"}, run: commit},
		// CreateTopic. FailIfExists is not safe to send twice.
		{name: "createTopic", want: []string{"createTopic/OK", "createTopic/ErrInvalid"}, run: func(w *world) {
			op := contract.CreateTopic{Name: pick(w.t, "name", topicNames), Partitions: int32(rapid.IntRange(0, 4).Draw(w.t, "partitions"))}
			if w.target.Inject == nil {
				op.FailIfExists = rapid.Bool().Draw(w.t, "failIfExists")
			}
			w.do("createTopic", op)
		}},
		{name: "groupInstall", once: true, want: []string{"groupInstall/OK", "groupInstall/ErrIllegalGeneration", "groupInstall/ErrInvalid"}, run: groupInstall},
		{name: "groupAssign", once: true, want: []string{"groupAssign/OK", "groupAssign.unknown/ErrUnknownMember", "groupAssign.stale/ErrIllegalGeneration"}, run: groupAssign},
		{name: "groupRemove", once: true, want: []string{"groupRemove/OK"}, run: func(w *world) {
			ids := rapid.SliceOfNDistinct(rapid.SampledFrom(memberNames), 0, 2, rapid.ID).Draw(w.t, "members")
			w.do("groupRemove", contract.GroupRemove{Group: pick(w.t, "group", groupNames), Members: ids, Reason: "leave", TimeMs: 1})
		}},
		{name: "offsetCommit", want: []string{"offsetCommit/OK", "offsetCommit/ErrIllegalGeneration", "offsetCommit/ErrUnknownMember"}, run: offsetCommit},
		// GroupAssign with one assignment of about 256 KiB: just below, at or
		// just above the size limit (68 bytes are the rest of the command).
		{name: "groupAssignLarge", once: true, want: []string{"groupAssign.large/ErrTooLarge"}, run: func(w *world) {
			big := bytes.Repeat([]byte("x"), 256<<10-68+rapid.IntRange(-1, 1).Draw(w.t, "edge"))
			w.do("groupAssign.large", contract.GroupAssign{Group: "g1", Generation: 1, Leader: "m1", Assignments: []contract.GroupAssignment{{Member: "m1", Bytes: big}}})
		}},
	}
	list = slices.DeleteFunc(list, func(o op) bool { return slices.Contains(tg.Lacks, o.name) })
	// Background operations do nothing in the model, so they must not change
	// any answer.
	if tg.Checkpoint != nil {
		list = append(list, op{name: "checkpoint", want: []string{"checkpoint"}, run: func(w *world) { w.target.Checkpoint() }})
	}
	if tg.Reboot != nil {
		list = append(list, op{name: "reboot", want: []string{"reboot", "sweep.find/OK", "sweep.describeGroup/OK", "sweep.replay/OK"}, run: func(w *world) {
			w.target.Reboot()
			w.afterRestart("forward progress")
		}})
	}
	if tg.DirtyReboot != nil {
		list = append(list, op{name: "dirtyReboot", want: []string{"dirtyReboot", "sweep.find/OK", "sweep.describeGroup/OK", "sweep.replay/OK"}, run: func(w *world) {
			w.target.DirtyReboot(rapid.IntRange(0, 2).Draw(w.t, "checkpoints kept"))
			w.afterRestart("persistence")
		}})
	}
	if tg.Inject != nil {
		list = slices.DeleteFunc(list, func(o op) bool { return o.once })
		list = append(list, op{name: "inject", want: []string{"inject"}, run: func(w *world) {
			w.target.Inject(rapid.IntRange(0, failureKinds-1).Draw(w.t, "failure"))
			w.hasFailed = true
		}})
	}
	return list
}

// pick draws one of values, or "" about 1 time in 10.
func pick(t *rapid.T, label string, values []string) string {
	i := rapid.IntRange(0, 3*len(values)).Draw(t, label)
	if i == 3*len(values) {
		return ""
	}
	return values[i/3]
}

// rare is true about 1 time in 20.
func rare(t *rapid.T, label string) bool { return rapid.IntRange(0, 19).Draw(t, label) == 0 }

func drawPartition(t *rapid.T) contract.TopicPartition {
	return contract.NewTopicPartition(pick(t, "topic", topicNames), int32(rapid.IntRange(0, 2).Draw(t, "partition")))
}

// known are the partitions that exist in the model now.
func (w *world) known() []contract.TopicPartition {
	var tps []contract.TopicPartition
	for _, topic := range w.model.Apply(contract.DescribeTopics{}).Resp.(contract.DescribeTopicsResp).Topics {
		for p := range topic.Partitions {
			tps = append(tps, contract.NewTopicPartition(topic.Name, p))
		}
	}
	return tps
}

// drawBatch draws a partition that exists 3 times in 4, if there is one. A
// commit to a partition that does not exist is ErrInvalid, so without this a
// commit that succeeds, and then a replay of it, would be rare.
func drawBatch(t *rapid.T, known []contract.TopicPartition) contract.PendingBatch {
	records := int64(rapid.IntRange(1, 4).Draw(t, "records"))
	if rare(t, "no records") {
		records = 0
	}
	loc := runtime.SegmentRef{Key: "seg", Off: int64(rapid.IntRange(0, 50).Draw(t, "off")), Len: int64(rapid.IntRange(1, 12).Draw(t, "len"))}
	tp := drawPartition(t)
	if len(known) > 0 && rapid.IntRange(0, 3).Draw(t, "partition exists") < 3 {
		tp = rapid.SampledFrom(known).Draw(t, "existing partition")
	}
	return contract.PendingBatch{TopicPartition: tp, Records: records, Loc: loc}
}

func find(w *world) {
	from := contract.Offset(rapid.IntRange(0, 12).Draw(w.t, "from"))
	if rare(w.t, "negative from") {
		from = -1
	}
	op := contract.FindSegments{TopicPartition: drawPartition(w.t), From: from, MaxBytes: int32(rapid.IntRange(0, 25).Draw(w.t, "maxBytes"))}
	w.do("find", op)
	w.edges(op)
}

// edges asks op again with MaxBytes just below, at and just above each running
// sum of Loc.Len, because the stop rule (the sum reaches MaxBytes) turns
// exactly there.
func (w *world) edges(op contract.FindSegments) {
	op.MaxBytes = 0
	all, ok := w.model.Apply(op).Resp.(contract.FindSegmentsResp)
	if !ok {
		return
	}
	var sum int64
	for _, r := range all.Ranges {
		sum += r.Loc.Len
		for _, edge := range []int64{sum - 1, sum, sum + 1} {
			if edge > 0 {
				op.MaxBytes = int32(edge)
				w.do("find.edge", op)
			}
		}
	}
}

func commit(w *world) {
	mode := rapid.IntRange(0, 2).Draw(w.t, "commit mode") // 0 fresh, 1 replay, 2 conflict
	if mode > 0 && len(w.commits) > 0 {
		old := w.commits[rapid.IntRange(0, len(w.commits)-1).Draw(w.t, "which commit")]
		if mode == 1 || len(old.Batches) == 0 {
			w.do("commit.replay", old)
			return
		}
		other := contract.CommitSegment{SegmentKey: old.SegmentKey, Batches: slices.Clone(old.Batches)}
		other.Batches[0].Records++
		w.do("commit.conflict", other)
		return
	}
	known := w.known()
	batch := rapid.Custom(func(t *rapid.T) contract.PendingBatch { return drawBatch(t, known) })
	op := contract.CommitSegment{SegmentKey: pick(w.t, "key", segmentKeys), Batches: rapid.SliceOfN(batch, 1, 4).Draw(w.t, "batches")}
	if rare(w.t, "no batches") {
		op.Batches = nil
	}
	w.commits = append(w.commits, op)
	w.do("commit", op)
	if len(op.Batches) > 0 {
		w.edges(contract.FindSegments{TopicPartition: op.Batches[0].TopicPartition})
	}
}

// current is what the model says about a group now.
func (w *world) current(group string) contract.DescribeGroupResp {
	return w.model.Apply(contract.DescribeGroup{Group: group}).Resp.(contract.DescribeGroupResp)
}

func groupInstall(w *world) {
	group := pick(w.t, "group", groupNames)
	gen := w.current(group).Generation + 1
	switch rapid.IntRange(0, 5).Draw(w.t, "generation") { // 4 times in 6 the next one
	case 4:
		gen--
	case 5:
		gen++
	}
	ids := rapid.SliceOfNDistinct(rapid.SampledFrom(memberNames), 0, 3, rapid.ID).Draw(w.t, "members")
	op := contract.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, TimeMs: int64(rapid.IntRange(0, 1000).Draw(w.t, "time"))}
	for _, id := range ids {
		meta := rapid.SliceOfN(rapid.Byte(), 0, 3).Draw(w.t, "metadata")
		op.Members = append(op.Members, contract.GroupMember{MemberID: id, ClientID: "c-" + id, SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: meta})
	}
	if len(ids) > 0 {
		op.Leader = ids[rapid.IntRange(0, len(ids)-1).Draw(w.t, "leader")]
	}
	if rare(w.t, "other leader") {
		op.Leader = "m9"
	}
	w.do("groupInstall", op)
}

func groupAssign(w *world) {
	group := pick(w.t, "group", groupNames)
	cur := w.current(group)
	if cur.Leader == "" && group != "" {
		// A group with no members would almost never get a leader, an
		// assignment and an unknown member in one check. So install one first.
		w.do("groupInstall.setup", contract.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: cur.Generation + 1,
			Leader: "m1", TimeMs: 1, Members: []contract.GroupMember{
				{MemberID: "m1", ClientID: "c-m1", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000},
				{MemberID: "m2", ClientID: "c-m2", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000},
			}})
		cur = w.current(group)
	}
	op := contract.GroupAssign{Group: group, Generation: cur.Generation, Leader: cur.Leader}
	stale := rapid.IntRange(0, 2).Draw(w.t, "stale") == 2
	if stale {
		op.Generation--
	}
	for _, m := range cur.Members {
		if rapid.Bool().Draw(w.t, "assigned") {
			op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: m.MemberID, Bytes: []byte("a-" + m.MemberID)})
		}
	}
	unknown := rapid.IntRange(0, 4).Draw(w.t, "unknown member") == 0
	if unknown {
		op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: "zz", Bytes: []byte("z")})
	}
	// The generation is checked first, then the leader, then the members. The
	// two labels are only for a group that has a leader, so that the answer
	// shows that the generation check and the member check ran, and not the
	// leader check of a group that does not exist.
	label := "groupAssign"
	switch {
	case cur.Leader == "":
	case stale:
		label = "groupAssign.stale"
	case unknown:
		label = "groupAssign.unknown"
	}
	w.do(label, op)
}

func offsetCommit(w *world) {
	group := pick(w.t, "group", groupNames)
	cur := w.current(group)
	op := contract.OffsetCommit{Group: group, Generation: -1, TimeMs: 7}
	switch rapid.IntRange(0, 3).Draw(w.t, "how") {
	case 0: // a member of the current generation, if there is one
		if len(cur.Members) > 0 {
			op.Generation, op.Member = cur.Generation, cur.Members[0].MemberID
		}
	case 2: // the next generation, which is not current
		op.Generation, op.Member = cur.Generation+1, "m1"
	case 3: // a manual commit with a member name
		op.Member = "m1"
	}
	for range rapid.IntRange(0, 3).Draw(w.t, "how many") {
		op.Offsets = append(op.Offsets, contract.PartitionOffset{TopicPartition: drawPartition(w.t),
			Offset: int64(rapid.IntRange(0, 50).Draw(w.t, "offset")), LeaderEpoch: int32(rapid.IntRange(0, 3).Draw(w.t, "epoch")), Metadata: pick(w.t, "meta", []string{"x"})})
	}
	if len(op.Offsets) > 0 && rare(w.t, "negative offset") {
		op.Offsets[0].Offset = -1
	}
	w.do("offsetCommit", op)
}
