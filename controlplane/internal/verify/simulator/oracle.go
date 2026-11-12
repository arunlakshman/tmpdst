package simulator

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/check"
	"github.com/manifesterio/mds/dst/machine"
)

// oracle judges the answers that clients see. It has two parts.
//
// While the run goes on it keeps lower bounds taken from real time: what a
// client was told before it sent a read, the read must show (see snapshot and
// checkRead). These bounds need no order of the writes.
//
// After the run it replays the log, slot by slot, into the spec
// (the specModel). The log is the truth about the order of the writes,
// including the writes whose answer was lost. Then every recorded read must
// equal the spec at the prefix of the log that the node had applied, and every
// acked write must be in the log (see verify).
type oracle struct {
	s      *sim
	acks   []ackRec
	reads  []readRec
	floor  floors
	groups groupChecks
}

type ackRec struct {
	op   runtime.ControlPlaneOperation
	done runtime.ControlPlaneDone
	at   int64
}

type readRec struct {
	op     runtime.ControlPlaneOperation
	done   runtime.ControlPlaneDone
	prefix runtime.LogPosition // slots applied by the node when it answered
	at     int64
	final  bool
}

// floors are the lower bounds from what clients have been told.
type floors struct {
	ends   check.Monotonic[contract.TopicPartition, contract.Offset]
	ranges map[contract.TopicPartition][]contract.BatchRange
	topics []contract.TopicInfo // sorted by name
	gens   check.Monotonic[string, int32]
}

// snapshot is a copy of the floors that matter to one read, taken when the read is sent.
type snapshot struct {
	end    contract.Offset
	ranges []contract.BatchRange
	topics []contract.TopicInfo
	gen    int32
}

func newOracle(s *sim) *oracle {
	return &oracle{s: s, floor: floors{
		ranges: map[contract.TopicPartition][]contract.BatchRange{},
	}, groups: groupChecks{leaders: map[groupGen]string{}, lastGen: map[string]int32{}, late: map[string]bool{}}}
}

func (f *floors) raiseTopic(name string, partitions int32) {
	i, found := slices.BinarySearchFunc(f.topics, name, func(t contract.TopicInfo, n string) int { return cmp.Compare(t.Name, n) })
	if found {
		f.topics[i].Partitions = max(f.topics[i].Partitions, partitions)
		return
	}
	f.topics = slices.Insert(f.topics, i, contract.TopicInfo{Name: name, Partitions: partitions})
}

// snapshotFor copies the floors that a read of op must respect.
func (o *oracle) snapshotFor(op runtime.ControlPlaneOperation) *snapshot {
	f := &o.floor
	switch q := op.(type) {
	case contract.FindSegments:
		return &snapshot{end: f.ends.Floor(q.TopicPartition), ranges: slices.Clone(f.ranges[q.TopicPartition])}
	case contract.DescribeTopics:
		snap := &snapshot{}
		for _, t := range f.topics {
			if len(q.Topics) == 0 || slices.Contains(q.Topics, t.Name) {
				snap.topics = append(snap.topics, t)
			}
		}
		return snap
	case contract.DescribeGroup:
		return &snapshot{gen: f.gens.Floor(q.Group)}
	}
	return nil
}

// record handles a final (not retried) answer of a client.
func (o *oracle) record(op *opState, d runtime.ControlPlaneDone, prefix runtime.LogPosition) {
	s := o.s
	if d.Err != runtime.OK {
		s.stats.Rejected++
		return
	}
	if op.write {
		s.stats.Acked++
		o.acks = append(o.acks, ackRec{op: op.op, done: d, at: int64(s.mono())})
		o.raiseFloors(op.op, d)
		s.clients.w.acked(op.op)
		return
	}
	s.stats.ReadsOK++
	o.reads = append(o.reads, readRec{op: op.op, done: d, prefix: prefix, at: int64(s.mono()), final: op.final})
	o.checkRead(op, d)
	o.observeRead(op.op, d)
}

// raiseFloors lifts the lower bounds after an acked write.
func (o *oracle) raiseFloors(op runtime.ControlPlaneOperation, d runtime.ControlPlaneDone) {
	f := &o.floor
	switch w := op.(type) {
	case contract.CommitSegment:
		resp, _ := d.Resp.(contract.CommitSegmentResp)
		for _, r := range resp.Assigned {
			tp := r.TopicPartition
			f.ends.Raise(tp, r.Base+contract.Offset(r.Count))
			f.ranges[tp] = append(f.ranges[tp], r)
			f.raiseTopic(tp.Topic, tp.Partition+1)
		}
	case contract.CreateTopic:
		f.raiseTopic(w.Name, w.Partitions)
	case contract.GroupInstall:
		f.gens.Raise(w.Group, w.Generation)
	}
}

// observeRead lifts the lower bounds after a read: a later read may not show less.
func (o *oracle) observeRead(op runtime.ControlPlaneOperation, d runtime.ControlPlaneDone) {
	f := &o.floor
	switch q := op.(type) {
	case contract.FindSegments:
		if r, ok := d.Resp.(contract.FindSegmentsResp); ok {
			f.ends.Raise(q.TopicPartition, r.End)
		}
	case contract.DescribeTopics:
		if r, ok := d.Resp.(contract.DescribeTopicsResp); ok {
			for _, t := range r.Topics {
				f.raiseTopic(t.Name, t.Partitions)
			}
		}
	}
}

// checkRead checks one read against the floors taken when it was sent: what was
// acked before the read was sent is visible, and offsets, segment lists and
// topics never go backwards.
func (o *oracle) checkRead(op *opState, d runtime.ControlPlaneDone) {
	snap := op.snap
	if snap == nil {
		return
	}
	s := o.s
	switch q := op.op.(type) {
	case contract.FindSegments:
		r, _ := d.Resp.(contract.FindSegmentsResp)
		if r.End < snap.end {
			s.violation(offsetsNeverGoBackwards, "%v: end offset %d is below %d, which was known before the read was sent", q.TopicPartition, r.End, snap.end)
		}
		if q.MaxBytes == 0 {
			o.checkRanges(q, r, snap)
		}
	case contract.DescribeTopics:
		r, _ := d.Resp.(contract.DescribeTopicsResp)
		for _, want := range snap.topics {
			i := slices.IndexFunc(r.Topics, func(t contract.TopicInfo) bool { return t.Name == want.Name })
			if i < 0 || r.Topics[i].Partitions < want.Partitions {
				s.violation(ackedWriteVisible, "topic %q with %d partitions was acked before the read, but describe shows %+v", want.Name, want.Partitions, r.Topics)
			}
		}
	case contract.DescribeGroup:
		r, _ := d.Resp.(contract.DescribeGroupResp)
		if snap.gen > 0 && (!r.Exists || r.Generation < snap.gen) {
			s.violation(ackedWriteVisible, "group %q generation %d was acked before the read, but describe shows exists=%v generation=%d", q.Group, snap.gen, r.Exists, r.Generation)
		}
	}
}

// checkRanges checks an unbounded FindSegments: every range that was acked
// before the read and ends after From must be in the answer.
func (o *oracle) checkRanges(q contract.FindSegments, r contract.FindSegmentsResp, snap *snapshot) {
	for _, want := range snap.ranges {
		if want.Base+contract.Offset(want.Count) <= q.From {
			continue
		}
		if !slices.Contains(r.Ranges, want) {
			o.s.violation(segmentListsNeverGoBackwards, "%v from %d: range %+v was acked before the read, but it is not in %+v", q.TopicPartition, q.From, want, r.Ranges)
			return
		}
	}
}

// sameAnswer compares two answers by error code and response. Nil and empty
// slices print the same with %+v, and that difference is not a bug.
func sameAnswer(a, b contract.Done) bool {
	return a.Err == b.Err && fmt.Sprintf("%+v", a.Resp) == fmt.Sprintf("%+v", b.Resp)
}

func describeAnswer(d contract.Done) string {
	return fmt.Sprintf("err=%v resp=%+v", d.Err, d.Resp)
}

// verify replays the log into the spec and judges everything that was recorded.
func (o *oracle) verify() {
	s := o.s
	end, _ := s.store.logEnd()
	s.check().NoHole(s.store)
	spec := newSpec()
	slices.SortStableFunc(o.reads, func(a, b readRec) int { return cmp.Compare(a.prefix, b.prefix) })
	next := 0
	judge := func(prefix runtime.LogPosition) {
		for next < len(o.reads) && o.reads[next].prefix == prefix {
			o.judgeRead(spec, o.reads[next], end)
			next++
		}
	}
	committed := map[string]contract.Done{}
	logged := newGroupLog()
	decode := func(entry []byte) ([]runtime.ControlPlaneOperation, error) {
		cmds, err := replica.EntryCommands(entry)
		var ops []runtime.ControlPlaneOperation
		for _, c := range cmds {
			if c.Op != nil {
				ops = append(ops, c.Op)
			}
		}
		return ops, err
	}
	apply := func(spec specModel, op runtime.ControlPlaneOperation) {
		d := spec.Apply(op)
		if cs, ok := op.(contract.CommitSegment); ok && d.Err == runtime.OK {
			if _, seen := committed[cs.SegmentKey]; !seen {
				committed[cs.SegmentKey] = d
			}
		}
		logged.note(op, d)
	}
	if !check.Replay(s.check(), s.store, spec, decode, apply, func(prefix check.Slot) {
		logged.slot = runtime.LogPosition(prefix)
		judge(runtime.LogPosition(prefix))
	}) {
		return
	}
	for ; next < len(o.reads); next++ {
		o.judgeRead(spec, o.reads[next], end)
	}
	o.judgeAcks(spec, committed)
	o.judgeGroups(spec, logged)
}

// judgeRead checks that a read equals the spec after the prefix that the node had applied.
func (o *oracle) judgeRead(spec specModel, r readRec, end runtime.LogPosition) {
	s := o.s
	if !s.verdict(r.prefix <= end, readsAgreeWithSpec, "a node answered %T at slot %d, but the log has only %d slots", r.op, r.prefix, end) {
		return
	}
	if r.final {
		s.verdict(s.store.quietAfter(r.prefix), liveReplicasConverge, "a final read of %T was answered at slot %d, but the log has %d slots", r.op, r.prefix, end)
	}
	want := spec.Apply(r.op)
	s.verdict(sameAnswer(want, r.done.Answer()), readsAgreeWithSpec, "at t=%dms after slot %d, %T: spec says %s, node says %s",
		r.at/int64(machine.Millisecond), r.prefix, r.op, describeAnswer(want), describeAnswer(r.done.Answer()))
}

// judgeAcks checks that every acked write is in the log, and gave the answer that the spec gives.
func (o *oracle) judgeAcks(spec specModel, committed map[string]contract.Done) {
	s := o.s
	for _, a := range o.acks {
		switch w := a.op.(type) {
		case contract.CommitSegment:
			want, in := committed[w.SegmentKey]
			if !s.verdict(in, ackedWriteInLog, "commit %q was acked at t=%dms, but it is not in the log", w.SegmentKey, a.at/int64(machine.Millisecond)) {
				continue
			}
			s.verdict(sameAnswer(want, a.done.Answer()), ackedWriteHasSpecAnswer, "commit %q: spec says %s, the ack says %s", w.SegmentKey, describeAnswer(want), describeAnswer(a.done.Answer()))
		case contract.CreateTopic:
			got := spec.Apply(contract.DescribeTopics{Topics: []string{w.Name}})
			r, _ := got.Resp.(contract.DescribeTopicsResp)
			s.verdict(len(r.Topics) == 1 && r.Topics[0].Partitions >= w.Partitions, ackedWriteInLog, "create topic %q (%d partitions) was acked, but the spec has %+v", w.Name, w.Partitions, r.Topics)
		case contract.GroupInstall:
			got := spec.Apply(contract.DescribeGroup{Group: w.Group})
			r, _ := got.Resp.(contract.DescribeGroupResp)
			s.verdict(r.Exists && r.Generation >= w.Generation, ackedWriteInLog, "install of group %q generation %d was acked, but the spec has exists=%v generation=%d", w.Group, w.Generation, r.Exists, r.Generation)
		}
	}
}

// The checks of the consumer groups. The consumers tell the oracle every
// answer they got (consumers.go). Some checks run at once, the others after
// the run, against the log.
//
//   - No two leaders of one generation: two join answers for the same group
//     and generation name the same leader.
//   - A generation never goes down: the join answers of one member have
//     generations that never go down, and a known member keeps its id.
//   - An answered generation is in the log: every join answer matches the
//     GroupInstall of its generation (the leader, and the member is in it).
//   - A sync answer is the logged assignment: the bytes a member got are the
//     ones a GroupAssign of its generation gave it.
//   - An acked offset commit is in the log, and it survives: the final state
//     has an offset for the partition, from a commit that applied at the same
//     slot or later.
//   - A silent member is removed, and the consumer groups settle: after the
//     quiet tail every group is Stable, and its members are its live
//     consumers, no more (checkGroupsSettled).
//   - Every call gets one answer (groups.go: a call sent in the quiet tail
//     that hits its deadline, or a reply to a call that does not wait).
//
// The log may get a write that the machine gave up (its term ended first),
// after the machine read the log again (docs/rebalance.md section 8). Such a
// write gives no answer to any call, so the checks of answers need no special
// case. But it can leave the log with other members than the memory of the
// machine, which does not see it until its next term: the group is "late"
// (late, set by the host when such a write is answered OK), and its members
// are not compared when the groups settle.
type groupChecks struct {
	leaders map[groupGen]string // the leader each generation was answered with
	lastGen map[string]int32    // the last generation each member was answered with
	late    map[string]bool     // groups that got a write the machine had given up
	joins   []joinRec
	syncs   []syncRec
	commits []commitRec
}

type groupGen struct {
	group string
	gen   int32
}

type joinRec struct {
	group, member, leader string
	gen                   int32
	at                    int64
}

type syncRec struct {
	group, member string
	gen           int32
	bytes         []byte
	at            int64
}

type commitRec struct {
	group  string
	offset contract.PartitionOffset
	at     int64
}

// joinAnswered checks a join answer at once, and keeps it for the log checks.
func (o *oracle) joinAnswered(c *consumer, op *groupOp, a rebalance.JoinAnswer) {
	s, g := o.s, &o.groups
	if op.member != "" && a.MemberID != op.member {
		s.violation(knownMemberKeepsID, "%s joined %s as %q and was answered as %q", c.name, c.group, op.member, a.MemberID)
	}
	key := groupGen{c.group, a.Generation}
	if first, ok := g.leaders[key]; !ok {
		g.leaders[key] = a.Leader
	} else if first != a.Leader {
		s.violation(noTwoLeadersInGeneration, "group %s generation %d was answered with leader %q and with leader %q", c.group, a.Generation, first, a.Leader)
	}
	if last, ok := g.lastGen[a.MemberID]; ok && a.Generation < last {
		s.violation(generationNeverGoesDown, "member %q of %s was answered generation %d after %d", a.MemberID, c.group, a.Generation, last)
	}
	g.lastGen[a.MemberID] = max(g.lastGen[a.MemberID], a.Generation)
	g.joins = append(g.joins, joinRec{group: c.group, member: a.MemberID, leader: a.Leader, gen: a.Generation, at: int64(s.mono())})
	s.env.Reachable(joinRoundCompletes)
	if len(a.Members) > 1 {
		s.env.Reachable(roundCompletesManyMembers)
	}
}

// syncAnswered keeps a sync answer for the log checks.
func (o *oracle) syncAnswered(c *consumer, op *groupOp, a rebalance.SyncAnswer) {
	o.groups.syncs = append(o.groups.syncs, syncRec{group: c.group, member: op.member, gen: op.gen, bytes: a.Assignment, at: int64(o.s.mono())})
	if len(a.Assignment) > 0 {
		o.s.env.Reachable(memberGetsAssignment)
	}
}

// commitAnswered keeps the offsets of a commit that were acked.
func (o *oracle) commitAnswered(c *consumer, op *groupOp, a rebalance.CommitAnswer) {
	s := o.s
	if !s.verdict(len(a.Errs) == len(op.offsets), commitAnswerHasOneCode,
		"%s committed %d offsets to %s and got %d codes", c.name, len(op.offsets), c.group, len(a.Errs)) {
		return
	}
	for i, code := range a.Errs {
		if code == runtime.OK {
			o.groups.commits = append(o.groups.commits, commitRec{group: c.group, offset: op.offsets[i], at: int64(s.mono())})
			s.env.Reachable(offsetCommitAcked)
		}
	}
}

// checkGroupsSettled checks the last read of the groups by the watcher, at the
// end of the quiet tail, which waited for them to settle (up to maxQuiet): no
// member of a silent consumer is left, and every group has settled
// (groupsUnsettled).
func (o *oracle) checkGroupsSettled(p *consumerPool) {
	s, a := o.s, p.last
	if !s.verdict(p.lastOK, consumerGroupsSettle, "no machine answered a read of the groups in %dms of quiet", s.stats.QuietMs) {
		return
	}
	for _, c := range p.all {
		if !c.silent || c.lastMember == "" || o.groups.late[c.group] {
			continue
		}
		i := slices.IndexFunc(a.Groups, func(d rebalance.GroupDescription) bool { return d.Name == c.group })
		left := i >= 0 && slices.ContainsFunc(a.Groups[i].Members, func(m contract.GroupMemberInfo) bool { return m.MemberID == c.lastMember })
		s.verdict(!left, silentMemberRemovedCheck, "%s went silent as member %q of %s, which is still a member after %dms of quiet",
			c.name, c.lastMember, c.group, s.stats.QuietMs)
	}
	why := groupsUnsettled(p, a)
	s.verdict(why == "", consumerGroupsSettle, "after %dms of quiet, %s", s.stats.QuietMs, why)
}

// groupsUnsettled says why the consumer groups in a read are not settled, or
// "" when they are: every live consumer is in its group, each group is
// Stable (Empty when none of its consumers runs), and its members are its
// live consumers, no more. The members of a late group are not compared.
func groupsUnsettled(p *consumerPool, a rebalance.DescribeAnswer) string {
	for _, d := range a.Groups {
		var want []string
		for _, c := range p.live(d.Name) {
			if c.stage != inGroup || c.member == "" {
				return fmt.Sprintf("%s is not in its group %s", c.name, d.Name)
			}
			want = append(want, c.member)
		}
		ok := d.State == "Stable"
		if len(want) == 0 {
			ok = d.State == "Empty" || d.State == "Dead"
		}
		if !ok {
			return fmt.Sprintf("group %s is %s with %d live consumers", d.Name, d.State, len(want))
		}
		if p.s.oracle.groups.late[d.Name] {
			continue
		}
		got := make([]string, 0, len(d.Members))
		for _, m := range d.Members {
			got = append(got, m.MemberID)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			return fmt.Sprintf("group %s has members %v, but its live consumers are %v", d.Name, got, want)
		}
	}
	return ""
}

// groupLog is what the replay of the log found for the group checks.
type groupLog struct {
	slot     runtime.LogPosition                       // the slot of the ops being replayed
	installs map[groupGen]contract.GroupInstall        // installs that applied
	assigns  map[groupGen][]contract.GroupAssign       // assignments that applied
	offsets  map[offsetKey]runtime.LogPosition         // offset -> the last slot where it applied
	latest   map[partitionKey]contract.PartitionOffset // partition -> the last offset that applied
}

type partitionKey struct {
	group string
	tp    contract.TopicPartition
}

type offsetKey struct {
	partitionKey
	offset int64
}

func newGroupLog() *groupLog {
	return &groupLog{
		installs: map[groupGen]contract.GroupInstall{},
		assigns:  map[groupGen][]contract.GroupAssign{},
		offsets:  map[offsetKey]runtime.LogPosition{},
		latest:   map[partitionKey]contract.PartitionOffset{},
	}
}

// note takes one op of the log and the answer of the spec to it.
func (l *groupLog) note(op runtime.ControlPlaneOperation, d contract.Done) {
	if d.Err != runtime.OK {
		return
	}
	switch w := op.(type) {
	case contract.GroupInstall:
		l.installs[groupGen{w.Group, w.Generation}] = w
	case contract.GroupAssign:
		key := groupGen{w.Group, w.Generation}
		l.assigns[key] = append(l.assigns[key], w)
	case contract.OffsetCommit:
		r, _ := d.Resp.(contract.OffsetCommitResp)
		for i, off := range w.Offsets {
			if i < len(r.Errs) && r.Errs[i] == runtime.OK {
				pk := partitionKey{w.Group, off.TopicPartition}
				l.offsets[offsetKey{pk, off.Offset}] = l.slot
				l.latest[pk] = off
			}
		}
	}
}

// judgeGroups checks the answers of the consumers against the log.
func (o *oracle) judgeGroups(spec specModel, l *groupLog) {
	s, g := o.s, &o.groups
	for _, j := range g.joins {
		in, ok := l.installs[groupGen{j.group, j.gen}]
		isMember := slices.ContainsFunc(in.Members, func(m contract.GroupMember) bool { return m.MemberID == j.member })
		s.verdict(ok && in.Leader == j.leader && isMember, answeredGenerationInLog,
			"at t=%dms %q got generation %d of %s with leader %q, but the log has it %v (leader %q, member %v)",
			j.at/int64(machine.Millisecond), j.member, j.gen, j.group, j.leader, ok, in.Leader, isMember)
	}
	for _, y := range g.syncs {
		assigns := l.assigns[groupGen{y.group, y.gen}]
		match := slices.ContainsFunc(assigns, func(a contract.GroupAssign) bool {
			i := slices.IndexFunc(a.Assignments, func(m contract.GroupAssignment) bool { return m.Member == y.member })
			if i < 0 {
				return len(y.bytes) == 0
			}
			return string(a.Assignments[i].Bytes) == string(y.bytes)
		})
		s.verdict(match, syncAnswerIsLoggedAssignment, "at t=%dms %q of %s generation %d got %q, but the log assigns %+v",
			y.at/int64(machine.Millisecond), y.member, y.group, y.gen, y.bytes, assigns)
	}
	for _, c := range g.commits {
		pk := partitionKey{c.group, c.offset.TopicPartition}
		slot, in := l.offsets[offsetKey{pk, c.offset.Offset}]
		if !s.verdict(in, ackedOffsetCommitInLog, "offset %d of %s %v was acked at t=%dms, but it is not in the log",
			c.offset.Offset, c.group, c.offset.TopicPartition, c.at/int64(machine.Millisecond)) {
			continue
		}
		got := spec.Apply(contract.OffsetFetch{Group: c.group, Partitions: []contract.TopicPartition{c.offset.TopicPartition}})
		r, _ := got.Resp.(contract.OffsetFetchResp)
		final := int64(-1)
		if len(r.Offsets) == 1 {
			final = r.Offsets[0].Offset
		}
		last, lastIn := l.offsets[offsetKey{pk, final}]
		s.verdict(lastIn && last >= slot && l.latest[pk].Offset == final, ackedOffsetCommitSurvives,
			"offset %d of %s %v was acked (slot %d), but the final offset is %d (slot %d, found %v)",
			c.offset.Offset, c.group, c.offset.TopicPartition, slot, final, last, lastIn)
	}
}
