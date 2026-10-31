package speccheck

import (
	"slices"

	"manifester/internal/core"
)

// Consumer groups (slice 4a). The spec of the group commands and reads. Plain
// maps, one rule per step, no code shared with stateupdater (decision 21).
//
//	Size       A group command over 256 KiB is ErrTooLarge. Size counts the
//	           fields: 16 for the command, 4 + length for each string or byte
//	           field, 10 for each number and each list length.
//	Install    Size, then ErrInvalid for an empty group, no members, an empty
//	           or repeated member id, or a leader that is not a member. Then
//	           Generation must be the current one + 1 (0 for a new group), else
//	           ErrIllegalGeneration. Replaces the members, sets the header, and
//	           clears every assignment. Offsets are kept.
//	Assign     Size. Generation must be the current one, else
//	           ErrIllegalGeneration. Leader must be the header leader and a
//	           member, else ErrUnknownMember. Each assigned member must be a
//	           member, else ErrUnknownMember; a repeated one is ErrInvalid.
//	           Every member gets the bytes listed for it, or none. Sets
//	           assignedGeneration = Generation. A repeat is the same result.
//	Remove     Size. An unknown group or member is ignored. A removed leader
//	           clears the header leader. With no member left, assignedGeneration
//	           is 0. The generation is not changed.
//	OffsetCommit  Size. Generation >= 0: the group must exist and Member must
//	           be a member (else ErrUnknownMember), and Generation must be the
//	           current one (else ErrIllegalGeneration). Generation -1 with an
//	           empty Member: allowed only if the group has no members (else
//	           ErrIllegalGeneration). Anything else: ErrIllegalGeneration.
//	           Per offset: unknown topic or partition is ErrUnknownPartition,
//	           offset < 0 is ErrInvalid, else it is stored (last one wins).
//	OffsetFetch   Committed offsets, or -1 and -1 for a partition with none.
//	           Empty request: every committed partition, by topic and partition.
//	DescribeGroup Exists false for a group never installed. State "Empty" with
//	           no members, "Stable" when assignedGeneration == generation, else
//	           "PreparingSync". Members sorted by id.
//	ListGroups    Every installed group, sorted by name.
//
// Empty byte fields are reported as nil.
const maxGroupCommand = 256 << 10

type groupState struct {
	generation  int32
	protoType   string
	protocol    string
	leader      string
	assignedGen int32
	members     map[string]*groupMember
}

type groupMember struct {
	core.GroupMember
	joined     int64
	assignment []byte
}

type committedOffset struct {
	offset int64
	epoch  int32
	meta   string
	time   int64
}

// cmdSize counts a command's size (see the rule above).
type cmdSize struct{ n int }

func (c *cmdSize) str(s string)   { c.n += 4 + len(s) }
func (c *cmdSize) bytes(b []byte) { c.n += 4 + len(b) }
func (c *cmdSize) num()           { c.n += 10 }

func sizeInstall(op core.GroupInstall) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.str(op.ProtocolType)
	c.str(op.Protocol)
	c.num()
	c.str(op.Leader)
	c.num()
	c.num()
	for _, m := range op.Members {
		c.str(m.MemberID)
		c.str(m.ClientID)
		c.str(m.ClientHost)
		c.str(m.GroupInstanceID)
		c.num()
		c.num()
		c.bytes(m.Metadata)
	}
	return c.n
}

func sizeAssign(op core.GroupAssign) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.num()
	c.str(op.Leader)
	c.num()
	c.num()
	for _, a := range op.Assignments {
		c.str(a.Member)
		c.bytes(a.Bytes)
	}
	return c.n
}

func sizeRemove(op core.GroupRemove) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.num()
	c.str(op.Reason)
	c.num()
	for _, m := range op.Members {
		c.str(m)
	}
	return c.n
}

func sizeOffsetCommit(op core.OffsetCommit) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.str(op.Member)
	c.num()
	c.num()
	c.num()
	for _, o := range op.Offsets {
		c.str(o.TP.Topic)
		c.num()
		c.num()
		c.num()
		c.str(o.Metadata)
	}
	return c.n
}

func noEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return slices.Clone(b)
}

// groupDone is the answer to a group write that has no response.
func groupDone(req core.RequestID, code core.ErrCode) core.ControlPlaneDone {
	if code != core.OK {
		return core.ControlPlaneDone{Req: req, Err: code}
	}
	return core.ControlPlaneDone{Req: req, Outcome: core.Applied}
}

func (c *ControlPlane) groupInstall(op core.GroupInstall) core.ErrCode {
	if sizeInstall(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	if op.Group == "" || len(op.Members) == 0 {
		return core.ErrInvalid
	}
	ids := map[string]bool{}
	for _, m := range op.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return core.ErrInvalid
		}
		ids[m.MemberID] = true
	}
	if !ids[op.Leader] {
		return core.ErrInvalid
	}
	var cur int32
	if g := c.groups[op.Group]; g != nil {
		cur = g.generation
	}
	if op.Generation != cur+1 {
		return core.ErrIllegalGeneration
	}
	if c.groups == nil {
		c.groups = map[string]*groupState{}
	}
	g := &groupState{
		generation: op.Generation,
		protoType:  op.ProtocolType,
		protocol:   op.Protocol,
		leader:     op.Leader,
		members:    map[string]*groupMember{},
	}
	for _, m := range op.Members {
		g.members[m.MemberID] = &groupMember{
			GroupMember: core.GroupMember{
				MemberID:           m.MemberID,
				ClientID:           m.ClientID,
				ClientHost:         m.ClientHost,
				GroupInstanceID:    m.GroupInstanceID,
				SessionTimeoutMs:   m.SessionTimeoutMs,
				RebalanceTimeoutMs: m.RebalanceTimeoutMs,
				Metadata:           noEmpty(m.Metadata),
			},
			joined: op.TimeMs,
		}
	}
	c.groups[op.Group] = g
	return core.OK
}

func (c *ControlPlane) groupAssign(op core.GroupAssign) core.ErrCode {
	if sizeAssign(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	g := c.groups[op.Group]
	var cur int32
	if g != nil {
		cur = g.generation
	}
	if op.Generation != cur {
		return core.ErrIllegalGeneration
	}
	if g == nil || op.Leader != g.leader || g.members[op.Leader] == nil {
		return core.ErrUnknownMember
	}
	seen := map[string]bool{}
	for _, a := range op.Assignments {
		if g.members[a.Member] == nil {
			return core.ErrUnknownMember
		}
		if seen[a.Member] {
			return core.ErrInvalid
		}
		seen[a.Member] = true
	}
	for _, id := range sortedKeys(g.members) {
		g.members[id].assignment = nil
	}
	for _, a := range op.Assignments {
		g.members[a.Member].assignment = noEmpty(a.Bytes)
	}
	g.assignedGen = op.Generation
	return core.OK
}

func (c *ControlPlane) groupRemove(op core.GroupRemove) core.ErrCode {
	if sizeRemove(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	g := c.groups[op.Group]
	if g == nil {
		return core.OK
	}
	for _, id := range op.Members {
		delete(g.members, id)
	}
	if g.members[g.leader] == nil {
		g.leader = ""
	}
	if len(g.members) == 0 {
		g.assignedGen = 0
	}
	return core.OK
}

func (c *ControlPlane) offsetCommit(request core.RequestID, op core.OffsetCommit) core.ControlPlaneDone {
	if sizeOffsetCommit(op) > maxGroupCommand {
		return core.ControlPlaneDone{Req: request, Err: core.ErrTooLarge}
	}
	g := c.groups[op.Group]
	switch {
	case op.Generation >= 0:
		if g == nil || g.members[op.Member] == nil {
			return core.ControlPlaneDone{Req: request, Err: core.ErrUnknownMember}
		}
		if op.Generation != g.generation {
			return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
		}
	case op.Generation == -1 && op.Member == "":
		if g != nil && len(g.members) > 0 {
			return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
		}
	default:
		return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
	}
	var errs []core.ErrCode
	saved := false
	for _, o := range op.Offsets {
		code := c.commitOne(op, o)
		if code == core.OK {
			saved = true
		}
		errs = append(errs, code)
	}
	done := core.ControlPlaneDone{Req: request, Resp: core.OffsetCommitResp{Errs: errs}}
	if saved {
		done.Outcome = core.Applied
	}
	return done
}

func (c *ControlPlane) commitOne(op core.OffsetCommit, o core.PartitionOffset) core.ErrCode {
	if o.TP.Partition < 0 || o.TP.Partition >= c.topics[o.TP.Topic] {
		return core.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return core.ErrInvalid
	}
	if c.offsets == nil {
		c.offsets = map[string]map[core.TopicPartition]committedOffset{}
	}
	if c.offsets[op.Group] == nil {
		c.offsets[op.Group] = map[core.TopicPartition]committedOffset{}
	}
	c.offsets[op.Group][o.TP] = committedOffset{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata, time: op.TimeMs}
	return core.OK
}

func (c *ControlPlane) offsetFetch(op core.OffsetFetch) core.OffsetFetchResp {
	var out []core.PartitionOffset
	committed := c.offsets[op.Group]
	if len(op.Partitions) == 0 {
		for _, tp := range sortedTPs(committed) {
			v := committed[tp]
			out = append(out, core.PartitionOffset{TP: tp, Offset: v.offset, LeaderEpoch: v.epoch, Metadata: v.meta})
		}
		return core.OffsetFetchResp{Offsets: out}
	}
	for _, tp := range op.Partitions {
		v, ok := committed[tp]
		if !ok {
			out = append(out, core.PartitionOffset{TP: tp, Offset: -1, LeaderEpoch: -1})
			continue
		}
		out = append(out, core.PartitionOffset{TP: tp, Offset: v.offset, LeaderEpoch: v.epoch, Metadata: v.meta})
	}
	return core.OffsetFetchResp{Offsets: out}
}

// sortedTPs returns the partitions of m in topic and partition order.
func sortedTPs(m map[core.TopicPartition]committedOffset) []core.TopicPartition {
	tps := make([]core.TopicPartition, 0, len(m))
	for tp := range m { //determinismcheck:ok keys are sorted before use
		tps = append(tps, tp)
	}
	slices.SortFunc(tps, compareTP)
	return tps
}

func compareTP(a, b core.TopicPartition) int {
	if a.Topic != b.Topic {
		if a.Topic < b.Topic {
			return -1
		}
		return 1
	}
	return int(a.Partition - b.Partition)
}

// groupStateName is the state of a group as DescribeGroup and ListGroups report it.
func groupStateName(g *groupState) string {
	if len(g.members) == 0 {
		return "Empty"
	}
	if g.assignedGen == g.generation {
		return "Stable"
	}
	return "PreparingSync"
}

func (c *ControlPlane) describeGroup(op core.DescribeGroup) core.DescribeGroupResp {
	g := c.groups[op.Group]
	if g == nil {
		return core.DescribeGroupResp{}
	}
	response := core.DescribeGroupResp{
		Exists:             true,
		Generation:         g.generation,
		ProtocolType:       g.protoType,
		Protocol:           g.protocol,
		Leader:             g.leader,
		AssignedGeneration: g.assignedGen,
		State:              groupStateName(g),
	}
	for _, id := range sortedKeys(g.members) {
		m := g.members[id]
		response.Members = append(response.Members, core.GroupMemberInfo{
			GroupMember:  m.GroupMember,
			JoinedTimeMs: m.joined,
			Assignment:   noEmpty(m.assignment),
		})
	}
	return response
}

func (c *ControlPlane) listGroups() core.ListGroupsResp {
	var resp core.ListGroupsResp
	for _, name := range sortedKeys(c.groups) {
		g := c.groups[name]
		resp.Groups = append(resp.Groups, core.GroupSummary{Name: name, ProtocolType: g.protoType, State: groupStateName(g)})
	}
	return resp
}
