package speccheck

import (
	"slices"

	"mds/internal/core"
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

func sizeInstall(operation core.GroupInstall) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.str(operation.ProtocolType)
	c.str(operation.Protocol)
	c.num()
	c.str(operation.Leader)
	c.num()
	c.num()
	for _, m := range operation.Members {
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

func sizeAssign(operation core.GroupAssign) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.num()
	c.str(operation.Leader)
	c.num()
	c.num()
	for _, a := range operation.Assignments {
		c.str(a.Member)
		c.bytes(a.Bytes)
	}
	return c.n
}

func sizeRemove(operation core.GroupRemove) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.num()
	c.str(operation.Reason)
	c.num()
	for _, m := range operation.Members {
		c.str(m)
	}
	return c.n
}

func sizeOffsetCommit(operation core.OffsetCommit) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.str(operation.Member)
	c.num()
	c.num()
	c.num()
	for _, o := range operation.Offsets {
		c.str(o.TopicPartition.Topic)
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
func groupDone(request core.RequestID, code core.ErrCode) core.ControlPlaneDone {
	if code != core.OK {
		return core.ControlPlaneDone{Req: request, Err: code}
	}
	return core.ControlPlaneDone{Req: request, Outcome: core.Applied}
}

func (c *ControlPlane) groupInstall(operation core.GroupInstall) core.ErrCode {
	if sizeInstall(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	if operation.Group == "" || len(operation.Members) == 0 {
		return core.ErrInvalid
	}
	ids := map[string]bool{}
	for _, m := range operation.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return core.ErrInvalid
		}
		ids[m.MemberID] = true
	}
	if !ids[operation.Leader] {
		return core.ErrInvalid
	}
	var cur int32
	if g := c.groups[operation.Group]; g != nil {
		cur = g.generation
	}
	if operation.Generation != cur+1 {
		return core.ErrIllegalGeneration
	}
	if c.groups == nil {
		c.groups = map[string]*groupState{}
	}
	g := &groupState{
		generation: operation.Generation,
		protoType:  operation.ProtocolType,
		protocol:   operation.Protocol,
		leader:     operation.Leader,
		members:    map[string]*groupMember{},
	}
	for _, m := range operation.Members {
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
			joined: operation.TimeMs,
		}
	}
	c.groups[operation.Group] = g
	return core.OK
}

func (c *ControlPlane) groupAssign(operation core.GroupAssign) core.ErrCode {
	if sizeAssign(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	g := c.groups[operation.Group]
	var cur int32
	if g != nil {
		cur = g.generation
	}
	if operation.Generation != cur {
		return core.ErrIllegalGeneration
	}
	if g == nil || operation.Leader != g.leader || g.members[operation.Leader] == nil {
		return core.ErrUnknownMember
	}
	seen := map[string]bool{}
	for _, a := range operation.Assignments {
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
	for _, a := range operation.Assignments {
		g.members[a.Member].assignment = noEmpty(a.Bytes)
	}
	g.assignedGen = operation.Generation
	return core.OK
}

func (c *ControlPlane) groupRemove(operation core.GroupRemove) core.ErrCode {
	if sizeRemove(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	g := c.groups[operation.Group]
	if g == nil {
		return core.OK
	}
	for _, id := range operation.Members {
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

func (c *ControlPlane) offsetCommit(request core.RequestID, operation core.OffsetCommit) core.ControlPlaneDone {
	if sizeOffsetCommit(operation) > maxGroupCommand {
		return core.ControlPlaneDone{Req: request, Err: core.ErrTooLarge}
	}
	g := c.groups[operation.Group]
	switch {
	case operation.Generation >= 0:
		if g == nil || g.members[operation.Member] == nil {
			return core.ControlPlaneDone{Req: request, Err: core.ErrUnknownMember}
		}
		if operation.Generation != g.generation {
			return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
		}
	case operation.Generation == -1 && operation.Member == "":
		if g != nil && len(g.members) > 0 {
			return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
		}
	default:
		return core.ControlPlaneDone{Req: request, Err: core.ErrIllegalGeneration}
	}
	var errs []core.ErrCode
	saved := false
	for _, o := range operation.Offsets {
		code := c.commitOne(operation, o)
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

func (c *ControlPlane) commitOne(operation core.OffsetCommit, o core.PartitionOffset) core.ErrCode {
	if o.TopicPartition.Partition < 0 || o.TopicPartition.Partition >= c.topics[o.TopicPartition.Topic] {
		return core.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return core.ErrInvalid
	}
	if c.offsets == nil {
		c.offsets = map[string]map[core.TopicPartition]committedOffset{}
	}
	if c.offsets[operation.Group] == nil {
		c.offsets[operation.Group] = map[core.TopicPartition]committedOffset{}
	}
	c.offsets[operation.Group][o.TopicPartition] = committedOffset{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata, time: operation.TimeMs}
	return core.OK
}

func (c *ControlPlane) offsetFetch(operation core.OffsetFetch) core.OffsetFetchResp {
	var out []core.PartitionOffset
	committed := c.offsets[operation.Group]
	if len(operation.Partitions) == 0 {
		for _, topicPartition := range sortedTPs(committed) {
			v := committed[topicPartition]
			out = append(out, core.PartitionOffset{TopicPartition: topicPartition, Offset: v.offset, LeaderEpoch: v.epoch, Metadata: v.meta})
		}
		return core.OffsetFetchResp{Offsets: out}
	}
	for _, topicPartition := range operation.Partitions {
		v, ok := committed[topicPartition]
		if !ok {
			out = append(out, core.PartitionOffset{TopicPartition: topicPartition, Offset: -1, LeaderEpoch: -1})
			continue
		}
		out = append(out, core.PartitionOffset{TopicPartition: topicPartition, Offset: v.offset, LeaderEpoch: v.epoch, Metadata: v.meta})
	}
	return core.OffsetFetchResp{Offsets: out}
}

// sortedTPs returns the partitions of m in topic and partition order.
func sortedTPs(m map[core.TopicPartition]committedOffset) []core.TopicPartition {
	tps := make([]core.TopicPartition, 0, len(m))
	for topicPartition := range m { //determinismcheck:ok keys are sorted before use
		tps = append(tps, topicPartition)
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

func (c *ControlPlane) describeGroup(operation core.DescribeGroup) core.DescribeGroupResp {
	g := c.groups[operation.Group]
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
	var response core.ListGroupsResp
	for _, name := range sortedKeys(c.groups) {
		g := c.groups[name]
		response.Groups = append(response.Groups, core.GroupSummary{Name: name, ProtocolType: g.protoType, State: groupStateName(g)})
	}
	return response
}
