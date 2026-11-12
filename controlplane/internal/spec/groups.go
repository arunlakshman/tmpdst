package spec

import (
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Rules for consumer groups. An empty byte field is reported as nil.
//
//	Size          A group command over 256 KiB is ErrTooLarge. The size is 16
//	              for the command, 4 + length for each string or byte field,
//	              and 10 for each number and each list length.
//	GroupInstall  Size, then ErrInvalid for an empty group, no members, an empty
//	              or repeated member id, or a leader that is not a member. Then
//	              Generation must be the current one + 1 (0 for a new group),
//	              else ErrIllegalGeneration. It replaces the members, sets the
//	              header and clears every assignment. Offsets are kept.
//	GroupAssign   Size. Generation must be the current one, else
//	              ErrIllegalGeneration. Leader must be the header leader and a
//	              member, else ErrUnknownMember. Each assigned member must be a
//	              member (else ErrUnknownMember) and appear once (else
//	              ErrInvalid). A member gets the bytes listed for it, or none.
//	GroupRemove   Size. An unknown group or member is ignored. A removed leader
//	              clears the header leader. With no member left, the assigned
//	              generation is 0. The generation does not change.
//	OffsetCommit  Size. Generation >= 0: the group must exist and Member must be
//	              a member (else ErrUnknownMember), and Generation must be the
//	              current one (else ErrIllegalGeneration). Generation -1 with an
//	              empty Member is allowed only if the group has no members (else
//	              ErrIllegalGeneration). Any other case is ErrIllegalGeneration.
//	              Then each offset: an unknown topic or partition is
//	              ErrUnknownPartition, offset < 0 is ErrInvalid, else it is
//	              stored (the last one wins). The answer has one code per offset.
//	OffsetFetch   The committed offsets, or -1 and -1 for a partition with none.
//	              No partitions means every committed one, by topic and partition.
//	DescribeGroup Exists is false for a group that was never installed. State is
//	              "Empty" with no members, "Stable" when the assigned generation
//	              is the generation, else "PreparingSync". Members sorted by id.
//	ListGroups    Every installed group, sorted by name.
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
	contract.GroupMember
	joined     int64
	assignment []byte
}

type committedOffset struct {
	offset int64
	epoch  int32
	meta   string
}

// size counts the size of a command by the Size rule.
type size int

func (s *size) str(v string)   { *s += size(4 + len(v)) }
func (s *size) bytes(v []byte) { *s += size(4 + len(v)) }
func (s *size) nums(n int)     { *s += size(10 * n) }

func installSize(op contract.GroupInstall) size {
	s := size(16)
	s.str(op.Group)
	s.str(op.ProtocolType)
	s.str(op.Protocol)
	s.str(op.Leader)
	s.nums(3)
	for _, m := range op.Members {
		s.str(m.MemberID)
		s.str(m.ClientID)
		s.str(m.ClientHost)
		s.str(m.GroupInstanceID)
		s.bytes(m.Metadata)
		s.nums(2)
	}
	return s
}

func assignSize(op contract.GroupAssign) size {
	s := size(16)
	s.str(op.Group)
	s.str(op.Leader)
	s.nums(3)
	for _, a := range op.Assignments {
		s.str(a.Member)
		s.bytes(a.Bytes)
	}
	return s
}

func removeSize(op contract.GroupRemove) size {
	s := size(16)
	s.str(op.Group)
	s.str(op.Reason)
	s.nums(2)
	for _, m := range op.Members {
		s.str(m)
	}
	return s
}

func commitSize(op contract.OffsetCommit) size {
	s := size(16)
	s.str(op.Group)
	s.str(op.Member)
	s.nums(3)
	for _, o := range op.Offsets {
		s.str(o.TopicPartition.Topic)
		s.str(o.Metadata)
		s.nums(3)
	}
	return s
}

func nonEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return slices.Clone(b)
}

// generation is the current generation of a group, 0 if it is unknown.
func (c *ControlPlane) generation(group string) int32 {
	if g := c.groups[group]; g != nil {
		return g.generation
	}
	return 0
}

func (c *ControlPlane) groupInstall(op contract.GroupInstall) runtime.ErrCode {
	if installSize(op) > maxGroupCommand {
		return runtime.ErrTooLarge
	}
	ids := map[string]bool{}
	for _, m := range op.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return runtime.ErrInvalid
		}
		ids[m.MemberID] = true
	}
	if op.Group == "" || len(op.Members) == 0 || !ids[op.Leader] {
		return runtime.ErrInvalid
	}
	if op.Generation != c.generation(op.Group)+1 {
		return runtime.ErrIllegalGeneration
	}
	g := &groupState{
		generation: op.Generation,
		protoType:  op.ProtocolType,
		protocol:   op.Protocol,
		leader:     op.Leader,
		members:    map[string]*groupMember{},
	}
	for _, m := range op.Members {
		m.Metadata = nonEmpty(m.Metadata)
		g.members[m.MemberID] = &groupMember{GroupMember: m, joined: op.TimeMs}
	}
	c.groups[op.Group] = g
	return runtime.OK
}

func (c *ControlPlane) groupAssign(op contract.GroupAssign) runtime.ErrCode {
	if assignSize(op) > maxGroupCommand {
		return runtime.ErrTooLarge
	}
	if op.Generation != c.generation(op.Group) {
		return runtime.ErrIllegalGeneration
	}
	g := c.groups[op.Group]
	if g == nil || op.Leader != g.leader || g.members[op.Leader] == nil {
		return runtime.ErrUnknownMember
	}
	seen := map[string]bool{}
	for _, a := range op.Assignments {
		if g.members[a.Member] == nil {
			return runtime.ErrUnknownMember
		}
		if seen[a.Member] {
			return runtime.ErrInvalid
		}
		seen[a.Member] = true
	}
	for _, m := range g.members { //determinismcheck:ok every member is cleared, the order does not matter
		m.assignment = nil
	}
	for _, a := range op.Assignments {
		g.members[a.Member].assignment = nonEmpty(a.Bytes)
	}
	g.assignedGen = op.Generation
	return runtime.OK
}

func (c *ControlPlane) groupRemove(op contract.GroupRemove) runtime.ErrCode {
	if removeSize(op) > maxGroupCommand {
		return runtime.ErrTooLarge
	}
	g := c.groups[op.Group]
	if g == nil {
		return runtime.OK
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
	return runtime.OK
}

func (c *ControlPlane) offsetCommit(op contract.OffsetCommit) runtime.ControlPlaneDone {
	if commitSize(op) > maxGroupCommand {
		return runtime.ControlPlaneDone{Err: runtime.ErrTooLarge}
	}
	g := c.groups[op.Group]
	switch {
	case op.Generation >= 0:
		if g == nil || g.members[op.Member] == nil {
			return runtime.ControlPlaneDone{Err: runtime.ErrUnknownMember}
		}
		if op.Generation != g.generation {
			return runtime.ControlPlaneDone{Err: runtime.ErrIllegalGeneration}
		}
	case op.Generation == -1 && op.Member == "":
		if g != nil && len(g.members) > 0 {
			return runtime.ControlPlaneDone{Err: runtime.ErrIllegalGeneration}
		}
	default:
		return runtime.ControlPlaneDone{Err: runtime.ErrIllegalGeneration}
	}
	var errs []runtime.ErrCode
	done := runtime.ControlPlaneDone{}
	for _, o := range op.Offsets {
		code := c.storeOffset(op.Group, o)
		if code == runtime.OK {
			done.Outcome = runtime.Applied
		}
		errs = append(errs, code)
	}
	done.Resp = contract.OffsetCommitResp{Errs: errs}
	return done
}

func (c *ControlPlane) storeOffset(group string, o contract.PartitionOffset) runtime.ErrCode {
	if o.TopicPartition.Partition < 0 || o.TopicPartition.Partition >= c.topics[o.TopicPartition.Topic] {
		return runtime.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return runtime.ErrInvalid
	}
	if c.offsets[group] == nil {
		c.offsets[group] = map[contract.TopicPartition]committedOffset{}
	}
	c.offsets[group][o.TopicPartition] = committedOffset{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata}
	return runtime.OK
}

func (c *ControlPlane) offsetFetch(op contract.OffsetFetch) contract.OffsetFetchResp {
	committed := c.offsets[op.Group]
	partitions := slices.Clone(op.Partitions)
	if len(partitions) == 0 {
		for tp := range committed { //determinismcheck:ok the partitions are sorted below
			partitions = append(partitions, tp)
		}
		slices.SortFunc(partitions, func(a, b contract.TopicPartition) int {
			if a.Topic != b.Topic {
				return strings.Compare(a.Topic, b.Topic)
			}
			return int(a.Partition - b.Partition)
		})
	}
	var out []contract.PartitionOffset
	for _, tp := range partitions {
		v, ok := committed[tp]
		if !ok {
			v = committedOffset{offset: -1, epoch: -1}
		}
		out = append(out, contract.PartitionOffset{TopicPartition: tp, Offset: v.offset, LeaderEpoch: v.epoch, Metadata: v.meta})
	}
	return contract.OffsetFetchResp{Offsets: out}
}

func stateName(g *groupState) string {
	switch {
	case len(g.members) == 0:
		return "Empty"
	case g.assignedGen == g.generation:
		return "Stable"
	}
	return "PreparingSync"
}

func (c *ControlPlane) describeGroup(op contract.DescribeGroup) contract.DescribeGroupResp {
	g := c.groups[op.Group]
	if g == nil {
		return contract.DescribeGroupResp{}
	}
	resp := contract.DescribeGroupResp{
		Exists: true, Generation: g.generation, ProtocolType: g.protoType, Protocol: g.protocol,
		Leader: g.leader, AssignedGeneration: g.assignedGen, State: stateName(g),
	}
	for _, id := range sortedKeys(g.members) {
		m := g.members[id]
		resp.Members = append(resp.Members, contract.GroupMemberInfo{GroupMember: m.GroupMember, JoinedTimeMs: m.joined, Assignment: nonEmpty(m.assignment)})
	}
	return resp
}

func (c *ControlPlane) listGroups() contract.ListGroupsResp {
	var resp contract.ListGroupsResp
	for _, name := range sortedKeys(c.groups) {
		g := c.groups[name]
		resp.Groups = append(resp.Groups, contract.GroupSummary{Name: name, ProtocolType: g.protoType, State: stateName(g)})
	}
	return resp
}
