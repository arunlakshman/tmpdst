package group

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The rules of the group context. A Decide function reads the state before a
// command through a View and returns the events the command makes and its
// answer. It writes nothing: Apply writes the events. So a test can call it
// with a plain View, and the answer depends only on the state before the
// command and on the command. Every command is checked first and written
// after, so an error makes no event. The rules are in spec
// (groups.go), which is the reference model.

// maxGroupCommand is the largest group command, by the size rule below.
const maxGroupCommand = 256 << 10

// Header is the G record of a group.
type Header struct {
	Generation         int32
	ProtocolType       string
	Protocol           string
	Leader             string
	AssignedGeneration int32
}

// View is the state that the rules read: the state before the command.
// tables.go has the View over a transaction.
type View interface {
	// Header is the header of a group; ok is false when the group was never
	// installed (the header is then zero).
	Header(group string) (header Header, ok bool)
	// HasMember reports whether member is in group.
	HasMember(group, member string) bool
	// MemberIDs lists the member ids of a group, in id order.
	MemberIDs(group string) []string
	// Members lists the members of a group, in id order. The caller may
	// change the slice.
	Members(group string) []contract.GroupMemberInfo
	// PartitionCount is how many partitions a topic has (0 when it does not
	// exist). The topic context knows it; the group context only asks.
	PartitionCount(topic string) int64
}

// cmdSize is the size rule of a group command: 16 for the command, 4 + length
// for each string or byte field, 10 for each number and each list length.
// The stored bytes are never longer than this count, so a command that passes
// the rule fits the log item.
type cmdSize struct{ n int }

func (c *cmdSize) str(s string)   { c.n += 4 + len(s) }
func (c *cmdSize) bytes(b []byte) { c.n += 4 + len(b) }
func (c *cmdSize) num()           { c.n += 10 }

func installSize(operation contract.GroupInstall) int {
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

func assignSize(operation contract.GroupAssign) int {
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

func removeSize(operation contract.GroupRemove) int {
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

func offsetCommitSize(operation contract.OffsetCommit) int {
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

// DecideInstall decides a GroupInstall: the next generation of a group, with
// the members of its join. ErrTooLarge over the size rule; ErrInvalid for a
// bad shape (see validInstall); ErrIllegalGeneration unless the generation is
// the stored one plus 1. The new members replace the old ones.
func DecideInstall(view View, operation contract.GroupInstall) ([]Event, runtime.ErrCode) {
	if installSize(operation) > maxGroupCommand {
		return nil, runtime.ErrTooLarge
	}
	if !validInstall(operation) {
		return nil, runtime.ErrInvalid
	}
	h, _ := view.Header(operation.Group)
	if operation.Generation != h.Generation+1 {
		return nil, runtime.ErrIllegalGeneration
	}
	replaced := view.MemberIDs(operation.Group)
	members := make([]contract.GroupMemberInfo, 0, len(operation.Members))
	for _, m := range operation.Members {
		members = append(members, contract.GroupMemberInfo{GroupMember: m, JoinedTimeMs: operation.TimeMs})
	}
	next := Header{Generation: operation.Generation, ProtocolType: operation.ProtocolType, Protocol: operation.Protocol, Leader: operation.Leader}
	return []Event{GroupInstalled{Group: operation.Group, Header: next, Members: members, Replaced: replaced}}, runtime.OK
}

// validInstall checks the shape of a join result: a group name, members with
// unique non-empty ids, and a leader that is one of them.
func validInstall(operation contract.GroupInstall) bool {
	if operation.Group == "" || len(operation.Members) == 0 {
		return false
	}
	ids := map[string]bool{} // lookups only
	for _, m := range operation.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return false
		}
		ids[m.MemberID] = true
	}
	return ids[operation.Leader]
}

// DecideAssign decides a GroupAssign: the leader's assignment for the current
// generation. ErrTooLarge over the size rule; ErrIllegalGeneration for another
// generation than the stored one; ErrUnknownMember when the group was never
// installed, the sender is not its leader, or an assigned member is not in the
// group; ErrInvalid for a member assigned twice. A member with no bytes gets no
// assignment.
func DecideAssign(view View, operation contract.GroupAssign) ([]Event, runtime.ErrCode) {
	if assignSize(operation) > maxGroupCommand {
		return nil, runtime.ErrTooLarge
	}
	h, found := view.Header(operation.Group)
	if operation.Generation != h.Generation {
		return nil, runtime.ErrIllegalGeneration
	}
	if !found || operation.Leader != h.Leader || !view.HasMember(operation.Group, operation.Leader) {
		return nil, runtime.ErrUnknownMember
	}
	want := map[string][]byte{} // lookups only
	for _, a := range operation.Assignments {
		if !view.HasMember(operation.Group, a.Member) {
			return nil, runtime.ErrUnknownMember
		}
		if _, dup := want[a.Member]; dup {
			return nil, runtime.ErrInvalid
		}
		want[a.Member] = a.Bytes
	}
	members := view.Members(operation.Group)
	for i := range members {
		members[i].Assignment = blobOrNil(want[members[i].MemberID])
	}
	h.AssignedGeneration = operation.Generation
	return []Event{AssignmentsSet{Group: operation.Group, Header: h, Members: members}}, runtime.OK
}

// DecideRemove decides a GroupRemove: the listed members leave the group.
// A group that was never installed is OK and makes no event. A member that is
// not in the group is ignored. The leader is cleared when it is gone, and so
// is the assigned generation when nobody is left. ErrTooLarge over the size rule.
func DecideRemove(view View, operation contract.GroupRemove) ([]Event, runtime.ErrCode) {
	if removeSize(operation) > maxGroupCommand {
		return nil, runtime.ErrTooLarge
	}
	h, found := view.Header(operation.Group)
	if !found {
		return nil, runtime.OK
	}
	left := len(view.MemberIDs(operation.Group))
	var removed []string
	gone := map[string]bool{} // lookups only
	for _, id := range operation.Members {
		if !gone[id] && view.HasMember(operation.Group, id) {
			gone[id] = true
			removed = append(removed, id)
			left--
		}
	}
	if gone[h.Leader] || !view.HasMember(operation.Group, h.Leader) {
		h.Leader = ""
	}
	if left == 0 {
		h.Leader = ""
		h.AssignedGeneration = 0
	}
	return []Event{MembersRemoved{Group: operation.Group, Members: removed, Header: h}}, runtime.OK
}

// DecideCommitOffsets decides an OffsetCommit and returns its events, the code
// of each offset (in request order) and the code of the whole command.
//
// The command is checked as a whole first. ErrTooLarge over the size rule
// comes first. With Generation >= 0 the member must be in the group
// (ErrUnknownMember) and the generation must be the stored one
// (ErrIllegalGeneration). A manual commit (Generation -1, no member) is only
// for a group with no members (else ErrIllegalGeneration). Any other mix is
// ErrIllegalGeneration.
//
// Then each offset is checked alone: ErrUnknownPartition for a partition the
// topic does not have, ErrInvalid for a negative offset. The offsets that pass
// are stored, and the others do not stop them.
func DecideCommitOffsets(view View, operation contract.OffsetCommit) ([]Event, []runtime.ErrCode, runtime.ErrCode) {
	if offsetCommitSize(operation) > maxGroupCommand {
		return nil, nil, runtime.ErrTooLarge
	}
	h, found := view.Header(operation.Group)
	switch {
	case operation.Generation >= 0:
		if !found || !view.HasMember(operation.Group, operation.Member) {
			return nil, nil, runtime.ErrUnknownMember
		}
		if operation.Generation != h.Generation {
			return nil, nil, runtime.ErrIllegalGeneration
		}
	case operation.Generation == -1 && operation.Member == "":
		if len(view.MemberIDs(operation.Group)) > 0 {
			return nil, nil, runtime.ErrIllegalGeneration
		}
	default:
		return nil, nil, runtime.ErrIllegalGeneration
	}
	var errs []runtime.ErrCode
	var saved []contract.PartitionOffset
	for _, o := range operation.Offsets {
		code := checkOffset(view, o)
		if code == runtime.OK {
			saved = append(saved, o)
		}
		errs = append(errs, code)
	}
	if len(saved) == 0 {
		return nil, errs, runtime.OK
	}
	return []Event{OffsetsCommitted{Group: operation.Group, TimeMs: operation.TimeMs, Offsets: saved}}, errs, runtime.OK
}

// checkOffset is the code of one offset of an OffsetCommit.
func checkOffset(view View, o contract.PartitionOffset) runtime.ErrCode {
	n := view.PartitionCount(o.TopicPartition.Topic)
	if o.TopicPartition.Partition < 0 || int64(o.TopicPartition.Partition) >= n {
		return runtime.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return runtime.ErrInvalid
	}
	return runtime.OK
}
