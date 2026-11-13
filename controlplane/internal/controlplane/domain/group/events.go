package group

import "github.com/manifesterio/mds/controlplane/contract"

// Event is a fact that a command made. The log holds commands, not events: an
// event is made when its command is applied, on every replica, and is never
// stored. Apply (tables.go) writes it to the tables. Each group command makes
// at most one event.
type Event interface{ isEvent() }

// GroupInstalled says that the join of Group settled: the group is now at the
// generation of Header, and its members are exactly Members. Replaced are the
// ids of the members that were there before; they are dropped. A GroupInstall
// makes it.
type GroupInstalled struct { //nolint:revive // the event names in docs/greenfield-refactor-plan.md (T5.2)
	Group    string
	Header   Header
	Members  []contract.GroupMemberInfo
	Replaced []string
}

// AssignmentsSet says that the leader's assignment is stored for the current
// generation. Header has AssignedGeneration set, and Members are all the
// members of the group, each with its assignment (none for a member that got
// no bytes). A GroupAssign makes it.
type AssignmentsSet struct {
	Group   string
	Header  Header
	Members []contract.GroupMemberInfo
}

// MembersRemoved says that Members left the group (only ids that were in it).
// Header is the group header after they left: the leader is cleared when it
// left, and so is the assigned generation when nobody is left. A GroupRemove
// of an installed group makes it, even when no listed member was in the
// group.
type MembersRemoved struct {
	Group   string
	Members []string
	Header  Header
}

// OffsetsCommitted says that Offsets are the committed offsets of Group, at
// time TimeMs. They are the offsets of an OffsetCommit that passed their
// checks, in request order. An OffsetCommit with no such offset makes no event.
type OffsetsCommitted struct {
	Group   string
	TimeMs  int64
	Offsets []contract.PartitionOffset
}

func (GroupInstalled) isEvent()   {}
func (AssignmentsSet) isEvent()   {}
func (MembersRemoved) isEvent()   {}
func (OffsetsCommitted) isEvent() {}
