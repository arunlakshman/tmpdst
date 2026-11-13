package rebalance

import (
	"slices"
	"strings"

	"github.com/manifesterio/mds/dst/machine"
)

// The Kafka names of the group states (Dead is a group the machine does not have).
const (
	stateEmpty   = "Empty"
	stateJoining = "PreparingRebalance"
	stateSyncing = "CompletingRebalance"
	stateStable  = "Stable"
	stateDead    = "Dead"
	reasonLeave  = "leave"
	reasonExpire = "expire"
)

// group is one group in memory. The fields here are the same in every phase.
// What only one phase needs is in the phase value.
type group struct {
	name       string
	generation int32
	protoType  string
	protocol   string
	leader     string
	members    []*member // sorted by id
	joinSeq    uint64    // join order of the last member that joined
	phase      phase
	round      timer // the round end in Joining, the sync deadline in Syncing and Stable
	delay      timer // the initial delay
	write      *write
	draining   bool    // a buggify site moved the drain of the queue to a later step
	queue      []Input // inputs that wait for the write, in order
}

// member is one member of a group.
type member struct {
	id          string
	clientID    string
	clientHost  string
	instanceID  string
	sessionMs   int32
	rebalanceMs int32
	order       uint64     // join order
	protocols   []Protocol // from its last join
	session     timer
	isNew       bool // joined with MemberID "" and has not been in a done round yet
	synced      bool // synced since the last install
}

// phase is what a group holds in one Kafka state. Each value holds only what
// its state needs, so a parked sync in Joining can not be written.
type phase interface{ state() string }

type (
	// empty: no member. The generation is kept.
	empty struct{}
	// joining is a join round (PreparingRebalance). joins holds the parked
	// join of each member by id.
	joining struct {
		start, end machine.NodeTime
		ended      bool // the round end passed: the round waits for its members with no end
		delay      bool // the initial delay still runs
		joins      map[string]CallID
	}
	// syncing waits for the leader's assignment (CompletingRebalance). syncs
	// holds the parked sync of each follower by id.
	syncing struct {
		syncs map[string]CallID
	}
	// stable holds the logged assignment of each member by id.
	stable struct {
		assign map[string][]byte
	}
)

func (empty) state() string    { return stateEmpty }
func (*joining) state() string { return stateJoining }
func (*syncing) state() string { return stateSyncing }
func (*stable) state() string  { return stateStable }

// find returns the member with id, or nil.
func (g *group) find(id string) *member {
	if i, ok := g.index(id); ok {
		return g.members[i]
	}
	return nil
}

func (g *group) index(id string) (int, bool) {
	return slices.BinarySearchFunc(g.members, id, func(m *member, id string) int { return strings.Compare(m.id, id) })
}

func (g *group) insert(m *member) {
	i, _ := g.index(m.id)
	g.members = slices.Insert(g.members, i, m)
}

func (g *group) delete(id string) {
	if i, ok := g.index(id); ok {
		g.members = slices.Delete(g.members, i, i+1)
	}
}

// busy reports that later inputs for the group must wait in its queue.
func (g *group) busy() bool { return g.write != nil || g.draining }

// longest is the largest rebalance timeout of the members.
func (g *group) longest() machine.NodeTime {
	var ms int32
	for _, m := range g.members {
		ms = max(ms, m.rebalanceMs)
	}
	return machine.NodeTime(ms) * machine.NodeTime(machine.Millisecond)
}

// parked returns the parked join or sync of m, if any.
func (g *group) parked(m *member) (CallID, bool) {
	switch p := g.phase.(type) {
	case *joining:
		c, ok := p.joins[m.id]
		return c, ok
	case *syncing:
		c, ok := p.syncs[m.id]
		return c, ok
	}
	return 0, false
}

// unpark forgets the parked call of m.
func (g *group) unpark(m *member) {
	switch p := g.phase.(type) {
	case *joining:
		delete(p.joins, m.id)
	case *syncing:
		delete(p.syncs, m.id)
	}
}

// ids lists the members, in id order, for which keep is true.
func (g *group) ids(keep func(*member) bool) []string {
	var out []string
	for _, m := range g.members {
		if keep(m) {
			out = append(out, m.id)
		}
	}
	return out
}

// insertName adds name to the sorted list names.
func insertName(names []string, name string) []string {
	i, found := slices.BinarySearch(names, name)
	if found {
		return names
	}
	return slices.Insert(names, i, name)
}

// clone returns a copy of b. An empty value is nil, as the log returns it.
func clone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return slices.Clone(b)
}
