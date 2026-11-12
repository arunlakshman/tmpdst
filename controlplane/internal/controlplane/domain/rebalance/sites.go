package rebalance

import "github.com/manifesterio/mds/controlplane/internal/runtime"

// The buggify sites of the machine. Each takes a path that is valid but rare
// in a real run, so the simulator reaches it often. A site never makes a
// valid machine fail: it only does what a slow host, a late timer or a
// replayed message could do anyway. The names are stable plain strings.
const (
	// SiteTimerLate: a timer fires up to 50 ms late, as a busy host can make it.
	SiteTimerLate = "rebalance.timer.late"
	// SiteStaleTimer: when a timer is set again, its old arm fires anyway (as
	// in live, when it fired just before it was replaced). The old Gen is ignored.
	SiteStaleTimer = "rebalance.timer.stale"
	// SiteDuplicateReply: a ReplicaReply comes twice. The second copy has an
	// unknown request id and is ignored.
	SiteDuplicateReply = "rebalance.reply.duplicate"
	// SiteLateDrain: after a write, the queue of the group is taken in a later
	// step. The group stays busy until then, so the order of its inputs is kept;
	// inputs of other groups may go first.
	SiteLateDrain = "rebalance.queue.late-drain"
	// SiteLoadRetry: a load read that came back OK is treated as refused and
	// sent again after LoadRetry (rule 39).
	SiteLoadRetry = "rebalance.load.retry"
)

// lateMax is the most a SiteTimerLate delay adds.
const lateMax = 50 * runtime.Millisecond

// SiteNames returns the names of all buggify sites of the machine, in order.
func SiteNames() []string {
	return []string{SiteTimerLate, SiteStaleTimer, SiteDuplicateReply, SiteLateDrain, SiteLoadRetry}
}

// maybe reports whether site fires now. With no fault injector none does.
func (m *Machine) maybe(site string) bool {
	return m.cfg.Faults != nil && m.cfg.Faults.Maybe(runtime.FaultPointID(site))
}
