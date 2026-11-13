package rebalance

import (
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
)

// The buggify sites of the machine. Each takes a path that is valid but rare
// in a real run, so the simulator reaches it often. A site never makes a
// valid machine fail: it only does what a slow host, a late timer or a
// replayed message could do anyway. The names are stable plain strings, and each site is declared once here.
var (
	// SiteTimerLate: a timer fires up to 50 ms late, as a busy host can make it.
	SiteTimerLate = fault.NewSite("rebalance.timer.late") //determinismcheck:ok catalog handle, set once at start
	// SiteStaleTimer: when a timer is set again, its old arm fires anyway (as
	// in live, when it fired just before it was replaced). The old Gen is ignored.
	SiteStaleTimer = fault.NewSite("rebalance.timer.stale") //determinismcheck:ok catalog handle, set once at start
	// SiteDuplicateReply: a ReplicaReply comes twice. The second copy has an
	// unknown request id and is ignored.
	SiteDuplicateReply = fault.NewSite("rebalance.reply.duplicate") //determinismcheck:ok catalog handle, set once at start
	// SiteLateDrain: after a write, the queue of the group is taken in a later
	// step. The group stays busy until then, so the order of its inputs is kept;
	// inputs of other groups may go first.
	SiteLateDrain = fault.NewSite("rebalance.queue.late-drain") //determinismcheck:ok catalog handle, set once at start
	// SiteLoadRetry: a load read that came back OK is treated as refused and
	// sent again after LoadRetry (rule 39).
	SiteLoadRetry = fault.NewSite("rebalance.load.retry") //determinismcheck:ok catalog handle, set once at start
)

// lateMax is the most a SiteTimerLate delay adds.
const lateMax = 50 * machine.Millisecond

// SiteNames returns the names of all buggify sites of the machine, in order.
func SiteNames() []string {
	return []string{
		SiteTimerLate.Name(), SiteStaleTimer.Name(), SiteDuplicateReply.Name(), SiteLateDrain.Name(), SiteLoadRetry.Name(),
	}
}

// maybe reports whether site fires now. The env of production never fires a site.
func (m *Machine) maybe(site fault.NamedSite) bool {
	return m.env.Maybe(site)
}
