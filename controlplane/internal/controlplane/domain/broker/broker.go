// Package broker is the broker context of the control plane:
// which brokers are alive. Each broker sends a heartbeat to renew its lease, and
// the active control-plane node answers with the live broker set (decision Q4,
// Q7b).
//
// The set is not logged and not stored, so it is not a module of the replicated
// state. After a restart or a failover it starts empty and fills again within one
// heartbeat interval. It has no lock: the caller uses it only on the run loop.
//
// Time comes from the caller, as machine.NodeTime, so the rules are the same with
// the real clock and the fake one.
package broker

import (
	"cmp"
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
)

// DefaultTTL is the lease length when the configuration leaves it zero. Brokers
// send a heartbeat every interval, so keep the lease at about 3 times it.
const DefaultTTL machine.Duration = 5 * machine.Second

// Valid reports whether b is an address a broker may register: a positive node
// id, a host and a positive port.
func Valid(b contract.BrokerAddr) bool {
	return b.NodeID > 0 && b.Host != "" && b.Port > 0
}

// Leases holds the leases of the brokers that sent a heartbeat recently.
type Leases struct {
	ttl    machine.Duration
	leases []lease // sorted by node id, one per node
}

type lease struct {
	addr contract.BrokerAddr
	seen machine.NodeTime // time of the last heartbeat
}

// New returns an empty set. A lease lives for ttl after its last heartbeat.
func New(ttl machine.Duration) *Leases {
	return &Leases{ttl: ttl}
}

// Renew records a heartbeat from b at now. A new address for a known node id
// replaces the old one.
func (s *Leases) Renew(b contract.BrokerAddr, now machine.NodeTime) {
	l := lease{addr: b, seen: now}
	i, found := slices.BinarySearchFunc(s.leases, b.NodeID, func(l lease, id int32) int {
		return cmp.Compare(l.addr.NodeID, id)
	})
	if found {
		s.leases[i] = l
		return
	}
	s.leases = slices.Insert(s.leases, i, l)
}

// Live drops the leases that expired by now and returns the rest, sorted by
// node id.
func (s *Leases) Live(now machine.NodeTime) []contract.BrokerAddr {
	// The slice is sorted by node id, so the output is too. No map is ranged over.
	var out []contract.BrokerAddr
	live := s.leases[:0]
	for _, l := range s.leases {
		if machine.Duration(now-l.seen) >= s.ttl {
			continue
		}
		live = append(live, l)
		out = append(out, l.addr)
	}
	clear(s.leases[len(live):]) // drop the expired entries
	s.leases = live
	return out
}
