// Package livebrokers is the in-memory set of live brokers kept by the active
// control plane (decision Q4, Q7b). Each broker renews its lease with a
// heartbeat. A lease that is not renewed for the whole TTL is gone.
//
// The set is not logged and not stored. After a restart or a failover it
// starts empty and fills again within one heartbeat interval.
//
// The set has no lock. The server calls it only on its run loop.
package livebrokers

import (
	"cmp"
	"slices"
	"time"

	"mds/internal/core"
)

// Set holds the leases of the brokers that sent a heartbeat recently.
type Set struct {
	ttl    time.Duration
	leases map[int32]lease // by node id
}

type lease struct {
	addr core.BrokerAddr
	seen time.Time // time of the last heartbeat
}

// New returns an empty set. A lease lives for ttl after its last heartbeat.
func New(ttl time.Duration) *Set {
	return &Set{ttl: ttl, leases: map[int32]lease{}}
}

// Renew records a heartbeat from b at now. A new address for a known node id
// replaces the old one.
func (s *Set) Renew(b core.BrokerAddr, now time.Time) {
	s.leases[b.NodeID] = lease{addr: b, seen: now}
}

// Live drops the leases that expired by now and returns the rest, sorted by
// node id.
func (s *Set) Live(now time.Time) []core.BrokerAddr {
	var out []core.BrokerAddr
	for id, l := range s.leases {
		if now.Sub(l.seen) >= s.ttl {
			delete(s.leases, id)
			continue
		}
		out = append(out, l.addr)
	}
	slices.SortFunc(out, func(a, b core.BrokerAddr) int {
		return cmp.Compare(a.NodeID, b.NodeID)
	})
	return out
}
