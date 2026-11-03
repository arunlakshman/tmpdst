package server

// Broker membership: each broker sends BrokerHeartbeat to renew its lease, and
// the active node answers with the live broker set (decision Q4, Q7b).
//
// Where the pieces live:
//   - wire messages: api/v1 (BrokerInfo, BrokerHeartbeat*)
//   - conversions: wireboundary, section "Broker heartbeat"
//   - this file: the handler and the lease set
//   - client call: controlplaneclient/brokers.go
//   - broker side: system/production/lease.go sends the heartbeat and turns the
//     answer into a core.LiveBrokers event
//
// The set is not logged and not stored. After a restart or a failover it starts
// empty and fills again within one heartbeat interval.

import (
	"cmp"
	"context"
	"slices"
	"time"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// defaultBrokerLeaseTTL is the lease length when Config leaves it zero.
const defaultBrokerLeaseTTL = 5 * time.Second

// BrokerHeartbeat implements ControlPlane. It renews the caller's lease and
// returns the live broker set (Q4). Only the active node keeps the set, so a
// standby answers Unavailable and the broker client tries the next address.
func (n *Node) BrokerHeartbeat(ctx context.Context, request *cpv1.BrokerHeartbeatRequest) (*cpv1.BrokerHeartbeatResponse, error) {
	b := wireboundary.BrokerHeartbeatToCore(request)
	if b.NodeID <= 0 || b.Host == "" || b.Port <= 0 {
		return nil, toRPCError(core.ErrInvalid, core.NotApplied)
	}
	type result struct {
		live []core.BrokerAddr
		ok   bool
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
		if !m.IsActive() || !m.Ready() {
			ch <- result{}
			return nil
		}
		now := time.Now()
		n.leases.Renew(b, now)
		ch <- result{live: n.leases.Live(now), ok: true}
		return nil
	})
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	r := <-ch
	if !r.ok {
		return nil, toRPCError(core.ErrUnavailable, core.NotApplied)
	}
	return wireboundary.BrokerHeartbeatRespFromCore(r.live), nil
}

// leaseSet holds the leases of the brokers that sent a heartbeat recently.
// It has no lock. The handler calls it only on the run loop.
type leaseSet struct {
	ttl    time.Duration
	leases map[int32]lease // by node id
}

type lease struct {
	addr core.BrokerAddr
	seen time.Time // time of the last heartbeat
}

// newLeaseSet returns an empty set. A lease lives for ttl after its last heartbeat.
func newLeaseSet(ttl time.Duration) *leaseSet {
	return &leaseSet{ttl: ttl, leases: map[int32]lease{}}
}

// Renew records a heartbeat from b at now. A new address for a known node id
// replaces the old one.
func (s *leaseSet) Renew(b core.BrokerAddr, now time.Time) {
	s.leases[b.NodeID] = lease{addr: b, seen: now}
}

// Live drops the leases that expired by now and returns the rest, sorted by
// node id.
func (s *leaseSet) Live(now time.Time) []core.BrokerAddr {
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
