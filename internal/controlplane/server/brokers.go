package server

// Broker membership: each broker sends BrokerHeartbeat to renew its lease, and
// the active node answers with the live broker set (decision Q4, Q7b).
//
// Where the pieces live:
//   - wire messages: api/v1 (BrokerInfo, BrokerHeartbeat*)
//   - conversions: wireboundary, section "Broker heartbeat"
//   - membership: the lease rules (who is alive)
//   - this file: the handler
//   - client call: controlplaneclient/brokers.go
//   - broker side: system/production/lease.go sends the heartbeat and turns the
//     answer into a core.LiveBrokers event
//
// The set is not logged and not stored. After a restart or a failover it starts
// empty and fills again within one heartbeat interval.

import (
	"context"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/membership"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// BrokerHeartbeat implements ControlPlane. It renews the caller's lease and
// returns the live broker set (Q4). Only the active node keeps the set, so a
// standby answers Unavailable and the broker client tries the next address.
func (n *Node) BrokerHeartbeat(ctx context.Context, request *cpv1.BrokerHeartbeatRequest) (*cpv1.BrokerHeartbeatResponse, error) {
	b := wireboundary.BrokerHeartbeatToCore(request)
	if !membership.Valid(b) {
		return nil, toRPCError(core.ErrInvalid, core.NotApplied)
	}
	type result struct {
		live []core.BrokerAddr
		ok   bool
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []core.Action {
		if !m.IsActive() || !m.Ready() {
			ch <- result{}
			return nil
		}
		now := n.config.Clock.Mono()
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
