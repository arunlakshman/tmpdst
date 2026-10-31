package server_test

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/statemachine"
	"manifester/internal/core"
)

func hb(id int32, host string, port int32) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: &cpv1.BrokerInfo{NodeId: id, Host: host, Port: port}}
}

// TestBrokerHeartbeatActiveReturnsLiveSet: the active node renews each
// caller and answers with every live broker, sorted by node id.
func TestBrokerHeartbeatActiveReturnsLiveSet(t *testing.T) {
	cli, _, stop := startNode(t, statemachine.Config{Rank: 0, Heartbeat: core.Duration(time.Second)})
	defer stop()
	ctx := t.Context()

	resp, err := cli.BrokerHeartbeat(ctx, hb(2, "b2", 9092))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Brokers) != 1 || resp.Brokers[0].NodeId != 2 || resp.Brokers[0].Host != "b2" {
		t.Fatalf("first heartbeat brokers = %+v", resp.Brokers)
	}

	resp, err = cli.BrokerHeartbeat(ctx, hb(1, "b1", 9093))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Brokers) != 2 {
		t.Fatalf("second heartbeat brokers = %+v, want 2", resp.Brokers)
	}
	if resp.Brokers[0].NodeId != 1 || resp.Brokers[0].Port != 9093 || resp.Brokers[1].NodeId != 2 {
		t.Fatalf("brokers not sorted by node id: %+v", resp.Brokers)
	}
}

// TestBrokerHeartbeatInvalidBroker: a broker with no node id, no host or no
// port is rejected as InvalidArgument before it reaches the run loop.
func TestBrokerHeartbeatInvalidBroker(t *testing.T) {
	cli, _, stop := startNode(t, statemachine.Config{Rank: 0, Heartbeat: core.Duration(time.Second)})
	defer stop()
	ctx := t.Context()
	cases := []struct {
		name string
		req  *cpv1.BrokerHeartbeatRequest
	}{
		{"zero node id", hb(0, "h", 9092)},
		{"negative node id", hb(-1, "h", 9092)},
		{"empty host", hb(1, "", 9092)},
		{"zero port", hb(1, "h", 0)},
		{"no broker", &cpv1.BrokerHeartbeatRequest{}},
	}
	for _, c := range cases {
		_, err := cli.BrokerHeartbeat(ctx, c.req)
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Fatalf("%s: err = %v, want InvalidArgument", c.name, err)
		}
	}
}

// TestBrokerHeartbeatStandbyUnavailable: only the active node keeps the live
// set, so a standby answers Unavailable and the broker tries another address.
func TestBrokerHeartbeatStandbyUnavailable(t *testing.T) {
	cli, _, stop := startNode(t, statemachine.Config{
		Rank: 1, Heartbeat: core.Duration(time.Second), TakeoverAfter: core.Duration(time.Hour),
	})
	defer stop()
	_, err := cli.BrokerHeartbeat(t.Context(), hb(1, "b1", 9092))
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
}

// TestBrokerHeartbeatLeaseExpires: a broker that stops heartbeating drops out
// of the live set after the lease TTL. The node uses the real clock here.
func TestBrokerHeartbeatLeaseExpires(t *testing.T) {
	const ttl = 150 * time.Millisecond
	cli, _, stop := startNodeWithLease(t, statemachine.Config{Rank: 0, Heartbeat: core.Duration(time.Second)}, ttl)
	defer stop()
	ctx := t.Context()

	if _, err := cli.BrokerHeartbeat(ctx, hb(1, "b1", 9092)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * ttl)
	resp, err := cli.BrokerHeartbeat(ctx, hb(2, "b2", 9092))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Brokers) != 1 || resp.Brokers[0].NodeId != 2 {
		t.Fatalf("after expiry brokers = %+v, want only broker 2", resp.Brokers)
	}
}
