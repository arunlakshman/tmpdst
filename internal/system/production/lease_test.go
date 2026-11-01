package production

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"mds/internal/core"
	"mds/internal/system"
)

// fakeLease answers the first beats with live and then fails, or always fails.
type fakeLease struct {
	live  []core.BrokerAddr
	okFor int32 // beats that succeed; later beats fail
	beats atomic.Int32
}

func (f *fakeLease) BrokerHeartbeat(context.Context, core.BrokerAddr) ([]core.BrokerAddr, error) {
	if f.beats.Add(1) <= f.okFor {
		return f.live, nil
	}
	return nil, errors.New("control plane down")
}

// nextEvent waits for the next control event in in.
func nextEvent(t *testing.T, in *system.Inbox) core.EventData {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if body, ok := in.Control().Pop(); ok {
			return body
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no event within 2s")
	return nil
}

func TestLeaseLoopDeliversLiveSet(t *testing.T) {
	in := system.NewInbox(8)
	b1 := core.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}
	b2 := core.BrokerAddr{NodeID: 2, Host: "h2", Port: 9092}
	lease := &fakeLease{live: []core.BrokerAddr{b1, b2}, okFor: 1 << 30}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go leaseLoop(ctx, in, lease, b1, 5*time.Millisecond, slog.Default())

	for range 3 {
		got, ok := nextEvent(t, in).(core.LiveBrokers)
		if !ok || len(got.Brokers) != 2 || got.Brokers[0] != b1 || got.Brokers[1] != b2 {
			t.Fatalf("event %+v", got)
		}
	}
}

func TestLeaseLoopEmptiesSetWhenStale(t *testing.T) {
	in := system.NewInbox(8)
	b1 := core.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}
	// One good beat, then the control plane is down for good.
	lease := &fakeLease{live: []core.BrokerAddr{b1}, okFor: 1}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go leaseLoop(ctx, in, lease, b1, 5*time.Millisecond, slog.Default())

	got, ok := nextEvent(t, in).(core.LiveBrokers)
	if !ok || len(got.Brokers) != 1 {
		t.Fatalf("first event %+v", got)
	}
	// After 3 missed beats the set goes empty, once.
	got, ok = nextEvent(t, in).(core.LiveBrokers)
	if !ok || len(got.Brokers) != 0 {
		t.Fatalf("stale event %+v", got)
	}
	if body, ok := in.Control().Pop(); ok {
		t.Fatalf("extra event %+v", body)
	}
}

func TestLeaseLoopStopsWithContext(t *testing.T) {
	in := system.NewInbox(8)
	b1 := core.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}
	lease := &fakeLease{okFor: 1 << 30}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		leaseLoop(ctx, in, lease, b1, time.Millisecond, slog.Default())
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("leaseLoop did not stop")
	}
}
