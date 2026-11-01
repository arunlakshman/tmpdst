package livebrokers

import (
	"reflect"
	"testing"
	"time"

	"mds/internal/core"
)

func ids(bs []core.BrokerAddr) []int32 {
	var out []int32
	for _, b := range bs {
		out = append(out, b.NodeID)
	}
	return out
}

func TestRegisterRenewExpire(t *testing.T) {
	const ttl = 5 * time.Second
	s := New(ttl)
	t0 := time.Unix(1000, 0)
	b1 := core.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}

	if got := s.Live(t0); len(got) != 0 {
		t.Fatalf("new set has %v", got)
	}
	s.Renew(b1, t0)
	if got := s.Live(t0.Add(ttl - time.Nanosecond)); !reflect.DeepEqual(got, []core.BrokerAddr{b1}) {
		t.Fatalf("just before ttl: %v", got)
	}
	// A renew moves the expiry forward.
	s.Renew(b1, t0.Add(4*time.Second))
	if got := s.Live(t0.Add(ttl)); !reflect.DeepEqual(got, []core.BrokerAddr{b1}) {
		t.Fatalf("after renew: %v", got)
	}
	// No renew for the whole ttl: the lease is gone, and stays gone.
	if got := s.Live(t0.Add(4*time.Second + ttl)); len(got) != 0 {
		t.Fatalf("expired lease still live: %v", got)
	}
	if got := s.Live(t0.Add(4 * time.Second)); len(got) != 0 {
		t.Fatalf("expired lease came back: %v", got)
	}
}

func TestRenewNewAddressReplaces(t *testing.T) {
	s := New(time.Minute)
	t0 := time.Unix(0, 0)
	s.Renew(core.BrokerAddr{NodeID: 2, Host: "old", Port: 1}, t0)
	s.Renew(core.BrokerAddr{NodeID: 2, Host: "new", Port: 2}, t0)
	got := s.Live(t0)
	if len(got) != 1 || got[0].Host != "new" || got[0].Port != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestLiveSortedByNodeID(t *testing.T) {
	s := New(time.Minute)
	t0 := time.Unix(0, 0)
	for _, id := range []int32{3, 1, 2} {
		s.Renew(core.BrokerAddr{NodeID: id, Host: "h", Port: 9092}, t0)
	}
	for range 5 {
		if got := ids(s.Live(t0)); !reflect.DeepEqual(got, []int32{1, 2, 3}) {
			t.Fatalf("order %v", got)
		}
	}
}

func TestOnlyFreshLeasesLive(t *testing.T) {
	const ttl = 3 * time.Second
	s := New(ttl)
	t0 := time.Unix(0, 0)
	s.Renew(core.BrokerAddr{NodeID: 1, Host: "a"}, t0)
	s.Renew(core.BrokerAddr{NodeID: 2, Host: "b"}, t0.Add(2*time.Second))
	s.Renew(core.BrokerAddr{NodeID: 3, Host: "c"}, t0.Add(3*time.Second))
	// At t0+3s broker 1 has expired, brokers 2 and 3 are fresh.
	if got := ids(s.Live(t0.Add(3 * time.Second))); !reflect.DeepEqual(got, []int32{2, 3}) {
		t.Fatalf("live %v", got)
	}
}
