package broker_test

import (
	"reflect"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/broker"
	"github.com/manifesterio/mds/dst/machine"
)

func ids(bs []contract.BrokerAddr) []int32 {
	var out []int32
	for _, b := range bs {
		out = append(out, b.NodeID)
	}
	return out
}

func TestRegisterRenewExpire(t *testing.T) {
	const ttl = 5 * machine.Second
	s := broker.New(ttl)
	t0 := machine.NodeTime(1000 * machine.Second)
	b1 := contract.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}

	if got := s.Live(t0); len(got) != 0 {
		t.Fatalf("new set has %v", got)
	}
	s.Renew(b1, t0)
	if got := s.Live(at(t0, ttl-machine.Nanosecond)); !reflect.DeepEqual(got, []contract.BrokerAddr{b1}) {
		t.Fatalf("just before ttl: %v", got)
	}
	// A renew moves the expiry forward.
	s.Renew(b1, at(t0, 4*machine.Second))
	if got := s.Live(at(t0, ttl)); !reflect.DeepEqual(got, []contract.BrokerAddr{b1}) {
		t.Fatalf("after renew: %v", got)
	}
	// No renew for the whole ttl: the lease is gone, and stays gone.
	if got := s.Live(at(t0, 4*machine.Second+ttl)); len(got) != 0 {
		t.Fatalf("expired lease still live: %v", got)
	}
	if got := s.Live(at(t0, 4*machine.Second)); len(got) != 0 {
		t.Fatalf("expired lease came back: %v", got)
	}
}

func TestRenewNewAddressReplaces(t *testing.T) {
	s := broker.New(60 * machine.Second)
	t0 := machine.NodeTime(0)
	s.Renew(contract.BrokerAddr{NodeID: 2, Host: "old", Port: 1}, t0)
	s.Renew(contract.BrokerAddr{NodeID: 2, Host: "new", Port: 2}, t0)
	got := s.Live(t0)
	if len(got) != 1 || got[0].Host != "new" || got[0].Port != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestLiveSortedByNodeID(t *testing.T) {
	s := broker.New(60 * machine.Second)
	t0 := machine.NodeTime(0)
	for _, id := range []int32{3, 1, 2} {
		s.Renew(contract.BrokerAddr{NodeID: id, Host: "h", Port: 9092}, t0)
	}
	for range 5 {
		if got := ids(s.Live(t0)); !reflect.DeepEqual(got, []int32{1, 2, 3}) {
			t.Fatalf("order %v", got)
		}
	}
}

func TestOnlyFreshLeasesLive(t *testing.T) {
	const ttl = 3 * machine.Second
	s := broker.New(ttl)
	t0 := machine.NodeTime(0)
	s.Renew(contract.BrokerAddr{NodeID: 1, Host: "a"}, t0)
	s.Renew(contract.BrokerAddr{NodeID: 2, Host: "b"}, at(t0, 2*machine.Second))
	s.Renew(contract.BrokerAddr{NodeID: 3, Host: "c"}, at(t0, 3*machine.Second))
	// At t0+3s broker 1 has expired, brokers 2 and 3 are fresh.
	if got := ids(s.Live(at(t0, 3*machine.Second))); !reflect.DeepEqual(got, []int32{2, 3}) {
		t.Fatalf("live %v", got)
	}
}

func TestLiveDropsExpiredAndKeepsSortedOrder(t *testing.T) {
	const ttl = 10 * machine.Second
	s := broker.New(ttl)
	t0 := machine.NodeTime(0)
	// Renew out of id order, with a mix of old and fresh heartbeats.
	s.Renew(contract.BrokerAddr{NodeID: 5, Host: "e"}, at(t0, 8*machine.Second))
	s.Renew(contract.BrokerAddr{NodeID: 2, Host: "b"}, t0)
	s.Renew(contract.BrokerAddr{NodeID: 4, Host: "d"}, at(t0, 9*machine.Second))
	s.Renew(contract.BrokerAddr{NodeID: 1, Host: "a"}, at(t0, 5*machine.Second))
	s.Renew(contract.BrokerAddr{NodeID: 3, Host: "c"}, t0)
	if got := ids(s.Live(at(t0, 12*machine.Second))); !reflect.DeepEqual(got, []int32{1, 4, 5}) {
		t.Fatalf("live %v", got)
	}
	// Later renewals still land in id order, and the dropped ones stay dropped.
	s.Renew(contract.BrokerAddr{NodeID: 3, Host: "c"}, at(t0, 12*machine.Second))
	if got := ids(s.Live(at(t0, 12*machine.Second))); !reflect.DeepEqual(got, []int32{1, 3, 4, 5}) {
		t.Fatalf("live after renew %v", got)
	}
	if got := ids(s.Live(at(t0, 30*machine.Second))); len(got) != 0 {
		t.Fatalf("live %v", got)
	}
}

// at is the instant d after t0.
func at(t0 machine.NodeTime, d machine.Duration) machine.NodeTime { return t0 + machine.NodeTime(d) }

func TestValid(t *testing.T) {
	ok := contract.BrokerAddr{NodeID: 1, Host: "h", Port: 9092}
	if !broker.Valid(ok) {
		t.Fatal("a good address was refused")
	}
	for name, b := range map[string]contract.BrokerAddr{
		"no node id": {Host: "h", Port: 9092},
		"no host":    {NodeID: 1, Port: 9092},
		"no port":    {NodeID: 1, Host: "h"},
	} {
		if broker.Valid(b) {
			t.Errorf("%s was accepted", name)
		}
	}
}
