package sched_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

func TestOrderByTimeThenTie(t *testing.T) {
	r := rng.New(1)
	s := sched.New(r)
	var order []string

	s.After(20, "late", func() { order = append(order, "late") })
	s.After(10, "early-a", func() { order = append(order, "early-a") })
	s.After(10, "early-b", func() { order = append(order, "early-b") })
	s.After(0, "now", func() { order = append(order, "now") })

	for s.Next() {
	}
	if s.Steps() != 4 {
		t.Fatalf("Steps = %d", s.Steps())
	}
	if order[0] != "now" {
		t.Fatalf("first = %s, want now", order[0])
	}
	if order[len(order)-1] != "late" {
		t.Fatalf("last = %s, want late", order[len(order)-1])
	}
	// Same-time early-* order is fixed by seed ties, not enqueue order.
	early := order[1] + "," + order[2]
	if early != "early-a,early-b" && early != "early-b,early-a" {
		t.Fatalf("unexpected early pair %s", early)
	}
}

func TestSameSeedSameOrder(t *testing.T) {
	run := func(seed uint64) []string {
		r := rng.New(seed)
		s := sched.New(r)
		var order []string
		for _, label := range []string{"a", "b", "c", "d"} {
			s.After(5, label, func() {
				order = append(order, label)
			})
		}
		for s.Next() {
		}
		return order
	}
	a, b := run(99), run(99)
	if len(a) != len(b) {
		t.Fatal("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("order diverged at %d: %v vs %v", i, a, b)
		}
	}
}

func TestNegativeDelayIsZero(t *testing.T) {
	r := rng.New(3)
	s := sched.New(r)
	ran := false
	s.After(-1, "neg", func() {
		ran = true
		if s.Mono() != 0 {
			t.Fatalf("Mono = %d after negative delay", s.Mono())
		}
	})
	if !s.Next() || !ran {
		t.Fatal("event missing")
	}
}

func TestMonoAdvances(t *testing.T) {
	r := rng.New(4)
	s := sched.New(r)
	s.After(100, "t", func() {})
	s.Next()
	if s.Mono() != 100 {
		t.Fatalf("Mono = %d want 100", s.Mono())
	}
	if s.Len() != 0 {
		t.Fatal("queue not empty")
	}
}

func TestPeekAndJump(t *testing.T) {
	s := sched.New(rng.New(5))
	if _, ok := s.Peek(); ok {
		t.Fatal("empty Peek want ok=false")
	}
	s.After(40, "t", func() {})
	at, ok := s.Peek()
	if !ok || at != 40 {
		t.Fatalf("Peek = %d,%v want 40,true", at, ok)
	}
	s.Jump(25)
	if s.Mono() != 25 {
		t.Fatalf("Mono = %d want 25", s.Mono())
	}
	s.Jump(10)
	if s.Mono() != 25 {
		t.Fatalf("Jump backward changed Mono to %d", s.Mono())
	}
	s.Next()
	if s.Mono() != 40 {
		t.Fatalf("Mono = %d want 40", s.Mono())
	}
}
