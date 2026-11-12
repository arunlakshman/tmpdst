package clock_test

import (
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/clock"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

func TestAdvanceFiresTimersInOrder(t *testing.T) {
	s := sched.New(rng.New(1))
	c := clock.New(s, 1_000)
	var order []string

	c.AfterFunc(30, func() { order = append(order, "c") })
	c.AfterFunc(10, func() { order = append(order, "a") })
	c.AfterFunc(20, func() { order = append(order, "b") })

	if c.Wall() != 1_000 {
		t.Fatalf("Wall = %d want 1000", c.Wall())
	}

	c.Advance(25)
	if got := join(order); got != "a,b" {
		t.Fatalf("after 25: %s want a,b", got)
	}
	if c.Mono() != 25 {
		t.Fatalf("Mono = %d want 25", c.Mono())
	}
	if c.Wall() != 1_025 {
		t.Fatalf("Wall = %d want 1025", c.Wall())
	}

	c.Advance(10)
	if got := join(order); got != "a,b,c" {
		t.Fatalf("after +10: %s want a,b,c", got)
	}
	if c.Mono() != 35 {
		t.Fatalf("Mono = %d want 35", c.Mono())
	}
}

func TestStopCancels(t *testing.T) {
	s := sched.New(rng.New(2))
	c := clock.New(s, 0)
	ran := false
	tm := c.AfterFunc(10, func() { ran = true })

	if !tm.Stop() {
		t.Fatal("first Stop want true")
	}
	if tm.Stop() {
		t.Fatal("second Stop want false")
	}

	c.Advance(100)
	if ran {
		t.Fatal("stopped timer fired")
	}
	if c.Mono() != 100 {
		t.Fatalf("Mono = %d want 100 after empty Advance", c.Mono())
	}
}

func TestStopAfterFire(t *testing.T) {
	s := sched.New(rng.New(3))
	c := clock.New(s, 0)
	tm := c.AfterFunc(5, func() {})
	c.Advance(5)
	if tm.Stop() {
		t.Fatal("Stop after fire want false")
	}
}

func TestNestedTimerFiresInSameAdvance(t *testing.T) {
	s := sched.New(rng.New(1))
	c := clock.New(s, 0)
	var order []string
	c.AfterFunc(3, func() { order = append(order, "c") })
	c.AfterFunc(1, func() {
		order = append(order, "a")
		c.AfterFunc(1, func() { order = append(order, "b") })
	})
	c.Advance(2)
	if got := join(order); got != "a,b" {
		t.Fatalf("order %s want a,b", got)
	}
	c.Advance(1)
	if got := join(order); got != "a,b,c" {
		t.Fatalf("order %s want a,b,c", got)
	}
}

func join(ss []string) string {
	return strings.Join(ss, ",")
}
