package clock

import (
	"slices"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/machine"
)

func TestTimersFireInTimeOrderOnAdvance(t *testing.T) {
	c := NewFake(time.Unix(1000, 0))
	var order []string
	c.AfterFunc(machine.Duration(3*time.Second), func() { order = append(order, "c") })
	c.AfterFunc(machine.Duration(1*time.Second), func() {
		order = append(order, "a")
		// a timer started by a timer fires in the same Advance if it is due
		c.AfterFunc(machine.Duration(time.Second), func() { order = append(order, "b") })
	})
	if c.Pending() != 2 {
		t.Fatalf("pending %d", c.Pending())
	}
	c.Advance(2500 * time.Millisecond)
	if !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("order %v", order)
	}
	c.Advance(time.Second)
	if !slices.Equal(order, []string{"a", "b", "c"}) || c.Pending() != 0 {
		t.Fatalf("order %v pending %d", order, c.Pending())
	}
}

func TestStopAndTimeMoves(t *testing.T) {
	c := NewFake(time.Unix(1000, 0))
	mono, wall := c.Mono(), c.Wall()
	tm := c.AfterFunc(machine.Duration(time.Second), func() { t.Error("a stopped timer fired") })
	if !tm.Stop() || tm.Stop() {
		t.Fatal("Stop must report true once")
	}
	fired := c.AfterFunc(1, func() {})
	c.Advance(time.Minute)
	if fired.Stop() {
		t.Fatal("Stop of a fired timer must report false")
	}
	if c.Mono()-mono != machine.NodeTime(time.Minute) || c.Wall()-wall != machine.RealTime(time.Minute) {
		t.Fatalf("mono moved %d, wall moved %d", c.Mono()-mono, c.Wall()-wall)
	}
}
