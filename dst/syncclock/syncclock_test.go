package syncclock_test

import (
	"slices"
	"sync"
	"testing"

	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/syncclock"
)

func TestSyncTimersFireInOrderAndChain(t *testing.T) {
	c := syncclock.NewSync(1_000_000, 10)
	var order []string
	c.AfterFunc(30, func() { order = append(order, "c") })
	c.AfterFunc(10, func() {
		order = append(order, "a")
		c.AfterFunc(10, func() { order = append(order, "b") }) // due in the same Advance
	})
	if c.Pending() != 2 {
		t.Fatalf("pending %d", c.Pending())
	}
	c.Advance(25)
	if !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("order %v", order)
	}
	c.Advance(10)
	if !slices.Equal(order, []string{"a", "b", "c"}) || c.Pending() != 0 {
		t.Fatalf("order %v pending %d", order, c.Pending())
	}
	if c.Mono() != 45 || c.Wall() != 1_000_035 {
		t.Fatalf("mono %d wall %d", c.Mono(), c.Wall())
	}
}

func TestSyncStop(t *testing.T) {
	c := syncclock.NewSync(0, 0)
	tm := c.AfterFunc(5, func() { t.Error("stopped timer fired") })
	if !tm.Stop() || tm.Stop() || c.Pending() != 0 {
		t.Fatal("Stop must report true once and drop the pending count")
	}
	fired := c.AfterFunc(1, func() {})
	c.Advance(sched.TimeNs(10))
	if fired.Stop() {
		t.Fatal("Stop after fire must report false")
	}
}

func TestSyncSafeFromManyGoroutines(t *testing.T) {
	c := syncclock.NewSync(0, 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	n := 0
	for range 8 {
		wg.Go(func() {
			for range 100 {
				c.AfterFunc(3, func() { mu.Lock(); n++; mu.Unlock() })
				_ = c.Mono()
			}
		})
	}
	wg.Wait()
	c.Advance(5)
	if n != 800 || c.Pending() != 0 {
		t.Fatalf("fired %d pending %d", n, c.Pending())
	}
}
