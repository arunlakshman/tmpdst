package realclock_test

import (
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/realclock"
)

func TestMonoGoesForwardAndWallIsNow(t *testing.T) {
	c := realclock.New()
	a := c.Mono()
	time.Sleep(2 * time.Millisecond)
	if b := c.Mono(); b <= a {
		t.Fatalf("mono did not move: %d then %d", a, b)
	}
	if d := time.Since(time.Unix(0, int64(c.Wall()))); d < 0 || d > time.Minute {
		t.Fatalf("wall is not now: %v ago", d)
	}
}

func TestAfterFuncFiresAndStops(t *testing.T) {
	c := realclock.New()
	fired := make(chan struct{})
	c.AfterFunc(1, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("timer did not fire")
	}
	never := c.AfterFunc(3600*1e9, func() { t.Error("a stopped timer fired") })
	if !never.Stop() {
		t.Fatal("Stop of a pending timer must report true")
	}
}
