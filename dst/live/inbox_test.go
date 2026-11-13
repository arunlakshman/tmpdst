package live_test

import (
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
)

// Control comes out before Data. Inside one queue the order is the order of Deliver.
func TestInboxControlFirst(t *testing.T) {
	ib := live.NewInbox[in](0)
	ib.Deliver(machine.Data, in{kind: "data", n: 1})
	ib.Deliver(machine.Data, in{kind: "data", n: 2})
	ib.Deliver(machine.Control, in{kind: "control", n: 3})
	ib.Deliver(machine.Control, in{kind: "control", n: 4})
	if ib.Len() != 4 {
		t.Fatalf("Len = %d", ib.Len())
	}
	var order []int
	for {
		msg, ok := ib.Pop()
		if !ok {
			break
		}
		order = append(order, msg.n)
	}
	want := []int{3, 4, 1, 2}
	if len(order) != 4 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] || order[3] != want[3] {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

// A bounded Data queue blocks the sender when it is full, and a Pop lets it go on.
func TestInboxDataBlocksAtCap(t *testing.T) {
	ib := live.NewInbox[in](2)
	ib.Deliver(machine.Data, in{n: 1})
	ib.Deliver(machine.Data, in{n: 2})
	ib.Deliver(machine.Control, in{n: 9}) // control never blocks
	sent := make(chan struct{})
	go func() {
		ib.Deliver(machine.Data, in{n: 3})
		close(sent)
	}()
	select {
	case <-sent:
		t.Fatal("Deliver did not block at the cap")
	case <-time.After(30 * time.Millisecond):
	}
	if msg, ok := ib.Pop(); !ok || msg.n != 9 {
		t.Fatalf("first Pop = %v %v, want the control input", msg, ok)
	}
	if msg, ok := ib.Pop(); !ok || msg.n != 1 {
		t.Fatalf("second Pop = %v %v", msg, ok)
	}
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver stayed blocked after a Pop")
	}
}

// Close releases a blocked sender and drops what waits.
func TestInboxCloseReleasesSenders(t *testing.T) {
	ib := live.NewInbox[in](1)
	ib.Deliver(machine.Data, in{n: 1})
	var wg sync.WaitGroup
	wg.Go(func() { ib.Deliver(machine.Data, in{n: 2}) })
	time.Sleep(20 * time.Millisecond)
	ib.Close()
	wg.Wait()
	if ib.Len() != 0 {
		t.Fatalf("Len = %d after Close", ib.Len())
	}
	ib.Deliver(machine.Control, in{n: 3})
	if ib.Len() != 0 {
		t.Fatal("a closed inbox queued an input")
	}
	ib.Close() // a second Close is harmless
}
