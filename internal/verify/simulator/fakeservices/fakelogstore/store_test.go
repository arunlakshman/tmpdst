package fakelogstore

import (
	"testing"

	"github.com/manifesterio/mds/internal/core"
)

// A rejected put at or below the floor still writes the row into an empty slot.
// The row is a stray row: the store keeps it, and no read returns it.
func TestStrayRowStaysAfterRejectedPut(t *testing.T) {
	s := newStore(false)
	s.exec(core.LogAppend{Req: 1, Slot: 0, Data: []byte("a")})
	s.exec(core.LogDeleteOld{Req: 2, Through: 0})
	d := s.exec(core.LogAppend{Req: 3, Slot: 0, Data: []byte("x")}).(core.LogAppendDone)
	if d.Err != core.ErrConflict || d.Outcome != core.NotApplied || !d.Taken || d.Existing != nil {
		t.Fatal(d)
	}
	if s.slots[0].kind != slotEntry || string(s.slots[0].data) != "x" {
		t.Fatal("the stray row was not written")
	}
	if s.floor != 0 {
		t.Fatal(s.floor)
	}
}

// A put below the floor does not overwrite a row that is already in the slot.
func TestRejectedPutLeavesOccupiedSlotAlone(t *testing.T) {
	s := newStore(false)
	s.exec(core.LogAppend{Req: 1, Slot: 0, Data: []byte("a")})
	s.exec(core.LogDeleteOld{Req: 2, Through: 0})
	s.exec(core.LogAppend{Req: 3, Slot: 0, Data: []byte("x")})
	d := s.exec(core.LogAppend{Req: 4, Slot: 0, Data: []byte("y")}).(core.LogAppendDone)
	if d.Err != core.ErrConflict || d.Existing != nil || string(s.slots[0].data) != "x" {
		t.Fatal(d, string(s.slots[0].data))
	}
}

// Trim only raises the floor. It never lowers it, even when a smaller Through comes later.
func TestFloorOnlyMovesUp(t *testing.T) {
	s := newStore(false)
	s.exec(core.LogDeleteOld{Req: 1, Through: 5})
	s.exec(core.LogDeleteOld{Req: 2, Through: 2})
	if s.floor != 5 {
		t.Fatal(s.floor)
	}
}

// The state hash covers the epoch, the owner and the floor. Each one alone changes it.
func TestStateHashCoversEpochOwnerAndFloor(t *testing.T) {
	base := func() *store {
		return &store{slots: []slotRec{{}}, floor: noFloor}
	}
	h0 := base().stateHash()
	if base().stateHash() != h0 {
		t.Fatal("hash is not stable")
	}
	withFloor := base()
	withFloor.floor = 0
	withEpoch := base()
	withEpoch.epoch = 1
	withOwner := base()
	withOwner.owner = "cp-0"
	for name, s := range map[string]*store{"floor": withFloor, "epoch": withEpoch, "owner": withOwner} {
		if s.stateHash() == h0 {
			t.Fatalf("%s does not change the state hash", name)
		}
	}
}
