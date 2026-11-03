package fakelogstore_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/simulator"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/fakelogservice"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/fakelogstore"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/internal/fakeservicetest"
)

const ms = core.Duration(1_000_000)

func base() simulator.LogStoreFaults {
	ts := simulator.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return simulator.LogStoreFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

func setup(f simulator.LogStoreFaults, seed uint64) (*fakelogstore.Engine, *fakeservicetest.Ctx, core.NodeInstance) {
	e := fakelogstore.New(f)
	return e, fakeservicetest.New(e, seed), fakeservicetest.Ref("a")
}

func only[T core.EventData](t *testing.T, c *fakeservicetest.Ctx) T {
	t.Helper()
	if len(c.Delivered) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(c.Delivered))
	}
	return c.Delivered[0].Body.(T)
}

func TestIfAbsentPut(t *testing.T) {
	e, c, a := setup(base(), 1)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 3, Data: []byte("first")})
	c.Run()
	d := only[core.LogAppendDone](t, c)
	if d.Err != core.OK || d.Outcome != core.Applied || d.Taken {
		t.Fatal(d)
	}
	e.Execute(c, a, core.LogAppend{Req: 2, Slot: 3, Data: []byte("second")})
	c.Run()
	d2 := c.Find(2).(core.LogAppendDone)
	if d2.Err != core.OK || d2.Outcome != core.Applied || !d2.Taken || string(d2.Existing) != "first" {
		t.Fatal(d2)
	}
	if v, ok := e.Entry(3); !ok || string(v) != "first" {
		t.Fatal(v, ok)
	}
	if e.Handles(core.ControlPlaneCall{}) || !e.Handles(core.LogAppend{}) {
		t.Fatal("Handles")
	}
}

func TestLostAppendMaybeApplied(t *testing.T) {
	f := base()
	f.LostResponse = 1
	var saw bool
	for seed := uint64(1); seed <= 5; seed++ {
		e, c, a := setup(f, seed)
		e.Execute(c, a, core.LogAppend{Req: 1, Slot: 0, Data: []byte("x")})
		c.Run()
		d := only[core.LogAppendDone](t, c)
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
			t.Fatalf("seed %d: %v", seed, d)
		}
		if v, ok := e.Entry(0); !ok || string(v) != "x" {
			t.Fatalf("seed %d: lost answer must still land", seed)
		}
		if !slices.Contains(c.Faults, "log_lost_response") {
			t.Fatalf("seed %d: want log_lost_response, got %v", seed, c.Faults)
		}
		saw = true
	}
	if !saw {
		t.Fatal("no seeds")
	}
}

func TestOrderedLandings(t *testing.T) {
	e, c, a := setup(base(), 2)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 2, Data: []byte("c")})
	e.Execute(c, a, core.LogAppend{Req: 2, Slot: 0, Data: []byte("a")})
	e.Execute(c, a, core.LogAppend{Req: 3, Slot: 1, Data: []byte("b")})
	c.Run()
	for _, request := range []core.RequestID{1, 2, 3} {
		if d := c.Find(request).(core.LogAppendDone); d.Err != core.OK || d.Taken {
			t.Fatal(request, d)
		}
	}
	e.Execute(c, a, core.LogRead{Req: 4, From: 0, Limit: 10})
	c.Run()
	r := c.Find(4).(core.LogReadDone)
	if r.Err != core.OK || len(r.Entries) != 3 {
		t.Fatal(r)
	}
	want := []core.LogPosition{0, 1, 2}
	for i, ent := range r.Entries {
		if ent.Slot != want[i] {
			t.Fatalf("entry %d slot %d, want %d", i, ent.Slot, want[i])
		}
	}
	e.Execute(c, a, core.LogGetEnd{Req: 5})
	c.Run()
	if d := c.Find(5).(core.LogGetEndDone); d.Tail != 3 || d.Err != core.OK {
		t.Fatal(d)
	}
}

func TestTrimDeletesSlots(t *testing.T) {
	e, c, a := setup(base(), 3)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 1, Data: []byte("x")})
	c.Run()
	e.Execute(c, a, core.LogDeleteOld{Req: 2, Through: 1})
	c.Run()
	if d := c.Find(2).(core.LogDeleteOldDone); d.Err != core.OK {
		t.Fatal(d)
	}
	if _, ok := e.Entry(1); ok {
		t.Fatal("deleted entry still readable")
	}
	// The slot is empty, but it is at or below the floor, so a new append there is
	// not a commit.
	e.Execute(c, a, core.LogAppend{Req: 3, Slot: 1, Data: []byte("y")})
	c.Run()
	if d := c.Find(3).(core.LogAppendDone); d.Err != core.ErrConflict || d.Outcome != core.NotApplied || !d.Taken || d.Existing != nil {
		t.Fatal(d)
	}
}

func TestTimeoutMaybeApplied(t *testing.T) {
	f := base()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, c, a := setup(f, seed)
		e.Execute(c, a, core.LogAppend{Req: 1, Slot: 0, Data: []byte("x")})
		c.Run()
		d := only[core.LogAppendDone](t, c)
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
			t.Fatal(d)
		}
		if _, ok := e.Entry(0); ok {
			applied++
		} else {
			notApplied++
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatal(applied, notApplied)
	}
}

func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = simulator.ServiceOutage{Prob: 0.05, Dur: [2]core.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, c, a := setup(f, seed)
		for i := range 40 {
			c.T += simulator.Time(ms)
			slot := core.LogPosition(i % 5)
			switch i % 4 {
			case 0:
				e.Execute(c, a, core.LogAppend{Req: core.RequestID(i), Slot: slot, Data: []byte{byte(i)}})
			case 1:
				e.Execute(c, a, core.LogRead{Req: core.RequestID(i), From: 0, Limit: 8})
			case 2:
				e.Execute(c, a, core.LogGetEnd{Req: core.RequestID(i)})
			default:
				e.Execute(c, a, core.LogDeleteOld{Req: core.RequestID(i), Through: 0})
			}
		}
		mid := e.StateHash()
		c.Run()
		return c.Trace(), e.StateHash(), mid
	}
	t1, h1, m1 := run(7)
	t2, h2, m2 := run(7)
	if t1 != t2 || h1 != h2 || m1 != m2 {
		t.Fatal("same seed diverged")
	}
	t3, _, _ := run(8)
	if t1 == t3 {
		t.Fatal("different seeds gave one trace")
	}
}

// put runs one LogAppend to the end of the queue and returns its answer.
func put(e *fakelogstore.Engine, c *fakeservicetest.Ctx, a core.NodeInstance, request core.RequestID, slot core.LogPosition, data []byte) core.LogAppendDone {
	e.Execute(c, a, core.LogAppend{Req: request, Slot: slot, Data: data})
	c.Run()
	return c.Find(request).(core.LogAppendDone)
}

// trim runs one LogDeleteOld to the end of the queue.
func trim(e *fakelogstore.Engine, c *fakeservicetest.Ctx, a core.NodeInstance, request core.RequestID, through core.LogPosition) {
	e.Execute(c, a, core.LogDeleteOld{Req: request, Through: through})
	c.Run()
}

// claim runs one LogClaimEpoch to the end of the queue and returns its answer.
func claim(e *fakelogstore.Engine, c *fakeservicetest.Ctx, a core.NodeInstance, request core.RequestID, expect uint64, owner string) core.LogClaimEpochDone {
	e.Execute(c, a, core.LogClaimEpoch{Req: request, Expect: expect, Owner: owner})
	c.Run()
	return c.Find(request).(core.LogClaimEpochDone)
}

// envelope builds a log payload with the machine envelope: marker, epoch, writer
// name, then the inner payload. The marker must match replica's envelope
// marker, and TestEnvelopeHelperMatchesStatemachine checks it.
func envelope(epoch uint64, writer string, inner []byte) []byte {
	b := []byte{0xC8}
	b = binary.AppendUvarint(b, epoch)
	b = binary.AppendUvarint(b, uint64(len(writer)))
	b = append(b, writer...)
	return append(b, inner...)
}

func noop() []byte {
	return metadata.Encode(metadata.Entry{ID: "n", Kind: metadata.KindNoop})
}

func TestEnvelopeHelperMatchesStatemachine(t *testing.T) {
	epoch, writer, ok := replica.EntryWriter(envelope(3, "cp-1", noop()))
	if !ok || epoch != 3 || writer != "cp-1" {
		t.Fatal(epoch, writer, ok)
	}
}

func TestClaimWinsThenLoses(t *testing.T) {
	e, c, a := setup(base(), 4)
	d := claim(e, c, a, 1, 0, "cp-0")
	if !d.Won || d.Epoch != 1 || d.Owner != "cp-0" || d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	lost := claim(e, c, a, 2, 0, "cp-1")
	if lost.Won || lost.Epoch != 1 || lost.Owner != "cp-0" || lost.Err != core.OK || lost.Outcome != core.Applied {
		t.Fatal(lost)
	}
	if epoch, owner := e.Epoch(); epoch != 1 || owner != "cp-0" {
		t.Fatal(epoch, owner)
	}
	if won := claim(e, c, a, 3, 1, "cp-1"); !won.Won || won.Epoch != 2 || won.Owner != "cp-1" {
		t.Fatal(won)
	}
}

func TestClaimRetryAfterLandingSeesItsOwnClaim(t *testing.T) {
	e, c, a := setup(base(), 5)
	claim(e, c, a, 1, 0, "cp-0")
	// The caller of claim 1 timed out and asks again with the same Expect.
	again := claim(e, c, a, 2, 0, "cp-0")
	if again.Won || again.Epoch != 1 || again.Owner != "cp-0" {
		t.Fatal(again)
	}
}

func TestClaimUnavailableHasNoEffect(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	e, c, a := setup(f, 6)
	e.Execute(c, a, core.LogClaimEpoch{Req: 1, Expect: 0, Owner: "cp-0"})
	c.Run()
	d := only[core.LogClaimEpochDone](t, c)
	if d.Won || d.Err != core.ErrUnavailable || d.Outcome != core.NotApplied {
		t.Fatal(d)
	}
	if epoch, owner := e.Epoch(); epoch != 0 || owner != "" {
		t.Fatal(epoch, owner)
	}
}

func TestClaimLostResponseStillApplies(t *testing.T) {
	f := base()
	f.LostResponse = 1
	e, c, a := setup(f, 7)
	e.Execute(c, a, core.LogClaimEpoch{Req: 1, Expect: 0, Owner: "cp-0"})
	c.Run()
	d := only[core.LogClaimEpochDone](t, c)
	if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
		t.Fatal(d)
	}
	if !slices.Contains(c.Faults, "log_lost_response") {
		t.Fatal(c.Faults)
	}
	// The caller only sees a timeout, but the claim landed.
	if epoch, owner := e.Epoch(); epoch != 1 || owner != "cp-0" {
		t.Fatal(epoch, owner)
	}
}

func TestClaimChangesStateHash(t *testing.T) {
	e, c, a := setup(base(), 11)
	before := e.StateHash()
	claim(e, c, a, 1, 0, "cp-0")
	if e.StateHash() == before {
		t.Fatal("a won claim must change the state hash")
	}
	after := e.StateHash()
	claim(e, c, a, 2, 0, "cp-1")
	if e.StateHash() != after {
		t.Fatal("a lost claim must not change the state hash")
	}
}

// Trim raises the floor and empties slots 0..Through. A put above the floor works.
func TestTrimRaisesFloorAndEmptiesSlots(t *testing.T) {
	e, c, a := setup(base(), 12)
	put(e, c, a, 1, 0, []byte("a"))
	put(e, c, a, 2, 1, []byte("b"))
	put(e, c, a, 3, 2, []byte("c"))
	trim(e, c, a, 4, 1)
	if _, ok := e.Entry(0); ok {
		t.Fatal("slot 0 still readable")
	}
	if _, ok := e.Entry(1); ok {
		t.Fatal("slot 1 still readable")
	}
	if v, ok := e.Entry(2); !ok || string(v) != "c" {
		t.Fatal(v, ok)
	}
	if d := put(e, c, a, 5, 3, []byte("d")); d.Err != core.OK || d.Outcome != core.Applied || d.Taken {
		t.Fatal(d)
	}
}

// A put into an empty slot at or below the floor is not a commit: ErrConflict,
// Taken, and no Existing bytes. The row may stay, but nobody reads it.
func TestPutAtOrBelowFloorIsNotACommit(t *testing.T) {
	e, c, a := setup(base(), 13)
	put(e, c, a, 1, 0, []byte("a"))
	put(e, c, a, 2, 1, []byte("b"))
	trim(e, c, a, 3, 1)
	for _, slot := range []core.LogPosition{0, 1} {
		d := put(e, c, a, core.RequestID(10+slot), slot, []byte("stray"))
		if d.Err != core.ErrConflict || d.Outcome != core.NotApplied || !d.Taken || d.Existing != nil {
			t.Fatalf("slot %d: %+v", slot, d)
		}
		if _, ok := e.Entry(slot); ok {
			t.Fatalf("slot %d: a stray row must not be readable", slot)
		}
	}
}

// LogRead never returns a row at or below the floor, a stray row included.
// Rows above the floor are returned in slot order.
func TestLogReadSkipsStrayRowsAtOrBelowFloor(t *testing.T) {
	e, c, a := setup(base(), 14)
	put(e, c, a, 1, 0, []byte("a"))
	put(e, c, a, 2, 1, []byte("b"))
	put(e, c, a, 3, 2, []byte("c"))
	trim(e, c, a, 4, 1)
	put(e, c, a, 5, 0, []byte("stray0"))
	put(e, c, a, 6, 1, []byte("stray1"))
	put(e, c, a, 7, 3, []byte("d"))
	for _, from := range []core.LogPosition{0, 1} {
		e.Execute(c, a, core.LogRead{Req: 8, From: from, Limit: 10})
		c.Run()
		r := c.Find(8).(core.LogReadDone)
		if r.Err != core.OK || len(r.Entries) != 2 {
			t.Fatalf("from %d: %+v", from, r)
		}
		if r.Entries[0].Slot != 2 || string(r.Entries[0].Data) != "c" || r.Entries[1].Slot != 3 || string(r.Entries[1].Data) != "d" {
			t.Fatalf("from %d: %+v", from, r.Entries)
		}
		c.Delivered = nil
	}
}

// Under the NoFloorCheck mutant a put at or below the floor is a normal success.
// Read still skips the row, because the mutant only changes the answer to the put.
func TestNoFloorCheckMutantAnswersBelowFloorAsSuccess(t *testing.T) {
	f := base()
	f.Bug = fakelogservice.InjectedBugNoFloorCheck
	e, c, a := setup(f, 15)
	put(e, c, a, 1, 0, []byte("a"))
	put(e, c, a, 2, 1, []byte("b"))
	trim(e, c, a, 3, 1)
	d := put(e, c, a, 4, 1, []byte("stray"))
	if d.Err != core.OK || d.Outcome != core.Applied || d.Taken {
		t.Fatal(d)
	}
	if _, ok := e.Entry(1); ok {
		t.Fatal("a row at or below the floor must not be readable")
	}
	e.Execute(c, a, core.LogRead{Req: 5, From: 0, Limit: 10})
	c.Run()
	if r := c.Find(5).(core.LogReadDone); len(r.Entries) != 0 {
		t.Fatal(r.Entries)
	}
	if d := put(e, c, a, 6, 3, []byte("d")); d.Err != core.OK || d.Outcome != core.Applied || d.Taken {
		t.Fatal(d)
	}
}

func TestOneEpochHasOneWriter(t *testing.T) {
	e, c, a := setup(base(), 16)
	inner := noop()
	put(e, c, a, 1, 0, envelope(1, "cp-0", inner))
	put(e, c, a, 2, 1, envelope(1, "cp-0", inner))
	put(e, c, a, 3, 2, envelope(2, "cp-1", inner))
	if len(c.Violations) != 0 {
		t.Fatal(c.Violations)
	}
}

func TestTwoWritersInOneEpochViolate(t *testing.T) {
	e, c, a := setup(base(), 17)
	inner := noop()
	put(e, c, a, 1, 0, envelope(1, "cp-0", inner))
	put(e, c, a, 2, 1, envelope(1, "cp-1", inner))
	if !slices.Contains(c.Violations, "logcp-sim: one epoch has one writer") {
		t.Fatal(c.Violations)
	}
}

func TestEpochZeroAndPlainRowsAreNotChecked(t *testing.T) {
	e, c, a := setup(base(), 18)
	inner := noop()
	put(e, c, a, 1, 0, envelope(0, "cp-0", inner))
	put(e, c, a, 2, 1, envelope(0, "cp-1", inner))
	put(e, c, a, 3, 2, []byte("foreign"))
	if len(c.Violations) != 0 {
		t.Fatal(c.Violations)
	}
}
