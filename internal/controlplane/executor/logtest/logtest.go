// Package logtest is the conformance suite of executor.Log. Each implementation
// runs it from its own package.
package logtest

import (
	"fmt"
	"sync"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/core"
)

// Open opens a fresh, empty Log for one test.
type Open func(t *testing.T) executor.Log

// Run runs the semantic tests of executor.Log on the implementation that open
// makes: the claim and the epoch, the floor, trim and the stray rows. Every Log
// (memory, DynamoDB, Postgres) must pass all of them.
func Run(t *testing.T, open Open) {
	t.Helper()
	cases := []struct {
		name string
		run  func(*testing.T, Open)
	}{
		{"ClaimEpoch", claimEpoch},
		{"ClaimEpochMissingItemLoses", claimEpochMissingItemLoses},
		{"ClaimEpochRace", claimEpochRace},
		{"AppendAfterTrim", appendAfterTrim},
		{"FloorNeverMovesDown", floorNeverMovesDown},
		{"ReadNeverReturnsStrayRowsAtFloor", readNeverReturnsStrayRowsAtFloor},
		{"AppendRetryBelowFloorIsNotStray", appendRetryBelowFloorIsNotStray},
		{"AppendLiveSlotAboveFloorReturnsExisting", appendLiveSlotAboveFloorReturnsExisting},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, open) })
	}
}

func claimEpoch(t *testing.T, open Open) {
	type step struct {
		expect uint64
		owner  string
		epoch  uint64
		stored string
		won    bool
	}
	steps := []step{
		{expect: 0, owner: "cp-0", epoch: 1, stored: "cp-0", won: true},  // first claim
		{expect: 1, owner: "cp-1", epoch: 2, stored: "cp-1", won: true},  // correct expect
		{expect: 1, owner: "cp-2", epoch: 2, stored: "cp-1", won: false}, // stale expect loses
		{expect: 0, owner: "cp-3", epoch: 2, stored: "cp-1", won: false}, // "first start" loses now
		{expect: 9, owner: "cp-4", epoch: 2, stored: "cp-1", won: false}, // expect ahead loses
		{expect: 2, owner: "cp-5", epoch: 3, stored: "cp-5", won: true},  // winner keeps going
	}
	l := open(t)
	for i, s := range steps {
		epoch, owner, won, err := l.ClaimEpoch(t.Context(), s.expect, s.owner)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if epoch != s.epoch || owner != s.stored || won != s.won {
			t.Fatalf("step %d: got (%d, %q, %v), want (%d, %q, %v)",
				i, epoch, owner, won, s.epoch, s.stored, s.won)
		}
	}
}

func claimEpochMissingItemLoses(t *testing.T, open Open) {
	// No epoch is stored yet. A claim that expects 3 must lose and see epoch 0.
	epoch, owner, won, err := open(t).ClaimEpoch(t.Context(), 3, "cp-0")
	if err != nil || won || epoch != 0 || owner != "" {
		t.Fatalf("got (%d, %q, %v, %v), want (0, \"\", false, nil)", epoch, owner, won, err)
	}
}

func claimEpochRace(t *testing.T, open Open) {
	const writers = 20
	l := open(t)
	type result struct {
		epoch uint64
		owner string
		won   bool
		err   error
	}
	results := make([]result, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		wg.Go(func() {
			<-start
			epoch, owner, won, err := l.ClaimEpoch(t.Context(), 0, fmt.Sprintf("cp-%d", i))
			results[i] = result{epoch, owner, won, err}
		})
	}
	close(start)
	wg.Wait()

	var winner string
	winners := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("writer %d: %v", i, r.err)
		}
		if r.won {
			winners++
			winner = fmt.Sprintf("cp-%d", i)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	for i, r := range results {
		if r.won {
			continue
		}
		if r.epoch != 1 || r.owner != winner {
			t.Fatalf("loser %d saw (%d, %q), want (1, %q)", i, r.epoch, r.owner, winner)
		}
	}
}

func appendAfterTrim(t *testing.T, open Open) {
	ctx := t.Context()
	l := open(t)
	for slot, data := range []string{"a", "b", "c"} {
		if _, taken, err := l.Append(ctx, core.LogPosition(slot), []byte(data)); err != nil || taken {
			t.Fatalf("append %d: taken=%v err=%v", slot, taken, err)
		}
	}
	if err := l.TrimThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	appendBelowFloorIsTaken(t, l)
	appendAboveFloor(t, l)
	// Read never returns the stray rows at 0 and 1.
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 2 || ents[0].Slot != 2 || string(ents[0].Data) != "c" ||
		ents[1].Slot != 3 || string(ents[1].Data) != "d" {
		t.Fatalf("read after trim: %+v %v, want slots 2 and 3", ents, err)
	}
}

// appendBelowFloorIsTaken: slots 0 and 1 are at or below the floor, so the puts
// are not commits. The puts land (they were free), so these are stray rows.
func appendBelowFloorIsTaken(t *testing.T, l executor.Log) {
	t.Helper()
	for _, slot := range []core.LogPosition{0, 1} {
		existing, taken, err := l.Append(t.Context(), slot, []byte("stale"))
		if err != nil || !taken || existing != nil {
			t.Fatalf("below floor slot %d: existing=%q taken=%v err=%v, want nil, true, nil",
				slot, existing, taken, err)
		}
	}
}

// appendAboveFloor: slot 2 holds a live entry and slot 3 is free.
func appendAboveFloor(t *testing.T, l executor.Log) {
	t.Helper()
	ctx := t.Context()
	existing, taken, err := l.Append(ctx, 2, []byte("z"))
	if err != nil || !taken || string(existing) != "c" {
		t.Fatalf("live slot 2: existing=%q taken=%v err=%v", existing, taken, err)
	}
	if existing, taken, err := l.Append(ctx, 3, []byte("d")); err != nil || taken || existing != nil {
		t.Fatalf("free slot 3: existing=%q taken=%v err=%v", existing, taken, err)
	}
}

func floorNeverMovesDown(t *testing.T, open Open) {
	ctx := t.Context()
	l := open(t)
	if err := l.TrimThrough(ctx, 5); err != nil {
		t.Fatal(err)
	}
	// A lower trim must succeed and must not lower the floor.
	if err := l.TrimThrough(ctx, 3); err != nil {
		t.Fatalf("lower trim: %v", err)
	}
	if _, taken, err := l.Append(ctx, 4, []byte("x")); err != nil || !taken {
		t.Fatalf("slot 4 under floor 5: taken=%v err=%v, want taken", taken, err)
	}
	if _, taken, err := l.Append(ctx, 6, []byte("y")); err != nil || taken {
		t.Fatalf("slot 6 above floor 5: taken=%v err=%v, want free", taken, err)
	}
}

func readNeverReturnsStrayRowsAtFloor(t *testing.T, open Open) {
	// The floor is raised to 5 and every row is trimmed. Then stray rows
	// land at 2 and 5. Rows above the floor are returned, and no others.
	ctx := t.Context()
	l := open(t)
	if err := l.TrimThrough(ctx, 5); err != nil {
		t.Fatal(err)
	}
	for _, slot := range []core.LogPosition{2, 5} {
		if _, taken, err := l.Append(ctx, slot, []byte("stray")); err != nil || !taken {
			t.Fatalf("stray at %d: taken=%v err=%v", slot, taken, err)
		}
	}
	if _, taken, err := l.Append(ctx, 6, []byte("live")); err != nil || taken {
		t.Fatalf("live at 6: taken=%v err=%v", taken, err)
	}
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 1 || ents[0].Slot != 6 || string(ents[0].Data) != "live" {
		t.Fatalf("read: %+v %v, want only slot 6", ents, err)
	}
}

// A put that is refused because the slot is taken, and the slot is at or below
// the floor, is not a commit. The stray bytes must not come back as the
// existing entry. Otherwise a writer that retries the same slot would take its
// own stray row for "my entry landed".
func appendRetryBelowFloorIsNotStray(t *testing.T, open Open) {
	ctx := t.Context()
	l := open(t)
	for slot := range 2 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.TrimThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	// The first put lands in a trimmed slot below the floor: a stray row.
	if _, taken, err := l.Append(ctx, 0, []byte("mine")); err != nil || !taken {
		t.Fatalf("stray put: taken=%v err=%v", taken, err)
	}
	// The answer was lost, so the writer retries the same slot. The row
	// now holds "mine", but the slot is below the floor.
	existing, taken, err := l.Append(ctx, 0, []byte("mine"))
	if err != nil || !taken || existing != nil {
		t.Fatalf("retry below floor: existing=%q taken=%v err=%v, want nil, true, nil", existing, taken, err)
	}
}

// An occupied slot above the floor still returns the existing bytes.
func appendLiveSlotAboveFloorReturnsExisting(t *testing.T, open Open) {
	ctx := t.Context()
	l := open(t)
	if _, _, err := l.Append(ctx, 5, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	existing, taken, err := l.Append(ctx, 5, []byte("other"))
	if err != nil || !taken || string(existing) != "live" {
		t.Fatalf("live slot 5: existing=%q taken=%v err=%v, want live", existing, taken, err)
	}
}
