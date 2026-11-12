//go:build integration

package sqllog_test

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/logtest"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/sqllog"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

func TestMain(m *testing.M) { testinfra.Main(m) }

// openPostgres returns a log in a fresh schema of a real PostgreSQL (started by
// testinfra), and the connection for tests that look at the tables directly.
// The schema is dropped at the end of the test. The table prefix is empty: the
// schema keeps the tests apart.
func openPostgres(t *testing.T) (*sqllog.Log, *sql.DB, string) {
	t.Helper()
	pg := testinfra.Postgres(t)
	db, err := sqllog.OpenPostgres(t.Context(), pg.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	const prefix = ""
	l, err := sqllog.New(db, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CreateSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	return l, db, prefix
}

func openPostgresLog(t *testing.T) executor.Log {
	l, _, _ := openPostgres(t)
	return l
}

func TestSQLLogCreateSchemaCanRunTwice(t *testing.T) {
	l, _, _ := openPostgres(t)
	if _, _, err := l.Append(t.Context(), 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := l.CreateSchema(t.Context()); err != nil {
		t.Fatalf("second CreateSchema: %v", err)
	}
	// The data and the meta rows survive the second run.
	entries, err := l.Read(t.Context(), 0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("after the second CreateSchema: %v, %v", entries, err)
	}
	if epoch, _, won, err := l.ClaimEpoch(t.Context(), 0, "x"); err != nil || !won || epoch != 1 {
		t.Fatalf("claim after the second CreateSchema: %d %v %v", epoch, won, err)
	}
}

// Many writers put different bytes at one slot: exactly one stores, and the
// others see the winner's bytes.
func TestSQLLogOneWinnerPerSlot(t *testing.T) {
	const writers = 20
	l, _, _ := openPostgres(t)
	type result struct {
		existing []byte
		taken    bool
		err      error
	}
	results := make([]result, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		wg.Go(func() {
			<-start
			existing, taken, err := l.Append(t.Context(), 7, fmt.Appendf(nil, "w%d", i))
			results[i] = result{existing, taken, err}
		})
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("writer %d: %v", i, r.err)
		}
		if !r.taken {
			if winner >= 0 {
				t.Fatalf("writers %d and %d both stored slot 7", winner, i)
			}
			winner = i
		}
	}
	if winner < 0 {
		t.Fatal("no writer stored slot 7")
	}
	want := fmt.Sprintf("w%d", winner)
	for i, r := range results {
		if r.taken && string(r.existing) != want {
			t.Fatalf("writer %d saw %q, want the winner's %q", i, r.existing, want)
		}
	}
}

// A trim raises the floor under the same lock that an append reads, so a put
// can never land at or below the floor: no row stays there once a trim is done.
func TestSQLLogTrimAndAppendLeaveNoStrayRows(t *testing.T) {
	l, db, prefix := openPostgres(t)
	const slots = 300
	var wg sync.WaitGroup
	wg.Go(func() {
		for through := runtime.LogPosition(10); through < slots; through += 10 {
			if err := l.TrimThrough(t.Context(), through); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for w := range 4 {
		wg.Go(func() {
			for s := runtime.LogPosition(w); s < slots; s += 4 {
				if _, _, err := l.Append(t.Context(), s, []byte("x")); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := l.TrimThrough(t.Context(), slots); err != nil { // the last trim runs after every writer is done
		t.Fatal(err)
	}
	var stray int
	query := fmt.Sprintf("SELECT count(*) FROM %slog WHERE slot <= (SELECT num FROM %smeta WHERE name = 'floor')", prefix, prefix) //nolint:gosec // the prefix is made by this test
	if err := db.QueryRowContext(t.Context(), query).Scan(&stray); err != nil {
		t.Fatal(err)
	}
	if stray != 0 {
		t.Fatalf("%d rows at or below the floor", stray)
	}
}

// A row that sits at or below the floor (put there by hand, as an old version or
// a failed trim could leave it) is never read, and a put to that slot is refused
// without showing its bytes.
func TestSQLLogRowsBelowTheFloorAreHidden(t *testing.T) {
	l, db, prefix := openPostgres(t)
	if _, _, err := l.Append(t.Context(), 5, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimThrough(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %slog (slot, data) VALUES (2, 'stray')", prefix)); err != nil {
		t.Fatal(err)
	}
	entries, err := l.Read(t.Context(), 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Slot != 5 {
		t.Fatalf("Read = %v, %v; want only slot 5", entries, err)
	}
	existing, taken, err := l.Append(t.Context(), 2, []byte("new"))
	if err != nil || !taken || len(existing) != 0 {
		t.Fatalf("Append at the floor = (%q, %v, %v); want (empty, true, nil)", existing, taken, err)
	}
}

func TestSQLLogTailIsNeverBelowTheFloor(t *testing.T) {
	l, _, _ := openPostgres(t)
	if tail, err := l.CheckTail(t.Context()); err != nil || tail != 0 {
		t.Fatalf("empty log: tail %d, %v; want 0", tail, err)
	}
	if err := l.TrimThrough(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if tail, err := l.CheckTail(t.Context()); err != nil || tail != 11 {
		t.Fatalf("after a trim through 10 with no rows: tail %d, %v; want 11", tail, err)
	}
}

func TestConformance(t *testing.T) { logtest.Run(t, openPostgresLog) }
