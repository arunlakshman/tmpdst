package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	// The pgx driver registers itself as "pgx" for database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/manifesterio/mds/internal/core"
)

// SQLLog is a Log in a SQL database. Today the SQL is for PostgreSQL. A store
// for another database changes only the statements below: the Log contract
// needs put-if-absent on a slot, an ordered consistent range read, and a
// compare-and-swap of one row.
//
// Two tables hold the log, both with a name prefix:
//
//	<prefix>log   one row per slot: slot (primary key), data
//	<prefix>meta  two rows: 'epoch' (num = writer epoch, txt = owner) and
//	              'floor' (num = highest slot a trim may have deleted, -1 = none)
//
// The floor and the slot put are one transaction. An append takes a shared lock
// on the floor row before it writes, and a trim needs an exclusive lock on that
// row to raise the floor. So a put can never land at or below the floor: there
// are no stray rows (the DynamoDB log needs a check after the put for this).
// Reads run at read committed on the primary, so they are never stale.
type SQLLog struct {
	db         *sql.DB
	stmtCreate [3]string
	stmts      sqlStatements
}

// sqlStatements are every statement the log runs, with the table names filled in.
type sqlStatements struct {
	lockFloor    string // SELECT the floor with a shared row lock
	insertSlot   string // put-if-absent; one row affected means stored
	selectSlot   string // the bytes of one slot
	read         string // entries at or after $1, above the floor, limit $2
	tail         string // highest slot + 1, but not below floor + 1
	raiseFloor   string // floor = max(floor, $1)
	deleteBatch  string // delete up to $2 slots at or below $1
	claimEpoch   string // compare-and-swap on the epoch row
	selectEpoch  string // the epoch row
	trimBatchMax int64
}

var sqlName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// NewSQLLog returns a log over db. The tables are <prefix>log and <prefix>meta.
// Call CreateSchema once before the first use. The caller owns db.
func NewSQLLog(db *sql.DB, prefix string) (*SQLLog, error) {
	if prefix != "" && !sqlName.MatchString(prefix) {
		return nil, fmt.Errorf("executor: bad SQL table prefix %q (use a-z, 0-9 and _)", prefix)
	}
	logTable, metaTable := prefix+"log", prefix+"meta"
	l := &SQLLog{db: db}
	l.stmtCreate = [3]string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (slot BIGINT PRIMARY KEY, data BYTEA NOT NULL)`, logTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (name TEXT PRIMARY KEY, num BIGINT NOT NULL, txt TEXT NOT NULL DEFAULT '')`, metaTable),
		fmt.Sprintf(`INSERT INTO %s (name, num) VALUES ('epoch', 0), ('floor', -1) ON CONFLICT (name) DO NOTHING`, metaTable),
	}
	floor := fmt.Sprintf(`(SELECT num FROM %s WHERE name = 'floor')`, metaTable)
	l.stmts = sqlStatements{
		lockFloor:    fmt.Sprintf(`SELECT num FROM %s WHERE name = 'floor' FOR SHARE`, metaTable),
		insertSlot:   fmt.Sprintf(`INSERT INTO %s (slot, data) VALUES ($1, $2) ON CONFLICT (slot) DO NOTHING`, logTable),
		selectSlot:   fmt.Sprintf(`SELECT data FROM %s WHERE slot = $1`, logTable),
		read:         fmt.Sprintf(`SELECT slot, data FROM %s WHERE slot >= $1 AND slot > %s ORDER BY slot LIMIT $2`, logTable, floor),
		tail:         fmt.Sprintf(`SELECT GREATEST(COALESCE(MAX(slot) + 1, 0), %s + 1) FROM %s`, floor, logTable),
		raiseFloor:   fmt.Sprintf(`UPDATE %s SET num = GREATEST(num, $1::bigint) WHERE name = 'floor'`, metaTable),
		deleteBatch:  fmt.Sprintf(`DELETE FROM %s WHERE slot IN (SELECT slot FROM %s WHERE slot <= $1 LIMIT $2)`, logTable, logTable),
		claimEpoch:   fmt.Sprintf(`UPDATE %s SET num = $1::bigint + 1, txt = $2 WHERE name = 'epoch' AND num = $1::bigint RETURNING num, txt`, metaTable),
		selectEpoch:  fmt.Sprintf(`SELECT num, txt FROM %s WHERE name = 'epoch'`, metaTable),
		trimBatchMax: 1000,
	}
	return l, nil
}

// OpenPostgres opens a connection pool for the pgx driver and checks it.
func OpenPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("executor: postgres: %w", err)
	}
	return db, nil
}

// CreateSchema creates the tables and the two meta rows if they are missing.
func (l *SQLLog) CreateSchema(ctx context.Context) error {
	return l.inTx(ctx, func(tx *sql.Tx) error {
		for _, statement := range l.stmtCreate {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

// inTx runs fn in a transaction and commits it, or rolls it back on an error.
func (l *SQLLog) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Append implements Log. See the Log contract for the three outcomes.
func (l *SQLLog) Append(ctx context.Context, slot core.LogPosition, data []byte) ([]byte, bool, error) {
	if slot < 0 {
		return nil, false, ErrInvalidSlot
	}
	var existing []byte
	var taken bool
	err := l.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		existing, taken, err = l.appendIn(ctx, tx, int64(slot), data)
		return err
	})
	return existing, taken, err
}

// appendIn is the body of Append. The shared lock on the floor row stays until
// the transaction ends, so a trim cannot raise the floor past this slot between
// the check and the insert.
func (l *SQLLog) appendIn(ctx context.Context, tx *sql.Tx, slot int64, data []byte) ([]byte, bool, error) {
	var floor int64
	if err := tx.QueryRowContext(ctx, l.stmts.lockFloor).Scan(&floor); err != nil {
		return nil, false, err
	}
	if slot <= floor {
		return nil, true, nil
	}
	return l.insertOrRead(ctx, tx, slot, data)
}

// insertOrRead stores the entry, or returns the bytes that already hold the slot.
func (l *SQLLog) insertOrRead(ctx context.Context, tx *sql.Tx, slot int64, data []byte) ([]byte, bool, error) {
	stored, err := l.insertSlot(ctx, tx, slot, data)
	if err != nil || stored {
		return nil, false, err
	}
	existing, err := l.slotBytes(ctx, tx, slot)
	return existing, true, err
}

func (l *SQLLog) insertSlot(ctx context.Context, tx *sql.Tx, slot int64, data []byte) (bool, error) {
	res, err := tx.ExecContext(ctx, l.stmts.insertSlot, slot, data)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (l *SQLLog) slotBytes(ctx context.Context, tx *sql.Tx, slot int64) ([]byte, error) {
	var existing []byte
	err := tx.QueryRowContext(ctx, l.stmts.selectSlot, slot).Scan(&existing)
	return existing, err
}

// Read implements Log. It is one statement, so the floor and the rows come from
// the same snapshot.
func (l *SQLLog) Read(ctx context.Context, from core.LogPosition, limit int) ([]core.LogEntry, error) {
	if from < 0 || limit < 0 {
		return nil, ErrInvalidSlot
	}
	rows, err := l.db.QueryContext(ctx, l.stmts.read, int64(from), int64(limit))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanEntries(rows)
}

func scanEntries(rows *sql.Rows) ([]core.LogEntry, error) {
	var out []core.LogEntry
	for rows.Next() {
		var slot int64
		var data []byte
		if err := rows.Scan(&slot, &data); err != nil {
			return nil, err
		}
		out = append(out, core.LogEntry{Slot: core.LogPosition(slot), Data: data})
	}
	return out, rows.Err()
}

// CheckTail implements Log. It is never below floor + 1.
func (l *SQLLog) CheckTail(ctx context.Context) (core.LogPosition, error) {
	var tail int64
	if err := l.db.QueryRowContext(ctx, l.stmts.tail).Scan(&tail); err != nil {
		return 0, err
	}
	return core.LogPosition(tail), nil
}

// TrimThrough implements Log. The floor is raised and committed first. Then the
// slots are deleted in batches, so a long trim never holds a long lock.
func (l *SQLLog) TrimThrough(ctx context.Context, through core.LogPosition) error {
	if through < 0 {
		return ErrInvalidSlot
	}
	if _, err := l.db.ExecContext(ctx, l.stmts.raiseFloor, int64(through)); err != nil {
		return err
	}
	return l.deleteThrough(ctx, int64(through))
}

// deleteThrough deletes the slots at or below through, one batch at a time.
func (l *SQLLog) deleteThrough(ctx context.Context, through int64) error {
	for {
		n, err := l.deleteBatch(ctx, through)
		if err != nil || n == 0 {
			return err
		}
	}
}

func (l *SQLLog) deleteBatch(ctx context.Context, through int64) (int64, error) {
	res, err := l.db.ExecContext(ctx, l.stmts.deleteBatch, through, l.stmts.trimBatchMax)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ClaimEpoch implements Log. The update is the compare-and-swap: two claims with
// the same expect cannot both match the row, because the second waits for the
// first and then sees the new epoch.
func (l *SQLLog) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	var epoch int64
	var stored string
	err := l.db.QueryRowContext(ctx, l.stmts.claimEpoch, int64(expect), owner).Scan(&epoch, &stored)
	if err == nil {
		return uint64(epoch), stored, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, err
	}
	err = l.db.QueryRowContext(ctx, l.stmts.selectEpoch).Scan(&epoch, &stored)
	return uint64(epoch), stored, false, err
}
