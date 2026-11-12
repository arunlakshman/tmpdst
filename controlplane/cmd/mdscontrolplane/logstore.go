package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"flag"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/controlplanemetrics"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/dynamolog"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/sqllog"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/system"
	"github.com/manifesterio/mds/controlplane/internal/system/s3store"
	"github.com/manifesterio/mds/shared/runinfo"
)

const (
	kindLog      = "dynamolog"
	kindPostgres = "postgres"
)

// postgresDSNEnv is the environment variable that holds the DSN when
// -postgres-dsn is empty. Using it keeps the password off the command line.
const postgresDSNEnv = "MDS_POSTGRES_DSN"

type logFlags struct {
	table                              string
	heartbeat, startTimeout            time.Duration
	checkpointBucket, checkpointPrefix string
	s3Endpoint                         string
	s3PathStyle                        bool
	nodeName                           string
	rank                               int
	takeoverAfter                      time.Duration
	takeoverRankStep                   time.Duration
	checkpointEvery, logTTL            time.Duration
	windowSlots                        int64
	pebbleDir                          string
	checkpointMinSlots                 int64
	postgresDSN, postgresPrefix        string
}

// secretFlag is a string flag whose value is never printed. The run manifest
// records the value of every flag, and a DSN holds a password.
type secretFlag struct{ value *string }

func (s secretFlag) String() string {
	if s.value == nil || *s.value == "" {
		return ""
	}
	return "(set)"
}

func (s secretFlag) Set(v string) error {
	*s.value = v
	return nil
}

func registerLogFlags(fs *flag.FlagSet, f *logFlags) {
	fs.StringVar(&f.table, "table", "", "-store dynamolog: DynamoDB table (default <table-prefix>cp). Not used by -store postgres")
	fs.StringVar(&f.checkpointBucket, "checkpoint-bucket", "", "-store dynamolog or postgres: S3 bucket for checkpoints")
	fs.StringVar(&f.checkpointPrefix, "checkpoint-prefix", "cp/", "-store dynamolog or postgres: key prefix of checkpoint objects")
	fs.StringVar(&f.s3Endpoint, "s3-endpoint", "", "-store dynamolog or postgres: S3 endpoint URL")
	fs.BoolVar(&f.s3PathStyle, "s3-path-style", false, "-store dynamolog or postgres: path-style S3 addressing")
	fs.StringVar(&f.nodeName, "node-name", "", "-store dynamolog or postgres: node name (default hostname)")
	fs.IntVar(&f.rank, "rank", 0, "-store dynamolog or postgres: 0 is the preferred appender")
	fs.DurationVar(&f.takeoverAfter, "takeover-after", 5*time.Second, "-store dynamolog or postgres: standby takeover silence")
	fs.DurationVar(&f.takeoverRankStep, "takeover-rank-step", time.Second, "-store dynamolog or postgres: each rank waits this much longer before it claims the write role")
	fs.DurationVar(&f.startTimeout, "start-timeout", 10*time.Minute, "-store dynamolog or postgres: startup budget")
	fs.DurationVar(&f.heartbeat, "heartbeat", time.Second, "-store dynamolog or postgres: idle heartbeat")
	fs.DurationVar(&f.checkpointEvery, "checkpoint-every", 5*time.Minute, "-store dynamolog or postgres: checkpoint period")
	fs.Int64Var(&f.checkpointMinSlots, "checkpoint-min-slots", 1000, "-store dynamolog or postgres: min slots between checkpoints")
	fs.Int64Var(&f.windowSlots, "window-slots", 131072, "-store dynamolog or postgres: idempotency window in log slots; every node must use the same value")
	fs.DurationVar(&f.logTTL, "log-ttl", 7*24*time.Hour, "-store dynamolog: ttl attribute on log slots. Not used by -store postgres")
	fs.StringVar(&f.pebbleDir, "pebble-dir", "", "-store dynamolog or postgres: directory of the Pebble state. It is rebuilt from the checkpoint and the log at every start, so it is wiped (only if it holds a Pebble database). Empty: a temporary directory")
	fs.Var(secretFlag{&f.postgresDSN}, "postgres-dsn", "-store postgres: connection string, e.g. postgres://user@host:5432/db. Empty: use the environment variable "+postgresDSNEnv+" (this keeps the password off the command line)")
	fs.StringVar(&f.postgresPrefix, "postgres-prefix", "mds_", "-store postgres: table name prefix. The tables are <prefix>log and <prefix>meta")
}

// isLogStore reports whether kind is a store with a log (dynamolog or postgres).
func isLogStore(kind string) bool {
	return kind == kindLog || kind == kindPostgres
}

func (l logFlags) tableName(prefix string) string {
	return cmp.Or(l.table, prefix+"cp")
}

// resolvedDSN is the PostgreSQL DSN to use: the flag, else the environment.
func resolvedDSN(l logFlags) string {
	return cmp.Or(l.postgresDSN, os.Getenv(postgresDSNEnv))
}

func nodeName(l logFlags) string {
	if l.nodeName != "" {
		return l.nodeName
	}
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "mdscontrolplane"
	}
	return h
}

func openDynLog(ctx context.Context, f cpFlags, m *controlplanemetrics.Metrics, log *slog.Logger) (executor.Log, executor.ObjectStore, func(), error) {
	nop := func() {}
	db, err := newDynamo(ctx, f.endpoint, f.region, m.APIOptions())
	if err != nil {
		return nil, nil, nop, err
	}
	table := f.log.tableName(f.prefix)
	if f.create {
		if err := dynamolog.CreateTable(ctx, db, table); err != nil {
			return nil, nil, nop, err
		}
	}
	lg := dynamolog.New(db, table, dynamolog.Options{TTL: f.log.logTTL})
	objs, err := openCheckpointObjects(ctx, f, log)
	if err != nil {
		return nil, nil, nop, err
	}
	log.Info("log store ready", "event", "log_ready", "node", nodeName(f.log), "rank", f.log.rank, "table", table, "checkpoint_bucket", f.log.checkpointBucket)
	return lg, objs, nop, nil
}

// openCheckpointObjects returns the checkpoint store: S3 when -checkpoint-bucket
// is set, else memory. Both log stores use it.
func openCheckpointObjects(ctx context.Context, f cpFlags, log *slog.Logger) (executor.ObjectStore, error) {
	if f.log.checkpointBucket == "" {
		log.Warn("no -checkpoint-bucket: no checkpoints and no log trim; a restart replays the whole log", "event", "log_no_checkpoint")
		return executor.NewMemoryObjects(), nil
	}
	store, err := s3store.New(ctx, s3store.Config{Endpoint: f.log.s3Endpoint, Region: f.region,
		Bucket: f.log.checkpointBucket, Prefix: f.log.checkpointPrefix, PathStyle: f.log.s3PathStyle})
	if err != nil {
		return nil, err
	}
	return sysObjects{store}, nil
}

// openPostgresLog opens the PostgreSQL log. Checkpoints work as they do for
// dynamolog. The DSN is never logged, and it is not in any error this adds.
func openPostgresLog(ctx context.Context, f cpFlags, log *slog.Logger) (executor.Log, executor.ObjectStore, func(), error) {
	nop := func() {}
	db, err := openSQLDB(ctx, resolvedDSN(f.log))
	if err != nil {
		return nil, nil, nop, err
	}
	closeDB := func() { _ = db.Close() }
	lg, objs, err := newSQLStore(ctx, db, f, log)
	if err != nil {
		closeDB()
		return nil, nil, nop, err
	}
	return lg, objs, closeDB, nil
}

// openSQLDB opens the PostgreSQL pool. The error for an empty DSN names the two
// ways to set it, but never shows a value.
func openSQLDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, errors.New("-store postgres needs -postgres-dsn or " + postgresDSNEnv)
	}
	return sqllog.OpenPostgres(ctx, dsn)
}

// newSQLStore builds the SQL log over db and the checkpoint store.
func newSQLStore(ctx context.Context, db *sql.DB, f cpFlags, log *slog.Logger) (executor.Log, executor.ObjectStore, error) {
	lg, err := sqllog.New(db, f.log.postgresPrefix)
	if err != nil {
		return nil, nil, err
	}
	if err := createSQLTables(ctx, lg, f.create); err != nil {
		return nil, nil, err
	}
	objs, err := openCheckpointObjects(ctx, f, log)
	if err != nil {
		return nil, nil, err
	}
	log.Info("log store ready", "event", "log_ready", "node", nodeName(f.log), "rank", f.log.rank,
		"postgres", redactDSN(resolvedDSN(f.log)), "prefix", f.log.postgresPrefix, "checkpoint_bucket", f.log.checkpointBucket)
	return lg, objs, nil
}

// createSQLTables creates the SQL tables when -create-tables is set.
func createSQLTables(ctx context.Context, lg *sqllog.Log, create bool) error {
	if !create {
		return nil
	}
	return lg.CreateSchema(ctx)
}

// redactDSN returns only the host, port and database name of a PostgreSQL URL,
// for logs. The user, the password and the query are left out. A DSN that is
// not a URL gives "", because then we cannot tell where the password is.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host + u.Path
}

// hideDSN takes the DSN out of a run manifest. The manifest copies the MDS_
// environment variables and the command line, and either one can hold the DSN.
func hideDSN(m runinfo.Manifest, dsn string) runinfo.Manifest {
	delete(m.Env, postgresDSNEnv)
	if dsn == "" {
		return m
	}
	m.Args = slices.Clone(m.Args)
	for i, arg := range m.Args {
		if strings.Contains(arg, dsn) {
			m.Args[i] = "<hidden>"
		}
	}
	return m
}

// sysObjects adapts system.ObjectStore to executor.ObjectStore.
type sysObjects struct{ s system.ObjectStore }

func (a sysObjects) Put(ctx context.Context, key string, data []byte) error {
	return a.s.Put(ctx, key, data)
}

// Get turns the S3 "not found" answer into executor.ErrNotFound. The machine
// reads this error as "no checkpoint yet, start fresh". Any other error means
// "try again later", so without this a new bucket would make boot wait forever.
func (a sysObjects) Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error) {
	data, err := a.s.Get(ctx, ref)
	if storeErr, ok := errors.AsType[*s3store.Error](err); ok && storeErr.Code == runtime.ErrNotFound {
		return nil, executor.ErrNotFound
	}
	return data, err
}
func (a sysObjects) Delete(ctx context.Context, key string) error { return a.s.Delete(ctx, key) }
func (a sysObjects) List(ctx context.Context, prefix string) ([]string, error) {
	return a.s.List(ctx, prefix)
}
