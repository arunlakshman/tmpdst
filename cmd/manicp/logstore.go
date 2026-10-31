package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"manifester/internal/controlplane/controlplanemetrics"
	"manifester/internal/controlplane/executor"
	"manifester/internal/core"
	"manifester/internal/system"
	"manifester/internal/system/s3store"
)

const kindLog = "dynamolog"

type logFlags struct {
	table                              string
	heartbeat, startTimeout            time.Duration
	checkpointBucket, checkpointPrefix string
	s3Endpoint                         string
	s3PathStyle                        bool
	nodeName                           string
	rank                               int
	takeoverAfter, gapAfter            time.Duration
	k                                  int
	checkpointEvery, logTTL            time.Duration
	windowSlots                        int64
	pebbleDir                          string
	checkpointMinSlots                 int64
}

func registerLogFlags(fs *flag.FlagSet, f *logFlags) {
	fs.StringVar(&f.table, "table", "", "-store dynamolog: DynamoDB table (default <table-prefix>cp)")
	fs.StringVar(&f.checkpointBucket, "checkpoint-bucket", "", "-store dynamolog: S3 bucket for checkpoints")
	fs.StringVar(&f.checkpointPrefix, "checkpoint-prefix", "cp/", "-store dynamolog: key prefix of checkpoint objects")
	fs.StringVar(&f.s3Endpoint, "s3-endpoint", "", "-store dynamolog: S3 endpoint URL")
	fs.BoolVar(&f.s3PathStyle, "s3-path-style", false, "-store dynamolog: path-style S3 addressing")
	fs.StringVar(&f.nodeName, "node-name", "", "-store dynamolog: node name (default hostname)")
	fs.IntVar(&f.rank, "rank", 0, "-store dynamolog: 0 is the preferred appender")
	fs.DurationVar(&f.takeoverAfter, "takeover-after", 5*time.Second, "-store dynamolog: standby takeover silence")
	fs.DurationVar(&f.startTimeout, "start-timeout", 10*time.Minute, "-store dynamolog: startup budget")
	fs.DurationVar(&f.heartbeat, "heartbeat", time.Second, "-store dynamolog: idle heartbeat")
	fs.IntVar(&f.k, "log-k", 4, "-store dynamolog: commit LogAppends in flight")
	fs.DurationVar(&f.gapAfter, "gap-after", 250*time.Millisecond, "-store dynamolog: gap-fill delay")
	fs.DurationVar(&f.checkpointEvery, "checkpoint-every", 5*time.Minute, "-store dynamolog: checkpoint period")
	fs.Int64Var(&f.checkpointMinSlots, "checkpoint-min-slots", 1000, "-store dynamolog: min slots between checkpoints")
	fs.Int64Var(&f.windowSlots, "window-slots", 131072, "-store dynamolog: idempotency window in log slots; every node must use the same value")
	fs.DurationVar(&f.logTTL, "log-ttl", 7*24*time.Hour, "-store dynamolog: ttl attribute on log slots")
	fs.StringVar(&f.pebbleDir, "pebble-dir", "", "-store dynamolog: directory of the Pebble state. It is rebuilt from the checkpoint and the log at every start, so it is wiped (only if it holds a Pebble database). Empty: a temporary directory")
}

type logDB interface {
	executor.DynamoClient
	executor.TableManager
}

type deps struct {
	db      func(ctx context.Context, f cpFlags, m *controlplanemetrics.Metrics) (logDB, error)
	objects func(ctx context.Context, f cpFlags) (system.ObjectStore, error)
}

func realDeps() deps {
	return deps{
		db: func(ctx context.Context, f cpFlags, m *controlplanemetrics.Metrics) (logDB, error) {
			return newDynamo(ctx, f.endpoint, f.region, m.APIOptions())
		},
		objects: func(ctx context.Context, f cpFlags) (system.ObjectStore, error) {
			return s3store.New(ctx, s3store.Config{Endpoint: f.log.s3Endpoint, Region: f.region,
				Bucket: f.log.checkpointBucket, Prefix: f.log.checkpointPrefix, PathStyle: f.log.s3PathStyle})
		},
	}
}

func (l logFlags) tableName(prefix string) string {
	if l.table != "" {
		return l.table
	}
	return prefix + "cp"
}

func nodeName(l logFlags) string {
	if l.nodeName != "" {
		return l.nodeName
	}
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "manicp"
	}
	return h
}

func openDynLog(ctx context.Context, d deps, f cpFlags, m *controlplanemetrics.Metrics, log *slog.Logger) (executor.Log, executor.ObjectStore, func(), error) {
	nop := func() {}
	db, err := d.db(ctx, f, m)
	if err != nil {
		return nil, nil, nop, err
	}
	table := f.log.tableName(f.prefix)
	if f.create {
		if err := executor.CreateDynamoTable(ctx, db, table); err != nil {
			return nil, nil, nop, err
		}
	}
	lg := executor.NewDynamoLog(db, table, executor.DynamoLogOptions{TTL: f.log.logTTL})
	var objs executor.ObjectStore = executor.NewMemoryObjects()
	if f.log.checkpointBucket != "" {
		store, err := d.objects(ctx, f)
		if err != nil {
			return nil, nil, nop, err
		}
		objs = sysObjects{store}
	} else {
		log.Warn("no -checkpoint-bucket: no checkpoints and no log trim; a restart replays the whole log", "event", "log_no_checkpoint")
	}
	log.Info("log store ready", "event", "log_ready", "node", nodeName(f.log), "rank", f.log.rank, "table", table, "checkpoint_bucket", f.log.checkpointBucket)
	return lg, objs, nop, nil
}

// sysObjects adapts system.ObjectStore to executor.ObjectStore.
type sysObjects struct{ s system.ObjectStore }

func (a sysObjects) Put(ctx context.Context, key string, data []byte) error {
	return a.s.Put(ctx, key, data)
}
func (a sysObjects) Get(ctx context.Context, ref core.SegmentRef) ([]byte, error) {
	return a.s.Get(ctx, ref)
}
func (a sysObjects) Delete(ctx context.Context, key string) error { return a.s.Delete(ctx, key) }
func (a sysObjects) List(ctx context.Context, prefix string) ([]string, error) {
	return a.s.List(ctx, prefix)
}
