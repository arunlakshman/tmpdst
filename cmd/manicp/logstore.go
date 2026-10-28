package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"manifester/internal/controlplane/cpobs"
	"manifester/internal/controlplane/exec"
	"manifester/internal/core"
	"manifester/internal/sys"
	"manifester/internal/sys/s3store"
)

const kindLog = "dynamolog"

type logFlags struct {
	table                    string
	heartbeat, startTimeout  time.Duration
	ckptBucket, ckptPrefix   string
	s3Endpoint               string
	s3PathStyle              bool
	nodeName                 string
	rank                     int
	takeoverAfter, holeAfter time.Duration
	k                        int
	slotRate                 float64
	ckptEvery, logTTL        time.Duration
	finalCkpt                bool
	windowSlots              int64
	pebbleDir                string
	trimDelete               bool
	ckptMinSlots             int64
}

func registerLogFlags(fs *flag.FlagSet, f *logFlags) {
	fs.StringVar(&f.table, "table", "", "-store dynamolog: DynamoDB table (default <table-prefix>cp)")
	fs.StringVar(&f.ckptBucket, "ckpt-bucket", "", "-store dynamolog: S3 bucket for checkpoints")
	fs.StringVar(&f.ckptPrefix, "ckpt-prefix", "cp/", "-store dynamolog: key prefix of checkpoint objects")
	fs.StringVar(&f.s3Endpoint, "s3-endpoint", "", "-store dynamolog: S3 endpoint URL")
	fs.BoolVar(&f.s3PathStyle, "s3-path-style", false, "-store dynamolog: path-style S3 addressing")
	fs.StringVar(&f.nodeName, "node-name", "", "-store dynamolog: node name (default hostname)")
	fs.IntVar(&f.rank, "rank", 0, "-store dynamolog: 0 is the preferred appender")
	fs.DurationVar(&f.takeoverAfter, "takeover-after", 5*time.Second, "-store dynamolog: standby takeover silence")
	fs.DurationVar(&f.startTimeout, "start-timeout", 10*time.Minute, "-store dynamolog: startup budget")
	fs.DurationVar(&f.heartbeat, "heartbeat", time.Second, "-store dynamolog: idle heartbeat")
	fs.IntVar(&f.k, "log-k", 4, "-store dynamolog: commit LogAppends in flight")
	fs.Float64Var(&f.slotRate, "slot-rate", 200, "-store dynamolog: advisory slot rate (unused by machine)")
	fs.DurationVar(&f.holeAfter, "hole-after", 250*time.Millisecond, "-store dynamolog: hole-fill delay")
	fs.DurationVar(&f.ckptEvery, "ckpt-every", 5*time.Minute, "-store dynamolog: checkpoint period")
	fs.Int64Var(&f.ckptMinSlots, "ckpt-min-slots", 1000, "-store dynamolog: min slots between checkpoints")
	fs.BoolVar(&f.finalCkpt, "final-checkpoint", true, "-store dynamolog: checkpoint on graceful shutdown (best effort)")
	fs.Int64Var(&f.windowSlots, "window-slots", 131072, "-store dynamolog: idempotency window hint")
	fs.DurationVar(&f.logTTL, "log-ttl", 7*24*time.Hour, "-store dynamolog: ttl attribute on log slots")
	fs.BoolVar(&f.trimDelete, "trim-delete", true, "-store dynamolog: tombstone trimmed slots")
	fs.StringVar(&f.pebbleDir, "pebble-dir", "", "-store dynamolog: unused (machine fold is in-memory); kept for flag compatibility")
}

type logDB interface {
	exec.DynamoClient
	exec.TableManager
}

type deps struct {
	db      func(ctx context.Context, f cpFlags, m *cpobs.Metrics) (logDB, error)
	objects func(ctx context.Context, f cpFlags) (sys.ObjectStore, error)
}

func realDeps() deps {
	return deps{
		db: func(ctx context.Context, f cpFlags, m *cpobs.Metrics) (logDB, error) {
			return newDynamo(ctx, f.endpoint, f.region, m.APIOptions())
		},
		objects: func(ctx context.Context, f cpFlags) (sys.ObjectStore, error) {
			return s3store.New(ctx, s3store.Config{Endpoint: f.log.s3Endpoint, Region: f.region,
				Bucket: f.log.ckptBucket, Prefix: f.log.ckptPrefix, PathStyle: f.log.s3PathStyle})
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

func openDynLog(ctx context.Context, d deps, f cpFlags, m *cpobs.Metrics, log *slog.Logger) (exec.Log, exec.ObjectStore, func(), error) {
	nop := func() {}
	db, err := d.db(ctx, f, m)
	if err != nil {
		return nil, nil, nop, err
	}
	table := f.log.tableName(f.prefix)
	if f.create {
		if err := exec.CreateDynamoTable(ctx, db, table); err != nil {
			return nil, nil, nop, err
		}
	}
	lg := exec.NewDynamoLog(db, table, exec.DynamoLogOptions{TTL: f.log.logTTL})
	var objs exec.ObjectStore = exec.NewMemoryObjects()
	if f.log.ckptBucket != "" {
		store, err := d.objects(ctx, f)
		if err != nil {
			return nil, nil, nop, err
		}
		objs = sysObjects{store}
	} else {
		log.Warn("no -ckpt-bucket: no checkpoints and no log trim; a restart replays the whole log", "event", "log_no_checkpoint")
	}
	log.Info("log store ready", "event", "log_ready", "node", nodeName(f.log), "rank", f.log.rank, "table", table, "ckpt_bucket", f.log.ckptBucket)
	return lg, objs, nop, nil
}

// sysObjects adapts sys.ObjectStore to exec.ObjectStore.
type sysObjects struct{ s sys.ObjectStore }

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
