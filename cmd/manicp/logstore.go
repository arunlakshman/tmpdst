package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"manifester/cp/applier/pebblekv"
	"manifester/cp/cpobs"
	"manifester/cp/dynamo"
	"manifester/cp/logcp"
	"manifester/sys"
	"manifester/sys/s3store"
)

// kindLog is the -store kind of the log-backed control plane (decisions
// 48-53, docs/log-cp-service.md): a DynamoDB log, Pebble state, S3 checkpoints.
const kindLog = "dynamolog"

// logFlags configure -store dynamolog. Defaults follow decisions 48-53.
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
	index                    bool
	indexEvery               time.Duration
	indexRate                float64
	indexParallel            int
	pebbleDir                string
	trimDelete               bool
	ckptMinSlots             int64
}

func registerLogFlags(fs *flag.FlagSet, f *logFlags) {
	fs.StringVar(&f.table, "table", "", "-store dynamolog: DynamoDB table (default <table-prefix>cp)")
	fs.StringVar(&f.ckptBucket, "ckpt-bucket", "", "-store dynamolog: S3 bucket for checkpoints (empty: no checkpoints and no log trim; a restart replays the whole log)")
	fs.StringVar(&f.ckptPrefix, "ckpt-prefix", "cp/", "-store dynamolog: key prefix of checkpoint objects in the bucket")
	fs.StringVar(&f.s3Endpoint, "s3-endpoint", "", "-store dynamolog: S3 endpoint URL (empty: AWS)")
	fs.BoolVar(&f.s3PathStyle, "s3-path-style", false, "-store dynamolog: path-style S3 addressing (SeaweedFS, MinIO)")
	fs.StringVar(&f.nodeName, "node-name", "", "-store dynamolog: node name, unique per node, stamped on every slot (default the hostname)")
	fs.IntVar(&f.rank, "rank", 0, "-store dynamolog: 0 is the preferred appender (starts active); higher ranks start as hot standbys")
	fs.DurationVar(&f.takeoverAfter, "takeover-after", 5*time.Second, "-store dynamolog: a standby that saw no slot of another writer for this long becomes active (0: never on its own). Must stay well under the brokers' -commit-deadline (120 s)")
	fs.DurationVar(&f.startTimeout, "start-timeout", 10*time.Minute, "-store dynamolog: give up if restoring the checkpoint and replaying the log takes longer (the port opens only after that, so brokers use the other CP URLs meanwhile)")
	fs.DurationVar(&f.heartbeat, "heartbeat", time.Second, "-store dynamolog: an idle active node appends a no-op slot this often; -takeover-after must be several times larger")
	fs.IntVar(&f.k, "log-k", 4, "-store dynamolog: slot appends in flight on the active node")
	fs.Float64Var(&f.slotRate, "slot-rate", 200, "-store dynamolog: slot appends per second the active node paces itself to (one hot DynamoDB key)")
	fs.DurationVar(&f.holeAfter, "hole-after", 250*time.Millisecond, "-store dynamolog: an empty slot below later ones is filled with a no-op after this long")
	fs.DurationVar(&f.ckptEvery, "ckpt-every", 5*time.Minute, "-store dynamolog: checkpoint period on the active node (negative: never)")
	fs.Int64Var(&f.ckptMinSlots, "ckpt-min-slots", 1000, "-store dynamolog: skip a periodic checkpoint when fewer slots were applied since the last")
	fs.BoolVar(&f.finalCkpt, "final-checkpoint", true, "-store dynamolog: an active node writes a checkpoint on graceful shutdown (faster next start)")
	fs.Int64Var(&f.windowSlots, "window-slots", 131072, "-store dynamolog: idempotency window in slots. A CLUSTER constant: every node of one log must use the same value")
	fs.BoolVar(&f.index, "index", true, "-store dynamolog: write the R# index rows after the ack (decision 50); needed to roll back to -store dynamo")
	fs.DurationVar(&f.indexEvery, "index-every", 10*time.Second, "-store dynamolog: index flush period")
	fs.Float64Var(&f.indexRate, "index-rate", 0, "-store dynamolog: index items per second (0: unpaced)")
	fs.IntVar(&f.indexParallel, "index-parallel", 8, "-store dynamolog: BatchWriteItem calls in flight")
	fs.DurationVar(&f.logTTL, "log-ttl", 7*24*time.Hour, "-store dynamolog: ttl attribute stamped on log slots (the table's TTL setting expires them); must far exceed -ckpt-every")
	fs.BoolVar(&f.trimDelete, "trim-delete", true, "-store dynamolog: overwrite trimmed slots with tombstones (false: leave them to the TTL)")
	fs.StringVar(&f.pebbleDir, "pebble-dir", "", "-store dynamolog: directory of the Pebble state. It is rebuilt from the checkpoint and the log at every start, so it is wiped (only if it holds a Pebble database). Empty: a temporary directory")
}

// logDB is what the log store needs of DynamoDB: the log's calls and table creation.
type logDB interface {
	logcp.DB
	dynamo.DB
}

// deps are the outside parts run builds the log store from; tests replace them.
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

// logConfig maps the flags onto logcp.Config (the parts that are not wiring).
func logConfig(f cpFlags, name string, log *slog.Logger) logcp.Config {
	l := f.log
	return logcp.Config{
		Name: name, Rank: l.rank, Logger: log,
		K: l.k, MaxSlotRate: l.slotRate, TakeoverAfter: l.takeoverAfter, Heartbeat: l.heartbeat, HoleAfter: l.holeAfter,
		WindowSlots: l.windowSlots, CkptEvery: l.ckptEvery, CkptMinSlots: l.ckptMinSlots, TrimDelete: l.trimDelete,
		NoIndex: !l.index, IndexEvery: l.indexEvery, IndexRate: l.indexRate, IndexParallel: l.indexParallel,
	}
}

// openLog builds, restores and starts the log-backed service and returns its
// handler. The returned func stops it (final checkpoint, loops, Pebble); call
// it once the HTTP server has drained.
func openLog(ctx context.Context, d deps, f cpFlags, m *cpobs.Metrics, log *slog.Logger) (http.Handler, func(), error) {
	nop := func() {}
	db, err := d.db(ctx, f, m)
	if err != nil {
		return nil, nop, err
	}
	table := f.log.tableName(f.prefix)
	if f.create {
		if err := dynamo.New(db, f.prefix).CreateTables(ctx); err != nil {
			return nil, nop, err
		}
	}
	cfg := logConfig(f, nodeName(f.log), log)
	cfg.DB, cfg.Table, cfg.Metrics = db, table, logcp.NewMetrics(m.Registry())
	cfg.Log = logcp.NewDynLog(db, table, logcp.DynLogOptions{TTL: f.log.logTTL})
	if f.log.ckptBucket != "" {
		if cfg.Objects, err = d.objects(ctx, f); err != nil {
			return nil, nop, err
		}
	} else {
		log.Warn("no -ckpt-bucket: no checkpoints and no log trim; a restart replays the whole log", "event", "log_no_checkpoint")
	}
	kv, dir, err := openState(f.log.pebbleDir)
	if err != nil {
		return nil, nop, err
	}
	cfg.KV = kv
	closeAll := func() { kv.Close(); removeTemp(f.log.pebbleDir, dir) }
	svc, err := logcp.New(cfg)
	if err != nil {
		closeAll()
		return nil, nop, err
	}
	sctx, cancel := context.WithTimeout(ctx, f.log.startTimeout)
	err = svc.Start(sctx)
	cancel()
	if err != nil {
		svc.Close()
		closeAll()
		return nil, nop, fmt.Errorf("log store start: %w", err)
	}
	log.Info("log store ready", "event", "log_ready", "node", cfg.Name, "rank", cfg.Rank, "applied_slot", svc.Applied(),
		"active", svc.IsActive(), "table", table, "ckpt_bucket", f.log.ckptBucket, "pebble_dir", dir)
	return svc.Handler(), func() { stopLog(svc, f.log, log); closeAll() }, nil
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

// stopLog writes the optional final checkpoint, then stops the loops.
func stopLog(svc *logcp.Service, l logFlags, log *slog.Logger) {
	if l.finalCkpt && l.ckptBucket != "" && svc.IsActive() && svc.Blocked() == nil {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		slot, err := svc.Checkpoint(cctx)
		cancel()
		if err != nil {
			log.Warn("final checkpoint failed", "event", "log_final_ckpt_failed", "err", err.Error())
		} else {
			log.Info("final checkpoint", "event", "log_final_ckpt", "slot", slot)
		}
	}
	svc.Close()
}

// openState opens the Pebble state in dir (a temporary directory when empty),
// first removing any previous Pebble database there.
func openState(dir string) (*pebblekv.DB, string, error) {
	if dir == "" {
		d, err := os.MkdirTemp("", "manicp-pebble-")
		if err != nil {
			return nil, "", err
		}
		dir = d
	} else if err := wipeState(dir); err != nil {
		return nil, "", err
	}
	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		return nil, "", err
	}
	return kv, dir, nil
}

// wipeState empties dir if it holds a Pebble database (a CURRENT file) or
// nothing; any other content is refused, so a wrong flag cannot delete data.
func wipeState(dir string) error {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) || (err == nil && len(ents) == 0) {
		return os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err != nil {
		return fmt.Errorf("-pebble-dir %s is not empty and holds no Pebble database; refusing to wipe it", dir)
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

func removeTemp(flagDir, used string) {
	if flagDir == "" {
		os.RemoveAll(used)
	}
}
