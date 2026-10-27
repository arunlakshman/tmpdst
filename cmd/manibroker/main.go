// Command manibroker runs one Kafka broker over S3 and the control plane.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"manifester/broker"
	"manifester/controlplane/cpclient"
	"manifester/core"
	"manifester/sys"
	"manifester/sys/awsobs"
	"manifester/sys/prod"
	"manifester/sys/runinfo"
	"manifester/sys/s3store"
	"manifester/veritas/vsys"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr, sys.ShutdownOnSignal(syscall.SIGTERM, os.Interrupt)))
}

// brokerFlags are the parsed command-line flags.
type brokerFlags struct {
	listen, advertise string
	brokerID          int
	nodeID, cpURL     string
	objects           string
	endpoint, bucket  string
	prefix, region    string
	pathStyle         bool
	createBucket      bool
	tn                tuning
	objTimeout, grace time.Duration
	commitDeadline    time.Duration
	metricsAddr       string
	debug             bool
	logLevel, runID   string
	manifestOut       string
}

func parseFlags(args []string, stderr io.Writer) (brokerFlags, *flag.FlagSet, error) {
	var f brokerFlags
	fs := flag.NewFlagSet("manibroker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.listen, "listen", "127.0.0.1:9092", "Kafka listen address")
	fs.StringVar(&f.advertise, "advertise", "", "host:port clients are told in Metadata (default: the listen address)")
	fs.IntVar(&f.brokerID, "broker-id", 1, "broker id advertised in Metadata")
	fs.StringVar(&f.nodeID, "node-id", "", "control-plane node name (default: broker-<id>)")
	fs.StringVar(&f.cpURL, "cp-url", "http://127.0.0.1:8080", "control plane base URL, or a comma-separated list for the log-backed control plane (e.g. http://cp0:8080,http://cp1:8080): calls go to the URL that last worked (start with the preferred appender first) and move to the next on a connect error or HTTP 503, each URL once per call; a failed-over commit is safe, it carries its segment key")
	fs.StringVar(&f.objects, "objects", "s3", "object store: s3, or mem (in-process, single broker, lost on exit)")
	fs.StringVar(&f.endpoint, "s3-endpoint", "", "S3 endpoint URL (empty: AWS)")
	fs.StringVar(&f.bucket, "s3-bucket", "manifester", "S3 bucket")
	fs.StringVar(&f.prefix, "s3-prefix", "", "key prefix inside the bucket")
	fs.StringVar(&f.region, "s3-region", "us-east-1", "S3 region")
	fs.BoolVar(&f.pathStyle, "s3-path-style", false, "path-style addressing (SeaweedFS, MinIO)")
	fs.BoolVar(&f.createBucket, "s3-create-bucket", false, "create the bucket at startup")
	dflt := broker.DefaultConfig()
	tn := &f.tn
	fs.StringVar(&tn.segmentBytes, "segment-bytes", "16MiB", "seal the open segment at this many batch bytes (16MiB, 16MB, plain bytes)")
	fs.DurationVar(&tn.window, "segment-window", time.Duration(dflt.BatchWindow), "seal the open segment this long after its first batch")
	fs.IntVar(&tn.parts, "max-segment-partitions", dflt.MaxSegmentPartitions, "seal at this many distinct partitions (the log control plane takes 4096; the DynamoDB transaction store needs at most 96)")
	fs.IntVar(&tn.batches, "max-segment-batches", dflt.MaxSegmentBatches, "seal at this many batches (bounds the control plane's segment record, about 300 bytes a batch)")
	fs.StringVar(&tn.maxBuffered, "max-buffered", "auto", "pause client reads above this much unacked produce data; auto = clamp(memory/16, 128MiB, 1GiB)")
	fs.StringVar(&tn.resumeBuffered, "resume-buffered", "", "resume reads below this (default: half of -max-buffered)")
	fs.DurationVar(&f.objTimeout, "obj-timeout", 30*time.Second, "base timeout of an object-store call; large bodies get extra time at 2 MiB/s")
	fs.DurationVar(&f.commitDeadline, "commit-deadline", time.Duration(dflt.CommitDeadline), "keep retrying a segment's control-plane commit (capped exponential backoff with jitter) this long after its first attempt, then fail its produce requests; keep under the producers' delivery.timeout.ms")
	fs.DurationVar(&f.grace, "stop-grace", 10*time.Second, "time to finish in-flight work after SIGTERM")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9100", "serve Prometheus metrics on /metrics at this address (empty: disabled)")
	fs.BoolVar(&f.debug, "debug", false, "debug logging (same as -log-level debug)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn, error (JSON lines on stderr; segment lines are one per sealed/put/committed segment, so warn keeps failures only)")
	fs.StringVar(&f.runID, "run-id", "", "run id stamped on every log line, metric (build_info) and the manifest (default $MANIFESTER_RUN_ID, else generated)")
	fs.StringVar(&f.manifestOut, "manifest-out", "", "write a JSON run manifest at startup: a file path or s3://bucket/key ({service}, {run_id} expand; a trailing / adds <service>-<run_id>.json); failures are logged, never fatal")
	err := fs.Parse(args)
	return f, fs, err
}

// run is the testable body of main: it serves until shutdown closes and
// returns the exit code.
func run(args []string, stderr io.Writer, shutdown <-chan struct{}) int {
	f, fs, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := serve(f, fs, stderr, shutdown); err != nil {
		slog.Error("manibroker", "err", err)
		return 1
	}
	return 0
}

// level resolves -log-level and -debug. The error is reported once the
// logger exists.
func (f brokerFlags) level() (slog.Level, error) {
	if f.debug {
		return slog.LevelDebug, nil
	}
	return runinfo.ParseLevel(f.logLevel)
}

// nodeConfig builds the prod.NodeConfig that does not depend on the object
// store or the listener. m is nil when metrics are disabled.
func nodeConfig(f brokerFlags, log *slog.Logger, info runinfo.Info, shutdown <-chan struct{}) (prod.NodeConfig, *prod.Metrics, uint64, string, error) {
	cpc := cpclient.New(f.cpURL, nil)
	cfg := prod.NodeConfig{
		NodeID: f.nodeID, Broker: broker.DefaultConfig(), Log: log,
		ControlPlane: cpc, CPClassify: cpclient.Classifier{},
		StopGrace: f.grace,
	}
	if cfg.NodeID == "" {
		cfg.NodeID = fmt.Sprintf("broker-%d", f.brokerID)
	}
	cfg.Broker.BrokerID = int32(f.brokerID)
	cfg.Exec.ObjTimeout = f.objTimeout
	if f.commitDeadline > 0 {
		cfg.Broker.CommitDeadline = core.Duration(f.commitDeadline)
	}
	var m *prod.Metrics
	if f.metricsAddr != "" {
		m = prod.NewMetrics()
		runinfo.Register(m.Registry(), "manibroker", info)
		cfg.Facts = m.Observe
		cpc.Observe(m.ObserveCPCall)
	}
	segLog := prod.NewSegmentLog(log)
	prev := cfg.Facts
	cfg.Facts = func(f core.Fact) {
		if prev != nil {
			prev(f)
		}
		segLog.Observe(f)
	}
	cfg.OnIO = segLog.IO
	mem, memSource := prod.MemoryLimit()
	err := f.tn.apply(&cfg.Broker, mem)
	cfg.Shutdown = shutdown
	return cfg, m, mem, memSource, err
}

// openObjects sets cfg.Objects from -objects.
func openObjects(ctx context.Context, f brokerFlags, cfg *prod.NodeConfig, m *prod.Metrics, log *slog.Logger) error {
	switch f.objects {
	case "mem":
		cfg.Objects, cfg.ObjClassify = vsys.NewObjectStore(), prod.MemClassifier{}
		return nil
	case "s3":
		var obs *awsobs.Recorder
		if m != nil {
			obs = awsobs.New(m.Registry(), "manibroker", "s3", log)
		}
		s, err := s3store.New(ctx, s3store.Config{
			Endpoint: f.endpoint, Region: f.region, Bucket: f.bucket, Prefix: f.prefix, PathStyle: f.pathStyle, Obs: obs,
		})
		if err != nil {
			return err
		}
		if f.createBucket {
			if err := s.EnsureBucket(ctx); err != nil {
				return err
			}
		}
		cfg.Objects, cfg.ObjClassify = s, s3store.Classifier{}
		return nil
	}
	return errors.New("unknown -objects " + f.objects)
}

// serveMetrics starts the /metrics listener when metrics are enabled.
func serveMetrics(ctx context.Context, log *slog.Logger, m *prod.Metrics, addr string) error {
	if m == nil {
		return nil
	}
	mln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           runinfo.Mux(m.Handler()),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() { _ = srv.Serve(mln) }()
	log.Info("manibroker metrics", "addr", mln.Addr().String())
	return nil
}

// bind opens the Kafka listener and sets the advertised host and port.
func bind(ctx context.Context, f brokerFlags, cfg *prod.NodeConfig) (net.Listener, string, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", f.listen)
	if err != nil {
		return nil, "", err
	}
	cfg.Listener = ln
	adv := f.advertise
	if adv == "" {
		adv = ln.Addr().String()
	}
	host, port, err := advertisedPort(adv)
	if err != nil {
		_ = ln.Close() // already returning the original error
		return nil, "", err
	}
	cfg.Broker.Host, cfg.Broker.Port = host, port
	return ln, adv, nil
}

// advertisedPort splits addr and rejects a port that does not fit in int32.
func advertisedPort(addr string) (string, int32, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	p32, err := checkedInt32(p)
	if err != nil {
		return "", 0, err
	}
	return host, p32, nil
}

// checkedInt32 converts n to int32 after rejecting values that would overflow.
func checkedInt32(n int) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d out of int32 range", n)
	}
	return int32(n), nil
}

func serve(f brokerFlags, fs *flag.FlagSet, stderr io.Writer, shutdown <-chan struct{}) error {
	lvl, lerr := f.level()
	info := runinfo.New("manibroker", f.runID, f.brokerID)
	log := runinfo.NewLogger(stderr, lvl, info)
	slog.SetDefault(log)
	if lerr != nil {
		return lerr
	}
	log.Info("manibroker starting", "event", "start", "run_id_generated", info.RunIDFresh, "go_version", info.GoVersion,
		"git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	ctx := context.Background()
	cfg, m, mem, memSource, err := nodeConfig(f, log, info, shutdown)
	if err != nil {
		return err
	}
	if err := openObjects(ctx, f, &cfg, m, log); err != nil {
		return err
	}
	if err := serveMetrics(ctx, log, m, f.metricsAddr); err != nil {
		return err
	}
	ln, adv, err := bind(ctx, f, &cfg)
	if err != nil {
		return err
	}
	n, err := prod.Start(ctx, cfg)
	if err != nil {
		return err
	}
	if m != nil {
		m.Registry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "manibroker", Name: "open_connections",
			Help: "Open Kafka client connections."}, func() float64 { return float64(n.OpenConns()) }))
	}
	log.Info("manibroker listening", "addr", ln.Addr().String(), "advertised", adv, "objects", f.objects)
	bc := cfg.Broker
	log.Info("manibroker tuning",
		"segment_bytes", bc.MaxSegmentBytes, "segment_window", time.Duration(bc.BatchWindow),
		"max_segment_partitions", bc.MaxSegmentPartitions, "max_segment_batches", bc.MaxSegmentBatches,
		"max_buffered", bc.MaxBufferedBytes, "resume_buffered", bc.ResumeBufferedBytes,
		"obj_timeout", f.objTimeout, "memory_bytes", mem, "memory_source", memSource)
	if f.manifestOut != "" {
		writeManifest(ctx, log, info, fs, f, manifestTuning(cfg, f, mem, memSource, adv, ln))
	}
	if err := n.Wait(); err != nil {
		return err // sys.ErrTripped, sys.ErrKilled, ...
	}
	log.Info("manibroker stopped")
	return nil
}

// manifestTuning is the manifest's "tuning" object (keys are read by the runbook).
func manifestTuning(cfg prod.NodeConfig, f brokerFlags, mem uint64, memSource, adv string, ln net.Listener) map[string]any {
	bc := cfg.Broker
	return map[string]any{
		"segment_bytes": bc.MaxSegmentBytes, "segment_window_ns": int64(bc.BatchWindow),
		"max_segment_partitions": bc.MaxSegmentPartitions, "max_segment_batches": bc.MaxSegmentBatches,
		"max_buffered_bytes": bc.MaxBufferedBytes, "resume_buffered_bytes": bc.ResumeBufferedBytes,
		"obj_timeout_ns": int64(f.objTimeout), "memory_bytes": mem, "memory_source": memSource,
		"node_id": cfg.NodeID, "advertised": adv, "listen": ln.Addr().String(),
	}
}

// writeManifest records the effective flags and tuning. Never fatal.
func writeManifest(ctx context.Context, log *slog.Logger, info runinfo.Info, fs *flag.FlagSet, f brokerFlags, tuning map[string]any) {
	m := runinfo.NewManifest(info, fs, f.region, tuning)
	m.Log(log)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	where, err := runinfo.Write(cctx, m, runinfo.WriteOptions{Dest: f.manifestOut, Region: f.region, S3Endpoint: f.endpoint, S3PathStyle: f.pathStyle, IMDS: true})
	if err != nil {
		log.Warn("manifest not written", "event", "manifest_failed", "dest", where, "err", err.Error())
		return
	}
	log.Info("manifest written", "event", "manifest_written", "dest", where)
}
