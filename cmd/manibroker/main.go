// Command manibroker runs one Kafka broker over S3 and the control plane.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"manifester/broker"
	"manifester/core"
	"manifester/cp/cpclient"
	"manifester/sys"
	"manifester/sys/awsobs"
	"manifester/sys/prod"
	"manifester/sys/runinfo"
	"manifester/sys/s3store"
	"manifester/veritas/vsys"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9092", "Kafka listen address")
	advertise := flag.String("advertise", "", "host:port clients are told in Metadata (default: the listen address)")
	brokerID := flag.Int("broker-id", 1, "broker id advertised in Metadata")
	nodeID := flag.String("node-id", "", "control-plane node name (default: broker-<id>)")
	cpURL := flag.String("cp-url", "http://127.0.0.1:8080", "control plane base URL")
	objects := flag.String("objects", "s3", "object store: s3, or mem (in-process, single broker, lost on exit)")
	endpoint := flag.String("s3-endpoint", "", "S3 endpoint URL (empty: AWS)")
	bucket := flag.String("s3-bucket", "manifester", "S3 bucket")
	prefix := flag.String("s3-prefix", "", "key prefix inside the bucket")
	region := flag.String("s3-region", "us-east-1", "S3 region")
	pathStyle := flag.Bool("s3-path-style", false, "path-style addressing (SeaweedFS, MinIO)")
	createBucket := flag.Bool("s3-create-bucket", false, "create the bucket at startup")
	dflt := broker.DefaultConfig()
	var tn tuning
	flag.StringVar(&tn.segmentBytes, "segment-bytes", "16MiB", "seal the open segment at this many batch bytes (16MiB, 16MB, plain bytes)")
	flag.DurationVar(&tn.window, "segment-window", time.Duration(dflt.BatchWindow), "seal the open segment this long after its first batch")
	flag.IntVar(&tn.parts, "max-segment-partitions", dflt.MaxSegmentPartitions, "seal at this many distinct partitions (control plane commit limit)")
	flag.IntVar(&tn.batches, "max-segment-batches", dflt.MaxSegmentBatches, "seal at this many batches (control plane commit limit)")
	flag.StringVar(&tn.maxBuffered, "max-buffered", "auto", "pause client reads above this much unacked produce data; auto = clamp(memory/16, 128MiB, 1GiB)")
	flag.StringVar(&tn.resumeBuffered, "resume-buffered", "", "resume reads below this (default: half of -max-buffered)")
	objTimeout := flag.Duration("obj-timeout", 30*time.Second, "base timeout of an object-store call; large bodies get extra time at 2 MiB/s")
	grace := flag.Duration("stop-grace", 10*time.Second, "time to finish in-flight work after SIGTERM")
	metricsAddr := flag.String("metrics-addr", ":9100", "serve Prometheus metrics on /metrics at this address (empty: disabled)")
	debug := flag.Bool("debug", false, "debug logging (same as -log-level debug)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error (JSON lines on stderr; segment lines are one per sealed/put/committed segment, so warn keeps failures only)")
	runID := flag.String("run-id", "", "run id stamped on every log line, metric (build_info) and the manifest (default $MANIFESTER_RUN_ID, else generated)")
	manifestOut := flag.String("manifest-out", "", "write a JSON run manifest at startup: a file path or s3://bucket/key ({service}, {run_id} expand; a trailing / adds <service>-<run_id>.json); failures are logged, never fatal")
	flag.Parse()

	lvl, err := runinfo.ParseLevel(*logLevel)
	if *debug {
		lvl, err = slog.LevelDebug, nil
	}
	info := runinfo.New("manibroker", *runID, *brokerID)
	log := runinfo.NewLogger(os.Stderr, lvl, info)
	slog.SetDefault(log)
	if err != nil {
		fatal(err)
	}
	log.Info("manibroker starting", "event", "start", "run_id_generated", info.RunIDFresh, "go_version", info.GoVersion,
		"git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	ctx := context.Background()
	cpc := cpclient.New(*cpURL, nil)
	cfg := prod.NodeConfig{
		NodeID: *nodeID, Broker: broker.DefaultConfig(), Log: log,
		ControlPlane: cpc, CPClassify: cpclient.Classifier{},
		StopGrace: *grace,
	}
	if cfg.NodeID == "" {
		cfg.NodeID = fmt.Sprintf("broker-%d", *brokerID)
	}
	cfg.Broker.BrokerID = int32(*brokerID)
	cfg.Exec.ObjTimeout = *objTimeout
	var m *prod.Metrics
	if *metricsAddr != "" {
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
	if err := tn.apply(&cfg.Broker, mem); err != nil {
		fatal(err)
	}
	switch *objects {
	case "mem":
		cfg.Objects, cfg.ObjClassify = vsys.NewObjectStore(), prod.MemClassifier{}
	case "s3":
		var obs *awsobs.Recorder
		if m != nil {
			obs = awsobs.New(m.Registry(), "manibroker", "s3", log)
		}
		s, err := s3store.New(ctx, s3store.Config{
			Endpoint: *endpoint, Region: *region, Bucket: *bucket, Prefix: *prefix, PathStyle: *pathStyle, Obs: obs,
		})
		if err != nil {
			fatal(err)
		}
		if *createBucket {
			if err := s.EnsureBucket(ctx); err != nil {
				fatal(err)
			}
		}
		cfg.Objects, cfg.ObjClassify = s, s3store.Classifier{}
	default:
		fatal(errors.New("unknown -objects " + *objects))
	}
	if m != nil {
		mln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			fatal(err)
		}
		go func() { _ = http.Serve(mln, runinfo.Mux(m.Handler())) }()
		log.Info("manibroker metrics", "addr", mln.Addr().String())
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fatal(err)
	}
	cfg.Listener = ln
	adv := *advertise
	if adv == "" {
		adv = ln.Addr().String()
	}
	host, port, err := net.SplitHostPort(adv)
	if err != nil {
		fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		fatal(err)
	}
	cfg.Broker.Host, cfg.Broker.Port = host, int32(p)
	cfg.Shutdown = sys.ShutdownOnSignal(syscall.SIGTERM, os.Interrupt)

	n, err := prod.Start(ctx, cfg)
	if err != nil {
		fatal(err)
	}
	if m != nil {
		m.Registry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "manibroker", Name: "open_connections",
			Help: "Open Kafka client connections."}, func() float64 { return float64(n.OpenConns()) }))
	}
	log.Info("manibroker listening", "addr", ln.Addr().String(), "advertised", adv, "objects", *objects)
	bc := cfg.Broker
	log.Info("manibroker tuning",
		"segment_bytes", bc.MaxSegmentBytes, "segment_window", time.Duration(bc.BatchWindow),
		"max_segment_partitions", bc.MaxSegmentPartitions, "max_segment_batches", bc.MaxSegmentBatches,
		"max_buffered", bc.MaxBufferedBytes, "resume_buffered", bc.ResumeBufferedBytes,
		"obj_timeout", *objTimeout, "memory_bytes", mem, "memory_source", memSource)
	if *manifestOut != "" {
		writeManifest(ctx, log, info, *manifestOut, *region, *endpoint, *pathStyle, map[string]any{
			"segment_bytes": bc.MaxSegmentBytes, "segment_window_ns": int64(bc.BatchWindow),
			"max_segment_partitions": bc.MaxSegmentPartitions, "max_segment_batches": bc.MaxSegmentBatches,
			"max_buffered_bytes": bc.MaxBufferedBytes, "resume_buffered_bytes": bc.ResumeBufferedBytes,
			"obj_timeout_ns": int64(*objTimeout), "memory_bytes": mem, "memory_source": memSource,
			"node_id": cfg.NodeID, "advertised": adv, "listen": ln.Addr().String(),
		})
	}
	switch err := n.Wait(); {
	case err == nil:
		log.Info("manibroker stopped")
	default:
		fatal(err) // sys.ErrTripped, sys.ErrKilled, ...
	}
}

// writeManifest records the effective flags and tuning. Never fatal.
func writeManifest(ctx context.Context, log *slog.Logger, info runinfo.Info, dest, region, s3endpoint string, pathStyle bool, tuning map[string]any) {
	m := runinfo.NewManifest(info, flag.CommandLine, region, tuning)
	m.Log(log)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	where, err := runinfo.Write(cctx, m, runinfo.WriteOptions{Dest: dest, Region: region, S3Endpoint: s3endpoint, S3PathStyle: pathStyle, IMDS: true})
	if err != nil {
		log.Warn("manifest not written", "event", "manifest_failed", "dest", where, "err", err.Error())
		return
	}
	log.Info("manifest written", "event", "manifest_written", "dest", where)
}

func fatal(err error) {
	slog.Error("manibroker", "err", err)
	os.Exit(1)
}
