// Command manicp serves the control plane over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"

	"manifester/cp"
	"manifester/cp/cpobs"
	"manifester/cp/dynamo"
	"manifester/cp/memstore"
	"manifester/sys/httppool"
	"manifester/sys/runinfo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

// cpFlags are the parsed command-line flags.
type cpFlags struct {
	addr, kind, endpoint, region, prefix string
	create                               bool
	metricsAddr, logLevel, runID         string
	manifestOut                          string
	sequencer                            bool
	seqPipeline, seqMaxItems, seqCache   int
}

func parseFlags(args []string, stderr io.Writer) (cpFlags, *flag.FlagSet, error) {
	var f cpFlags
	fs := flag.NewFlagSet("manicp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.addr, "addr", ":8080", "listen address")
	fs.StringVar(&f.kind, "store", "mem", "mem or dynamo")
	fs.StringVar(&f.endpoint, "dynamo-endpoint", "", "DynamoDB endpoint URL (empty: AWS)")
	fs.StringVar(&f.region, "region", "us-east-1", "AWS region")
	fs.StringVar(&f.prefix, "table-prefix", "manifester_", "DynamoDB table name prefix")
	fs.BoolVar(&f.create, "create-tables", false, "create the DynamoDB table at startup")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9101", "serve Prometheus metrics on /metrics at this address (empty: disabled)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn, error (JSON lines on stderr; one \"cp call\" line per request, so warn keeps retries, throttles and failures only)")
	fs.StringVar(&f.runID, "run-id", "", "run id stamped on every log line, metric (build_info) and the manifest (default $MANIFESTER_RUN_ID, else generated)")
	fs.StringVar(&f.manifestOut, "manifest-out", "", "write a JSON run manifest at startup: a file path or s3://bucket/key ({service}, {run_id} expand; a trailing / adds <service>-<run_id>.json); failures are logged, never fatal")
	fs.BoolVar(&f.sequencer, "sequencer", true, "-store dynamo: serialise commits through one in-process sequencer (group commit, offsets from memory, contention retried here). Run exactly ONE manicp and point every broker's -cp-url at it; correctness never depends on that (the run-item compare-and-set stays), a second manicp only costs retries. false: every commit does its own read-and-CAS (the pre-decision-46 path)")
	fs.IntVar(&f.seqPipeline, "seq-pipeline", 4, "sequencer: group transactions in flight at once (they touch disjoint partitions; 1 = strictly serial)")
	fs.IntVar(&f.seqMaxItems, "seq-max-group-items", dynamo.MaxTxItems, "sequencer: item budget of one group transaction (at most 100, DynamoDB's limit)")
	fs.IntVar(&f.seqCache, "seq-cache-keys", 4096, "sequencer: recently committed segment keys remembered for replay without a read (negative: none)")
	err := fs.Parse(args)
	return f, fs, err
}

// run is the testable body of main: it serves until ctx ends and returns the
// exit code.
func run(ctx context.Context, args []string, stderr io.Writer) int {
	f, fs, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := serve(ctx, f, fs, stderr); err != nil {
		slog.Error("manicp", "err", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, f cpFlags, fs *flag.FlagSet, stderr io.Writer) error {
	lvl, lerr := runinfo.ParseLevel(f.logLevel)
	info := runinfo.New("manicp", f.runID, -1)
	log := runinfo.NewLogger(stderr, lvl, info)
	slog.SetDefault(log)
	if lerr != nil {
		return lerr
	}
	log.Info("manicp starting", "event", "start", "run_id_generated", info.RunIDFresh, "go_version", info.GoVersion,
		"git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	// Metrics also drive the per-call log line, so they exist even when the
	// /metrics port is disabled.
	m := cpobs.NewWithLogger(log)
	runinfo.Register(m.Registry(), "manicp", info)
	if err := serveMetrics(log, m, f.metricsAddr); err != nil {
		return err
	}
	st, closeStore, err := openStore(ctx, f, m)
	if err != nil {
		return err
	}
	defer closeStore()
	srv := &http.Server{Addr: f.addr, Handler: m.Middleware(cp.NewHandler(st)), ReadHeaderTimeout: 10 * time.Second}
	go shutdownOnDone(ctx, srv)
	log.Info("manicp listening", "addr", f.addr, "store", f.kind, "sequencer", f.sequencer && f.kind == "dynamo")
	if f.manifestOut != "" {
		writeManifest(ctx, log, info, fs, f)
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("manicp stopped")
	return nil
}

// serveMetrics starts the /metrics listener unless addr is empty.
func serveMetrics(log *slog.Logger, m *cpobs.Metrics, addr string) error {
	if addr == "" {
		return nil
	}
	mln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() { _ = http.Serve(mln, runinfo.Mux(m.Handler())) }()
	log.Info("manicp metrics", "addr", mln.Addr().String())
	return nil
}

// openStore builds the -store backend. The returned func releases it (the
// sequencer's loop); call it once the server has stopped.
func openStore(ctx context.Context, f cpFlags, m *cpobs.Metrics) (cp.Store, func(), error) {
	nop := func() {}
	switch f.kind {
	case "mem":
		return memstore.New(), nop, nil
	case "dynamo":
		return openDynamo(ctx, f, m)
	}
	return nil, nop, errors.New("unknown -store " + f.kind)
}

func openDynamo(ctx context.Context, f cpFlags, m *cpobs.Metrics) (cp.Store, func(), error) {
	nop := func() {}
	c, err := newDynamo(ctx, f.endpoint, f.region, m.APIOptions())
	if err != nil {
		return nil, nop, err
	}
	d := dynamo.New(c, f.prefix)
	d.Obs = m
	if f.create {
		if err := d.CreateTables(ctx); err != nil {
			return nil, nop, err
		}
	}
	if !f.sequencer {
		return d, nop, nil
	}
	s := dynamo.NewSequencer(d, dynamo.SeqOptions{Pipeline: f.seqPipeline, MaxItems: f.seqMaxItems, CacheKeys: f.seqCache})
	return s, s.Close, nil
}

// shutdownOnDone shuts srv down gracefully once ctx ends.
func shutdownOnDone(ctx context.Context, srv *http.Server) {
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
}

// writeManifest records the run manifest. Never fatal.
func writeManifest(ctx context.Context, log *slog.Logger, info runinfo.Info, fs *flag.FlagSet, f cpFlags) {
	mf := runinfo.NewManifest(info, fs, f.region, map[string]any{
		"table": f.prefix + "cp", "dynamo_endpoint": f.endpoint, "max_commit_attempts": 20, "max_tx_items": dynamo.MaxTxItems,
		"sequencer": f.sequencer && f.kind == "dynamo", "seq_pipeline": f.seqPipeline, "seq_max_group_items": f.seqMaxItems, "seq_cache_keys": f.seqCache})
	mf.Log(log)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	where, err := runinfo.Write(cctx, mf, runinfo.WriteOptions{Dest: f.manifestOut, Region: f.region, IMDS: true})
	cancel()
	if err != nil {
		log.Warn("manifest not written", "event", "manifest_failed", "dest", where, "err", err.Error())
		return
	}
	log.Info("manifest written", "event", "manifest_written", "dest", where)
}

// newDynamo builds the client. With a custom endpoint and no AWS credentials
// in the environment it signs with AWS's documented example key, which
// ExtendDB dev seeds.
func newDynamo(ctx context.Context, endpoint, region string, apiOpts []func(*middleware.Stack) error) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")))
	}
	opts = append(opts, config.WithHTTPClient(httppool.AWS())) // sized pool, audit item 4
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, apiOpts...)
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}
