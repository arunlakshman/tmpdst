// Command manicp serves the control plane over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
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
	"manifester/sys/runinfo"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	kind := flag.String("store", "mem", "mem or dynamo")
	endpoint := flag.String("dynamo-endpoint", "", "DynamoDB endpoint URL (empty: AWS)")
	region := flag.String("region", "us-east-1", "AWS region")
	prefix := flag.String("table-prefix", "manifester_", "DynamoDB table name prefix")
	create := flag.Bool("create-tables", false, "create the DynamoDB table at startup")
	metricsAddr := flag.String("metrics-addr", ":9101", "serve Prometheus metrics on /metrics at this address (empty: disabled)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error (JSON lines on stderr; one \"cp call\" line per request, so warn keeps retries, throttles and failures only)")
	runID := flag.String("run-id", "", "run id stamped on every log line, metric (build_info) and the manifest (default $MANIFESTER_RUN_ID, else generated)")
	manifestOut := flag.String("manifest-out", "", "write a JSON run manifest at startup: a file path or s3://bucket/key ({service}, {run_id} expand; a trailing / adds <service>-<run_id>.json); failures are logged, never fatal")
	flag.Parse()

	lvl, lerr := runinfo.ParseLevel(*logLevel)
	info := runinfo.New("manicp", *runID, -1)
	log := runinfo.NewLogger(os.Stderr, lvl, info)
	slog.SetDefault(log)
	if lerr != nil {
		fatal(lerr)
	}
	log.Info("manicp starting", "event", "start", "run_id_generated", info.RunIDFresh, "go_version", info.GoVersion,
		"git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Metrics also drive the per-call log line, so they exist even when the
	// /metrics port is disabled.
	m := cpobs.NewWithLogger(log)
	runinfo.Register(m.Registry(), "manicp", info)
	if *metricsAddr != "" {
		mln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			fatal(err)
		}
		go func() { _ = http.Serve(mln, runinfo.Mux(m.Handler())) }()
		log.Info("manicp metrics", "addr", mln.Addr().String())
	}

	var st cp.Store
	switch *kind {
	case "mem":
		st = memstore.New()
	case "dynamo":
		d := dynamo.New(newDynamo(ctx, *endpoint, *region, m.APIOptions()), *prefix)
		d.Obs = m
		if *create {
			if err := d.CreateTables(ctx); err != nil {
				fatal(err)
			}
		}
		st = d
	default:
		fatal(errors.New("unknown -store " + *kind))
	}

	srv := &http.Server{Addr: *addr, Handler: m.Middleware(cp.NewHandler(st)), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("manicp listening", "addr", *addr, "store", *kind)
	if *manifestOut != "" {
		mf := runinfo.NewManifest(info, flag.CommandLine, *region, map[string]any{
			"table": *prefix + "cp", "dynamo_endpoint": *endpoint, "max_commit_attempts": 20, "max_tx_items": dynamo.MaxTxItems})
		mf.Log(log)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		where, err := runinfo.Write(cctx, mf, runinfo.WriteOptions{Dest: *manifestOut, Region: *region, IMDS: true})
		cancel()
		if err != nil {
			log.Warn("manifest not written", "event", "manifest_failed", "dest", where, "err", err.Error())
		} else {
			log.Info("manifest written", "event", "manifest_written", "dest", where)
		}
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(err)
	}
	log.Info("manicp stopped")
}

// newDynamo builds the client. With a custom endpoint and no AWS credentials
// in the environment it signs with AWS's documented example key, which
// ExtendDB dev seeds.
func newDynamo(ctx context.Context, endpoint, region string, apiOpts []func(*middleware.Stack) error) *dynamodb.Client {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		fatal(err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, apiOpts...)
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
}

func fatal(err error) {
	slog.Error("manicp", "err", err)
	os.Exit(1)
}
