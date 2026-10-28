// Command manicp serves the control plane over gRPC.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	"google.golang.org/grpc"

	"manifester/internal/controlplane/cpobs"
	"manifester/internal/controlplane/exec"
	"manifester/internal/controlplane/machine"
	"manifester/internal/controlplane/server"
	"manifester/internal/core"
	"manifester/internal/sys/httppool"
	"manifester/internal/sys/runinfo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

type cpFlags struct {
	addr, kind, endpoint, region, prefix string
	create                               bool
	metricsAddr, logLevel, runID         string
	manifestOut                          string
	log                                  logFlags
}

func parseFlags(args []string, stderr io.Writer) (cpFlags, *flag.FlagSet, error) {
	var f cpFlags
	fs := flag.NewFlagSet("manicp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.addr, "addr", ":8080", "gRPC listen address")
	fs.StringVar(&f.kind, "store", "mem", "mem (in-memory) or dynamolog (Dynamo log + optional S3 checkpoints)")
	fs.StringVar(&f.endpoint, "dynamo-endpoint", "", "DynamoDB endpoint URL (empty: AWS)")
	fs.StringVar(&f.region, "region", "us-east-1", "AWS region")
	fs.StringVar(&f.prefix, "table-prefix", "manifester_", "DynamoDB table name prefix")
	fs.BoolVar(&f.create, "create-tables", false, "create the DynamoDB table at startup")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9101", "serve Prometheus metrics on /metrics (empty: disabled)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	fs.StringVar(&f.runID, "run-id", "", "run id stamped on every log line and metric (default $MANIFESTER_RUN_ID)")
	fs.StringVar(&f.manifestOut, "manifest-out", "", "write a JSON run manifest at startup")
	registerLogFlags(fs, &f.log)
	err := fs.Parse(args)
	return f, fs, err
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	return runWith(ctx, args, stderr, realDeps())
}

func runWith(ctx context.Context, args []string, stderr io.Writer, d deps) int {
	f, fs, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := serve(ctx, d, f, fs, stderr); err != nil {
		slog.Error("manicp", "err", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, d deps, f cpFlags, fs *flag.FlagSet, stderr io.Writer) error {
	lvl, lerr := runinfo.ParseLevel(f.logLevel)
	info := runinfo.New("manicp", f.runID, -1)
	log := runinfo.NewLogger(stderr, lvl, info)
	slog.SetDefault(log)
	if lerr != nil {
		return lerr
	}
	log.Info("manicp starting", "event", "start", "run_id_generated", info.RunIDFresh,
		"go_version", info.GoVersion, "git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	m := cpobs.NewWithLogger(log)
	runinfo.Register(m.Registry(), "manicp", info)
	if err := runinfo.ServeMetrics(ctx, log, "manicp", f.metricsAddr, m.Handler()); err != nil {
		return err
	}
	node, closeNode, err := openNode(ctx, d, f, m, log)
	if err != nil {
		return err
	}
	defer closeNode()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", f.addr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	node.Register(gs)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	log.Info("manicp listening", "addr", ln.Addr().String(), "store", f.kind)
	runinfo.Publish(
		ctx,
		log,
		runinfo.NewManifest(info, fs, f.region, map[string]any{
			"table":           f.log.tableName(f.prefix),
			"store":           f.kind,
			"rank":            f.log.rank,
			"takeover_after":  f.log.takeoverAfter.String(),
			"ckpt_bucket":     f.log.ckptBucket,
			"dynamo_endpoint": f.endpoint,
		}),
		runinfo.WriteOptions{
			Dest:   f.manifestOut,
			Region: f.region,
			IMDS:   true,
		},
	)
	err = gs.Serve(ln)
	log.Info("manicp stopped")
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !strings.Contains(err.Error(), "the server has been stopped") {
		return err
	}
	return nil
}

func openNode(ctx context.Context, d deps, f cpFlags, m *cpobs.Metrics, log *slog.Logger) (*server.Node, func(), error) {
	nop := func() {}
	mcfg := machine.Config{
		// Short catch-up period so Ready (and health Serving) arrives quickly;
		// subsequent rounds keep the same cadence.
		CatchUpEvery:  core.Duration(5 * time.Millisecond),
		K:             f.log.k,
		Rank:          f.log.rank,
		Heartbeat:     core.Duration(f.log.heartbeat),
		TakeoverAfter: core.Duration(f.log.takeoverAfter),
		HoleAfter:     core.Duration(f.log.holeAfter),
		Writer:        nodeName(f.log),
		CkptEvery:     core.Duration(f.log.ckptEvery),
		CkptMinSlots:  f.log.ckptMinSlots,
		KeepSlots:     f.log.windowSlots / 8,
	}
	if mcfg.K <= 0 {
		mcfg.K = 4
	}

	var logStore exec.Log
	var objs exec.ObjectStore
	var closeStore func()
	var err error
	switch f.kind {
	case "mem":
		logStore = exec.NewMemLog()
		objs = exec.NewMemObjects()
		closeStore = nop
	case kindLog:
		logStore, objs, closeStore, err = openDynLog(ctx, d, f, m, log)
		if err != nil {
			return nil, nop, err
		}
		if f.log.ckptBucket != "" {
			mcfg.CkptKey = "ckpt/latest"
		} else {
			mcfg.CkptEvery = 0
			mcfg.KeepSlots = 0
		}
	default:
		return nil, nop, errors.New("unknown -store " + f.kind)
	}

	n := server.New(server.Config{
		Machine: mcfg,
		Log:     logStore,
		Objects: objs,
		Self:    core.NodeRef{Node: core.NodeID(nodeName(f.log))},
	})
	sctx, cancel := context.WithTimeout(ctx, f.log.startTimeout)
	err = n.Start(sctx)
	cancel()
	if err != nil {
		closeStore()
		return nil, nop, err
	}
	return n, func() { _ = n.Close(); closeStore() }, nil
}

func newDynamo(ctx context.Context, endpoint, region string, apiOpts []func(*middleware.Stack) error) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")))
	}
	opts = append(opts, config.WithHTTPClient(httppool.AWS()))
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
