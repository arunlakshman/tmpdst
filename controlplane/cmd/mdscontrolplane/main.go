// Command mdscontrolplane serves the control plane over gRPC.
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

	"github.com/manifesterio/mds/controlplane/internal/controlplane/controlplanemetrics"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/shared/httpconnections"
	"github.com/manifesterio/mds/shared/runinfo"
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
	brokerLeaseTTL                       time.Duration
	log                                  logFlags
}

func parseFlags(args []string, stderr io.Writer) (cpFlags, *flag.FlagSet, error) {
	var f cpFlags
	fs := flag.NewFlagSet("mdscontrolplane", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.addr, "addr", ":8080", "gRPC listen address")
	fs.StringVar(&f.kind, "store", "mem", "mem (in-memory), dynamolog (Dynamo log + optional S3 checkpoints) or postgres (PostgreSQL log + optional S3 checkpoints)")
	fs.StringVar(&f.endpoint, "dynamo-endpoint", "", "DynamoDB endpoint URL (empty: AWS)")
	fs.StringVar(&f.region, "region", "us-east-1", "AWS region")
	fs.StringVar(&f.prefix, "table-prefix", "mds_", "DynamoDB table name prefix")
	fs.BoolVar(&f.create, "create-tables", false, "create the tables at startup: the DynamoDB table with -store dynamolog, the SQL tables with -store postgres")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9101", "serve Prometheus metrics on /metrics (empty: disabled)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	fs.StringVar(&f.runID, "run-id", "", "run id stamped on every log line and metric (default $MDS_RUN_ID)")
	fs.StringVar(&f.manifestOut, "manifest-out", "", "write a JSON run manifest at startup")
	fs.DurationVar(&f.brokerLeaseTTL, "broker-lease-ttl", 5*time.Second, "a broker is live for this long after its last heartbeat; brokers beat every -lease-interval (default 1s), so keep this 3x that")
	registerLogFlags(fs, &f.log)
	err := fs.Parse(args)
	return f, fs, err
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	f, fs, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := serve(ctx, f, fs, stderr); err != nil {
		slog.Error("mdscontrolplane", "err", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, f cpFlags, fs *flag.FlagSet, stderr io.Writer) error {
	lvl, lerr := runinfo.ParseLevel(f.logLevel)
	info := runinfo.New("mdscontrolplane", f.runID, -1)
	log := runinfo.NewLogger(stderr, lvl, info)
	slog.SetDefault(log)
	if lerr != nil {
		return lerr
	}
	log.Info("mdscontrolplane starting", "event", "start", "run_id_generated", info.RunIDFresh,
		"go_version", info.GoVersion, "git_dirty", info.GitDirty, "hostname", info.Hostname, "log_level", lvl.String())

	m := controlplanemetrics.NewWithLogger(log)
	runinfo.Register(m.Registry(), "mdscontrolplane", info)
	if err := runinfo.ServeMetrics(ctx, log, "mdscontrolplane", f.metricsAddr, m.Handler()); err != nil {
		return err
	}
	node, closeNode, err := openNode(ctx, f, m, log)
	if err != nil {
		return err
	}
	defer closeNode()
	m.RegisterLogRole(node.LogRole)

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
	log.Info("mdscontrolplane listening", "addr", ln.Addr().String(), "store", f.kind)
	runinfo.Publish(
		ctx,
		log,
		hideDSN(runinfo.NewManifest(info, fs, f.region, map[string]any{
			"table":              f.log.tableName(f.prefix),
			"store":              f.kind,
			"rank":               f.log.rank,
			"takeover_after":     f.log.takeoverAfter.String(),
			"takeover_rank_step": f.log.takeoverRankStep.String(),
			"checkpoint_bucket":  f.log.checkpointBucket,
			"pebble_dir":         f.log.pebbleDir,
			"window_slots":       f.log.windowSlots,
			"dynamo_endpoint":    f.endpoint,
			"postgres_prefix":    f.log.postgresPrefix,
		}), resolvedDSN(f.log)),
		runinfo.WriteOptions{
			Dest:   f.manifestOut,
			Region: f.region,
			IMDS:   true,
		},
	)
	err = gs.Serve(ln)
	log.Info("mdscontrolplane stopped")
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !strings.Contains(err.Error(), "the server has been stopped") {
		return err
	}
	return nil
}

func openNode(ctx context.Context, f cpFlags, m *controlplanemetrics.Metrics, log *slog.Logger) (*server.Node, func(), error) {
	nop := func() {}
	mcfg := machineConfig(f)

	var logStore executor.Log
	var objs executor.ObjectStore
	var closeStore func()
	var err error
	switch f.kind {
	case "mem":
		logStore = executor.NewMemoryLog()
		objs = executor.NewMemoryObjects()
		closeStore = nop
	case kindLog:
		logStore, objs, closeStore, err = openDynLog(ctx, f, m, log)
	case kindPostgres:
		logStore, objs, closeStore, err = openPostgresLog(ctx, f, log)
	default:
		return nil, nop, errors.New("unknown -store " + f.kind)
	}
	if err != nil {
		return nil, nop, err
	}
	if isLogStore(f.kind) {
		if f.log.checkpointBucket != "" {
			mcfg.CheckpointPrefix = "checkpoint/"
		} else {
			mcfg.CheckpointEvery = 0
			mcfg.KeepSlots = 0
		}
		state := &pebbleState{flagDir: f.log.pebbleDir}
		if err := state.start(); err != nil {
			closeStore()
			return nil, nop, err
		}
		mcfg.OpenStore = state.openStore
		closeLog := closeStore
		closeStore = func() { closeLog(); state.close() }
	}

	n := server.New(server.Config{
		Machine:        mcfg,
		Log:            logStore,
		Objects:        objs,
		Self:           runtime.NodeInstance{Node: runtime.NodeID(nodeName(f.log))},
		BrokerLeaseTTL: f.brokerLeaseTTL,
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

// trimStride is how often, in log slots, the idempotency window is cut. Every
// node must use the same value (and the same -window-slots): the cut is a
// function of the slot number, so all replicas forget the same keys.
const trimStride = 1024

// machineConfig turns the flags into the control-plane machine's settings.
func machineConfig(f cpFlags) replica.Config {
	mcfg := replica.Config{
		OnCheckpointDecision: logCheckpointDecision,
		// Short catch-up period so Ready (and health Serving) arrives quickly;
		// subsequent rounds keep the same cadence.
		CatchUpEvery:       runtime.Duration(5 * time.Millisecond),
		Rank:               f.log.rank,
		Heartbeat:          runtime.Duration(f.log.heartbeat),
		TakeoverAfter:      runtime.Duration(f.log.takeoverAfter),
		TakeoverRankStep:   runtime.Duration(f.log.takeoverRankStep),
		Writer:             nodeName(f.log),
		CheckpointEvery:    runtime.Duration(f.log.checkpointEvery),
		CheckpointMinSlots: f.log.checkpointMinSlots,
		KeepSlots:          f.log.windowSlots / 8,
		WindowSlots:        f.log.windowSlots,
		TrimStride:         trimStride,
	}
	return mcfg
}

func newDynamo(ctx context.Context, endpoint, region string, apiOpts []func(*middleware.Stack) error) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")))
	}
	opts = append(opts, config.WithHTTPClient(httpconnections.ConfigureAWSClient()))
	awsConfig, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return dynamodb.NewFromConfig(awsConfig, func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, apiOpts...)
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

// logCheckpointDecision writes one log line each time the checkpoint timer
// fires, so an operator can see why a checkpoint was or was not written.
func logCheckpointDecision(d replica.CheckpointDecision) {
	attrs := []any{
		"event", "checkpoint_decision", "written", d.Written, "reason", d.Reason,
		"applied", d.Applied, "goal_tail", d.GoalTail, "last_checkpoint", d.LastCheckpoint,
		"min_slots", d.MinSlots, "in_flight", d.PendingKind,
	}
	if d.Written {
		slog.Info("checkpoint started", attrs...)
		return
	}
	slog.Info("checkpoint skipped", attrs...)
}
