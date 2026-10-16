// Command manibroker runs one Kafka broker over S3 and the control plane.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	"manifester/broker"
	"manifester/cp/cpclient"
	"manifester/sys"
	"manifester/sys/prod"
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
	grace := flag.Duration("stop-grace", 10*time.Second, "time to finish in-flight work after SIGTERM")
	debug := flag.Bool("debug", false, "debug logging")
	flag.Parse()

	lvl := slog.LevelInfo
	if *debug {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	ctx := context.Background()
	cfg := prod.NodeConfig{
		NodeID: *nodeID, Broker: broker.DefaultConfig(), Log: log,
		ControlPlane: cpclient.New(*cpURL, nil), CPClassify: cpclient.Classifier{},
		StopGrace: *grace,
	}
	if cfg.NodeID == "" {
		cfg.NodeID = fmt.Sprintf("broker-%d", *brokerID)
	}
	cfg.Broker.BrokerID = int32(*brokerID)
	switch *objects {
	case "mem":
		cfg.Objects, cfg.ObjClassify = vsys.NewObjectStore(), prod.MemClassifier{}
	case "s3":
		s, err := s3store.New(ctx, s3store.Config{
			Endpoint: *endpoint, Region: *region, Bucket: *bucket, Prefix: *prefix, PathStyle: *pathStyle,
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
	log.Info("manibroker listening", "addr", ln.Addr().String(), "advertised", adv, "broker_id", *brokerID, "objects", *objects)
	switch err := n.Wait(); {
	case err == nil:
		log.Info("manibroker stopped")
	default:
		fatal(err) // sys.ErrTripped, sys.ErrKilled, ...
	}
}

func fatal(err error) {
	slog.Error("manibroker", "err", err)
	os.Exit(1)
}
