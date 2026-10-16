// Command manicp serves the control plane over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/cp"
	"manifester/cp/dynamo"
	"manifester/cp/memstore"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	kind := flag.String("store", "mem", "mem or dynamo")
	endpoint := flag.String("dynamo-endpoint", "", "DynamoDB endpoint URL (empty: AWS)")
	region := flag.String("region", "us-east-1", "AWS region")
	prefix := flag.String("table-prefix", "manifester_", "DynamoDB table name prefix")
	create := flag.Bool("create-tables", false, "create the DynamoDB table at startup")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	var st cp.Store
	switch *kind {
	case "mem":
		st = memstore.New()
	case "dynamo":
		d := dynamo.New(newDynamo(ctx, *endpoint, *region), *prefix)
		if *create {
			if err := d.CreateTables(ctx); err != nil {
				fatal(err)
			}
		}
		st = d
	default:
		fatal(errors.New("unknown -store " + *kind))
	}

	srv := &http.Server{Addr: *addr, Handler: cp.NewHandler(st), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	slog.Info("manicp listening", "addr", *addr, "store", *kind)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(err)
	}
	slog.Info("manicp stopped")
}

// newDynamo builds the client. With a custom endpoint and no AWS credentials
// in the environment it signs with AWS's documented example key, which
// ExtendDB dev seeds.
func newDynamo(ctx context.Context, endpoint, region string) *dynamodb.Client {
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
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
}

func fatal(err error) {
	slog.Error("manicp", "err", err)
	os.Exit(1)
}
