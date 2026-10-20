// Command ddbprobe is a small DynamoDB load tool for the commit-path experiments X1, X3 and X4
// (docs/x1-x3-x4-results.md). Run it from an EC2 instance in the table's region. It prints one JSON summary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, realAPI)
	stop()
	os.Exit(code)
}

type flags struct {
	mode, table, region, qkind string
	rate                       float64
	k, writers, size, gsize    int
	per, parts                 int
	dur, window                time.Duration
	start                      int64
	gap                        int64
}

func parseFlags(args []string, stderr io.Writer) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("ddbprobe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.mode, "mode", "put", "put, txn, contend, query or batch")
	fs.StringVar(&f.table, "table", "", "table name (pk S, sk S)")
	fs.StringVar(&f.region, "region", "ap-south-2", "AWS region")
	fs.Float64Var(&f.rate, "rate", 64, "operations per second (put/txn/contend/query: slots or lookups; batch: items per second)")
	fs.IntVar(&f.k, "k", 4, "max operations in flight (per writer for contend)")
	fs.IntVar(&f.writers, "writers", 3, "contend: number of racing appenders")
	fs.IntVar(&f.size, "size", 4096, "LOG item payload bytes")
	fs.IntVar(&f.gsize, "gsize", 2048, "txn: G# item payload bytes")
	fs.IntVar(&f.per, "per", 25, "batch: items per BatchWriteItem call")
	fs.IntVar(&f.parts, "parts", 40000, "batch: distinct partition keys")
	fs.DurationVar(&f.dur, "dur", 2*time.Minute, "run length")
	fs.DurationVar(&f.window, "window", 0, "query: coalescing window (0: one Query per lookup)")
	fs.StringVar(&f.qkind, "qkind", "desc", "query: desc (Limit 1 descending) or after (sk > tail-gap)")
	fs.Int64Var(&f.gap, "gap", 4, "query after: n = tail - gap")
	fs.Int64Var(&f.start, "start", 0, "first slot (0: tail+1 found with a consistent query)")
	if err := fs.Parse(args); err != nil {
		return f, err
	}
	if f.table == "" || f.rate <= 0 || f.k <= 0 || f.dur <= 0 {
		return f, errors.New("need -table, positive -rate, -k, -dur")
	}
	return f, nil
}

func realAPI(ctx context.Context, region string) (API, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }))
	if err != nil {
		return nil, err
	}
	return dynamodb.NewFromConfig(cfg), nil
}

type newAPI func(ctx context.Context, region string) (API, error)

func run(ctx context.Context, args []string, stdout, stderr io.Writer, mk newAPI) int {
	f, err := parseFlags(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "ddbprobe:", err)
		return 2
	}
	api, err := mk(ctx, f.region)
	if err != nil {
		fmt.Fprintln(stderr, "ddbprobe:", err)
		return 1
	}
	s, err := execute(ctx, api, f)
	if err != nil {
		fmt.Fprintln(stderr, "ddbprobe:", err)
		return 1
	}
	enc := json.NewEncoder(stdout)
	if err := enc.Encode(s); err != nil {
		fmt.Fprintln(stderr, "ddbprobe:", err)
		return 1
	}
	return 0
}

func params(f flags, start int64) map[string]any {
	return map[string]any{"rate": f.rate, "k": f.k, "writers": f.writers, "size": f.size, "gsize": f.gsize,
		"per": f.per, "parts": f.parts, "dur": f.dur.String(), "window": f.window.String(), "qkind": f.qkind,
		"gap": f.gap, "start": start, "table": f.table}
}

// execute builds the op for the mode and runs it paced.
func execute(ctx context.Context, api API, f flags) (summary, error) {
	start := f.start
	if start == 0 {
		tail, err := tailSlot(ctx, api, f.table)
		if err != nil {
			return summary{}, err
		}
		start = tail + 1
	}
	rec := newRecorder(time.Now())
	rate, k := f.rate, f.k
	var o op
	switch f.mode {
	case "put":
		o = putOp(api, f.table, start, f.size)
	case "txn":
		o = txnOp(api, f.table, start, f.size, f.gsize)
	case "contend":
		runContend(ctx, api, f, start, rec)
		return rec.summarize(f.mode, params(f, start), time.Since(rec.start)), nil
	case "query":
		o = queryOp(ctx, api, f, start)
	case "batch":
		o = batchOp(api, f.table, f.per, f.parts, 60)
		rate = f.rate / float64(f.per)
	default:
		return summary{}, fmt.Errorf("unknown mode %q", f.mode)
	}
	runPaced(ctx, rate, k, f.dur, o, rec)
	return rec.summarize(f.mode, params(f, start), time.Since(rec.start)), nil
}

func queryOp(ctx context.Context, api API, f flags, tail int64) op {
	after := tail - 1 - f.gap
	fn := func(c context.Context) result { return lookupQuery(c, api, f.table, f.qkind, after) }
	if f.window > 0 {
		return newCoalescer(ctx, f.window, fn).lookup
	}
	return func(c context.Context, _ int64) result { return fn(c) }
}

// runContend races f.writers appenders, each at rate/writers with k in flight.
func runContend(ctx context.Context, api API, f flags, start int64, rec *recorder) {
	c := newContenders(api, f.table, start, f.size, f.writers)
	done := make(chan struct{})
	for w := 0; w < f.writers; w++ {
		go func(w int) {
			runPaced(ctx, f.rate/float64(f.writers), f.k, f.dur, c.opFor(w), rec)
			done <- struct{}{}
		}(w)
	}
	for w := 0; w < f.writers; w++ {
		<-done
	}
}
