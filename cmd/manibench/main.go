// Command manibench is a load generator for manibroker, modelled on the
// WarpStream CLI benchmark-producer.
//
// Usage:
//
//	manibench benchmark-producer -topic T [-brokers 127.0.0.1:9092]
//	    [-num-clients 1] [-record-size 1024] [-duration 30s] [-interval 5s]
//	    [-partitions 0] [-compression none] [-max-inflight 0] [-buffered-records 5000]
//	    [-cp-url http://127.0.0.1:8080 -create-partitions N]
//
// Each client is its own kgo.Client (acks=all, idempotence disabled: the broker rejects
// idempotent batches, decision 41; -compression picks the batch codec, default
// none: the broker stores compressed batches as opaque bytes, decision 45). It produces as fast as
// the buffer allows: Produce blocks once -buffered-records are in flight.
// Latency is the time from the Produce call to its callback.
//
// Partitioning: with -partitions 0 the client uses kgo's UniformBytes sticky
// partitioner (64 KiB per partition, then a random other one), so every
// partition gets traffic and batches stay large. With -partitions N the
// records are spread round-robin over partitions 0..N-1 per record (manual).
//
// Every -interval one line is printed for that interval, across all clients;
// at the end (or on SIGINT) a summary. Not part of the determinism gate.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() { os.Exit(mainRun(os.Args[1:], os.Stdout, os.Stderr, newKgoClient)) }

// benchConfig is the parsed benchmark-producer command line.
type benchConfig struct {
	topic       string
	brokers     []string
	clients     int
	size        int
	dur         time.Duration
	every       time.Duration
	parts       int
	inflight    int
	buffered    int
	cpURL       string
	createParts int
	compression string
}

// producer is the part of *kgo.Client the benchmark uses (a fake in tests).
type producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	BufferedProduceRecords() int64
	Flush(ctx context.Context) error
	Close()
}

type clientFactory func(opts ...kgo.Opt) (producer, error)

func newKgoClient(opts ...kgo.Opt) (producer, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// parseArgs parses the arguments after "benchmark-producer". ok is false when
// usage was printed (exit 2).
func parseArgs(args []string, stderr io.Writer) (benchConfig, bool) {
	var c benchConfig
	fs := flag.NewFlagSet("benchmark-producer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.topic, "topic", "", "topic to produce to (required)")
	brokers := fs.String("brokers", "127.0.0.1:9092", "comma-separated seed brokers")
	fs.IntVar(&c.clients, "num-clients", 1, "number of independent kgo clients")
	fs.IntVar(&c.size, "record-size", 1024, "record value size in bytes")
	fs.DurationVar(&c.dur, "duration", 30*time.Second, "how long to produce")
	fs.DurationVar(&c.every, "interval", 5*time.Second, "reporting interval")
	fs.IntVar(&c.parts, "partitions", 0, "0: kgo uniform-bytes sticky partitioner; N: round-robin per record over partitions 0..N-1")
	fs.IntVar(&c.inflight, "max-inflight", 0, "kgo MaxProduceRequestsInflightPerBroker (0: kgo default, 1; above 1 a retry can reorder, there is no idempotence)")
	fs.IntVar(&c.buffered, "buffered-records", 5000, "kgo MaxBufferedRecords per client")
	fs.StringVar(&c.compression, "compression", "none", "batch compression: none, gzip, snappy, lz4 or zstd")
	fs.StringVar(&c.cpURL, "cp-url", "", "control plane URL, used with -create-partitions")
	fs.IntVar(&c.createParts, "create-partitions", 0, "if > 0, POST /v1/create-topic first")
	if fs.Parse(args) != nil {
		return c, false
	}
	if _, known := codecByName(c.compression); !known || c.topic == "" || c.clients < 1 || c.size < 8 {
		fs.Usage()
		return c, false
	}
	c.brokers = strings.Split(*brokers, ",")
	return c, true
}

func mainRun(args []string, stdout, stderr io.Writer, nc clientFactory) int {
	if len(args) < 1 || args[0] != "benchmark-producer" {
		_, _ = fmt.Fprintln(stderr, "usage: manibench benchmark-producer -topic T [flags]")
		return 2
	}
	c, ok := parseArgs(args[1:], stderr)
	if !ok {
		return 2
	}
	if c.createParts > 0 {
		if err := createTopic(c.cpURL, c.topic, c.createParts); err != nil {
			_, _ = fmt.Fprintln(stderr, "create topic:", err)
			return 1
		}
	}
	return run(c, stdout, stderr, nc)
}

func createTopic(url, topic string, n int) error {
	if url == "" {
		url = "http://127.0.0.1:8080"
	}
	body, _ := json.Marshal(map[string]any{"topic": topic, "partitions": n})
	resp, err := http.Post(strings.TrimRight(url, "/")+"/v1/create-topic", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }() // read-only response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

// clientOpts builds the kgo options shared by every client.
func clientOpts(c benchConfig) []kgo.Opt {
	codec, _ := codecByName(c.compression)
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(), // the broker rejects idempotent batches (decision 41)
		kgo.ProducerBatchCompression(codec),
		kgo.MaxBufferedRecords(c.buffered),
		kgo.DefaultProduceTopic(c.topic),
	}
	if c.parts > 0 {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	} else {
		opts = append(opts, kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)))
	}
	if c.inflight > 0 {
		opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(c.inflight))
	}
	return opts
}

// codecByName maps the -compression flag to a kgo codec.
func codecByName(name string) (kgo.CompressionCodec, bool) {
	switch strings.ToLower(name) {
	case "", "none":
		return kgo.NoCompression(), true
	case "gzip":
		return kgo.GzipCompression(), true
	case "snappy":
		return kgo.SnappyCompression(), true
	case "lz4":
		return kgo.Lz4Compression(), true
	case "zstd":
		return kgo.ZstdCompression(), true
	}
	return kgo.NoCompression(), false
}

// makeValue builds record seq of client ci: ids in the first 8 bytes, a
// letter pattern after.
func makeValue(ci int, seq uint64, size int) []byte {
	val := make([]byte, size)
	binary.BigEndian.PutUint32(val, uint32(ci))
	binary.BigEndian.PutUint32(val[4:], uint32(seq))
	for j := 8; j < size; j++ {
		val[j] = byte('a' + j%26)
	}
	return val
}

// produceLoop produces as fast as the buffer allows until pctx ends. Records
// carry ctx, not pctx: the end of the run must not fail buffered records.
func produceLoop(ctx, pctx context.Context, cl producer, ci int, c benchConfig, st *stats) {
	var seq uint64
	for pctx.Err() == nil {
		rec := &kgo.Record{Value: makeValue(ci, seq, c.size), Context: ctx}
		if c.parts > 0 {
			rec.Partition = int32(seq % uint64(c.parts))
		}
		seq++
		t0 := time.Now()
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			st.add(len(r.Value), time.Since(t0), err)
		})
	}
}

// reportLoop prints one interval line per tick until pctx ends or done is set.
func reportLoop(pctx context.Context, w io.Writer, tick <-chan time.Time, start time.Time, cls []producer, st *stats, done *atomic.Bool) {
	last := start
	for !done.Load() {
		select {
		case now := <-tick:
			var buf int64
			for _, cl := range cls {
				buf += cl.BufferedProduceRecords()
			}
			_, _ = fmt.Fprintln(w, formatInterval(st.takeInterval(), now.Sub(last), buf))
			last = now
		case <-pctx.Done():
			return
		}
	}
}

func run(c benchConfig, stdout, stderr io.Writer, nc clientFactory) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := clientOpts(c)
	cls := make([]producer, c.clients)
	for i := range cls {
		cl, err := nc(opts...)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "new client:", err)
			return 1
		}
		defer cl.Close()
		cls[i] = cl
	}

	st := newStats()
	start := time.Now()
	pctx, pcancel := context.WithDeadline(ctx, start.Add(c.dur))
	defer pcancel()

	var wg sync.WaitGroup
	for ci, cl := range cls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			produceLoop(ctx, pctx, cl, ci, c, st)
		}()
	}

	var done atomic.Bool
	tick := time.NewTicker(c.every)
	defer tick.Stop()
	report := make(chan struct{})
	go func() {
		defer close(report)
		reportLoop(pctx, stdout, tick.C, start, cls, st, &done)
	}()

	wg.Wait()
	<-report
	// Let in-flight records finish (or fail) before summarizing.
	fctx, fcancel := context.WithTimeout(context.Background(), 15*time.Second)
	for _, cl := range cls {
		_ = cl.Flush(fctx)
	}
	fcancel()
	done.Store(true)
	_, _ = fmt.Fprint(stdout, formatSummary(st.summarize(time.Since(start))))
	return 0
}
