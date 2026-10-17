// Command manibench is a load generator for manibroker, modelled on the
// WarpStream CLI benchmark-producer.
//
// Usage:
//
//	manibench benchmark-producer -topic T [-brokers 127.0.0.1:9092]
//	    [-num-clients 1] [-record-size 1024] [-duration 30s] [-interval 5s]
//	    [-partitions 0] [-max-inflight 0] [-buffered-records 5000]
//	    [-cp-url http://127.0.0.1:8080 -create-partitions N]
//
// Each client is its own kgo.Client (idempotent, acks=all, no compression:
// the broker rejects compressed batches, decision 33). It produces as fast as
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

func main() {
	if len(os.Args) < 2 || os.Args[1] != "benchmark-producer" {
		fmt.Fprintln(os.Stderr, "usage: manibench benchmark-producer -topic T [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("benchmark-producer", flag.ExitOnError)
	topic := fs.String("topic", "", "topic to produce to (required)")
	brokers := fs.String("brokers", "127.0.0.1:9092", "comma-separated seed brokers")
	clients := fs.Int("num-clients", 1, "number of independent kgo clients")
	size := fs.Int("record-size", 1024, "record value size in bytes")
	dur := fs.Duration("duration", 30*time.Second, "how long to produce")
	every := fs.Duration("interval", 5*time.Second, "reporting interval")
	parts := fs.Int("partitions", 0, "0: kgo uniform-bytes sticky partitioner; N: round-robin per record over partitions 0..N-1")
	inflight := fs.Int("max-inflight", 0, "kgo MaxProduceRequestsInflightPerBroker (0: kgo default, 1; max 5 with idempotence)")
	buffered := fs.Int("buffered-records", 5000, "kgo MaxBufferedRecords per client")
	cpURL := fs.String("cp-url", "", "control plane URL, used with -create-partitions")
	createParts := fs.Int("create-partitions", 0, "if > 0, POST /v1/create-topic first")
	_ = fs.Parse(os.Args[2:])
	if *topic == "" || *clients < 1 || *size < 8 {
		fs.Usage()
		os.Exit(2)
	}
	if *createParts > 0 {
		if err := createTopic(*cpURL, *topic, *createParts); err != nil {
			fmt.Fprintln(os.Stderr, "create topic:", err)
			os.Exit(1)
		}
	}
	os.Exit(run(*topic, strings.Split(*brokers, ","), *clients, *size, *dur, *every, *parts, *inflight, *buffered))
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
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

func run(topic string, seeds []string, nClients, size int, dur, every time.Duration, parts, inflight, buffered int) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.MaxBufferedRecords(buffered),
		kgo.DefaultProduceTopic(topic),
	}
	if parts > 0 {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	} else {
		opts = append(opts, kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)))
	}
	if inflight > 0 {
		opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(inflight))
	}
	cls := make([]*kgo.Client, nClients)
	for i := range cls {
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			fmt.Fprintln(os.Stderr, "new client:", err)
			return 1
		}
		defer cl.Close()
		cls[i] = cl
	}

	st := newStats()
	start := time.Now()
	deadline := start.Add(dur)
	pctx, pcancel := context.WithDeadline(ctx, deadline)
	defer pcancel()

	var wg sync.WaitGroup
	for ci, cl := range cls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq uint64
			for pctx.Err() == nil {
				val := make([]byte, size)
				binary.BigEndian.PutUint32(val, uint32(ci))
				binary.BigEndian.PutUint32(val[4:], uint32(seq))
				for j := 8; j < size; j++ {
					val[j] = byte('a' + j%26)
				}
				rec := &kgo.Record{Value: val}
				if parts > 0 {
					rec.Partition = int32(seq % uint64(parts))
				}
				seq++
				t0 := time.Now()
				cl.Produce(pctx, rec, func(r *kgo.Record, err error) {
					st.add(len(r.Value), time.Since(t0), err)
				})
			}
		}()
	}

	var done atomic.Bool
	tick := time.NewTicker(every)
	defer tick.Stop()
	report := make(chan struct{})
	go func() {
		defer close(report)
		last := start
		for !done.Load() {
			select {
			case now := <-tick.C:
				var buf int64
				for _, cl := range cls {
					buf += cl.BufferedProduceRecords()
				}
				fmt.Println(formatInterval(st.takeInterval(), now.Sub(last), buf))
				last = now
			case <-pctx.Done():
				return
			}
		}
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
	fmt.Print(formatSummary(st.summarize(time.Since(start))))
	return 0
}
