// Command mdscpverify drives a running control plane over gRPC and checks the
// answers. It needs no broker and no Kafka client.
//
// Two modes:
//
//	-mode smoke (default): a few checks on a fresh topic
//	 1. wait until the control plane is ready (GetStatus answers)
//	 2. create a topic
//	 3. commit many segments over several partitions
//	 4. send the same commits again: the answers must be identical (idempotency)
//	 5. per partition: offsets are contiguous, never overlap, and the end offset
//	    equals the number of records that were acked
//
//	-mode load: keep committing for -duration with -workers workers. A commit
//	 that fails in a way that may be retried is sent again with the same key
//	 until it is acked. Every ack is written to -acked-out, for mdscpinspect
//	 -check -acked. Used in failure experiments (nodes killed during the load).
//
// Exit code 0 means every check passed (smoke) or no commit failed (load).
// Not part of the determinism gate.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"mds/internal/controlplane/controlplaneclient"
	"mds/internal/core"
)

func main() {
	mode := flag.String("mode", "smoke", "smoke, load (sustained commits with retries), ready, ends (compare cluster end offsets with -acked), big (one oversized commit)")
	target := flag.String("cp", "127.0.0.1:8080", "control plane address(es), comma separated")
	topic := flag.String("topic", "", "topic name (smoke default: verify-<unix time>; load default: verify-load)")
	partitions := flag.Int("partitions", 4, "number of partitions")
	commits := flag.Int("commits", 50, "smoke: number of segment commits")
	batchesPer := flag.Int("batches", 3, "batches in each commit")
	records := flag.Int64("records", 10, "records in each batch")
	duration := flag.Duration("duration", 60*time.Second, "load: how long to keep committing")
	workers := flag.Int("workers", 4, "load: concurrent committers")
	rate := flag.Float64("rate", 0, "load: total commits per second (0 = as fast as possible)")
	runID := flag.String("run-id", time.Now().UTC().Format("20060102-150405"), "load: part of every segment key, so keys are unique per run")
	ackedOut := flag.String("acked-out", "", "load: append one JSON line per acked commit to this file")
	ackedIn := flag.String("acked", "", "ends: the acked file written by a load run")
	bigBatches := flag.Int("big-batches", 40000, "big: batches in the oversized commit")
	flag.Parse()

	switch *mode {
	case "smoke":
		name := cmp.Or(*topic, fmt.Sprintf("verify-%d", time.Now().Unix()))
		if err := run(*target, name, int32(*partitions), *commits, *batchesPer, *records); err != nil {
			fmt.Fprintln(os.Stderr, "FAIL:", err)
			os.Exit(1)
		}
		fmt.Println("PASS")
	case "load":
		os.Exit(runLoadMain(loadConfig{
			target: *target, topic: *topic, partitions: int32(*partitions),
			batches: *batchesPer, records: *records, runID: *runID,
			workers: *workers, duration: *duration, rate: *rate, ackedOut: *ackedOut,
		}))
	case "ready":
		os.Exit(runReadyMain(*target))
	case "ends":
		if *ackedIn == "" {
			fmt.Fprintln(os.Stderr, "FAIL: -mode ends needs -acked FILE")
			os.Exit(2)
		}
		os.Exit(runEndsMain(*target, *ackedIn))
	case "big":
		name := cmp.Or(*topic, "verify-big")
		os.Exit(runBigMain(*target, name, *bigBatches))
	default:
		fmt.Fprintf(os.Stderr, "FAIL: unknown -mode %q (want smoke, load, ready, ends or big)\n", *mode)
		os.Exit(2)
	}
}

func run(target, topic string, partitions int32, commits, batchesPer int, records int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client := controlplaneclient.New(target, nil)
	defer func() { _ = client.Close() }()

	if err := waitReady(ctx, client, os.Stdout); err != nil {
		return err
	}
	if err := client.CreateTopic(ctx, topic, partitions); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	fmt.Printf("created topic %s with %d partitions\n", topic, partitions)

	// Step 3: commit, and remember every acked answer.
	acked := make([]core.CommitSegmentResp, commits)
	started := time.Now()
	for i := range commits {
		operation := buildCommit(topic, partitions, i, batchesPer, records)
		response, err := client.Call(ctx, operation)
		if err != nil {
			return fmt.Errorf("commit %d: %w", i, err)
		}
		acked[i] = response.(core.CommitSegmentResp)
	}
	fmt.Printf("%d commits acked in %s\n", commits, time.Since(started).Round(time.Millisecond))

	// Step 4: replay. Same key must give the same answer.
	for i := range commits {
		operation := buildCommit(topic, partitions, i, batchesPer, records)
		response, err := client.Call(ctx, operation)
		if err != nil {
			return fmt.Errorf("replay %d: %w", i, err)
		}
		if !reflect.DeepEqual(response.(core.CommitSegmentResp), acked[i]) {
			return fmt.Errorf("replay %d gave a different answer:\n first:  %+v\n second: %+v", i, acked[i], response)
		}
	}
	fmt.Println("idempotency ok: replay gave identical answers")

	return checkOffsets(ctx, client, topic, partitions, acked)
}

// waitReady polls GetStatus until the control plane answers, and reports the
// status line to out.
func waitReady(ctx context.Context, client *controlplaneclient.Client, out io.Writer) error {
	for {
		status, err := client.GetStatus(ctx)
		if err == nil {
			_, writeErr := fmt.Fprintf(out, "control plane role=%s applied_slot=%d log_tail=%d\n", status.Role, status.AppliedSlot, status.LogTail)
			return writeErr
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("control plane never became ready: %w", err)
		case <-time.After(time.Second):
		}
	}
}

// buildCommit makes commit number i. The segment key is a pure function of i,
// so the replay in step 4 sends exactly the same commit.
func buildCommit(topic string, partitions int32, i, batchesPer int, records int64) core.CommitSegment {
	key := fmt.Sprintf("verify/%s/segment-%06d", topic, i)
	return makeCommit(topic, key, batchesPer, records, func(b int) int32 {
		return int32((i + b) % int(partitions))
	})
}

// makeCommit builds a commit with the given segment key and batches. The
// partition of batch b comes from partitionOf.
func makeCommit(topic, key string, batches int, records int64, partitionOf func(b int) int32) core.CommitSegment {
	operation := core.CommitSegment{SegmentKey: key}
	for b := range batches {
		operation.Batches = append(operation.Batches, core.PendingBatch{
			TopicPartition: core.NewTopicPartition(topic, partitionOf(b)),
			Records:        records,
			Loc:            core.SegmentRef{Key: key, Off: int64(b) * 1000, Len: 1000},
		})
	}
	return operation
}

// checkOffsets proves, per partition, that the acked ranges tile [0, end)
// with no gap and no overlap, and that the control plane reports the same end.
func checkOffsets(ctx context.Context, client *controlplaneclient.Client, topic string, partitions int32, acked []core.CommitSegmentResp) error {
	type span struct{ base, count int64 }
	perPartition := make(map[int32][]span)
	for _, response := range acked {
		for _, batch := range response.Assigned {
			p := batch.TopicPartition.Partition
			perPartition[p] = append(perPartition[p], span{int64(batch.Base), batch.Count})
		}
	}
	for p := range partitions {
		var next int64
		for _, s := range perPartition[p] {
			if s.base != next {
				return fmt.Errorf("partition %d: expected next offset %d, got base %d (gap or overlap)", p, next, s.base)
			}
			next += s.count
		}
		response, err := client.Call(ctx, core.FindSegments{
			TopicPartition: core.NewTopicPartition(topic, p), From: 0, MaxBytes: 1 << 30,
		})
		if err != nil {
			return fmt.Errorf("find segments %d: %w", p, err)
		}
		found := response.(core.FindSegmentsResp)
		if int64(found.End) != next {
			return fmt.Errorf("partition %d: control plane end offset %d, acked records %d", p, found.End, next)
		}
		fmt.Printf("partition %d ok: %d records, end offset %d\n", p, next, found.End)
	}
	return nil
}
