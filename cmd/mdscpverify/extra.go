package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mds/internal/controlplane/controlplaneclient"
	"mds/internal/core"
)

// runReadyMain exits 0 when the control plane answers GetStatus within 10 seconds.
func runReadyMain(target string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := controlplaneclient.New(target, nil)
	defer func() { _ = client.Close() }()
	status, err := client.GetStatus(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "not ready:", err)
		return 1
	}
	fmt.Printf("ready role=%s applied_slot=%d log_tail=%d\n", status.Role, status.AppliedSlot, status.LogTail)
	return 0
}

// recordsPerPartition adds up the records of every acked key in the file.
// A key that appears twice is counted once. A cut last line is ignored.
func recordsPerPartition(path string) (map[core.TopicPartition]int64, int, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = file.Close() }()
	totals := map[core.TopicPartition]int64{}
	seen := map[string]bool{}
	reader := bufio.NewReaderSize(file, 1<<20)
	for {
		line, readErr := reader.ReadString('\n')
		if strings.HasSuffix(line, "\n") {
			var acked ackedLine
			if err := json.Unmarshal([]byte(line), &acked); err != nil {
				return nil, 0, fmt.Errorf("bad line in %s: %w", path, err)
			}
			if !seen[acked.Key] {
				seen[acked.Key] = true
				for _, r := range acked.Assigned {
					totals[core.NewTopicPartition(r.Topic, r.Partition)] += r.Count
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	return totals, len(seen), nil
}

// runEndsMain asks the live control plane for the end offset of every
// partition in the acked file and compares it with the records acked.
// This proves the running cluster (not only the log in DynamoDB) is right.
func runEndsMain(target, ackedPath string) int {
	totals, keys, err := recordsPerPartition(ackedPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := controlplaneclient.New(target, nil)
	defer func() { _ = client.Close() }()

	partitions := make([]core.TopicPartition, 0, len(totals))
	for tp := range totals {
		partitions = append(partitions, tp)
	}
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
	wrong := 0
	for _, tp := range partitions {
		response, err := client.Call(ctx, core.FindSegments{TopicPartition: tp, From: 0, MaxBytes: 1})
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: find segments %s/%d: %v\n", tp.Topic, tp.Partition, err)
			return 1
		}
		end := int64(response.(core.FindSegmentsResp).End)
		if end != totals[tp] {
			wrong++
			fmt.Printf("MISMATCH %s/%d: cluster end offset %d, records acked %d\n", tp.Topic, tp.Partition, end, totals[tp])
		}
	}
	fmt.Printf("ends: %d acked keys, %d partitions checked, %d wrong\n", keys, len(partitions), wrong)
	if wrong > 0 {
		return 1
	}
	fmt.Println("PASS")
	return 0
}

// runBigMain sends one commit that is too big for one log entry and expects
// ErrTooLarge. Then a normal commit must still work (the log is not harmed).
func runBigMain(target, topic string, bigBatches int) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := controlplaneclient.New(target, nil)
	defer func() { _ = client.Close() }()
	if _, err := waitStatus(ctx, client); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		return 1
	}
	const partitions = 256
	if err := client.CreateTopic(ctx, topic, partitions); err != nil {
		if _, isBusiness := errors.AsType[*controlplaneclient.Error](err); !isBusiness { // "already exists" is fine
			fmt.Fprintln(os.Stderr, "FAIL: create topic:", err)
			return 1
		}
	}
	big := core.CommitSegment{SegmentKey: fmt.Sprintf("verify/%s/big-%d", topic, time.Now().UnixNano())}
	for i := range bigBatches {
		big.Batches = append(big.Batches, core.PendingBatch{
			TopicPartition: core.NewTopicPartition(topic, int32(i%partitions)),
			Records:        1,
			Loc:            core.SegmentRef{Key: big.SegmentKey, Off: int64(i) * 100, Len: 100},
		})
	}
	_, err := client.Call(ctx, big)
	business, isBusiness := errors.AsType[*controlplaneclient.Error](err)
	switch {
	case err == nil:
		fmt.Printf("FAIL: a commit of %d batches was accepted; the entry size limit did not apply\n", bigBatches)
		return 1
	case !isBusiness || business.Code != core.ErrTooLarge:
		fmt.Printf("FAIL: big commit gave %v, want ErrTooLarge\n", err)
		return 1
	}
	fmt.Printf("big commit of %d batches refused with %s\n", bigBatches, business.Code)

	small := buildCommit(topic, partitions, 0, 3, 10)
	small.SegmentKey += fmt.Sprintf("-after-big-%d", time.Now().UnixNano())
	for i := range small.Batches {
		small.Batches[i].Loc.Key = small.SegmentKey
	}
	if _, err := client.Call(ctx, small); err != nil {
		fmt.Println("FAIL: a normal commit after the big one failed:", err)
		return 1
	}
	fmt.Println("normal commit after the big one: acked")
	fmt.Println("PASS")
	return 0
}

func waitStatus(ctx context.Context, client *controlplaneclient.Client) (string, error) {
	for {
		status, err := client.GetStatus(ctx)
		if err == nil {
			return status.Role, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("control plane never became ready: %w", err)
		case <-time.After(time.Second):
		}
	}
}
