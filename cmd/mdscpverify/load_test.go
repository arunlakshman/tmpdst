package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"mds/internal/controlplane/controlplaneclient"
	"mds/internal/core"
)

// fakeCaller fails its first failures calls with failure (every call when
// always is set), then acks. It records the segment key of every call.
type fakeCaller struct {
	failures int
	failure  error
	always   bool
	keys     []string
}

func (f *fakeCaller) Call(_ context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	commit, ok := operation.(core.CommitSegment)
	if !ok {
		return nil, errors.New("fake only takes commits")
	}
	f.keys = append(f.keys, commit.SegmentKey)
	if f.always || len(f.keys) <= f.failures {
		return nil, f.failure
	}
	var resp core.CommitSegmentResp
	var offset core.Offset
	for _, b := range commit.Batches {
		resp.Assigned = append(resp.Assigned, core.BatchRange{
			TopicPartition: b.TopicPartition, Base: offset, Count: b.Records, Loc: b.Loc,
		})
		offset += core.Offset(b.Records)
	}
	return resp, nil
}

// testWorker builds a worker that writes its acks to a file in the test's temp dir.
func testWorker(t *testing.T, caller commitCaller, deadline time.Duration) (*loadWorker, *loadStats, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acked.jsonl")
	acked, err := openAckedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = acked.close() })
	stats := newLoadStats()
	cfg := loadConfig{topic: "t", partitions: 2, batches: 2, records: 5, runID: "run-test", workers: 1, duration: time.Second}
	w := &loadWorker{id: 3, caller: caller, cfg: cfg, stats: stats, acked: acked, deadline: deadline}
	return w, stats, path
}

// TestRetrySendsSameKey: two retryable failures, then an ack. All three calls
// carry the same segment key, and the acked file gets exactly one line.
func TestRetrySendsSameKey(t *testing.T) {
	caller := &fakeCaller{
		failures: 2,
		failure:  &controlplaneclient.CallError{Op: "CommitSegment", Sent: true, Code: codes.Unavailable, Err: errors.New("node down")},
	}
	w, stats, path := testWorker(t, caller, 10*time.Second)
	w.commitOne(0)

	want := loadKey("run-test", 3, 0)
	if len(caller.keys) != 3 {
		t.Fatalf("calls = %d, want 3", len(caller.keys))
	}
	for i, key := range caller.keys {
		if key != want {
			t.Fatalf("call %d used key %q, want %q", i, key, want)
		}
	}
	acked, failed, retries := stats.counts()
	if acked != 1 || failed != 0 || retries != 2 {
		t.Fatalf("acked=%d failed=%d retries=%d, want 1, 0, 2", acked, failed, retries)
	}
	if n := stats.summary(0).ErrorsByKind["ErrUnavailable"]; n != 2 {
		t.Fatalf("ErrUnavailable count = %d, want 2", n)
	}

	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("acked file has %d lines, want 1", len(lines))
	}
	var line ackedLine
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatal(err)
	}
	if line.Key != want || line.Worker != 3 || line.Sequence != 0 || len(line.Assigned) != 2 {
		t.Fatalf("acked line %+v", line)
	}
	// batch 1 of sequence 0 on worker 3 goes to partition (0+3+1) % 2 = 0, at offset 5
	if line.Assigned[1].Partition != 0 || line.Assigned[1].Base != 5 || line.Assigned[1].Count != 5 || line.Assigned[1].Topic != "t" {
		t.Fatalf("second range %+v", line.Assigned[1])
	}
}

// TestBusinessErrorIsNotRetried: ErrConflict is an answer, so the commit fails at once.
func TestBusinessErrorIsNotRetried(t *testing.T) {
	caller := &fakeCaller{always: true, failure: &controlplaneclient.Error{Code: core.ErrConflict}}
	w, stats, _ := testWorker(t, caller, 10*time.Second)
	w.commitOne(0)

	if len(caller.keys) != 1 {
		t.Fatalf("calls = %d, want 1", len(caller.keys))
	}
	acked, failed, retries := stats.counts()
	if acked != 0 || failed != 1 || retries != 0 {
		t.Fatalf("acked=%d failed=%d retries=%d, want 0, 1, 0", acked, failed, retries)
	}
}

// TestGivesUpAtDeadline: a node that never answers makes the commit fail when its deadline passes.
func TestGivesUpAtDeadline(t *testing.T) {
	caller := &fakeCaller{always: true, failure: &controlplaneclient.CallError{Op: "CommitSegment", Sent: true, Code: codes.Unavailable, Err: errors.New("down")}}
	w, stats, _ := testWorker(t, caller, 150*time.Millisecond)
	started := time.Now()
	w.commitOne(0)

	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("gave up after %s, want about the 150ms deadline", elapsed)
	}
	acked, failed, retries := stats.counts()
	if acked != 0 || failed != 1 || retries < 1 {
		t.Fatalf("acked=%d failed=%d retries=%d, want 0, 1, at least 1", acked, failed, retries)
	}
}

// readLines returns the lines of a file the test wrote.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // the test reads the file it just wrote in t.TempDir
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, string(data[start:i]))
			start = i + 1
		}
	}
	return lines
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	cases := map[int]time.Duration{
		0: 50 * time.Millisecond, 1: 100 * time.Millisecond, 2: 200 * time.Millisecond,
		5: 1600 * time.Millisecond, 6: 2 * time.Second, 100: 2 * time.Second,
	}
	for attempt, want := range cases {
		if got := backoffFor(attempt); got != want {
			t.Errorf("backoffFor(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestLatencyPercentilesNearestRank(t *testing.T) {
	var values []time.Duration
	for i := 100; i >= 1; i-- { // unsorted on purpose
		values = append(values, time.Duration(i)*time.Millisecond)
	}
	got := summarizeLatencies(values)
	if got.P50 != 50 || got.P99 != 99 || got.Max != 100 {
		t.Fatalf("latency summary %+v, want p50 50 p99 99 max 100", got)
	}
	if empty := summarizeLatencies(nil); empty != (latencySummary{}) {
		t.Fatalf("empty summary %+v, want zeros", empty)
	}
}

func TestStallTrackerKeepsLongestGap(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	var tracker stallTracker
	tracker.add(base)
	if tracker.longest != 0 {
		t.Fatalf("one ack has a gap %s", tracker.longest)
	}
	tracker.add(base.Add(2 * time.Second))
	tracker.add(base.Add(3 * time.Second))
	tracker.add(base.Add(10 * time.Second))
	tracker.add(base.Add(11 * time.Second))
	if tracker.longest != 7*time.Second {
		t.Fatalf("longest gap %s, want 7s", tracker.longest)
	}
}

func TestLoadKeyIsUniquePerRunWorkerSequence(t *testing.T) {
	seen := map[string]bool{}
	for _, run := range []string{"r1", "r2"} {
		for worker := range 3 {
			for sequence := range int64(3) {
				key := loadKey(run, worker, sequence)
				if seen[key] {
					t.Fatalf("key %q made twice", key)
				}
				seen[key] = true
			}
		}
	}
	if got, want := loadKey("r1", 2, 7), "verify/r1/w2/7"; got != want {
		t.Fatalf("key %q, want %q", got, want)
	}
}

func TestPacingDoesNotBurstAfterStall(t *testing.T) {
	base := time.Unix(2_000_000, 0)
	if got := paceInterval(10, 4); got != 400*time.Millisecond {
		t.Fatalf("interval %s, want 400ms", got)
	}
	if got := paceInterval(0, 4); got != 0 {
		t.Fatalf("rate 0 interval %s, want 0", got)
	}
	if got := nextStart(base, base, 100*time.Millisecond); !got.Equal(base.Add(100 * time.Millisecond)) {
		t.Fatalf("on time: next start %s", got)
	}
	late := base.Add(500 * time.Millisecond)
	if got := nextStart(base, late, 100*time.Millisecond); !got.Equal(late) {
		t.Fatalf("after a stall: next start %s, want now", got)
	}
}

// TestSummaryFieldNames checks the JSON names that other tools read.
func TestSummaryFieldNames(t *testing.T) {
	data, err := json.Marshal(loadSummary{ErrorsByKind: map[string]int{"ErrIO": 1}})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"acked", "failed", "retries", "errors_by_kind", "latency_ms", "max_stall_ms", "duration_s"} {
		if _, ok := fields[name]; !ok {
			t.Errorf("summary has no %q field: %s", name, data)
		}
	}
	var latency map[string]json.RawMessage
	if err := json.Unmarshal(fields["latency_ms"], &latency); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"p50", "p99", "max"} {
		if _, ok := latency[name]; !ok {
			t.Errorf("latency_ms has no %q field", name)
		}
	}
}
