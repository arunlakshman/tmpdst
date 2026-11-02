package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"mds/internal/controlplane/controlplaneclient"
	"mds/internal/core"
)

const (
	defaultLoadTopic = "verify-load"
	// commitDeadline is the most time one commit may take, retries included.
	commitDeadline = 60 * time.Second
	// attemptTimeout bounds one call, so a silent node cannot hold a commit for the whole deadline.
	attemptTimeout = 10 * time.Second
	// readyWait is how long the load waits for the control plane to answer GetStatus.
	readyWait = 60 * time.Second
	// progressEvery is the interval of the progress lines on stderr.
	progressEvery = 5 * time.Second

	firstBackoff = 50 * time.Millisecond
	maxBackoff   = 2 * time.Second
)

// commitCaller is the part of controlplaneclient.Client that a worker uses.
// A fake can stand in for it in tests.
type commitCaller interface {
	Call(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error)
}

// loadConfig holds the -mode load flags.
type loadConfig struct {
	target     string
	topic      string
	partitions int32
	batches    int
	records    int64
	runID      string
	workers    int
	duration   time.Duration
	rate       float64
	ackedOut   string
}

func (c loadConfig) validate() error {
	switch {
	case c.workers < 1:
		return errors.New("-workers must be at least 1")
	case c.duration <= 0:
		return errors.New("-duration must be positive")
	case c.partitions < 1:
		return errors.New("-partitions must be at least 1")
	case c.batches < 1:
		return errors.New("-batches must be at least 1")
	case c.records < 1:
		return errors.New("-records must be at least 1")
	case c.rate < 0:
		return errors.New("-rate cannot be negative")
	case c.runID == "" || strings.Contains(c.runID, "/"):
		return errors.New("-run-id must not be empty and must not contain /")
	}
	return nil
}

// ackedRange is one batch range that the control plane gave a commit.
type ackedRange struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Base      int64  `json:"base"`
	Count     int64  `json:"count"`
}

// ackedLine is one JSON line of the -acked-out file: a commit the control plane acked.
type ackedLine struct {
	Key      string       `json:"key"`
	Worker   int          `json:"worker"`
	Sequence int64        `json:"sequence"`
	Assigned []ackedRange `json:"assigned"`
	AckedAt  string       `json:"acked_at"`
}

// runLoadMain runs one load, prints the summary line on stdout, and returns the exit code.
func runLoadMain(cfg loadConfig) int {
	cfg.topic = cmp.Or(cfg.topic, defaultLoadTopic)
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		return 2
	}
	summary, err := runLoad(cfg)
	line, marshalErr := json.Marshal(summary)
	if marshalErr != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", marshalErr)
		return 1
	}
	fmt.Println(string(line))
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		return 1
	}
	if summary.Failed > 0 {
		return 1
	}
	return 0
}

// runLoad waits for the control plane, runs the workers for cfg.duration, and
// returns the summary. The error is a setup failure or a failed write to the
// acked file; the summary is valid either way.
func runLoad(cfg loadConfig) (loadSummary, error) {
	stats := newLoadStats()
	started := time.Now()
	err := runLoadWith(cfg, stats)
	return stats.summary(time.Since(started)), err
}

func runLoadWith(cfg loadConfig, stats *loadStats) error {
	client := controlplaneclient.New(cfg.target, nil)
	defer func() { _ = client.Close() }()
	if err := prepareLoad(client, cfg); err != nil {
		return err
	}
	acked, err := openAckedFile(cfg.ackedOut)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go reportProgress(stats, time.Now(), done)
	runWorkers(cfg, client, acked, stats, time.Now().Add(cfg.duration))
	close(done)
	closeErr := acked.close()
	return errors.Join(stats.writeError(), closeErr)
}

// prepareLoad waits until the control plane answers, then creates the topic if it is missing.
func prepareLoad(client *controlplaneclient.Client, cfg loadConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), readyWait)
	defer cancel()
	if err := waitReady(ctx, client, os.Stderr); err != nil {
		return err
	}
	err := client.CreateTopic(ctx, cfg.topic, cfg.partitions)
	if be, ok := errors.AsType[*controlplaneclient.Error](err); ok && be.Code == core.ErrTopicExists {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	fmt.Fprintf(os.Stderr, "topic %s ready with %d partitions\n", cfg.topic, cfg.partitions)
	return nil
}

// runWorkers starts cfg.workers workers and waits until all have stopped.
func runWorkers(cfg loadConfig, caller commitCaller, acked *ackedFile, stats *loadStats, end time.Time) {
	interval := paceInterval(cfg.rate, cfg.workers)
	var wg sync.WaitGroup
	for id := range cfg.workers {
		worker := &loadWorker{
			id: id, caller: caller, cfg: cfg, stats: stats, acked: acked,
			deadline: commitDeadline, interval: interval,
		}
		wg.Go(func() { worker.run(end) })
	}
	wg.Wait()
}

// reportProgress prints one line to stderr every progressEvery until done is closed.
func reportProgress(stats *loadStats, started time.Time, done <-chan struct{}) {
	ticker := time.NewTicker(progressEvery)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			acked, failed, retries := stats.counts()
			fmt.Fprintf(os.Stderr, "progress: elapsed=%s acked=%d failed=%d retries=%d\n",
				time.Since(started).Round(time.Second), acked, failed, retries)
		}
	}
}

// loadKey is the segment key of one commit. The run id, the worker and the
// sequence make it unique. A retry of the same commit uses the same key.
func loadKey(runID string, worker int, sequence int64) string {
	return fmt.Sprintf("verify/%s/w%d/%d", runID, worker, sequence)
}

// buildLoadCommit makes the commit with that key. Its batches rotate over the partitions.
func buildLoadCommit(cfg loadConfig, worker int, sequence int64) core.CommitSegment {
	key := loadKey(cfg.runID, worker, sequence)
	return makeCommit(cfg.topic, key, cfg.batches, cfg.records, func(b int) int32 {
		return int32((sequence + int64(worker) + int64(b)) % int64(cfg.partitions))
	})
}

// classifyFailure names a failed call for errors_by_kind. It also says whether
// the commit may be sent again: a call that never reached a node, an
// unavailable or timed-out call, and a call whose effect is unknown (MaybeApplied).
// A business answer such as ErrInvalid or ErrConflict is final.
func classifyFailure(operation core.ControlPlaneOperation, err error) (string, bool) {
	code, outcome := controlplaneclient.Classifier{}.Classify(core.ControlPlaneCall{Op: operation}, err)
	_, isCallError := errors.AsType[*controlplaneclient.CallError](err)
	retry := isCallError || code == core.ErrUnavailable || code == core.ErrTimeout || outcome == core.MaybeApplied
	return code.String(), retry
}

// backoffFor is the wait before retry number attempt (0 is the first retry):
// 50ms, 100ms, 200ms, ... up to 2s.
func backoffFor(attempt int) time.Duration {
	wait := firstBackoff
	for range attempt {
		wait *= 2
		if wait >= maxBackoff {
			return maxBackoff
		}
	}
	return wait
}

// paceInterval is how far apart one worker starts its commits so that all
// workers together reach rate commits per second. 0 means no pacing.
func paceInterval(rate float64, workers int) time.Duration {
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(workers) / rate * float64(time.Second))
}

// nextStart is when a worker may start its next commit. A worker that fell
// behind starts at once, and does not send a burst to catch up.
func nextStart(planned, now time.Time, interval time.Duration) time.Time {
	next := planned.Add(interval)
	if next.Before(now) {
		return now
	}
	return next
}

// loadWorker commits segments one after another until the run ends.
type loadWorker struct {
	id       int
	caller   commitCaller
	cfg      loadConfig
	stats    *loadStats
	acked    *ackedFile
	deadline time.Duration // per commit; a field so that a test can use a short one
	interval time.Duration // 0 means no pacing
}

// run commits until end. The first commit starts at once.
func (w *loadWorker) run(end time.Time) {
	planned := time.Now()
	for sequence := int64(0); time.Now().Before(end); sequence++ {
		if w.interval > 0 {
			time.Sleep(time.Until(planned))
			planned = nextStart(planned, time.Now(), w.interval)
		}
		w.commitOne(sequence)
	}
}

// commitOne sends one commit until it is acked, or until its deadline passes.
// Every retry sends the same commit with the same key, so the control plane
// answers a retry with the ranges of the first answer.
func (w *loadWorker) commitOne(sequence int64) {
	operation := buildLoadCommit(w.cfg, w.id, sequence)
	started := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), started.Add(w.deadline))
	defer cancel()
	for attempt := 0; ; attempt++ {
		response, err := w.callOnce(ctx, operation)
		if err == nil {
			w.recordAck(sequence, operation, response, started)
			return
		}
		kind, retry := classifyFailure(operation, err)
		w.stats.recordError(kind)
		if !retry || !waitBeforeRetry(ctx, attempt) {
			w.stats.recordFailed()
			return
		}
		w.stats.recordRetry()
	}
}

// callOnce makes one call, with its own timeout inside the commit deadline.
func (w *loadWorker) callOnce(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	return w.caller.Call(callCtx, operation)
}

// waitBeforeRetry sleeps the backoff for this attempt. It returns false when
// the commit deadline passes first.
func waitBeforeRetry(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(backoffFor(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// recordAck writes the acked line first, then counts the ack.
// A write error is kept, and the run exits with status 1.
func (w *loadWorker) recordAck(sequence int64, operation core.CommitSegment, response core.ControlPlaneResponse, started time.Time) {
	resp, _ := response.(core.CommitSegmentResp)
	line := ackedLine{
		Key:      operation.SegmentKey,
		Worker:   w.id,
		Sequence: sequence,
		Assigned: toAckedRanges(resp.Assigned),
		AckedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := w.acked.write(line); err != nil {
		w.stats.noteWriteError(err)
	}
	w.stats.recordAck(time.Since(started))
}

func toAckedRanges(ranges []core.BatchRange) []ackedRange {
	out := make([]ackedRange, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, ackedRange{
			Topic:     r.TopicPartition.Topic,
			Partition: r.TopicPartition.Partition,
			Base:      int64(r.Base),
			Count:     r.Count,
		})
	}
	return out
}

// ackedFile appends one JSON line per acked commit. All workers share it, so writes are serialised.
type ackedFile struct {
	mu   sync.Mutex
	file *os.File // nil when -acked-out is not given
}

func openAckedFile(path string) (*ackedFile, error) {
	if path == "" {
		return &ackedFile{}, nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // the path is the -acked-out flag that the operator gave
	if err != nil {
		return nil, fmt.Errorf("open -acked-out: %w", err)
	}
	return &ackedFile{file: file}, nil
}

// write appends one line with a single write call. The line is in the OS
// before the next commit is started, so a killed verifier loses at most a
// line that was being written.
func (a *ackedFile) write(line ackedLine) error {
	if a.file == nil {
		return nil
	}
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err = a.file.Write(append(data, '\n'))
	return err
}

func (a *ackedFile) close() error {
	if a.file == nil {
		return nil
	}
	syncErr := a.file.Sync()
	closeErr := a.file.Close()
	return errors.Join(syncErr, closeErr)
}
