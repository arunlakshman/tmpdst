package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater/pebblestore"
	"github.com/manifesterio/mds/internal/core"
)

const (
	entryByteLimit  = 350 * 1024 // Config.MaxEntryBytes default
	dynamoItemLimit = 400 * 1024
)

// checker prints one PASS or FAIL line per check and remembers failures.
// Lines go to out, or to standard output when out is nil (tests set out).
type checker struct {
	failed int
	out    io.Writer
}

func (c *checker) printf(format string, args ...any) {
	w := c.out
	if w == nil {
		w = os.Stdout
	}
	_, _ = fmt.Fprintf(w, format, args...)
}

func (c *checker) check(name string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		c.failed++
	}
	c.printf("%s  %-52s %s\n", status, name, detail)
}

func (c *checker) note(name, detail string) { c.printf("SKIP  %-52s %s\n", name, detail) }

// runChecks verifies the stored log by itself, without asking the control plane.
// meta holds the META items read from the same table.
// ackedPath is the optional acked file from mdscpverify; "" skips the acked checks.
// It exits with status 1 when any check fails.
func runChecks(ctx context.Context, rows []row, meta metaItems, s3Client *s3.Client, bucket, checkpointPrefix, ackedPath string) {
	slots, _ := decodeRows(rows)
	c := &checker{}
	fmt.Printf("== checks on %d log slots\n", len(slots))
	if len(slots) == 0 {
		c.check("log has slots", false, "table has no LOG rows")
		os.Exit(1)
	}

	// Slots below the first live slot were trimmed, so a log that starts above 0 is trimmed.
	trimmed := slots[0].slot > 0
	checkStructure(c, slots)
	checkOneWriterPerEpoch(c, slots)
	checkStoredEpochNotBehind(c, slots, meta)
	noteStrayRows(c, slots, meta)
	entries := orderedEntries(slots)
	var replayHash uint64 // zero when no full replay was done
	if trimmed {
		c.note("full replay checks", fmt.Sprintf("log starts at slot %d (trimmed); only checkpoint plus tail can be replayed", slots[0].slot))
	} else {
		replayHash = checkFullReplay(c, slots, entries)
	}
	if ackedPath != "" {
		checkAcked(c, ackedPath, entries, trimmed)
	}
	if bucket != "" {
		checkCheckpoint(ctx, c, s3Client, bucket, checkpointPrefix, slots, trimmed, replayHash)
	}

	fmt.Printf("== %d check(s) failed\n", c.failed)
	if c.failed > 0 {
		os.Exit(1)
	}
}

// checkStructure covers the log shape. The contiguity check runs from the first
// live slot, so a trimmed log (one that starts above 0) is checked from its start.
func checkStructure(c *checker, slots []slotInfo) {
	missing, undecodable, tooBig, noWriter := 0, 0, 0, 0
	for i, s := range slots {
		if i > 0 && s.slot != slots[i-1].slot+1 {
			missing++
		}
		if s.bad != "" {
			undecodable++
		}
		if s.size > entryByteLimit {
			tooBig++
		}
		if s.writer == "" {
			noWriter++
		}
	}
	c.check("slots are contiguous (none missing)", missing == 0, fmt.Sprintf("%d missing, first slot %d, last slot %d", missing, slots[0].slot, slots[len(slots)-1].slot))
	c.check("every slot decodes", undecodable == 0, fmt.Sprintf("%d undecodable", undecodable))
	c.check(fmt.Sprintf("every slot <= %d KiB (DynamoDB limit %d KiB)", entryByteLimit/1024, dynamoItemLimit/1024), tooBig == 0, fmt.Sprintf("%d too big", tooBig))
	c.check("every slot names its writer", noWriter == 0, fmt.Sprintf("%d without writer", noWriter))
}

// checkOneWriterPerEpoch: one epoch has one writer name. Entries without an
// envelope, and entries with epoch 0, are skipped.
func checkOneWriterPerEpoch(c *checker, slots []slotInfo) {
	writerOf := map[uint64]string{}
	clashes, detail := 0, ""
	for _, s := range slots {
		if !s.enveloped || s.epoch == 0 {
			continue
		}
		prev, seen := writerOf[s.epoch]
		if !seen {
			writerOf[s.epoch] = s.writer
			continue
		}
		if prev != s.writer {
			clashes++
			if detail == "" {
				detail = fmt.Sprintf("epoch %d has two writers: %q and %q (slot %d)", s.epoch, prev, s.writer, s.slot)
			}
		}
	}
	if clashes > 0 {
		detail = fmt.Sprintf("%d epoch(s) with two writers; first: %s", clashes, detail)
	} else {
		detail = fmt.Sprintf("%d epoch(s) checked", len(writerOf))
	}
	c.check("one epoch has one writer", clashes == 0, detail)
}

// checkStoredEpochNotBehind: the stored writer epoch is at least the highest
// epoch in any log entry. A stored epoch behind the log would let a node claim
// an epoch that is already in use.
func checkStoredEpochNotBehind(c *checker, slots []slotInfo, meta metaItems) {
	var highest uint64
	for _, s := range slots {
		if s.enveloped {
			highest = max(highest, s.epoch)
		}
	}
	var stored uint64
	if meta.hasWriter {
		stored = uint64(meta.epoch)
	}
	c.check("stored epoch is not behind the log", stored >= highest,
		fmt.Sprintf("stored %d, highest in log %d", stored, highest))
}

// noteStrayRows counts live rows at or below the floor. A rejected put can leave
// such a row. Nothing reads below the floor, and the next trim removes it. This is
// allowed, so it is a note and not a failure.
func noteStrayRows(c *checker, slots []slotInfo, meta metaItems) {
	if !meta.hasFloor {
		c.note("live rows at or below the floor", "no floor item, nothing to compare")
		return
	}
	stray := 0
	for _, s := range slots {
		if s.slot <= meta.floor {
			stray++
		}
	}
	c.note("live rows at or below the floor", fmt.Sprintf("%d stray row(s) at or below floor %d (allowed; the next trim removes them)", stray, meta.floor))
}

func orderedEntries(slots []slotInfo) []slotEntry {
	var out []slotEntry
	for _, s := range slots {
		for _, e := range s.entries {
			out = append(out, slotEntry{slot: s.slot, entry: e})
		}
	}
	return out
}

type slotEntry struct {
	slot  int64
	entry stateupdater.Entry
}

// newFold makes an empty fold on a Pebble store in a temporary folder. The memory store
// is too slow for a real log (each insert moves the whole array, so 50,000 commits take
// minutes). The returned function closes the store and removes the folder.
func newFold() (*stateupdater.StateUpdater, func()) {
	dir, err := os.MkdirTemp("", "mdscpinspect-fold-")
	check(err)
	store, err := pebblestore.Open(dir, pebblestore.Options{})
	check(err)
	fold := stateupdater.New(store)
	return fold, func() {
		_ = fold.Close()
		_ = os.RemoveAll(dir)
	}
}

// checkFullReplay folds the whole log into an empty store and compares the
// result with a number we count ourselves from the entries. It returns the
// state hash of the replay.
func checkFullReplay(c *checker, slots []slotInfo, entries []slotEntry) uint64 {
	fold, closeFold := newFold()
	defer closeFold()
	wanted := map[core.TopicPartition]int64{}
	seen := map[string]bool{}
	topicOf := map[string]bool{}
	for _, se := range entries {
		fold.Apply(se.slot, se.entry)
		if se.entry.Kind == stateupdater.KindCreateTopic || se.entry.Kind == stateupdater.KindCreateTopicExclusive {
			topicOf[se.entry.Topic] = true
		}
		if se.entry.Kind != stateupdater.KindCommit || seen[se.entry.Commit.SegmentKey] {
			continue
		}
		seen[se.entry.Commit.SegmentKey] = true
		for _, b := range se.entry.Commit.Batches {
			if topicOf[b.TopicPartition.Topic] {
				wanted[b.TopicPartition] += b.Records
			}
		}
	}
	keys := slices.Collect(maps.Keys(wanted))
	slices.SortFunc(keys, func(a, b core.TopicPartition) int {
		return cmp.Or(strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	wrong := 0
	for _, k := range keys {
		if int64(fold.End(k)) != wanted[k] {
			wrong++
			fmt.Printf("      %s/%d: fold end offset %d, records in log %d\n", k.Topic, k.Partition, fold.End(k), wanted[k])
		}
	}
	c.check("replayed end offset == records in log, per partition", wrong == 0, fmt.Sprintf("%d partition(s) checked, %d wrong", len(keys), wrong))
	c.check("distinct segment keys committed", len(seen) > 0, strconv.Itoa(len(seen)))
	fmt.Printf("      full replay state hash: %016x\n", fold.StateHash())
	return fold.StateHash()
}

// checkpointStore is the part of the S3 client that the checkpoint check uses.
type checkpointStore interface {
	s3.ListObjectsV2APIClient
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// newestKey returns the last key under prefix. Checkpoint keys end in a
// fixed-width slot number, so the last key in sorted order is the newest.
func newestKey(ctx context.Context, client s3.ListObjectsV2APIClient, bucket, prefix string) (string, error) {
	newest := ""
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return "", err
		}
		for _, o := range page.Contents {
			newest = max(newest, aws.ToString(o.Key))
		}
	}
	if newest == "" {
		return "", errors.New("no checkpoint object")
	}
	return newest, nil
}

// fetchedCheckpoint is the newest checkpoint object. findErr is set when no
// object could be found or opened. readErr is set when the object was there
// but its bytes could not be read.
type fetchedCheckpoint struct {
	key     string
	data    []byte
	findErr error
	readErr error
}

// fetchCheckpoint finds the newest checkpoint under prefix and reads it.
func fetchCheckpoint(ctx context.Context, client checkpointStore, bucket, prefix string) fetchedCheckpoint {
	key, err := newestKey(ctx, client, bucket, prefix)
	if err != nil {
		return fetchedCheckpoint{key: prefix, findErr: err}
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fetchedCheckpoint{key: key, findErr: err}
	}
	data, err := io.ReadAll(out.Body)
	_ = out.Body.Close()
	return fetchedCheckpoint{key: key, data: data, readErr: err}
}

// checkCheckpoint reads the newest S3 checkpoint, checks its CRC, restores it, replays
// the log after it, and compares the state with the full replay.
// replayHash is the hash of that full replay; zero when it was not done.
func checkCheckpoint(ctx context.Context, c *checker, client checkpointStore, bucket, prefix string, slots []slotInfo, trimmed bool, replayHash uint64) {
	got := fetchCheckpoint(ctx, client, bucket, prefix)
	if got.findErr != nil {
		c.check("checkpoint object exists in S3", false, fmt.Sprintf("%s: %v", got.key, got.findErr))
		return
	}
	data := got.data
	c.check("checkpoint object can be read", got.readErr == nil, fmt.Sprintf("%s, %d bytes", got.key, len(data)))
	if got.readErr != nil {
		return
	}
	crcErr := stateupdater.CheckSnapshot(data)
	c.check("checkpoint CRC is good", crcErr == nil, fmt.Sprint(crcErr))
	if crcErr != nil {
		return
	}
	fold, closeFold := newFold()
	defer closeFold()
	checkpointSlot, err := fold.Restore(bytes.NewReader(data))
	c.check("checkpoint restores", err == nil, fmt.Sprintf("covers slots before %d, err=%v", checkpointSlot, err))
	if err != nil {
		return
	}
	last := slots[len(slots)-1].slot
	c.check("checkpoint is not newer than the log", checkpointSlot <= last+1, fmt.Sprintf("checkpoint %d, last slot %d", checkpointSlot, last))
	applied := 0
	for _, s := range slots {
		if s.slot < checkpointSlot {
			continue
		}
		for _, e := range s.entries {
			fold.Apply(s.slot, e)
			applied++
		}
	}
	fmt.Printf("      replayed %d command(s) after the checkpoint\n", applied)
	if trimmed || replayHash == 0 {
		c.note("checkpoint + tail == full replay", "no full replay to compare with")
		return
	}
	c.check("checkpoint + tail state == full replay state", fold.StateHash() == replayHash, fmt.Sprintf("%016x vs %016x", fold.StateHash(), replayHash))
}
