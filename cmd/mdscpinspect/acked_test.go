package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

// honestAcked is what mdscpverify would write for honestLog: one line per acked commit.
const honestAcked = `{"key":"k1","worker":0,"sequence":0,"assigned":[{"topic":"t","partition":0,"base":0,"count":5},{"topic":"t","partition":1,"base":0,"count":3}],"acked_at":"2026-11-01T00:00:00Z"}
{"key":"k2","worker":1,"sequence":0,"assigned":[{"topic":"t","partition":0,"base":5,"count":4}],"acked_at":"2026-11-01T00:00:01Z"}
{"key":"k3","worker":0,"sequence":1,"assigned":[{"topic":"t","partition":1,"base":3,"count":2}],"acked_at":"2026-11-01T00:00:02Z"}
`

func synthBatch(topic string, partition int32, records int64) core.PendingBatch {
	return core.PendingBatch{
		TopicPartition: core.NewTopicPartition(topic, partition),
		Records:        records,
		Loc:            core.SegmentRef{Key: "loc", Off: 0, Len: 10},
	}
}

func synthCommit(key string, batches ...core.PendingBatch) stateupdater.Entry {
	return stateupdater.Entry{ID: key, Kind: stateupdater.KindCommit, Commit: core.CommitSegment{SegmentKey: key, Batches: batches}}
}

// honestLog: topic t with 2 partitions, then three commits. The fold gives
// k1 t/0 [0,5) and t/1 [0,3), k2 t/0 [5,9), k3 t/1 [3,5).
func honestLog() []slotInfo {
	return []slotInfo{
		{slot: 0, entries: []stateupdater.Entry{{ID: "create", Kind: stateupdater.KindCreateTopic, Topic: "t", Parts: 2}}},
		{slot: 1, entries: []stateupdater.Entry{synthCommit("k1", synthBatch("t", 0, 5), synthBatch("t", 1, 3))}},
		{slot: 2, entries: []stateupdater.Entry{synthCommit("k2", synthBatch("t", 0, 4))}},
		{slot: 3, entries: []stateupdater.Entry{synthCommit("k3", synthBatch("t", 1, 2))}},
	}
}

// runAcked writes acked text to a file, runs checkAcked on the log, and returns
// the checker (failure count) and everything it printed.
func runAcked(t *testing.T, slots []slotInfo, ackedText string, trimmed bool) (*checker, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acked.jsonl")
	if err := os.WriteFile(path, []byte(ackedText), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	c := &checker{out: &out}
	checkAcked(c, path, orderedEntries(slots), trimmed)
	return c, out.String()
}

// (1) an honest log and acked set pass every check.
func TestAckedHonestSetPasses(t *testing.T) {
	c, out := runAcked(t, honestLog(), honestAcked, false)
	if c.failed != 0 {
		t.Fatalf("%d check(s) failed:\n%s", c.failed, out)
	}
	for _, want := range []string{
		"PASS  " + checkPresentName,
		"PASS  " + checkRangesName,
		"PASS  " + checkDuplicatesName,
		"PASS  " + checkOverlapName,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output has no %q line:\n%s", want, out)
		}
	}
}

// (2) an acked key that is not in the log is a LOST ACK, and the message names it.
func TestAckedMissingKeyFails(t *testing.T) {
	lost := honestAcked + `{"key":"k9","worker":2,"sequence":0,"assigned":[{"topic":"t","partition":0,"base":9,"count":1}],"acked_at":"2026-11-01T00:00:03Z"}` + "\n"
	c, out := runAcked(t, honestLog(), lost, false)
	if c.failed != 1 {
		t.Fatalf("%d check(s) failed, want exactly the presence check:\n%s", c.failed, out)
	}
	if !strings.Contains(out, "FAIL  "+checkPresentName) || !strings.Contains(out, "LOST ACK k9") {
		t.Fatalf("message does not name the lost key:\n%s", out)
	}
}

// (3) an acked range that differs from the fold's range fails the range check.
func TestAckedRangeDifferenceFails(t *testing.T) {
	wrong := strings.Replace(honestAcked, `"base":5,"count":4`, `"base":6,"count":4`, 1)
	c, out := runAcked(t, honestLog(), wrong, false)
	if c.failed != 1 {
		t.Fatalf("%d check(s) failed, want exactly the ranges check:\n%s", c.failed, out)
	}
	if !strings.Contains(out, "FAIL  "+checkRangesName) || !strings.Contains(out, "MISMATCH k2") {
		t.Fatalf("message does not name the mismatched key:\n%s", out)
	}
	if !strings.Contains(out, "t/0[6:+4]") || !strings.Contains(out, "t/0[5:+4]") {
		t.Fatalf("message does not show both ranges:\n%s", out)
	}
}

// (4) a truncated last line is ignored, and the output says so.
func TestAckedTruncatedLastLineTolerated(t *testing.T) {
	cut := honestAcked + `{"key":"k4","worker":1,"sequence":`
	acked, cutShort, err := parseAcked([]byte(cut))
	if err != nil {
		t.Fatalf("truncated last line is an error: %v", err)
	}
	if !cutShort || len(acked) != 3 {
		t.Fatalf("cutShort=%v, %d commits; want true and 3", cutShort, len(acked))
	}

	c, out := runAcked(t, honestLog(), cut, false)
	if c.failed != 0 {
		t.Fatalf("%d check(s) failed:\n%s", c.failed, out)
	}
	if !strings.Contains(out, "cut short") {
		t.Fatalf("output does not say the last line was ignored:\n%s", out)
	}
}

// A bad line in the middle is not a cut-off write: it is an error.
func TestAckedBadMiddleLineIsError(t *testing.T) {
	bad := `{"key":"k1"` + "\n" + honestAcked
	if _, _, err := parseAcked([]byte(bad)); err == nil {
		t.Fatal("a broken line in the middle was accepted")
	}
}

// A key committed twice with the same answer passes, and is counted.
func TestDuplicateAppendWithSameAnswerPasses(t *testing.T) {
	slots := []slotInfo{
		{slot: 0, entries: []stateupdater.Entry{{ID: "create", Kind: stateupdater.KindCreateTopic, Topic: "t", Parts: 1}}},
		{slot: 1, entries: []stateupdater.Entry{synthCommit("k1", synthBatch("t", 0, 5))}},
		{slot: 2, entries: []stateupdater.Entry{synthCommit("k1", synthBatch("t", 0, 5))}},
	}
	acked := `{"key":"k1","worker":0,"sequence":0,"assigned":[{"topic":"t","partition":0,"base":0,"count":5}],"acked_at":"x"}` + "\n"
	c, out := runAcked(t, slots, acked, false)
	if c.failed != 0 {
		t.Fatalf("%d check(s) failed:\n%s", c.failed, out)
	}
	if !strings.Contains(out, "1 key(s) in more than one slot, 0 with a different answer") {
		t.Fatalf("duplicate count not reported:\n%s", out)
	}
}

// A key committed again with a different request gets ErrConflict: that
// answer differs from the first, so the duplicate check fails.
func TestDuplicateAppendWithDifferentAnswerFails(t *testing.T) {
	slots := []slotInfo{
		{slot: 0, entries: []stateupdater.Entry{{ID: "create", Kind: stateupdater.KindCreateTopic, Topic: "t", Parts: 1}}},
		{slot: 1, entries: []stateupdater.Entry{synthCommit("k1", synthBatch("t", 0, 5))}},
		{slot: 2, entries: []stateupdater.Entry{synthCommit("k1", synthBatch("t", 0, 7))}},
	}
	acked := `{"key":"k1","worker":0,"sequence":0,"assigned":[{"topic":"t","partition":0,"base":0,"count":5}],"acked_at":"x"}` + "\n"
	c, out := runAcked(t, slots, acked, false)
	if c.failed != 1 || !strings.Contains(out, "DIFFERENT ANSWER k1") {
		t.Fatalf("%d failed, want the duplicate check to fail for k1:\n%s", c.failed, out)
	}
}

// Two acked ranges that share an offset in one partition are an overlap.
func TestFindOverlapsFindsSharedOffsets(t *testing.T) {
	tp := core.NewTopicPartition("t", 0)
	byPartition := map[core.TopicPartition][]ackedRange{
		tp: {{Topic: "t", Partition: 0, Base: 0, Count: 10}, {Topic: "t", Partition: 0, Base: 8, Count: 4}},
	}
	if lines := findOverlaps(byPartition); len(lines) != 1 {
		t.Fatalf("overlaps %v, want one", lines)
	}
	byPartition[tp] = []ackedRange{{Topic: "t", Partition: 0, Base: 0, Count: 10}, {Topic: "t", Partition: 0, Base: 10, Count: 4}}
	if lines := findOverlaps(byPartition); len(lines) != 0 {
		t.Fatalf("touching ranges reported as overlap: %v", lines)
	}
}

// A trimmed log cannot be folded from the start, so every acked check is SKIP.
func TestAckedChecksSkipOnTrimmedLog(t *testing.T) {
	c, out := runAcked(t, honestLog(), "this is not read", true)
	if c.failed != 0 {
		t.Fatalf("%d failed on a skipped check:\n%s", c.failed, out)
	}
	for _, name := range []string{checkPresentName, checkRangesName, checkDuplicatesName, checkOverlapName} {
		if !strings.Contains(out, "SKIP  "+name) {
			t.Errorf("no SKIP line for %q:\n%s", name, out)
		}
	}
}
