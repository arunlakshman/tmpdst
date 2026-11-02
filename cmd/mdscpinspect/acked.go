package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

// The four acked checks. Their names are used for PASS, FAIL and SKIP lines.
const (
	checkPresentName    = "every acked key is in the log"
	checkRangesName     = "acked ranges equal the fold's ranges"
	checkDuplicatesName = "a key in two slots got the same answer"
	checkOverlapName    = "acked ranges do not overlap in a partition"
)

// listLimit is how many keys or ranges a failing check lists.
const listLimit = 10

// ackedRange is one batch range in the acked file (JSON form).
type ackedRange struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Base      int64  `json:"base"`
	Count     int64  `json:"count"`
}

// ackedCommit is one JSON line of the acked file that mdscpverify -acked-out writes.
type ackedCommit struct {
	Key      string       `json:"key"`
	Worker   int          `json:"worker"`
	Sequence int64        `json:"sequence"`
	Assigned []ackedRange `json:"assigned"`
	AckedAt  string       `json:"acked_at"`
}

// foldedKey is what the fold answered for one segment key. The answer is taken
// from the first commit of the key in the log.
type foldedKey struct {
	firstSlot int64
	lastSlot  int64
	slots     int          // distinct log slots that hold a commit of this key
	errCode   core.ErrCode // code of the first answer
	ranges    []ackedRange // ranges of the first answer
	differs   bool         // a later commit of this key got an answer that is not the first one
}

// checkAcked runs the four acked checks. With trimmed slots the fold cannot be
// replayed from the start, so each check prints SKIP instead.
func checkAcked(c *checker, path string, entries []slotEntry, trimmed bool) {
	if trimmed {
		for _, name := range []string{checkPresentName, checkRangesName, checkDuplicatesName, checkOverlapName} {
			c.note(name, "log has trimmed slots; the fold cannot be replayed from the start")
		}
		return
	}
	data, err := os.ReadFile(path) //nolint:gosec // the path is the -acked flag that the operator gave
	if err != nil {
		c.check("acked file can be read", false, err.Error())
		return
	}
	acked, cutShort, err := parseAcked(data)
	if err != nil {
		c.check("acked file parses", false, err.Error())
		return
	}
	if cutShort {
		c.printf("      NOTE  the last line of the acked file is cut short (no newline): ignored\n")
	}
	distinct, repeated := distinctAcked(acked)
	c.printf("      %d acked line(s), %d distinct key(s), %d repeated line(s)\n", len(acked), len(distinct), repeated)

	keys := foldKeys(entries)
	checkAckedPresent(c, distinct, keys)
	checkAckedRanges(c, distinct, keys)
	checkDuplicateAnswers(c, keys)
	checkNoOverlap(c, distinct)
}

// parseAcked reads the JSON lines. A last line with no newline that does not
// parse is a line cut short by a killed writer: it is dropped and cutShort is
// true. Any other line that does not parse is an error.
func parseAcked(data []byte) ([]ackedCommit, bool, error) {
	var acked []ackedCommit
	lines := strings.SplitAfter(string(data), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for i, line := range lines {
		var commit ackedCommit
		if parseErr := json.Unmarshal([]byte(line), &commit); parseErr != nil {
			if i == len(lines)-1 && !strings.HasSuffix(line, "\n") {
				return acked, true, nil
			}
			return nil, false, fmt.Errorf("line %d is not a JSON commit: %w", i+1, parseErr)
		}
		acked = append(acked, commit)
	}
	return acked, false, nil
}

// distinctAcked keeps the first line of each key and counts the repeats.
func distinctAcked(acked []ackedCommit) ([]ackedCommit, int) {
	seen := map[string]bool{}
	var out []ackedCommit
	repeated := 0
	for _, a := range acked {
		if seen[a.Key] {
			repeated++
			continue
		}
		seen[a.Key] = true
		out = append(out, a)
	}
	return out, repeated
}

// foldKeys applies the log entries in order to an empty state, and records the
// answer to every commit of every key.
func foldKeys(entries []slotEntry) map[string]*foldedKey {
	fold, closeFold := newFold()
	defer closeFold()
	keys := map[string]*foldedKey{}
	for _, se := range entries {
		done := fold.Apply(se.slot, se.entry)
		if se.entry.Kind == stateupdater.KindCommit {
			recordAnswer(keys, se.slot, se.entry.Commit.SegmentKey, done)
		}
	}
	return keys
}

func recordAnswer(keys map[string]*foldedKey, slot int64, key string, done core.ControlPlaneDone) {
	ranges := answerRanges(done.Resp)
	fk, seen := keys[key]
	if !seen {
		keys[key] = &foldedKey{firstSlot: slot, lastSlot: slot, slots: 1, errCode: done.Err, ranges: ranges}
		return
	}
	if slot != fk.lastSlot {
		fk.slots++
		fk.lastSlot = slot
	}
	if done.Err != fk.errCode || !slices.Equal(ranges, fk.ranges) {
		fk.differs = true
	}
}

// answerRanges converts the ranges of a commit answer to the acked-file form.
func answerRanges(resp core.ControlPlaneResponse) []ackedRange {
	commit, _ := resp.(core.CommitSegmentResp)
	out := make([]ackedRange, 0, len(commit.Assigned))
	for _, r := range commit.Assigned {
		out = append(out, ackedRange{
			Topic:     r.TopicPartition.Topic,
			Partition: r.TopicPartition.Partition,
			Base:      int64(r.Base),
			Count:     r.Count,
		})
	}
	return out
}

// checkAckedPresent: every acked key must be in the log. An acked key that is
// not in the log is a LOST ACK.
func checkAckedPresent(c *checker, acked []ackedCommit, keys map[string]*foldedKey) {
	var lines []string
	for _, a := range acked {
		if _, ok := keys[a.Key]; !ok {
			lines = append(lines, "LOST ACK "+a.Key)
		}
	}
	printFirst(c, lines)
	c.check(checkPresentName, len(lines) == 0,
		fmt.Sprintf("%d of %d acked key(s) in the log, %d LOST ACK(s)", len(acked)-len(lines), len(acked), len(lines)))
}

// checkAckedRanges: for every acked key in the log, the fold's ranges must
// equal the acked ranges exactly.
func checkAckedRanges(c *checker, acked []ackedCommit, keys map[string]*foldedKey) {
	var lines []string
	checked := 0
	for _, a := range acked {
		fk, ok := keys[a.Key]
		if !ok {
			continue // a LOST ACK, reported by checkAckedPresent
		}
		checked++
		if fk.errCode == core.OK && slices.Equal(fk.ranges, a.Assigned) {
			continue
		}
		lines = append(lines, fmt.Sprintf("MISMATCH %s: acked %s, fold %s (fold code %s)",
			a.Key, rangesText(a.Assigned), rangesText(fk.ranges), fk.errCode.String()))
	}
	printFirst(c, lines)
	c.check(checkRangesName, len(lines) == 0,
		fmt.Sprintf("%d of %d checked key(s) differ from the fold", len(lines), checked))
}

// checkDuplicateAnswers: a key that is in more than one slot must get the same
// answer each time. A duplicate that got a different answer is a failure.
func checkDuplicateAnswers(c *checker, keys map[string]*foldedKey) {
	repeated, differing := 0, 0
	var lines []string
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		fk := keys[key]
		if fk.slots > 1 {
			repeated++
		}
		if fk.differs {
			differing++
			lines = append(lines, "DIFFERENT ANSWER "+key)
		}
	}
	printFirst(c, lines)
	c.check(checkDuplicatesName, differing == 0,
		fmt.Sprintf("%d key(s) in more than one slot, %d with a different answer", repeated, differing))
}

// checkNoOverlap: within one topic partition, no two acked ranges may share an offset.
func checkNoOverlap(c *checker, acked []ackedCommit) {
	byPartition := map[core.TopicPartition][]ackedRange{}
	total := 0
	for _, a := range acked {
		for _, r := range a.Assigned {
			tp := core.NewTopicPartition(r.Topic, r.Partition)
			byPartition[tp] = append(byPartition[tp], r)
			total++
		}
	}
	lines := findOverlaps(byPartition)
	printFirst(c, lines)
	c.check(checkOverlapName, len(lines) == 0,
		fmt.Sprintf("%d range(s) in %d partition(s), %d overlap(s)", total, len(byPartition), len(lines)))
}

// findOverlaps returns one line for each range that starts before an earlier
// range in the same partition has ended.
func findOverlaps(byPartition map[core.TopicPartition][]ackedRange) []string {
	var lines []string
	for _, tp := range sortedPartitions(byPartition) {
		ranges := slices.Clone(byPartition[tp])
		slices.SortFunc(ranges, func(a, b ackedRange) int { return cmp.Compare(a.Base, b.Base) })
		var reach ackedRange // the range that ends last so far
		for i, r := range ranges {
			if i > 0 && r.Base < reach.Base+reach.Count {
				lines = append(lines, fmt.Sprintf("OVERLAP %s/%d: %s and %s",
					tp.Topic, tp.Partition, rangesText([]ackedRange{reach}), rangesText([]ackedRange{r})))
			}
			if r.Base+r.Count > reach.Base+reach.Count {
				reach = r
			}
		}
	}
	return lines
}

func sortedPartitions(byPartition map[core.TopicPartition][]ackedRange) []core.TopicPartition {
	out := slices.Collect(maps.Keys(byPartition))
	slices.SortFunc(out, func(a, b core.TopicPartition) int {
		return cmp.Or(cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	return out
}

// printFirst prints at most listLimit detail lines under a check.
func printFirst(c *checker, lines []string) {
	for _, line := range lines[:min(listLimit, len(lines))] {
		c.printf("      %s\n", line)
	}
	if len(lines) > listLimit {
		c.printf("      ... and %d more\n", len(lines)-listLimit)
	}
}

// rangesText writes ranges as "topic/partition[base:+count]", or "none".
func rangesText(ranges []ackedRange) string {
	if len(ranges) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		parts = append(parts, fmt.Sprintf("%s/%d[%d:+%d]", r.Topic, r.Partition, r.Base, r.Count))
	}
	return strings.Join(parts, " ")
}
