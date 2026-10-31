package statemachine_test

import (
	"fmt"
	"testing"

	"manifester/internal/controlplane/statemachine"
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

// dynamoItemLimit is the largest DynamoDB item, in bytes (400 KiB).
const dynamoItemLimit = 400 << 10

// sizedCommit is a commit with the given number of batches, one per partition.
func sizedCommit(key string, batches int) stateupdater.Entry {
	bs := make([]core.PendingBatch, batches)
	for i := range bs {
		bs[i] = core.PendingBatch{
			TopicPartition: core.NewTopicPartition("orders", int32(i)), Records: 100,
			Loc: core.SegmentRef{Key: key, Off: int64(i) * 1000, Len: 1000},
		}
	}
	return stateupdater.Entry{Kind: stateupdater.KindCommit, Commit: core.CommitSegment{SegmentKey: key, Batches: bs}}
}

// appendsUntilQueueEmpty finishes each append in turn, so the machine builds
// the next group from the queue, and returns every LogAppend it emitted.
func appendsUntilQueueEmpty(t *testing.T, m *statemachine.Machine, first core.LogAppend) []core.LogAppend {
	t.Helper()
	all := []core.LogAppend{first}
	cur := first
	for m.QueueDepth() > 0 {
		acts := step(m, core.LogAppendDone{Req: cur.Req, Slot: cur.Slot, Err: core.OK, Outcome: core.Applied})
		var next *core.LogAppend
		for _, a := range acts {
			if ap, ok := a.(core.LogAppend); ok {
				next = &ap
			}
		}
		if next == nil {
			t.Fatalf("queue still holds %d commands but no group was emitted", m.QueueDepth())
		}
		all = append(all, *next)
		cur = *next
	}
	return all
}

func commandsIn(t *testing.T, ap core.LogAppend) int {
	t.Helper()
	_, cmds, ok := statemachine.DecodeGroup(ap.Data)
	if !ok {
		t.Fatal("entry is not a group")
	}
	return len(cmds)
}

func TestGroupStopsAtTheEntryByteLimit(t *testing.T) {
	const limit = 20000
	m := newMachine(t, statemachine.Config{K: 1, MaxEntryBytes: limit})
	first := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: sizedCommit("seg/0", 50)}))
	const queued = 80
	for i := 2; i <= queued+1; i++ {
		m.Offer(statemachine.ClientOperation{Req: core.RequestID(i), Cmd: sizedCommit(fmt.Sprintf("seg/%d", i), 50)})
	}
	appends := appendsUntilQueueEmpty(t, m, first)
	if len(appends) < 3 {
		t.Fatalf("groups = %d, want the queue split into several groups", len(appends))
	}
	total := 0
	for i, ap := range appends {
		if len(ap.Data) > limit {
			t.Errorf("group %d is %d bytes, over the limit of %d", i, len(ap.Data), limit)
		}
		total += commandsIn(t, ap)
	}
	if total != queued+1 {
		t.Errorf("commands in all groups = %d, want %d (none lost)", total, queued+1)
	}
}

func TestDefaultLimitKeepsEntriesUnderTheDynamoDBItemLimit(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1})
	first := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: sizedCommit("seg/0", 256)}))
	const queued = 200 // 256 commands of 256 batches made a 725 KB entry before the limit
	for i := 2; i <= queued+1; i++ {
		m.Offer(statemachine.ClientOperation{Req: core.RequestID(i), Cmd: sizedCommit(fmt.Sprintf("seg/%d", i), 256)})
	}
	total := 0
	for i, ap := range appendsUntilQueueEmpty(t, m, first) {
		if len(ap.Data) > 350<<10 || len(ap.Data) >= dynamoItemLimit {
			t.Errorf("group %d is %d bytes, over the default limit of 350 KiB", i, len(ap.Data))
		}
		total += commandsIn(t, ap)
	}
	if total != queued+1 {
		t.Errorf("commands in all groups = %d, want %d (none lost)", total, queued+1)
	}
}

func TestCommandTooLargeForOneEntryIsRefused(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, MaxEntryBytes: 2000})
	acts := m.Offer(statemachine.ClientOperation{Req: 7, Cmd: sizedCommit("seg/big", 256)})
	if len(acts) != 0 {
		t.Fatalf("a command too large for one entry must not be appended, got %v", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 7 || ans[0].Done.Err != core.ErrTooLarge {
		t.Fatalf("answers = %+v, want one ErrTooLarge for request 7", ans)
	}
	if m.QueueDepth() != 0 || m.InFlightCommits() != 0 {
		t.Errorf("queue %d, in flight %d: the refused command must not be kept", m.QueueDepth(), m.InFlightCommits())
	}
	// A command that fits is still accepted by the same machine.
	onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 8, Cmd: sizedCommit("seg/small", 2)}))
}
