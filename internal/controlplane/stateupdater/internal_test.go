package stateupdater

import (
	"errors"
	"strings"
	"testing"

	"mds/internal/core"
)

func TestRunRoundTripAndCorruption(t *testing.T) {
	topicPartition := core.NewTopicPartition("t", 3)
	rs := []core.BatchRange{
		{TopicPartition: topicPartition, Base: 10, Count: 2, Loc: core.SegmentRef{Key: "s1", Off: 0, Len: 50}},
		{TopicPartition: topicPartition, Base: 12, Count: 5, Loc: core.SegmentRef{Key: "s1", Off: 50, Len: -1}},
		{TopicPartition: topicPartition, Base: 17, Count: 1, Loc: core.SegmentRef{Key: "s2", Off: 7, Len: 9}},
	}
	b := encodeBatchRun(rs)
	got, err := decodeBatchRun(topicPartition, 10, b)
	if err != nil || len(got) != 3 || got[2] != rs[2] || got[1] != rs[1] {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	bad := [][]byte{nil, {9}, b[:len(b)-1], append(append([]byte{}, b...), 0), {runFormat, 0xFF}, {runFormat, 1, 5, 'a'},
		{runFormat, 0, 200}, {runFormat, 1, 1, 'k', 1, 5, 1, 0, 0}, {runFormat, 100}}
	for i, x := range bad {
		if _, err := decodeBatchRun(topicPartition, 0, x); !errors.Is(err, errCorrupt) {
			t.Errorf("case %d: err %v", i, err)
		}
	}
}

func TestIdemRecordCorruption(t *testing.T) {
	var dig [checksumLength]byte
	rec := duplicateCheckRecord(7, dig, []core.BatchRange{{Base: 3}, {Base: 300}})
	if _, bases, err := parseDuplicateCheck(rec); err != nil || len(bases) != 2 || bases[1] != 300 {
		t.Fatalf("%v %v", bases, err)
	}
	for _, x := range [][]byte{nil, {0x80}, rec[:5], append(append([]byte{}, rec[:1+checksumLength]...), 0x80)} {
		if _, _, err := parseDuplicateCheck(x); err == nil {
			t.Errorf("accepted %v", x)
		}
	}
}

func TestPrefixEnd(t *testing.T) {
	if string(prefixEnd([]byte("ab"))) != "ac" || string(prefixEnd([]byte{'a', 0xFF})) != "b" || prefixEnd([]byte{0xFF}) != nil {
		t.Fatal("prefixEnd")
	}
}

// brokenKV fails every transaction.
type brokenKV struct{}

func (*brokenKV) View(func(Transaction)) error   { return errors.New("disk gone") }
func (*brokenKV) Update(func(Transaction)) error { return errors.New("disk gone") }
func (*brokenKV) Close() error                   { return nil }

func TestStoreFailureIsFailStop(t *testing.T) {
	a := New(&brokenKV{})
	if _, err := a.Commit(0, core.CommitSegment{SegmentKey: "k"}); err == nil {
		t.Error("Commit must return the store error")
	}
	for name, f := range map[string]func(){
		"Exec":        func() { a.Exec(core.ControlPlaneCall{Op: core.DescribeTopics{}}) },
		"CreateTopic": func() { a.CreateTopic("t", 1) },
		"End":         func() { a.End(core.NewTopicPartition("t", 0)) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(r.(error).Error(), "store failed") {
					t.Errorf("%s: recover = %v", name, r)
				}
			}()
			f()
		}()
	}
}

func TestCorruptStoredRecordsPanic(t *testing.T) {
	a := New(NewMemoryKeyValueStore())
	topicPartition := core.NewTopicPartition("t", 0)
	_ = a.keyValue.Update(func(transaction Transaction) {
		transaction.Set(batchRunKey(topicPartition, 0), []byte{9, 9})
		transaction.Set(endKey(topicPartition), []byte{5})
		transaction.Set(duplicateCheckKey("k"), []byte{0x80})
	})
	for name, f := range map[string]func(){
		"lookup": func() { _, _ = a.Lookup(core.FindSegments{TopicPartition: topicPartition}) },
		"replay": func() { _, _ = a.Commit(1, core.CommitSegment{SegmentKey: "k"}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic on a corrupt record", name)
				}
			}()
			f()
		}()
	}
}
