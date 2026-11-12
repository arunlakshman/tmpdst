package topic

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// FuzzDecodeBatchRun: a stored R record decodes to ranges or to errCorrupt, and
// never panics. (The caller panics on errCorrupt on purpose, fail-stop on
// corrupt state; that is outside this decoder.) A record that decoded encodes
// to a record that decodes to the same ranges.
func FuzzDecodeBatchRun(f *testing.F) {
	tp := contract.NewTopicPartition("t", 3)
	f.Add(encodeBatchRun([]contract.BatchRange{
		{TopicPartition: tp, Base: 7, Count: 3, Loc: runtime.SegmentRef{Key: "a", Off: 1, Len: 2}},
		{TopicPartition: tp, Base: 10, Count: 4, Loc: runtime.SegmentRef{Key: "b", Off: 5, Len: 6}},
		{TopicPartition: tp, Base: 14, Count: 1, Loc: runtime.SegmentRef{Key: "a", Off: 9, Len: 1}},
	}))
	f.Add([]byte{})
	f.Add([]byte{runFormat})
	f.Fuzz(func(t *testing.T, b []byte) {
		rs, err := decodeBatchRun(tp, 7, b)
		if err != nil {
			return
		}
		rs2, err := decodeBatchRun(tp, 7, encodeBatchRun(rs))
		if err != nil {
			t.Fatalf("a record that decoded does not decode after encode: %v", err)
		}
		if len(rs) != len(rs2) {
			t.Fatalf("round trip changed the count: %d, %d", len(rs), len(rs2))
		}
		for i := range rs {
			if rs[i] != rs2[i] {
				t.Fatalf("range %d: %+v, %+v", i, rs[i], rs2[i])
			}
		}
	})
}

// FuzzParseDuplicateCheck: a stored duplicate-check record parses or returns
// errCorrupt, and never panics.
func FuzzParseDuplicateCheck(f *testing.F) {
	f.Add(duplicateCheckRecord(5, [checksumLength]byte{1, 2, 3}, []contract.BatchRange{{Base: 7}, {Base: 12}}))
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = parseDuplicateCheck(b)
	})
}
