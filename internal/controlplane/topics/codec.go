package topics

import (
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// The log entry kinds of this module. The numbers are part of the log format.
const (
	// KindCommit is a CommitSegment.
	KindCommit module.Kind = 1
	// KindCreateTopic is a CreateTopic.
	KindCreateTopic module.Kind = 2
	// KindCreateTopicExclusive is a CreateTopic with FailIfExists. It is a kind of
	// its own, not a flag on KindCreateTopic, so old log entries keep their bytes.
	KindCreateTopicExclusive module.Kind = 7
)

// Kinds lists the log entry kinds of the module.
func (Module) Kinds() []module.Kind {
	return []module.Kind{KindCommit, KindCreateTopic, KindCreateTopicExclusive}
}

// KindOf is the kind of a CommitSegment or a CreateTopic.
func (Module) KindOf(operation core.ControlPlaneOperation) (module.Kind, bool) {
	switch x := operation.(type) {
	case core.CommitSegment:
		return KindCommit, true
	case core.CreateTopic:
		if x.FailIfExists {
			return KindCreateTopicExclusive, true
		}
		return KindCreateTopic, true
	}
	return 0, false
}

// Encode writes the payload of a CommitSegment or a CreateTopic.
func (Module) Encode(w *kv.Writer, kind module.Kind, operation core.ControlPlaneOperation) {
	switch x := operation.(type) {
	case core.CommitSegment:
		encodeCommit(w, x)
	case core.CreateTopic:
		w.String(x.Name)
		w.Varint(int64(x.Partitions))
	}
}

// Decode reads the payload of an entry of kind.
func (Module) Decode(r *kv.Reader, kind module.Kind) core.ControlPlaneOperation {
	switch kind {
	case KindCommit:
		return decodeCommit(r)
	case KindCreateTopic, KindCreateTopicExclusive:
		name := r.String()
		return core.CreateTopic{Name: name, Partitions: int32(r.Varint()), FailIfExists: kind == KindCreateTopicExclusive}
	}
	r.Fail()
	return nil
}

// encodeCommit writes the segment key, then a table of the strings that the
// batches use (each topic and each object key once), then the batches, which
// refer to the table by index.
func encodeCommit(w *kv.Writer, c core.CommitSegment) {
	w.String(c.SegmentKey)
	var table []string
	index := map[string]int{}
	ref := func(s string) uint64 {
		if n, ok := index[s]; ok {
			return uint64(n)
		}
		index[s] = len(table)
		table = append(table, s)
		return uint64(len(table) - 1)
	}
	body := kv.NewWriter()
	for _, b := range c.Batches {
		body.Uvarint(ref(b.TopicPartition.Topic))
		body.Varint(int64(b.TopicPartition.Partition))
		body.Varint(b.Records)
		body.Uvarint(ref(b.Loc.Key))
		body.Varint(b.Loc.Off)
		body.Varint(b.Loc.Len)
	}
	w.Uvarint(uint64(len(table)))
	for _, s := range table {
		w.String(s)
	}
	w.Uvarint(uint64(len(c.Batches)))
	w.Append(body.Bytes())
}

func decodeCommit(r *kv.Reader) core.CommitSegment {
	c := core.CommitSegment{SegmentKey: r.String()}
	table := make([]string, 0, 8)
	for n := r.Uvarint(); n > 0 && r.OK(); n-- {
		table = append(table, r.String())
	}
	n := r.Count()
	c.Batches = make([]core.PendingBatch, 0, n)
	ref := func() string {
		i := r.Uvarint()
		if i >= uint64(len(table)) {
			r.Fail()
			return ""
		}
		return table[i]
	}
	for ; n > 0 && r.OK(); n-- {
		var b core.PendingBatch
		b.TopicPartition.Topic = ref()
		b.TopicPartition.Partition = int32(r.Varint())
		b.Records = r.Varint()
		b.Loc.Key = ref()
		b.Loc.Off = r.Varint()
		b.Loc.Len = r.Varint()
		c.Batches = append(c.Batches, b)
	}
	return c
}
