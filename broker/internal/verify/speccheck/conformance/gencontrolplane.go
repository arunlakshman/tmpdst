package conformance

import (
	"fmt"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/rng"
)

// CreateTopic is the direct topic-creation call. It is not a ControlPlaneCall because
// the control plane takes it outside the broker protocol.
type CreateTopic struct {
	Name       string
	Partitions int32
}

// ControlPlaneTestStep is one control-plane operation. Exactly one field is not nil.
type ControlPlaneTestStep struct {
	CreateTopic *CreateTopic
	Call        *core.ControlPlaneCall
}

// NewCreateTopic returns an operation that creates a topic.
func NewCreateTopic(name string, partitions int32) ControlPlaneTestStep {
	return ControlPlaneTestStep{CreateTopic: &CreateTopic{Name: name, Partitions: partitions}}
}

// NewCall returns an operation that sends call.
func NewCall(call core.ControlPlaneCall) ControlPlaneTestStep {
	return ControlPlaneTestStep{Call: &call}
}

// Describe returns a short plain-English text for an operation, for failure
// messages.
func (o ControlPlaneTestStep) Describe() string {
	switch {
	case o.CreateTopic != nil:
		return fmt.Sprintf("create topic %q with %d partitions", o.CreateTopic.Name, o.CreateTopic.Partitions)
	case o.Call == nil:
		return "empty operation"
	}
	switch operation := o.Call.Op.(type) {
	case contract.CommitSegment:
		return fmt.Sprintf("request %d: commit segment %q with %d batches", o.Call.Req, operation.SegmentKey, len(operation.Batches))
	case contract.FindSegments:
		return fmt.Sprintf("request %d: look up %v from offset %d with max bytes %d", o.Call.Req, operation.TopicPartition, operation.From, operation.MaxBytes)
	case contract.DescribeTopics:
		return fmt.Sprintf("request %d: describe topics %q", o.Call.Req, operation.Topics)
	case contract.CreateTopic:
		return fmt.Sprintf("request %d: create topic %q with %d partitions, failing if it exists", o.Call.Req, operation.Name, operation.Partitions)
	}
	return fmt.Sprintf("request %d: %T", o.Call.Req, o.Call.Op)
}

// ControlPlaneOps returns a generator of control-plane operations for profile
// p. Commits cover a fresh key, a replay, a conflict, the same batches under a
// fresh key, and five kinds of malformed request. Lookups cover From = -1,
// From inside long runs and MaxBytes cuts. DescribeTopics covers known,
// unknown and all topics.
//
// A call has no request id. The run gives each call the id of its position
// (1, 2, 3 and so on) just before it goes to the subject.
//
// The kinds are listed simplest first, because rapid shrinks toward the first
// kind. The weights are the ones in the profile.
func ControlPlaneOps(p InputProfile) *rapid.Generator[ControlPlaneTestStep] {
	return Custom(func(r Rand) ControlPlaneTestStep {
		switch chooseByWeight(r, p.DescribeWeight, p.LookupWeight, p.CreateTopicWeight, p.CommitWeight) {
		case 0:
			return NewCall(core.ControlPlaneCall{Op: genDescribe(p, r)})
		case 1:
			return NewCall(core.ControlPlaneCall{Op: genLookup(p, r)})
		case 2:
			if r.Intn(2) == 0 {
				return genCreateTopic(p, r)
			}
			return genExclusiveCreate(p, r)
		}
		return NewCall(core.ControlPlaneCall{Op: genCommit(p, r)})
	})
}

// genExclusiveCreate is a CreateTopic call with FailIfExists. It takes its
// name and count from genCreateTopic, so it also covers the invalid creates.
func genExclusiveCreate(p InputProfile, r Rand) ControlPlaneTestStep {
	c := genCreateTopic(p, r).CreateTopic
	return NewCall(core.ControlPlaneCall{Op: contract.CreateTopic{Name: c.Name, Partitions: c.Partitions, FailIfExists: true}})
}

func genCreateTopic(p InputProfile, r Rand) ControlPlaneTestStep {
	if !percent(r, p.CreateInvalidPercent) {
		return NewCreateTopic(pick(r, p.Topics), int32(1+r.Intn(p.MaxPartitions)))
	}
	if r.Intn(2) == 0 {
		return NewCreateTopic("", int32(1+r.Intn(p.MaxPartitions)))
	}
	return NewCreateTopic(pick(r, p.Topics), int32(-r.Intn(2)))
}

func poolKey(p InputProfile, r Rand) string {
	return fmt.Sprintf("seg%d", r.Intn(p.SegmentKeys))
}

// genCommit picks one kind of commit. A pool key with variant 0 is a first
// commit the first time and a replay every later time, because Batches gives
// the same batches for the same key.
func genCommit(p InputProfile, r Rand) contract.CommitSegment {
	// The draw is turned around, so that rapid shrinks toward the plain fresh
	// commit (the last branch) and not toward a malformed one.
	roll := 99 - r.Intn(100)
	switch {
	case roll < p.InvalidPercent:
		return genInvalidCommit(p, r)
	case roll < p.InvalidPercent+p.ConflictPercent:
		key := poolKey(p, r)
		return contract.CommitSegment{SegmentKey: key, Batches: Batches(p, key, key, 1+r.Intn(3))}
	case roll < p.InvalidPercent+p.ConflictPercent+p.RetryNewKeyPercent:
		shape := poolKey(p, r)
		key := fmt.Sprintf("retry%d", r.Intn(1000))
		return contract.CommitSegment{SegmentKey: key, Batches: Batches(p, shape, key, 0)}
	}
	key := poolKey(p, r)
	return contract.CommitSegment{SegmentKey: key, Batches: Batches(p, key, key, 0)}
}

// genInvalidCommit returns a commit that the control plane must reject. It
// uses a key from a separate "bad" pool, so the key is never committed and
// the answer is always ErrInvalid and never ErrConflict.
func genInvalidCommit(p InputProfile, r Rand) contract.CommitSegment {
	key := fmt.Sprintf("bad%d", r.Intn(4))
	batches := Batches(p, key, key, 0)
	at := r.Intn(len(batches))
	switch r.Intn(5) {
	case 0:
		key = ""
	case 1:
		batches = nil
	case 2:
		batches[at].Records = -int64(r.Intn(2))
	case 3:
		batches[at].TopicPartition.Partition = -1
	default:
		batches[at].TopicPartition.Topic = ""
	}
	return contract.CommitSegment{SegmentKey: key, Batches: batches}
}

func genLookup(p InputProfile, r Rand) contract.FindSegments {
	topicPartition := contract.NewTopicPartition(pick(r, p.Topics), int32(r.Intn(p.PartitionCount)))
	from := int64(r.Intn(p.LookupShortSpan))
	if percent(r, p.LookupLongPercent) {
		from = int64(r.Intn(p.LookupLongSpan))
	}
	if percent(r, p.LookupNegativeFromPercent) {
		from = -1
	}
	return contract.FindSegments{TopicPartition: topicPartition, From: core.Offset(from), MaxBytes: pick(r, p.MaxBytesChoices)}
}

func genDescribe(p InputProfile, r Rand) contract.DescribeTopics {
	var d contract.DescribeTopics
	for j := r.Intn(p.MaxDescribeNames + 1); j > 0; j-- {
		name := pick(r, p.Topics)
		if percent(r, p.UnknownTopicPercent) {
			name = "nope"
		}
		d.Topics = append(d.Topics, name)
	}
	return d
}

// Batches returns the batches of a commit. Its result depends only on p, shape,
// segmentKey and variant, so a caller that asks twice gets the same batches
// and the control plane sees a replay.
//
// The shape picks the number, topics, sizes and records of the batches. Two
// calls with the same shape and variant but different segmentKey give the same
// batches except for Loc.Key. This is the "same batches under a fresh key"
// case. A variant above 0 always gives batches that differ from variant 0:
// the first batch has more records, and an even variant also adds one batch.
// This is the "same key, different request" case, which is a conflict.
func Batches(p InputProfile, shape, segmentKey string, variant int) []contract.PendingBatch {
	r := rng.New(makeSeedShape(shape))
	n := 1 + r.Intn(p.MaxBatches)
	if r.Intn(100) < p.ManyBatchesPercent { // not percent(): the batches of a key keep their old shape
		n = p.ManyBatchesMin + r.Intn(p.ManyBatchesExtra+1)
	}
	out := make([]contract.PendingBatch, 0, n+1)
	for range n {
		out = append(out, genBatch(p, r, segmentKey))
	}
	if variant > 0 {
		out[0].Records += int64(variant)
		if variant%2 == 0 {
			out = append(out, out[0])
		}
	}
	return out
}

// makeSeedShape turns a shape name into a random seed with the FNV-1a hash. The
// same name always gives the same seed, and the hash is stable on every run.
func makeSeedShape(shape string) uint64 {
	h := uint64(14695981039346656037)
	for i := range len(shape) {
		h = (h ^ uint64(shape[i])) * 1099511628211
	}
	return h
}

func genBatch(p InputProfile, r Rand, segmentKey string) contract.PendingBatch {
	topics := p.Topics[:min(p.BatchTopics, len(p.Topics))]
	return contract.PendingBatch{
		TopicPartition: contract.NewTopicPartition(pick(r, topics), int32(r.Intn(p.BatchPartitions))),
		Records:        1 + int64(r.Intn(3)),
		Loc:            core.SegmentRef{Key: segmentKey, Off: int64(r.Intn(4)) * 100, Len: int64(1 + r.Intn(3)*50)},
	}
}
