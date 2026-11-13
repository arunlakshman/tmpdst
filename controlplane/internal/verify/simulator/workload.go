package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/rng"
)

// The client work is made from a few topics and groups, so that operations meet
// each other on the same names.
const (
	topicCount     = 4
	partitionCount = 4
	groupCount     = 2
)

func topicName(i int) string { return fmt.Sprintf("t%d", i) }
func groupName(i int) string { return fmt.Sprintf("g%d", i) }

// workload makes the random client operations of a run. It remembers the group
// generation and leader that clients have seen, so that most group operations
// are valid. A wrong guess is fine: the spec says what the right answer is.
type workload struct {
	rand    *rng.Rand
	nextSeg int
	nextMem int
	nextOff int64
	gen     [groupCount]int32
	leader  [groupCount]string
}

func (w *workload) tp() contract.TopicPartition {
	return contract.NewTopicPartition(topicName(w.rand.Intn(topicCount)), int32(w.rand.Intn(partitionCount)))
}

// next returns a random operation and whether it changes state.
func (w *workload) next(nowMs int64) (runtime.ControlPlaneOperation, bool) {
	switch p := w.rand.Intn(100); {
	case p < 38:
		return w.commit(), true
	case p < 46:
		return contract.CreateTopic{
			Name: topicName(w.rand.Intn(topicCount)), Partitions: int32(1 + w.rand.Intn(partitionCount)),
			FailIfExists: w.rand.Chance(0.3),
		}, true
	case p < 62:
		return w.groupWrite(nowMs), true
	case p < 76:
		return w.find(), false
	case p < 84:
		return w.describeTopics(), false
	default:
		return w.groupRead(), false
	}
}

func (w *workload) commit() contract.CommitSegment {
	w.nextSeg++
	key := fmt.Sprintf("seg-%d", w.nextSeg)
	batches := make([]contract.PendingBatch, 1+w.rand.Intn(3))
	var off int64
	for i := range batches {
		records := int64(1 + w.rand.Intn(9))
		batches[i] = contract.PendingBatch{
			TopicPartition: w.tp(), Records: records,
			Loc: runtime.SegmentRef{Key: key, Off: off, Len: records * 10},
		}
		off += records * 10
	}
	return contract.CommitSegment{SegmentKey: key, Batches: batches}
}

func (w *workload) find() contract.FindSegments {
	f := contract.FindSegments{TopicPartition: w.tp(), From: contract.Offset(w.rand.Intn(8))}
	if w.rand.Chance(0.4) {
		f.MaxBytes = int32(20 + w.rand.Intn(200))
	}
	return f
}

func (w *workload) describeTopics() contract.DescribeTopics {
	if w.rand.Chance(0.5) {
		return contract.DescribeTopics{}
	}
	return contract.DescribeTopics{Topics: []string{topicName(w.rand.Intn(topicCount)), topicName(w.rand.Intn(topicCount))}}
}

func (w *workload) groupRead() runtime.ControlPlaneOperation {
	g := groupName(w.rand.Intn(groupCount))
	switch w.rand.Intn(3) {
	case 0:
		return contract.DescribeGroup{Group: g}
	case 1:
		return contract.ListGroups{}
	}
	if w.rand.Chance(0.5) {
		return contract.OffsetFetch{Group: g}
	}
	return contract.OffsetFetch{Group: g, Partitions: []contract.TopicPartition{w.tp(), w.tp()}}
}

func (w *workload) groupWrite(nowMs int64) runtime.ControlPlaneOperation {
	gi := w.rand.Intn(groupCount)
	g, gen, leader := groupName(gi), w.gen[gi], w.leader[gi]
	if leader == "" || w.rand.Chance(0.1) {
		return w.install(gi, nowMs)
	}
	switch w.rand.Intn(4) {
	case 0:
		return contract.GroupAssign{
			Group: g, Generation: gen, Leader: leader, TimeMs: nowMs,
			Assignments: []contract.GroupAssignment{{Member: leader, Bytes: []byte("assign")}},
		}
	case 1:
		return contract.GroupRemove{Group: g, Members: []string{leader}, TimeMs: nowMs, Reason: "leave"}
	}
	w.nextOff++
	return contract.OffsetCommit{
		Group: g, Member: leader, Generation: gen, TimeMs: nowMs,
		Offsets: []contract.PartitionOffset{{TopicPartition: w.tp(), Offset: w.nextOff, LeaderEpoch: -1}},
	}
}

func (w *workload) install(gi int, nowMs int64) contract.GroupInstall {
	w.nextMem++
	member := fmt.Sprintf("m-%d", w.nextMem)
	gen := w.gen[gi] + 1
	if w.rand.Chance(0.1) {
		gen = int32(w.rand.Intn(5)) // a wrong guess
	}
	return contract.GroupInstall{
		Group: groupName(gi), ProtocolType: "consumer", Protocol: "range", Generation: gen,
		Leader: member, TimeMs: nowMs,
		Members: []contract.GroupMember{{MemberID: member, ClientID: "c", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000}},
	}
}

// acked tells the workload that an operation succeeded, so that later group
// operations use the new generation and leader.
func (w *workload) acked(op runtime.ControlPlaneOperation) {
	if in, ok := op.(contract.GroupInstall); ok {
		for i := range groupCount {
			if groupName(i) == in.Group {
				w.gen[i], w.leader[i] = in.Generation, in.Leader
			}
		}
	}
}

// finalReads are the reads that every live node must answer the same way when
// the faults have stopped. They cover all the state the workload can make,
// and the state of the consumer groups when the run has them.
func finalReads(consumers bool) []runtime.ControlPlaneOperation {
	ops := []runtime.ControlPlaneOperation{contract.DescribeTopics{}, contract.ListGroups{}}
	for g := range groupCount {
		ops = append(ops, contract.DescribeGroup{Group: groupName(g)}, contract.OffsetFetch{Group: groupName(g)})
	}
	if consumers {
		for _, g := range consumerGroups() {
			ops = append(ops, contract.DescribeGroup{Group: g}, contract.OffsetFetch{Group: g})
		}
	}
	for t := range topicCount {
		for p := range partitionCount {
			ops = append(ops, contract.FindSegments{TopicPartition: contract.NewTopicPartition(topicName(t), int32(p))})
		}
	}
	return ops
}
