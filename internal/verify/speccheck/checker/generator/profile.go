package generator

import "manifester/internal/verify/speccheck/checker"

// InputProfile holds the weights and value ranges of both generators. A weight is
// a relative chance, so a weight of 0 turns that kind of operation off. A
// percent is a chance out of 100. The named constructors below are the normal
// way to get a InputProfile; change a field only to build a new named profile.
type InputProfile struct {
	// Name is the profile name. Suites use it as the subtest name.
	Name string

	// Object store: the relative chance of each kind of operation.
	PutWeight, GetWeight, DeleteWeight, ListWeight int
	// ObjectKeys are the non-empty keys that operations draw from. A small
	// list makes keys collide often.
	ObjectKeys []string
	// ObjectPrefixes are the prefixes that List draws from.
	ObjectPrefixes []string
	// EmptyKeyPercent is the chance that an operation uses the empty key.
	EmptyKeyPercent int
	// MaxDataLen is the longest value that Put writes.
	MaxDataLen int
	// RangeSpan sets the range of Off and Len in Get: both are drawn from
	// [-1, RangeSpan-1). A large span gives more out-of-range reads.
	RangeSpan int

	// Control plane: the relative chance of each kind of operation.
	CreateTopicWeight, CommitWeight, LookupWeight, DescribeWeight int
	// Topics are the topic names that operations draw from.
	Topics []string
	// PartitionCount is the number of partitions that Lookup draws from.
	PartitionCount int
	// BatchTopics and BatchPartitions limit where commit batches go. They
	// are small so that commits build long runs on a few partitions.
	BatchTopics, BatchPartitions int
	// CreateInvalidPercent is the chance that CreateTopic gets an empty name
	// or a partition count of zero or less.
	CreateInvalidPercent int
	// MaxPartitions is the largest partition count in a valid CreateTopic.
	MaxPartitions int

	// SegmentKeys is the number of segment keys in the pool. A small pool
	// makes a key come back often, which gives replays and conflicts.
	SegmentKeys int
	// InvalidPercent is the chance that a commit is malformed.
	InvalidPercent int
	// ConflictPercent is the chance that a commit reuses a pool key with
	// different batches.
	ConflictPercent int
	// RetryNewKeyPercent is the chance that a commit sends the batches of a
	// pool key under a fresh segment key.
	RetryNewKeyPercent int
	// MaxBatches is the largest number of batches in a normal commit.
	MaxBatches int
	// ManyBatchesPercent is the chance that a commit shape is long. A long
	// shape has between ManyBatchesMin and ManyBatchesMin+ManyBatchesExtra
	// batches.
	ManyBatchesPercent, ManyBatchesMin, ManyBatchesExtra int

	// LookupShortSpan is the range of From for a normal Lookup. LookupLong
	// is the chance of drawing From from the larger range LookupLongSpan,
	// which reaches into the middle of long runs.
	LookupShortSpan, LookupLongSpan, LookupLongPercent int
	// LookupNegativeFromPercent is the chance that Lookup uses From = -1.
	LookupNegativeFromPercent int
	// MaxBytesChoices are the MaxBytes values that Lookup draws from.
	MaxBytesChoices []int32

	// MaxDescribeNames is the most topic names in a DescribeTopics. Zero
	// names means "all topics".
	MaxDescribeNames int
	// UnknownTopicPercent is the chance that a name in DescribeTopics is a
	// topic that does not exist.
	UnknownTopicPercent int
}

// InputProfiles returns every named profile, in a fixed order.
func InputProfiles() []InputProfile {
	return []InputProfile{Default(), FewDistinctKeys(), MostlyRepeats(), MostlyInvalid(), ManyBatches(), LookupCuts()}
}

// Default is the balanced profile: every kind of operation, a few keys and
// topics, short commits, and a small share of replays, conflicts and invalid
// requests.
func Default() InputProfile {
	return InputProfile{
		Name:      "default",
		PutWeight: 3, GetWeight: 2, DeleteWeight: 1, ListWeight: 1,
		ObjectKeys:      []string{"a", "a/b", "a/c", "b"},
		ObjectPrefixes:  []string{"", "a", "a/", "b", "z", "a/b"},
		EmptyKeyPercent: 8,
		MaxDataLen:      4,
		RangeSpan:       7,

		CreateTopicWeight: 4, CommitWeight: 16, LookupWeight: 8, DescribeWeight: 4,
		Topics:               []string{"t1", "t2", "t3"},
		PartitionCount:       3,
		BatchTopics:          2,
		BatchPartitions:      2,
		CreateInvalidPercent: 30,
		MaxPartitions:        4,

		SegmentKeys:        8,
		InvalidPercent:     8,
		ConflictPercent:    12,
		RetryNewKeyPercent: 20,
		MaxBatches:         3,
		ManyBatchesPercent: 12,
		ManyBatchesMin:     8,
		ManyBatchesExtra:   40,

		LookupShortSpan:           8,
		LookupLongSpan:            80,
		LookupLongPercent:         35,
		LookupNegativeFromPercent: 5,
		MaxBytesChoices:           []int32{0, 1, 51, 60, 120, 200},

		MaxDescribeNames:    2,
		UnknownTopicPercent: 25,
	}
}

// FewDistinctKeys uses two object keys, two segment keys and one batch partition, so
// operations collide on the same key or partition all the time.
func FewDistinctKeys() InputProfile {
	p := Default()
	p.Name = "small-keys"
	p.ObjectKeys = []string{"a", "a/b"}
	p.ObjectPrefixes = []string{"", "a", "a/"}
	p.MaxDataLen = 3
	p.SegmentKeys = 2
	p.Topics = []string{"t1", "t2"}
	p.PartitionCount = 2
	p.BatchTopics = 1
	p.BatchPartitions = 1
	return p
}

// MostlyRepeats has three segment keys and spends most commits on replays,
// conflicts and the same batches under a fresh key.
func MostlyRepeats() InputProfile {
	p := Default()
	p.Name = "replay-heavy"
	p.CreateTopicWeight, p.CommitWeight, p.LookupWeight, p.DescribeWeight = 2, 24, 4, 2
	p.SegmentKeys = 3
	p.InvalidPercent = 3
	p.ConflictPercent = 25
	p.RetryNewKeyPercent = 25
	return p
}

// MostlyInvalid sends many malformed requests: empty keys, short or negative
// ranges, zero records, negative partitions, empty topics and From = -1.
func MostlyInvalid() InputProfile {
	p := Default()
	p.Name = "invalid-heavy"
	p.EmptyKeyPercent = 30
	p.RangeSpan = 12
	p.CreateInvalidPercent = 60
	p.InvalidPercent = 40
	p.LookupNegativeFromPercent = 25
	p.UnknownTopicPercent = 50
	return p
}

// ManyBatches makes most commits long, with 40 or more batches, so partitions
// build long runs and one commit touches the same partition many times.
func ManyBatches() InputProfile {
	p := Default()
	p.Name = "many-batches"
	p.SegmentKeys = 6
	p.ManyBatchesPercent = 80
	p.ManyBatchesMin = 40
	p.ManyBatchesExtra = 20
	return p
}

// LookupCuts sends mostly Lookup calls with MaxBytes values that sit on the
// byte totals of a run, and with From inside long runs.
func LookupCuts() InputProfile {
	p := Default()
	p.Name = "lookup-cuts"
	p.CreateTopicWeight, p.CommitWeight, p.LookupWeight, p.DescribeWeight = 2, 12, 20, 2
	p.BatchTopics = 1
	p.BatchPartitions = 1
	p.ManyBatchesPercent = 40
	p.ManyBatchesMin = 8
	p.ManyBatchesExtra = 12
	p.LookupLongPercent = 70
	p.LookupLongSpan = 60
	p.MaxBytesChoices = []int32{0, 1, 51, 52, 101, 102, 152, 153, 202, 300}
	return p
}

// chooseByWeight returns the index of one weight, chosen with a chance in
// proportion to the weights. The weights must not all be zero.
func chooseByWeight(r *checker.Rand, weights ...int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	n := r.Intn(total)
	for i, w := range weights {
		if n < w {
			return i
		}
		n -= w
	}
	return len(weights) - 1
}

// percent returns true with a chance of p out of 100.
func percent(r *checker.Rand, p int) bool { return r.Intn(100) < p }

// pick returns one element of items.
func pick[T any](r *checker.Rand, items []T) T { return items[r.Intn(len(items))] }
