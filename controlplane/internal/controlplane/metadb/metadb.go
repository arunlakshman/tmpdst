package metadb

import (
	"fmt"
	"reflect"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/group"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/topic"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// DB is the control-plane replicated state over an engine.Engine. It is the kernel:
// it holds the schemas (topic, group) and sends each operation to the row that
// handles it. Writes (commands) must be applied by one goroutine in log order;
// reads may run concurrently.
type DB struct {
	keyValue engine.Engine
	table    table
}

// New returns a metadb over the engine (empty, or restored from a checkpoint).
func New(keyValue engine.Engine) *DB {
	return &DB{keyValue: keyValue, table: newTable(topicSchema(), groupSchema())}
}

// topicSchema is the topics and segments context: CommitSegment and
// CreateTopic are logged, FindSegments and DescribeTopics are reads.
func topicSchema() Schema {
	return Schema{
		Commands: []Command{
			CommandOf(topic.CommitSegment),
			CommandOf(topic.CreateTopic),
		},
		Reads: []Read{
			ReadOf(topic.FindSegments),
			ReadOf(topic.DescribeTopics),
		},
	}
}

// groupSchema is the consumer group context: the four group writes are logged
// and the three group reads are not. An offset commit asks the topic context
// how many partitions a topic has, through the function given here.
func groupSchema() Schema {
	return Schema{
		Commands: []Command{
			CommandOf(group.Install),
			CommandOf(group.Assign),
			CommandOf(group.Remove),
			CommandOf(group.CommitOffsets(topic.PartitionCount)),
		},
		Reads: []Read{
			ReadOf(group.OffsetFetch),
			ReadOf(group.DescribeGroup),
			ReadOf(group.ListGroups),
		},
	}
}

// Close closes the store.
func (a *DB) Close() error { return a.keyValue.Close() }

// mustStore panics when a store call failed. This is on purpose: the
// metadb stops (fail-stop) instead of going on with bad state.
func mustStore(err error) {
	if err != nil {
		panic(fmt.Errorf("metadata: store failed: %w", err))
	}
}

// Mode reports how the store treats operation: Logged (a command), ReadOnly, or
// NotMine (an operation no schema handles).
func (a *DB) Mode(operation runtime.ControlPlaneOperation) Mode {
	return a.table.mode(reflect.TypeOf(operation))
}

// Write applies a Logged operation at log slot slot, in its own transaction.
// ErrInvalid if no schema handles it as a command.
func (a *DB) Write(slot int64, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	c, ok := a.table.commands[reflect.TypeOf(operation)]
	if !ok {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
	}
	var d runtime.ControlPlaneDone
	err := a.keyValue.Update(func(transaction engine.Txn) { d = c.apply(transaction, slot, operation) })
	return d, err
}

// Read answers a ReadOnly operation in a read transaction. ErrInvalid if no
// schema handles it as a read.
func (a *DB) Read(operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	r, ok := a.table.reads[reflect.TypeOf(operation)]
	if !ok {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
	}
	var d runtime.ControlPlaneDone
	err := a.keyValue.View(func(transaction engine.Txn) { d = r.read(transaction, operation) })
	return d, err
}
