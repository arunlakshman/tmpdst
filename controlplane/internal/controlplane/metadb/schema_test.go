package metadb_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// TestModeOfEveryOperation checks the table that the schemas build: every
// operation has one row, as a command or as a read, and an operation with no
// row is NotMine and gets ErrInvalid.
func TestModeOfEveryOperation(t *testing.T) {
	a := metadb.NewMem()
	for _, c := range []struct {
		operation runtime.ControlPlaneOperation
		want      metadb.Mode
	}{
		{contract.CommitSegment{}, metadb.Logged},
		{contract.CreateTopic{}, metadb.Logged},
		{contract.FindSegments{}, metadb.ReadOnly},
		{contract.DescribeTopics{}, metadb.ReadOnly},
		{contract.GroupInstall{}, metadb.Logged},
		{contract.GroupAssign{}, metadb.Logged},
		{contract.GroupRemove{}, metadb.Logged},
		{contract.OffsetCommit{}, metadb.Logged},
		{contract.OffsetFetch{}, metadb.ReadOnly},
		{contract.DescribeGroup{}, metadb.ReadOnly},
		{contract.ListGroups{}, metadb.ReadOnly},
		{nil, metadb.NotMine},
		{&contract.CreateTopic{}, metadb.NotMine},
	} {
		if got := a.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
		if c.want == metadb.NotMine {
			if d, err := a.Write(0, c.operation); err != nil || d.Err != runtime.ErrInvalid {
				t.Errorf("%T: Write %+v %v", c.operation, d, err)
			}
			if d, err := a.Read(c.operation); err != nil || d.Err != runtime.ErrInvalid {
				t.Errorf("%T: Read %+v %v", c.operation, d, err)
			}
		}
	}
}
