package replica_test

import (
	"errors"
	"github.com/manifesterio/mds/dst/sim"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// trackedStore is an in-memory store that remembers whether it was closed.
type trackedStore struct {
	*memory.Engine
	closed bool
}

func (s *trackedStore) Close() error {
	s.closed = true
	return nil
}

// storeSource is a Config.OpenStore that counts its calls and can fail on one.
type storeSource struct {
	opened []*trackedStore
	failOn int // 1-based call number that fails; 0 never fails
}

func (s *storeSource) open() (engine.Engine, error) {
	if s.failOn == len(s.opened)+1 {
		return nil, errors.New("disk is full")
	}
	st := &trackedStore{Engine: memory.New()}
	s.opened = append(s.opened, st)
	return st, nil
}

func tombConfig(src *storeSource) replica.Config {
	return replica.Config{
		CatchUpEvery: machine.Millisecond, Writer: "w",
		CheckpointPrefix: "checkpoint/t/", CheckpointEvery: machine.Second, Rank: 1,
		OpenStore: src.open,
	}
}

func TestOpenStoreFailureAtBuildStopsOnStart(t *testing.T) {
	m := newMachine(t, tombConfig(&storeSource{failOn: 1}))
	acts := step(m, runtime.NodeStarted{})
	stop, ok := acts[0].(sim.Crash)
	if !ok || stop.Cause != "disk is full" {
		t.Fatalf("acts = %#v, want Crash with the open error", acts)
	}
}
