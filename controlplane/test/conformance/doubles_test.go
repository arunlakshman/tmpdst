package conformance

import (
	"context"
	"flag"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Test doubles for bugs that need a broken environment, not broken code. Each
// test wraps a correct replica target with one known fault and asserts that Run
// FAILS. A pass of the real tests means something only if these tests show that
// the same property test can find a bug. (Bugs in the code itself are the
// mutants of ops/mutants.txt.)

// recorder is a testing.TB that does not fail the real test. rapid reports its
// failure to the recorder, and the test then checks that it was told.
type recorder struct {
	testing.TB
	failed bool
}

func (r *recorder) Errorf(string, ...any) { r.failed = true }
func (r *recorder) Error(...any)          { r.failed = true }
func (r *recorder) Fatalf(string, ...any) { r.failed = true }
func (r *recorder) Fatal(...any)          { r.failed = true }
func (r *recorder) Fail()                 { r.failed = true }
func (r *recorder) FailNow()              { r.failed = true }
func (r *recorder) Failed() bool          { return r.failed }
func (r *recorder) Logf(string, ...any)   {}
func (r *recorder) Log(...any)            {}

// requireFound runs Run on the targets and fails the real test if Run passes.
func requireFound(t *testing.T, newTarget func(*rapid.T) Target) {
	t.Helper()
	fewerChecks(t, 100, 20)
	// An expected failure leaves no fail file, and it needs no long shrinking.
	for name, value := range map[string]string{"rapid.nofailfile": "true", "rapid.shrinktime": "100ms"} {
		old := flag.Lookup(name).Value.String()
		if err := flag.Set(name, value); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = flag.Set(name, old) }()
	}
	r := &recorder{TB: t}
	Run(r, newTarget)
	if !r.failed {
		t.Fatal("the planted bug was not found")
	}
}

// TestDoubleCrashLosesAnsweredWrites: the replica runs on a disk with a
// write-back cache and no flush. The last log entries were answered but were
// still in the cache, so a crash loses them. This breaks persistence.
func TestDoubleCrashLosesAnsweredWrites(t *testing.T) {
	requireFound(t, func(rt *rapid.T) Target {
		n := newReplicaNode(rt, false)
		return Target{Exec: n.exec, Reboot: n.reboot, Checkpoint: n.checkpoint, DirtyReboot: func(int) {
			n.proc.dead.Store(true)
			_ = n.node.Close()
			ctx := rt.Context()
			entries, _ := n.log.Read(ctx, 0, 1<<20)
			epoch, owner, _, _ := n.log.ClaimEpoch(ctx, 1<<62, "")
			lost := executor.NewMemoryLog()
			for _, e := range entries[:max(0, len(entries)-2)] {
				_, _, _ = lost.Append(ctx, e.Slot, e.Data)
			}
			for i := range epoch {
				_, _, _, _ = lost.ClaimEpoch(ctx, i, owner)
			}
			n.log = lost
			n.dirtyReboot(0) // the checkpoints were in the cache too
		}}
	})
}

// TestDoubleNoDataAfterFailure: a read answers "no data" after a failure was
// injected. The "has failed" flag allows an error there, never a wrong answer.
func TestDoubleNoDataAfterFailure(t *testing.T) {
	requireFound(t, func(rt *rapid.T) Target {
		n := newReplicaNode(rt, false)
		injected := false
		return Target{
			Exec: func(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
				if _, ok := op.(contract.FindSegments); ok && injected {
					return runtime.ControlPlaneDone{Resp: contract.FindSegmentsResp{}}, nil
				}
				return n.exec(ctx, op)
			},
			Reboot: n.reboot, DirtyReboot: n.dirtyReboot, Checkpoint: n.checkpoint,
			Inject: func(kind int) {
				injected = injected || kind >= 0
				n.inject(kind)
			},
		}
	})
}
