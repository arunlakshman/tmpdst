package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The real replica in one process, on the in-memory log and object store. The
// log and the objects are the disk: they survive a restart. A process is what
// one run of the node sees of the disk. A crash kills the process: from then on
// its storage calls fail and change nothing, so nothing it had in flight lands
// later.

var errInjected = errors.New("injected failure")

// never is the kind of a storage call that Inject cannot make fail.
const never = -1

type process struct {
	log   *executor.MemoryLog
	objs  *executor.MemoryObjects
	dead  atomic.Bool
	mu    sync.Mutex
	armed [failureKinds]int // failures still to inject, by kind
}

var (
	_ executor.Log         = (*process)(nil)
	_ executor.ObjectStore = (*process)(nil)
)

// fail says if a storage call of this kind fails: the process is dead, or a
// failure of the kind is armed (and is used up).
func (p *process) fail(kind int) error {
	if p.dead.Load() {
		return errInjected
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind != never && p.armed[kind] > 0 {
		p.armed[kind]--
		return errInjected
	}
	return nil
}

func (p *process) Append(ctx context.Context, slot runtime.LogPosition, data []byte) ([]byte, bool, error) {
	if err := p.fail(failAppend); err != nil {
		return nil, false, err
	}
	existing, taken, err := p.log.Append(ctx, slot, data)
	if err == nil && p.fail(failAppendLate) != nil {
		return nil, false, errInjected // stored, but the answer is lost
	}
	return existing, taken, err
}

func (p *process) Read(ctx context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error) {
	if err := p.fail(failRead); err != nil {
		return nil, err
	}
	return p.log.Read(ctx, from, limit)
}

func (p *process) CheckTail(ctx context.Context) (runtime.LogPosition, error) {
	if err := p.fail(failRead); err != nil {
		return 0, err
	}
	return p.log.CheckTail(ctx)
}

func (p *process) TrimThrough(ctx context.Context, through runtime.LogPosition) error {
	if err := p.fail(never); err != nil {
		return err
	}
	return p.log.TrimThrough(ctx, through)
}

func (p *process) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	if err := p.fail(never); err != nil {
		return 0, "", false, err
	}
	return p.log.ClaimEpoch(ctx, expect, owner)
}

func (p *process) Put(ctx context.Context, key string, data []byte) error {
	if err := p.fail(failPut); err != nil {
		return err
	}
	return p.objs.Put(ctx, key, data)
}

func (p *process) Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error) {
	if err := p.fail(never); err != nil {
		return nil, err
	}
	return p.objs.Get(ctx, ref)
}

func (p *process) Delete(ctx context.Context, key string) error {
	if err := p.fail(never); err != nil {
		return err
	}
	return p.objs.Delete(ctx, key)
}

func (p *process) List(ctx context.Context, prefix string) ([]string, error) {
	if err := p.fail(never); err != nil {
		return nil, err
	}
	return p.objs.List(ctx, prefix)
}

const checkpointPrefix = "checkpoint/"

// replicaNode is a server.Node that is restarted on the same disk.
type replicaNode struct {
	rt   *rapid.T
	cfg  replica.Config
	log  *executor.MemoryLog
	objs *executor.MemoryObjects
	proc *process
	node *server.Node
}

// newReplicaNode starts a node. With trim, the log is cut behind each
// checkpoint, so the state must come back from the checkpoint and the rest of
// the log.
func newReplicaNode(rt *rapid.T, trim bool) *replicaNode {
	n := &replicaNode{rt: rt, log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects(), cfg: replica.Config{
		Rank: 0, Writer: "cp-0", CatchUpEvery: runtime.Duration(time.Millisecond), Heartbeat: runtime.Duration(time.Second),
		TakeoverAfter:   runtime.Duration(20 * time.Millisecond), // a restarted node takes the write role after this
		CheckpointEvery: runtime.Duration(3 * time.Millisecond), CheckpointMinSlots: 1, CheckpointPrefix: checkpointPrefix,
	}}
	if trim {
		n.cfg.KeepSlots = 2
	}
	n.start()
	rt.Cleanup(func() {
		if n.node != nil {
			_ = n.node.Close()
		}
	})
	return n
}

func (n *replicaNode) start() {
	n.proc = &process{log: n.log, objs: n.objs}
	n.node = server.New(server.Config{Machine: n.cfg, Log: n.proc, Objects: n.proc,
		Self: runtime.NodeInstance{Node: "cp-0"}, StopGrace: runtime.Duration(50 * time.Millisecond)})
	ctx, cancel := context.WithTimeout(n.rt.Context(), 10*time.Second)
	defer cancel()
	if err := n.node.Start(ctx); err != nil {
		n.node = nil // Start already stopped it, and Close would wait for ever
		n.rt.Fatalf("the node did not start: %v", err)
	}
}

// exec asks again while the node answers ErrUnavailable: it is not the writer
// yet, or not ready. That answer means "not accepted, try later".
func (n *replicaNode) exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	for {
		d, err := n.node.Exec(ctx, op)
		if err != nil || d.Err != runtime.ErrUnavailable || ctx.Err() != nil {
			return d, err
		}
		time.Sleep(time.Millisecond)
	}
}

// reboot stops the node, and a new one starts on the same disk.
func (n *replicaNode) reboot() {
	_ = n.node.Close()
	n.start()
}

// dirtyReboot kills the process, drops checkpoints as keep says, and starts a
// new process on the same disk. The log is never touched: a write that was
// answered is in it. With a trimmed log the log alone cannot rebuild the
// state, so all checkpoints stay.
func (n *replicaNode) dirtyReboot(keep int) {
	n.proc.dead.Store(true)
	_ = n.node.Close()
	if n.cfg.KeepSlots > 0 {
		keep = 0
	}
	keys := n.checkpoints()
	switch keep {
	case 0:
		keys = nil
	case 1:
		keys = keys[max(0, len(keys)-1):] // the upload of the newest had not finished
	}
	for _, k := range keys {
		_ = n.objs.Delete(n.rt.Context(), k)
	}
	n.start()
}

// checkpoints are the checkpoint keys on the disk, oldest first.
func (n *replicaNode) checkpoints() []string {
	keys, _ := n.objs.List(n.rt.Context(), checkpointPrefix)
	slices.Sort(keys)
	return keys
}

// checkpoint waits (a little) until a checkpoint covers the whole log.
func (n *replicaNode) checkpoint() {
	tail, _ := n.log.CheckTail(n.rt.Context())
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if keys := n.checkpoints(); len(keys) > 0 {
			slot, err := strconv.ParseInt(strings.TrimPrefix(keys[len(keys)-1], checkpointPrefix), 10, 64)
			if err == nil && slot >= int64(tail) {
				return
			}
		}
	}
}

// inject arms one failure of a kind for the next call of that kind. -1 stops
// all failures.
func (n *replicaNode) inject(kind int) {
	p := n.proc
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind < 0 {
		p.armed = [failureKinds]int{}
		return
	}
	p.armed[kind]++
}

// TestReplica is the crash test of the paper (section 5) on the real replica:
// clean restarts, crashes and checkpoints between the operations, with and
// without log trim, and with and without injected storage failures (a failed
// append, an append whose answer is lost, a failed read, a failed checkpoint
// upload).
func TestReplica(t *testing.T) {
	fewerChecks(t, 30, 20)
	for _, trim := range []bool{false, true} {
		for _, failures := range []bool{false, true} {
			t.Run(fmt.Sprintf("trim=%t,failures=%t", trim, failures), func(t *testing.T) {
				t.Parallel() // the time goes to waiting (takeover, checkpoints), not to work
				Run(t, func(rt *rapid.T) Target {
					n := newReplicaNode(rt, trim)
					tg := Target{Exec: n.exec, Reboot: n.reboot, DirtyReboot: n.dirtyReboot, Checkpoint: n.checkpoint}
					if failures {
						tg.Inject = n.inject
					}
					return tg
				})
			})
		}
	}
}
