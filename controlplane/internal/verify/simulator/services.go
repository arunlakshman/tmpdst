package simulator

import (
	"context"
	"slices"
	"sort"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/check"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

// Bug names one planted fault. The tests use it to prove that the simulator
// finds a bug when there is one. A normal run uses BugNone.
type Bug string

// The planted bugs.
const (
	BugNone Bug = ""
	// BugLogOverwrite makes the log say "stored" for a put into a slot that is
	// already taken, and keep the old bytes.
	BugLogOverwrite Bug = "log-overwrite"
	// BugClaimAlwaysWins makes a claim of the writer epoch win even when the
	// stored epoch is not the one expected, so two nodes can hold one epoch.
	BugClaimAlwaysWins Bug = "claim-always-wins"
)

// callMode is what the fault plan decides for one call to the log or the
// object store.
type callMode uint8

const (
	callOK      callMode = iota
	callFail             // error reply, nothing applied
	callTimeout          // timeout reply, nothing applied
	callLost             // applied, but the reply is lost: a timeout reply
)

const (
	callTimeoutAfter = 1 * machine.Second
	netMin           = 1 * machine.Millisecond
	netMax           = 4 * machine.Millisecond
	slowMin          = 50 * machine.Millisecond // extra delay of a slow call or a slow answer
	slowMax          = 400 * machine.Millisecond
)

// storage is the shared fake log and the fake object store. Calls reach it
// with a delay, take effect at one instant, and the reply comes back with
// another delay. Faults and partitions act on the calls of one node.
type storage struct {
	s    *sim
	log  *executor.MemoryLog
	objs *executor.MemoryObjects
	ctx  context.Context

	// history is the first entry that was committed in each slot. Trims delete
	// slots from the log but not from here: it is the truth for the oracle.
	history     map[runtime.LogPosition][]byte
	epochWriter check.Holders[string]        // writer epoch -> writer that wrote entries with it
	epochOwner  check.Holders[string]        // writer epoch -> owner that won the claim
	end         runtime.LogPosition          // history has every slot below end
	changes     map[runtime.LogPosition]bool // slots that carry a command that changes state (not a heartbeat or a fence)
}

func newStorage(s *sim) *storage {
	return &storage{
		s: s, log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects(), ctx: context.Background(),
		history: map[runtime.LogPosition][]byte{},
		changes: map[runtime.LogPosition]bool{},
	}
}

func (st *storage) netDelay(r *rng.Rand) machine.Duration {
	d := machine.Duration(r.Between(int64(netMin), int64(netMax)))
	if r.Chance(st.s.faults.Slow) {
		d += machine.Duration(r.Between(int64(slowMin), int64(slowMax)))
	}
	return d
}

func isWrite(call runtime.IOCall) bool {
	switch call.(type) {
	case runtime.LogAppend, runtime.LogDeleteOld, runtime.LogClaimEpoch, runtime.ObjectPut, runtime.ObjectDelete:
		return true
	}
	return false
}

func (st *storage) decide(r *rng.Rand, call runtime.IOCall) callMode {
	f := st.s.faults
	if st.s.quiet {
		return callOK
	}
	if r.Chance(f.Fail) {
		return callFail
	}
	if r.Chance(f.Timeout) {
		return callTimeout
	}
	if isWrite(call) && r.Chance(f.Lost) {
		return callLost
	}
	return callOK
}

// storage is a dst engine (eng.Engine). A process hands it an IOCall as an
// outbound value. The call reaches the storage after a delay, takes effect at
// one instant, and the reply goes back to the caller with another delay.
var _ eng.Engine = (*storage)(nil)

// ID implements eng.Engine.
func (st *storage) ID() eng.ID { return "storage" }

// StateHash implements eng.Engine. The oracle reads the history, not the hash.
func (st *storage) StateHash() uint64 { return 0 }

// Handle implements eng.Engine: it takes an IOCall and queues its effect.
func (st *storage) Handle(cx eng.Context, act any) error {
	call, ok := act.(runtime.IOCall)
	if !ok {
		return eng.ErrNotMine
	}
	cx.Self(sched.TimeNs(st.netDelay(cx.Rand())), call)
	return nil
}

// OnSelf implements eng.Engine: the call arrives at the storage.
func (st *storage) OnSelf(cx eng.Context, body any) {
	res, back := st.execute(cx, body.(runtime.IOCall))
	cx.Deliver(cx.From(), sched.TimeNs(back), res)
}

// execute takes the effect of call (or not) and returns the reply with the
// delay after which it arrives.
func (st *storage) execute(cx eng.Context, call runtime.IOCall) (runtime.IOResult, machine.Duration) {
	n := st.s.nodeOf(cx.From())
	timeoutReply := func() (runtime.IOResult, machine.Duration) {
		out := runtime.NotApplied
		if isWrite(call) {
			out = runtime.MaybeApplied
		}
		return errorReply(call, runtime.ErrTimeout, out), callTimeoutAfter
	}
	if n.cut {
		st.s.stats.CutCalls++
		return timeoutReply()
	}
	switch st.decide(cx.Rand(), call) {
	case callFail:
		st.s.stats.FailedCalls++
		return errorReply(call, runtime.ErrUnavailable, runtime.NotApplied), st.netDelay(cx.Rand())
	case callTimeout:
		st.s.stats.FailedCalls++
		return timeoutReply()
	case callLost:
		st.s.stats.LostReplies++
		st.apply(call)
		return timeoutReply()
	}
	res := st.apply(call)
	return res, st.netDelay(cx.Rand()) + st.slowAnswer(cx.Rand(), call)
}

// errorReply is the failure completion that matches call.
func errorReply(call runtime.IOCall, code runtime.ErrCode, out runtime.Outcome) runtime.IOResult {
	switch c := call.(type) {
	case runtime.LogAppend:
		return runtime.LogAppendDone{Req: c.Req, Slot: c.Slot, Err: code, Outcome: out}
	case runtime.LogRead:
		return runtime.LogReadDone{Req: c.Req, Err: code, Outcome: out}
	case runtime.LogGetEnd:
		return runtime.LogGetEndDone{Req: c.Req, Err: code, Outcome: out}
	case runtime.LogDeleteOld:
		return runtime.LogDeleteOldDone{Req: c.Req, Err: code, Outcome: out}
	case runtime.LogClaimEpoch:
		return runtime.LogClaimEpochDone{Req: c.Req, Err: code, Outcome: out}
	case runtime.ObjectPut:
		return runtime.ObjectPutDone{Req: c.Req, Key: c.Key, Err: code, Outcome: out}
	case runtime.ObjectGet:
		return runtime.ObjectGetDone{Req: c.Req, Err: code}
	case runtime.ObjectDelete:
		return runtime.ObjectDeleteDone{Req: c.Req, Err: code, Outcome: out}
	case runtime.ObjectList:
		return runtime.ObjectListDone{Req: c.Req, Err: code}
	}
	panic("simulator: unknown IO call")
}

// apply runs call on the real in-memory log and object store, and answers like
// the executor does.
//
//nolint:dupl // the cases are alike but each calls a different method
func (st *storage) apply(call runtime.IOCall) runtime.IOResult {
	switch c := call.(type) {
	case runtime.LogAppend:
		return st.applyAppend(c)
	case runtime.LogRead:
		ents, err := st.log.Read(st.ctx, c.From, int(c.Limit))
		if err != nil {
			return st.failed(call, err)
		}
		return runtime.LogReadDone{Req: c.Req, Entries: st.shortPage(ents), Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogGetEnd:
		tail, err := st.log.CheckTail(st.ctx)
		if err != nil {
			return st.failed(call, err)
		}
		return runtime.LogGetEndDone{Req: c.Req, Tail: tail, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogDeleteOld:
		if err := st.log.TrimThrough(st.ctx, c.Through); err != nil {
			return st.failed(call, err)
		}
		return runtime.LogDeleteOldDone{Req: c.Req, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogClaimEpoch:
		return st.applyClaim(c)
	}
	return st.applyObject(call)
}

func (st *storage) failed(call runtime.IOCall, err error) runtime.IOResult {
	code, out := executor.Classify(call, err)
	return errorReply(call, code, out)
}

func (st *storage) applyAppend(c runtime.LogAppend) runtime.IOResult {
	existing, taken, err := st.log.Append(st.ctx, c.Slot, c.Data)
	if err != nil {
		return st.failed(c, err)
	}
	switch {
	case taken && len(existing) == 0:
		// At or below the floor: not a commit, and no entry to show.
		return runtime.LogAppendDone{Req: c.Req, Slot: c.Slot, Taken: true, Err: runtime.ErrConflict, Outcome: runtime.NotApplied}
	case taken && st.s.bug == BugLogOverwrite:
		return runtime.LogAppendDone{Req: c.Req, Slot: c.Slot, Err: runtime.OK, Outcome: runtime.Applied}
	case taken:
		return runtime.LogAppendDone{Req: c.Req, Slot: c.Slot, Taken: true, Existing: existing, Err: runtime.OK, Outcome: runtime.Applied}
	}
	st.committed(c.Slot, c.Data)
	return runtime.LogAppendDone{Req: c.Req, Slot: c.Slot, Err: runtime.OK, Outcome: runtime.Applied}
}

// committed records a new entry in the history and checks that one epoch has
// one writer.
func (st *storage) committed(slot runtime.LogPosition, data []byte) {
	st.history[slot] = slices.Clone(data)
	for _, ok := st.history[st.end]; ok; _, ok = st.history[st.end] {
		st.end++
	}
	if cmds, err := replica.EntryCommands(data); err == nil {
		st.changes[slot] = slices.ContainsFunc(cmds, func(c metadb.Entry) bool { return c.Op != nil })
	}
	epoch, writer, ok := replica.EntryWriter(data)
	if !ok {
		return
	}
	if first, ok := st.epochWriter.Note(epoch, writer); !ok {
		st.s.violation(oneWriterPerEpoch, "epoch %d has entries from %q and from %q (slot %d)", epoch, first, writer, slot)
	}
}

func (st *storage) applyClaim(c runtime.LogClaimEpoch) runtime.IOResult {
	epoch, owner, won, err := st.log.ClaimEpoch(st.ctx, c.Expect, c.Owner)
	if err != nil {
		return st.failed(c, err)
	}
	if !won && st.s.bug == BugClaimAlwaysWins {
		epoch, owner, won = c.Expect+1, c.Owner, true
	}
	if won {
		if first, ok := st.epochOwner.Note(epoch, owner); !ok {
			st.s.violation(oneWriterPerEpoch, "epoch %d was won by %q and by %q", epoch, first, owner)
		}
	}
	return runtime.LogClaimEpochDone{Req: c.Req, Epoch: epoch, Owner: owner, Won: won, Err: runtime.OK, Outcome: runtime.Applied}
}

//nolint:dupl // the cases are alike but each calls a different method
func (st *storage) applyObject(call runtime.IOCall) runtime.IOResult {
	switch c := call.(type) {
	case runtime.ObjectPut:
		if err := st.objs.Put(st.ctx, c.Key, c.Data); err != nil {
			return st.failed(call, err)
		}
		return runtime.ObjectPutDone{Req: c.Req, Key: c.Key, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.ObjectGet:
		data, err := st.objs.Get(st.ctx, c.Ref)
		if err != nil {
			return st.failed(call, err)
		}
		if len(data) > 0 && !st.s.quiet && st.s.rand.Chance(st.s.faults.Corrupt) {
			data[len(data)/2] ^= 0xff // Get returns a copy, so the stored object is not changed
		}
		return runtime.ObjectGetDone{Req: c.Req, Data: data}
	case runtime.ObjectDelete:
		if err := st.objs.Delete(st.ctx, c.Key); err != nil {
			return st.failed(call, err)
		}
		return runtime.ObjectDeleteDone{Req: c.Req, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.ObjectList:
		keys, err := st.objs.List(st.ctx, c.Prefix)
		if err != nil {
			return st.failed(call, err)
		}
		sort.Strings(keys) // the real store lists in key order; MemoryObjects does not
		return runtime.ObjectListDone{Req: c.Req, Keys: keys}
	}
	return errorReply(call, runtime.ErrInvalid, runtime.NotApplied)
}

// logEnd is the number of slots in the history if it has no hole, and the
// first hole otherwise.
func (st *storage) logEnd() (runtime.LogPosition, bool) {
	return st.end, len(st.history) > int(st.end)
}

// storage is the check.Log of the oracle.
var _ check.Log = (*storage)(nil)

// End implements check.Log.
func (st *storage) End() (check.Slot, bool) {
	end, hole := st.logEnd()
	return check.Slot(end), hole
}

// Entry implements check.Log.
func (st *storage) Entry(slot check.Slot) []byte { return st.history[runtime.LogPosition(slot)] }

// QuietAfter implements check.Log.
func (st *storage) QuietAfter(from check.Slot) bool { return st.quietAfter(runtime.LogPosition(from)) }

// quietAfter reports that the slots from `from` to the end of the log carry no
// command that changes state. A node that has applied up to `from` then has the
// final state, even if a heartbeat is still on its way to it.
func (st *storage) quietAfter(from runtime.LogPosition) bool {
	end, _ := st.logEnd()
	for slot := from; slot < end; slot++ {
		if st.changes[slot] {
			return false
		}
	}
	return true
}
