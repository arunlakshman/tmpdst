package sim

import (
	"encoding/binary"
	"flag"
	"fmt"
	"slices"
	"testing"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// Deliberately broken servers. Each wraps the real control.Controller and rewrites
// its actions or the events it sees, so the bug is exactly one protocol step.
// TestCatchesBugs proves the simulator finds every one of them.

// fakeOK is the OK completion of a filesystem action, for a mutant that
// pretends it ran.
func fakeOK(a control.Action) control.EventBody {
	switch x := a.(type) {
	case control.FSWrite:
		return control.FSWriteDone{IO: x.IO}
	case control.FSFsync:
		return control.FSFsyncDone{IO: x.IO}
	case control.FSRename:
		return control.FSRenameDone{IO: x.IO}
	case control.FSFsyncDir:
		return control.FSFsyncDirDone{IO: x.IO}
	case control.FSUnlink:
		return control.FSUnlinkDone{IO: x.IO}
	}
	panic(fmt.Sprintf("fakeOK: %T", a))
}

// truncAfterTrip drops anything after a Trip (the runtime contract).
func truncAfterTrip(acts []control.Action) []control.Action {
	for i, a := range acts {
		if _, ok := a.(control.Trip); ok {
			return acts[:i+1]
		}
	}
	return acts
}

// skipper never issues the actions skip selects: it feeds the inner server
// an immediate OK completion instead. With deferReal, the real action is
// still issued, but only after everything else the step produced (i.e.
// after the reply).
type skipper struct {
	inner     control.Machine
	skip      func(control.Action) bool
	deferReal bool
	deferred  []control.Action
}

func (m *skipper) Step(ev control.Event) []control.Action {
	m.deferred = nil
	out := m.expand(ev.At, m.inner.Step(ev))
	return truncAfterTrip(append(out, m.deferred...))
}

func (m *skipper) expand(at control.Time, acts []control.Action) []control.Action {
	var out []control.Action
	for _, a := range acts {
		if !m.skip(a) {
			out = append(out, a)
			continue
		}
		if m.deferReal {
			m.deferred = append(m.deferred, a)
		}
		out = append(out, m.expand(at, m.inner.Step(control.Event{At: at, Body: fakeOK(a)}))...)
	}
	return out
}

func (m *skipper) StateHash() uint64 { return m.inner.StateHash() }

func isObjFsyncDir(dir string) func(control.Action) bool {
	return func(a control.Action) bool {
		d, ok := a.(control.FSFsyncDir)
		return ok && d.Dir == format.ObjDir(dir)
	}
}

// (a) Rename is never made durable: no FSFsyncDir(obj/).
func mutSkipDirFsync(env control.Env, cfg control.Config) control.Machine {
	return &skipper{inner: control.New(env, cfg), skip: isObjFsyncDir(cfg.Dir)}
}

// (b) The temp file's bytes are never fsynced before the rename.
func mutSkipFileFsync(env control.Env, cfg control.Config) control.Machine {
	return &skipper{inner: control.New(env, cfg), skip: func(a control.Action) bool {
		_, ok := a.(control.FSFsync)
		return ok
	}}
}

// (c) Reply 200 first, then rename and fsync the directory (ack before
// durable, and even before visible).
func mutAckBeforeRename(env control.Env, cfg control.Config) control.Machine {
	obj := isObjFsyncDir(cfg.Dir)
	return &skipper{inner: control.New(env, cfg), deferReal: true, skip: func(a control.Action) bool {
		_, ren := a.(control.FSRename)
		return ren || obj(a)
	}}
}

// (d) An error after obj/ may have changed replies 500 but does not Trip:
// the server keeps serving a page cache that a crash can undo. Implemented by
// hiding the error from the inner server (it continues as if OK) and turning
// its eventual 200 into a 500.
type noTrip struct {
	inner   control.Machine
	serving bool           // inner emitted Recovered (errors before it still trip)
	dirOnly bool           // only swallow FSFsyncDir errors
	doomed  []control.IOID // IOs of requests that already saw an error
	clients []control.ClientReq
}

func mutNoTripOnError(env control.Env, cfg control.Config) control.Machine {
	return &noTrip{inner: control.New(env, cfg)}
}

// (d2) Like (d), but only for a failed FSFsyncDir(obj/): the rename is
// visible, not durable, and the server keeps serving it.
func mutNoTripOnDirFsyncError(env control.Env, cfg control.Config) control.Machine {
	return &noTrip{inner: control.New(env, cfg), dirOnly: true}
}

func (m *noTrip) Step(ev control.Event) []control.Action {
	io, isErr, doomed := control.IOID(0), false, false
	if _, dir := ev.Body.(control.FSFsyncDirDone); m.dirOnly && !dir {
		return m.stepDoomedOnly(ev)
	}
	switch b := ev.Body.(type) {
	case control.FSFsyncDone:
		io, isErr = b.IO, b.Err == control.ErrIO
		if isErr {
			ev.Body = control.FSFsyncDone{IO: b.IO}
		}
	case control.FSFsyncDirDone:
		io, isErr = b.IO, b.Err == control.ErrIO
		if isErr {
			ev.Body = control.FSFsyncDirDone{IO: b.IO}
		}
	case control.FSRenameDone:
		io, isErr = b.IO, b.Err == control.ErrIO
		if isErr {
			ev.Body = control.FSRenameDone{IO: b.IO}
		}
	case control.FSUnlinkDone:
		io, isErr = b.IO, b.Err == control.ErrIO
		if isErr {
			ev.Body = control.FSUnlinkDone{IO: b.IO}
		}
	}
	if !m.serving && isErr {
		ev = control.Event{At: ev.At, Body: restoreErr(ev.Body)} // recovery errors still trip
		isErr = false
	}
	for i, d := range m.doomed {
		if d == io && io != 0 {
			doomed = true
			m.doomed = append(m.doomed[:i], m.doomed[i+1:]...)
			break
		}
	}
	acts := m.inner.Step(ev)
	for _, a := range acts {
		if e, ok := a.(control.Emit); ok {
			if _, ok := e.Telemetry.(control.Recovered); ok {
				m.serving = true
			}
		}
	}
	if !isErr && !doomed {
		return m.rewrite(acts)
	}
	// This request is doomed: follow its next IO, or fail its final reply.
	for _, a := range acts {
		if e, ok := a.(control.Emit); ok {
			if c, ok := e.Telemetry.(control.Committed); ok {
				m.clients = append(m.clients, c.Client)
				return m.rewrite(acts)
			}
		}
	}
	if len(acts) == 1 {
		if next, ok := ioOfAction(acts[0]); ok {
			m.doomed = append(m.doomed, next)
		}
	}
	return m.rewrite(acts)
}

// stepDoomedOnly passes ev through unchanged, but still tracks doomed IOs.
func (m *noTrip) stepDoomedOnly(ev control.Event) []control.Action {
	acts := m.inner.Step(ev)
	for _, a := range acts {
		if e, ok := a.(control.Emit); ok {
			if _, ok := e.Telemetry.(control.Recovered); ok {
				m.serving = true
			}
		}
	}
	return m.rewrite(acts)
}

// rewrite drops the Committed of doomed clients and turns their 200 into 500.
func (m *noTrip) rewrite(acts []control.Action) []control.Action {
	var out []control.Action
	for _, a := range acts {
		switch x := a.(type) {
		case control.Emit:
			if c, ok := x.Telemetry.(control.Committed); ok && m.isDoomed(c.Client) {
				continue
			}
		case control.Reply:
			if m.isDoomed(x.Client) {
				m.forget(x.Client)
				x.Status = control.StatusServerError
				a = x
			}
		}
		out = append(out, a)
	}
	return out
}

func (m *noTrip) isDoomed(c control.ClientReq) bool {
	for _, d := range m.clients {
		if d == c {
			return true
		}
	}
	return false
}

func (m *noTrip) forget(c control.ClientReq) {
	for i, d := range m.clients {
		if d == c {
			m.clients = append(m.clients[:i], m.clients[i+1:]...)
			return
		}
	}
}

func (m *noTrip) StateHash() uint64 { return m.inner.StateHash() }

func restoreErr(b control.EventBody) control.EventBody {
	switch x := b.(type) {
	case control.FSFsyncDone:
		x.Err = control.ErrIO
		return x
	case control.FSFsyncDirDone:
		x.Err = control.ErrIO
		return x
	case control.FSRenameDone:
		x.Err = control.ErrIO
		return x
	case control.FSUnlinkDone:
		x.Err = control.ErrIO
		return x
	}
	return b
}

func ioOfAction(a control.Action) (control.IOID, bool) {
	if x, ok := a.(control.Actuation); ok {
		return x.ID(), true
	}
	return 0, false
}

// eventRewriter rewrites events before the inner server sees them.
type eventRewriter struct {
	inner   control.Machine
	rewrite func(control.EventBody) control.EventBody
}

func (m *eventRewriter) Step(ev control.Event) []control.Action {
	ev.Body = m.rewrite(ev.Body)
	return m.inner.Step(ev)
}

func (m *eventRewriter) StateHash() uint64 { return m.inner.StateHash() }

// (e) The CRC is not verified on read: a file with an intact header and
// length but a bad checksum is served (GET) or copied (COPY).
func mutSkipCRC(env control.Env, cfg control.Config) control.Machine {
	return &eventRewriter{inner: control.New(env, cfg), rewrite: func(b control.EventBody) control.EventBody {
		d, ok := b.(control.FSReadDone)
		if !ok || d.Err != control.OK || len(d.Data) < format.HeaderSize || string(d.Data[:4]) != format.Magic ||
			binary.LittleEndian.Uint64(d.Data[8:]) != uint64(len(d.Data)-format.HeaderSize) {
			return b
		}
		d.Data = format.EncodeObject(d.Data[format.HeaderSize:]) // "verified"
		return d
	}}
}

// (f) Recovery does not remove leftover temp files.
func mutSkipTempCleanup(env control.Env, cfg control.Config) control.Machine {
	return &eventRewriter{inner: control.New(env, cfg), rewrite: func(b control.EventBody) control.EventBody {
		if d, ok := b.(control.FSListDone); ok && d.Err == control.OK {
			d.Names = nil
			return d
		}
		return b
	}}
}

type mutant struct {
	name    string
	factory control.Factory
}

var mutants = []mutant{
	{"a-skip-dir-fsync", mutSkipDirFsync},
	{"b-skip-file-fsync", mutSkipFileFsync},
	{"c-ack-before-rename", mutAckBeforeRename},
	{"d-no-halt-on-error", mutNoTripOnError},
	{"d2-no-halt-on-dir-fsync-error", mutNoTripOnDirFsyncError},
	{"e-skip-crc", mutSkipCRC},
	{"f-skip-temp-cleanup", mutSkipTempCleanup},
}

var mutantSeeds = flag.Int("mutant-seeds", 500, "TestCatchesBugs: seeds to try per mutant")

// firstCatch runs seeds 1..n against factory and returns the first failing
// seed (0 if none) and its failure.
func firstCatch(factory control.Factory, invs func() []Invariant, n int) (uint64, Failure) {
	for seed := uint64(1); seed <= uint64(n); seed++ {
		cfg := DefaultConfig(seed)
		cfg.Factory = factory
		cfg.Invariants = invs()
		if r := Run(cfg); len(r.Failures) > 0 {
			return seed, r.Failures[0]
		}
	}
	return 0, Failure{}
}

// TestCatchesBugs is the proof that the simulator has teeth: every mutant
// must fail some seed within -mutant-seeds seeds. It also reports, for
// learning, whether the bug is visible to clients alone (BlackBoxInvariants:
// no peeking at the storage model's durable state).
func TestCatchesBugs(t *testing.T) {
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			seed, f := firstCatch(m.factory, DefaultInvariants, *mutantSeeds)
			if seed == 0 {
				t.Fatalf("not caught in %d seeds", *mutantSeeds)
			}
			t.Logf("caught at seed %d by %s (step %d): %.200s", seed, f.Invariant, f.Step, f.Msg)
			bseed, bf := firstCatch(m.factory, BlackBoxInvariants, *mutantSeeds)
			if bseed == 0 {
				t.Logf("black-box invariants only: not caught in %d seeds", *mutantSeeds)
			} else {
				t.Logf("black-box invariants only: caught at seed %d by %s", bseed, bf.Invariant)
			}
		})
	}
}

// Functional mutants: bugs in what a request does, not in how it survives
// faults or crashes. They are kept out of mutants (and so out of TestGolden
// and TestCatchesBugs) because DST is not required to catch them: its
// workload never sends an oversized Put, for one. TestMutantTable records
// who catches each.

// requestRewriter rewrites client requests before the inner server sees them.
type requestRewriter struct {
	inner   control.Machine
	rewrite func(control.Op) control.Op
}

func (m *requestRewriter) Step(ev control.Event) []control.Action {
	if r, ok := ev.Body.(control.Request); ok {
		r.Op = m.rewrite(r.Op)
		ev.Body = r
	}
	return m.inner.Step(ev)
}

func (m *requestRewriter) StateHash() uint64 { return m.inner.StateHash() }

// (g) Copy writes to its source key: Copy(src, dst) copies src onto itself
// and leaves dst alone, yet replies 200.
func mutCopyToSrc(env control.Env, cfg control.Config) control.Machine {
	return &requestRewriter{inner: control.New(env, cfg), rewrite: func(op control.Op) control.Op {
		if c, ok := op.(control.Copy); ok {
			c.Dst = c.Src
			return c
		}
		return op
	}}
}

// deleteMissingOK is mutant (h): a Delete of a missing key replies 200, not
// 404.
type deleteMissingOK struct {
	inner   control.Machine
	deletes []control.ClientReq // Delete requests not yet replied to
}

func mutDeleteMissingOK(env control.Env, cfg control.Config) control.Machine {
	return &deleteMissingOK{inner: control.New(env, cfg)}
}

func (m *deleteMissingOK) Step(ev control.Event) []control.Action {
	if r, ok := ev.Body.(control.Request); ok {
		if _, del := r.Op.(control.Delete); del {
			m.deletes = append(m.deletes, r.Client)
		}
	}
	acts := m.inner.Step(ev)
	for i, a := range acts {
		rep, ok := a.(control.Reply)
		if !ok {
			continue
		}
		for j, c := range m.deletes {
			if c == rep.Client {
				m.deletes = append(m.deletes[:j], m.deletes[j+1:]...)
				if rep.Status == control.StatusNotFound {
					rep.Status = control.StatusOK
					acts[i] = rep
				}
				break
			}
		}
	}
	return acts
}

func (m *deleteMissingOK) StateHash() uint64 { return m.inner.StateHash() }

// (i) Off by one in the size check: a Put of MaxObjectSize+1 bytes is
// accepted (as if the check were len > MaxObjectSize+1).
func mutSizeOffByOne(env control.Env, cfg control.Config) control.Machine {
	if cfg.MaxObjectSize > 0 {
		cfg.MaxObjectSize++
	}
	return control.New(env, cfg)
}

// recoverObj is mutant (j): recovery also clears obj/. Before the real
// recovery lists tmp/, it lists obj/ and unlinks every committed object, as
// a recovery that loops over both directories would. Its own storage
// commands use IOIDs far above any the controller issues, and their
// completions never reach the controller; their errors are ignored.
type recoverObj struct {
	inner   control.Machine
	obj     string
	held    []control.Action // the controller's FSList(tmp), until obj/ is clear
	pending []control.IOID   // the mutant's outstanding commands
	nextIO  control.IOID
}

func mutRecoveryDeletesObj(env control.Env, cfg control.Config) control.Machine {
	return &recoverObj{inner: control.New(env, cfg), obj: format.ObjDir(cfg.Dir), nextIO: 1 << 62}
}

func (m *recoverObj) issue(a func(control.IOID) control.Action) control.Action {
	m.nextIO++
	m.pending = append(m.pending, m.nextIO)
	return a(m.nextIO)
}

func (m *recoverObj) Step(ev control.Event) []control.Action {
	if fb, ok := ev.Body.(control.Feedback); ok {
		if i := slices.Index(m.pending, fb.ID()); i >= 0 {
			m.pending = slices.Delete(m.pending, i, i+1)
			var out []control.Action
			if d, ok := ev.Body.(control.FSListDone); ok {
				for _, name := range d.Names {
					out = append(out, m.issue(func(io control.IOID) control.Action {
						return control.FSUnlink{IO: io, Path: m.obj + "/" + name}
					}))
				}
			}
			if len(m.pending) == 0 {
				out, m.held = append(out, m.held...), nil
			}
			return out
		}
	}
	acts := m.inner.Step(ev)
	if _, ok := ev.Body.(control.Started); ok && len(acts) == 1 {
		m.held = acts
		return []control.Action{m.issue(func(io control.IOID) control.Action {
			return control.FSList{IO: io, Dir: m.obj}
		})}
	}
	return acts
}

func (m *recoverObj) StateHash() uint64 { return m.inner.StateHash() }

var functionalMutants = []mutant{
	{"g-copy-to-src", mutCopyToSrc},
	{"h-delete-missing-ok", mutDeleteMissingOK},
	{"i-size-off-by-one", mutSizeOffByOne},
	{"j-recovery-deletes-obj", mutRecoveryDeletesObj},
}
