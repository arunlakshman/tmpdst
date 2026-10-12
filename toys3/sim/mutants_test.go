package sim

import (
	"encoding/binary"
	"flag"
	"fmt"
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

// truncAfterHalt drops anything after a Halt (the runtime contract).
func truncAfterHalt(acts []control.Action) []control.Action {
	for i, a := range acts {
		if _, ok := a.(control.Halt); ok {
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
	return truncAfterHalt(append(out, m.deferred...))
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

// (d) An error after obj/ may have changed replies 500 but does not Halt:
// the server keeps serving a page cache that a crash can undo. Implemented by
// hiding the error from the inner server (it continues as if OK) and turning
// its eventual 200 into a 500.
type noHalt struct {
	inner   control.Machine
	serving bool           // inner emitted Recovered (errors before it still halt)
	dirOnly bool           // only swallow FSFsyncDir errors
	doomed  []control.IOID // IOs of requests that already saw an error
	clients []control.ClientReq
}

func mutNoHaltOnError(env control.Env, cfg control.Config) control.Machine {
	return &noHalt{inner: control.New(env, cfg)}
}

// (d2) Like (d), but only for a failed FSFsyncDir(obj/): the rename is
// visible, not durable, and the server keeps serving it.
func mutNoHaltOnDirFsyncError(env control.Env, cfg control.Config) control.Machine {
	return &noHalt{inner: control.New(env, cfg), dirOnly: true}
}

func (m *noHalt) Step(ev control.Event) []control.Action {
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
		ev = control.Event{At: ev.At, Body: restoreErr(ev.Body)} // recovery errors still halt
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
			if _, ok := e.Fact.(control.Recovered); ok {
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
			if c, ok := e.Fact.(control.Committed); ok {
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
func (m *noHalt) stepDoomedOnly(ev control.Event) []control.Action {
	acts := m.inner.Step(ev)
	for _, a := range acts {
		if e, ok := a.(control.Emit); ok {
			if _, ok := e.Fact.(control.Recovered); ok {
				m.serving = true
			}
		}
	}
	return m.rewrite(acts)
}

// rewrite drops the Committed of doomed clients and turns their 200 into 500.
func (m *noHalt) rewrite(acts []control.Action) []control.Action {
	var out []control.Action
	for _, a := range acts {
		switch x := a.(type) {
		case control.Emit:
			if c, ok := x.Fact.(control.Committed); ok && m.isDoomed(c.Client) {
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

func (m *noHalt) isDoomed(c control.ClientReq) bool {
	for _, d := range m.clients {
		if d == c {
			return true
		}
	}
	return false
}

func (m *noHalt) forget(c control.ClientReq) {
	for i, d := range m.clients {
		if d == c {
			m.clients = append(m.clients[:i], m.clients[i+1:]...)
			return
		}
	}
}

func (m *noHalt) StateHash() uint64 { return m.inner.StateHash() }

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
	{"d-no-halt-on-error", mutNoHaltOnError},
	{"d2-no-halt-on-dir-fsync-error", mutNoHaltOnDirFsyncError},
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
// no peeking at the fake disk's durable state).
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
