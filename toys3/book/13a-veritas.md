# Chapter 13a: The Veritas: Executable Truth and Conformance Checks

> *"For each ShardStore component we developed a reference model: an executable specification in Rust that provides the same interface as the component but using a simpler implementation."* (Bornholt et al., *Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3*, SOSP 2021, §3.2)

Chapter 13 built the oracle: a black-box judge that takes a concurrent history, full of crashes, 500s and abandoned requests, and decides whether some sequential key-value store could have produced it. The oracle has to be clever, because the question it answers is hard. It must tolerate every indeterminate outcome a fault can cause and still notice the one outcome no fault can excuse.

This chapter is about a much less clever kind of check, and about why toys3 needed it anyway. A **veritas** is the small, executable, obviously correct version of one component: a map where the real thing has a protocol, a map where the real thing has a disk. A **conformance check** runs random operations against the real component and its veritas in lockstep, one at a time, with no faults and no crashes, and compares every result exactly. When the two disagree, the check **shrinks** the failing sequence until only the operations that matter are left, and prints them as a Go literal you can paste into a regression test.

None of this replaces deterministic simulation. It asks an earlier, narrower question (*on the happy path, does each component do exactly what it should?*) and answers it in a few lines instead of a few thousand log lines. It also catches a class of bug DST is structurally blind to, and it is what finally tied the simulator's storage model to a real disk. The idea, and most of the design, comes from the ShardStore paper quoted above; §13a.15 maps each section of the paper onto the code.

The code in this chapter was added after the rest of the book was written, in commits `0f46dfe..3ff1385`. Line references in this chapter are against `3ff1385`, not `10777a2`.

---

## 13a.1 What DST could not tell us

By the end of Chapter 16 the simulator is a formidable tool. It crashes the server thousands of times a second, tears files, flips bits, lies about fsync, and checks every client-visible history for linearizability. So what was missing? Three things, which `toys3/VERITAS-PROPOSAL.md` lists under "Why":

1. **No small executable spec per component.** The only spec of the object store was the oracle's sequential model, buried inside a linearizability search (Chapter 13, §13.4). Storage had no spec at all: the nearest thing was a private map inside the controller's unit-test harness.
2. **No step-by-step check of each component against its spec.** Every failure arrived through a full DST run, as a concurrent history with crashes in it. Even when the bug was "DELETE of a missing key returns 200", you met it as a window of interleaved requests at step 628.
3. **Nothing checked the simulator against a real disk.** DST is only as trustworthy as `sim.StorageModel`. If the model and Linux disagreed about something as basic as "does `rename` onto an existing file replace it?", every passing seed would be a statement about a filesystem that does not exist. Nothing checked that, not even on the plain no-fault, no-crash path.

There is a fourth gap, which nobody wrote down until the mutation table made it visible (§13a.12): **DST only explores the states its workload reaches.** The simulator's clients write values of at most 40 bytes (`MaxValueSize: 40`, `toys3/sim/config.go:157`), and the controller's limit is 64 (`toys3/sim/config.go:173`). So the 400-for-oversize path never runs in any seed, and a bug in it is invisible to a million passing seeds.

The two kinds of check divide the work like this:

| | Deterministic simulation (Chapters 10 to 16) | Conformance checks (this chapter) |
|---|---|---|
| Question | Does the closed loop survive faults? | Does each component do exactly what its veritas does? |
| Faults, crashes | Every kind the storage model has | None |
| Concurrency | Many clients, many requests in flight | One operation at a time |
| Spec | The oracle: tolerant of indeterminate outcomes | A veritas: exact |
| Comparison | Is there *some* linearization? | Status, body, bytes and error codes must be equal |
| Failure report | Seed, invariant, window, event log | Seed, shrunk sequence of one to three operations |
| Inputs | Workload chosen for realism | Alphabet biased toward edges and collisions |

The right column is weaker in every row except the last three. That is the point: it is cheap to be exact when nothing is indeterminate.

## 13a.2 The word "veritas"

The ShardStore paper calls this kind of spec a **reference model**. So did an earlier draft of the proposal. The word was dropped for a practical reason: in toys3, "model" was already taken twice. `sim.StorageModel` is the simulated filesystem (Chapter 11), and the oracle's sequential spec of a key, one state of which is a `refState`, had been called the reference model in Chapters 3a and 13. A third "model" would have made every sentence ambiguous. The proposal's first decision fixes it:

> **Name:** "veritas" replaces "reference model" everywhere. A veritas is the small, executable, obviously correct version of one component. ("Model" is already overloaded: `sim.StorageModel`, the oracle's register model.)

and the package doc repeats it (`toys3/veritas/doc.go:6-7`). The oracle's sequential spec is now called the **register model** (one register per key, as the invariant table has always said: "linearizable against a per-key register model"). Chapter 3a §3a.11 lists the collisions; this chapter adds one row to that table:

| Word | In toys3 | In the paper |
|---|---|---|
| storage model | `sim.StorageModel`: the adversarial simulated filesystem | the in-memory user-space disk |
| register model | the oracle's sequential spec of one key (`refState`) | — |
| veritas | `veritas.Store`, `veritas.FS`: the exact spec of one component, also its test fake | reference model |
| conformance check | `TestC1`..`TestC4`: implementation vs veritas in lockstep | conformance checking (§4) |

Why Latin? Because a short, unused, slightly odd word is easy to grep and impossible to confuse. "Spec" names a document; "fake" names only half the job (§13a.5); "oracle" was taken. *Veritas* is truth, and the veritas of a component is the truth about what it should do, written as code. The plural in the code and the docs is *veritases*.

## 13a.3 V1: `veritas.Store`, the object store

The first veritas is the controller as a client sees it: put, get, delete and copy on a map.

```go
// toys3/veritas/store.go:10-34
// Store is the veritas of the object store (control.Controller seen from a
// client): a map from key to payload. It is sequential and never fails, so it
// never replies 500. The rules (DESIGN.md, control/event.go):
//
//	any op      400 and no change if a key is invalid (format.ValidKey)
//	Put         400 and no change if Data is over MaxObjectSize; else stores
//	            a copy of Data under Key, replacing any object, and 200
//	Get         200 with the payload; 404 if Key is absent
//	Delete      removes Key and 200; 404 if Key is absent
//	Copy        stores Src's payload under Dst and 200, Src == Dst included
//	            (it rewrites the object); 404 and no change if Src is absent
//
// Copy needs no size check: Src got in through Put. Store never aliases a
// caller's slice.
type Store struct {
	// MaxObjectSize bounds Put payloads, as control.Config.MaxObjectSize
	// (0: no limit).
	MaxObjectSize int
	objs          map[string][]byte
}

// NewStore returns an empty store.
func NewStore(maxObjectSize int) *Store {
	return &Store{MaxObjectSize: maxObjectSize, objs: map[string][]byte{}}
}
```

and the whole behaviour is one switch:

```go
// toys3/veritas/store.go:38-76
func (s *Store) Apply(op control.Op) (control.Status, []byte) {
	switch op := op.(type) {
	case control.Put:
		if !format.ValidKey(op.Key) || (s.MaxObjectSize > 0 && len(op.Data) > s.MaxObjectSize) {
			return control.StatusBadRequest, nil
		}
		s.objs[op.Key] = clone(op.Data)
		return control.StatusOK, nil
	case control.Get:
		if !format.ValidKey(op.Key) {
			return control.StatusBadRequest, nil
		}
		data, ok := s.objs[op.Key]
		if !ok {
			return control.StatusNotFound, nil
		}
		return control.StatusOK, clone(data)
	case control.Delete:
		if !format.ValidKey(op.Key) {
			return control.StatusBadRequest, nil
		}
		if _, ok := s.objs[op.Key]; !ok {
			return control.StatusNotFound, nil
		}
		delete(s.objs, op.Key)
		return control.StatusOK, nil
	case control.Copy:
		if !format.ValidKey(op.Src) || !format.ValidKey(op.Dst) {
			return control.StatusBadRequest, nil
		}
		data, ok := s.objs[op.Src]
		if !ok {
			return control.StatusNotFound, nil
		}
		s.objs[op.Dst] = data // payloads are never mutated in place
		return control.StatusOK, nil
	}
	return control.StatusBadRequest, nil // an Op the store does not know
}
```

Compare this with Chapter 6. The controller's version of the same behaviour is spread over `onRequest`, `start`, `writeTmp`, five cases of `onRequestIO`, `finish` and `commit`, plus the key interlock and recovery: about 300 lines. Here it is 40, and you can check each case against the comment above the type by reading it once. That is the bar a veritas must clear: **short enough to be obviously right.** A veritas that needs its own tests to be believed has failed at its one job (it has them anyway, `toys3/veritas/store_test.go`, but they are there to pin the rules, not to find bugs).

Three things it deliberately says:

- **400 beats 404.** An invalid key is rejected before the map is consulted, so `Copy("", "a")` is a 400 even if nothing exists. The controller does the same, in `onRequest` (`toys3/control/controller.go:150-174`), before the request is queued.
- **Copy onto itself rewrites.** `Copy(a, a)` is a 200 and leaves the same payload. For the controller it is a real read, temp write, fsync, rename and directory fsync (Chapter 6, `TestCopySameKey`).
- **No size check on Copy.** The source got in through a Put, so it is within the limit. §13a.13 returns to the one case where that reasoning fails.

And the things it deliberately does not say: no 500, no crash, no concurrency, no durability. A veritas describes what a component does when nothing goes wrong. Everything else is DST's job.

## 13a.4 V2: `veritas.FS`, the storage port

The second veritas sits one level down: the Storage port of Chapter 3, §3.5. It answers each `Actuation` with its `Feedback`, over a map from path to bytes.

```go
// toys3/veritas/fs.go:10-34
// FS is the veritas of the Storage port (control/storage.go): a filesystem as
// a map from path to content. It is sequential, never fails and never
// crashes, so every change is immediately durable and FSFsync and FSFsyncDir
// only check that their target exists. What it pins down is everything else
// a filesystem promises on the happy path:
//
//	FSWrite     creates Path, or truncates it, and writes Data
//	FSFsync     OK; ENOENT if Path does not exist
//	FSRename    replaces To with From atomically; From == To is a no-op;
//	            ENOENT if From does not exist
//	FSFsyncDir  OK
//	FSRead      the whole content; ENOENT if Path does not exist
//	FSUnlink    removes Path; ENOENT if it does not exist
//	FSList      base names of the files in Dir, sorted ascending
//
// Directories are fixed at construction, as in the real data root (there is
// no mkdir): an operation on a path whose directory does not exist, or an
// FSFsyncDir or FSList of such a directory, is ENOENT.
//
// FS never aliases a caller's slice: it copies Data on the way in and on the
// way out.
type FS struct {
	dirs  map[string]bool
	files map[string][]byte // full path → content
}
```

`Exec` is a seven-case switch, one case per command (`toys3/veritas/fs.go:47-95`). Two of them show the style:

```go
// toys3/veritas/fs.go:57-64
	case control.FSRename:
		data, ok := fs.files[a.From]
		if !ok || !fs.dirOf(a.To) {
			return control.FSRenameDone{IO: a.IO, Err: control.ErrNotFound}
		}
		delete(fs.files, a.From)
		fs.files[a.To] = data
		return control.FSRenameDone{IO: a.IO}
```

Delete-then-insert makes `From == To` a no-op for free, which is what the Linux `rename(2)` manual page says ("If oldpath and newpath are existing hard links referring to the same file, then rename() does nothing"). And the end of the switch:

```go
// toys3/veritas/fs.go:93-94
	}
	panic("toys3/veritas: FS.Exec: unknown storage command")
```

A panic, not an `ErrIO`. A command the veritas does not know is not a fault of the disk; it is a gap in the spec, and it should stop whoever finds it.

Notice what is missing compared with Chapter 11's `StorageModel`: no inodes, no durable versus volatile content, no directory entry versions, no plan, no crash. `StorageModel` is 651 lines because it models what POSIX *permits* after a power cut. `veritas.FS` is 162 lines because it models only what POSIX *promises* when nothing goes wrong. The two are not rivals. The second is the spec the first must meet on its happy path, and §13a.7 checks exactly that.

## 13a.5 The veritas as a test fake

The paper makes a point that is easy to skim past (§3.2, "Mocking"): because the reference model is written in the implementation language with the same interface, unit tests can use it as a mock of the component, and "this reuse helps keep the reference models up-to-date over time, as writing unit tests for new features often requires updating the mock."

toys3 already had a fake of the Storage port: the harness in `toys3/control/controller_test.go` that Chapters 5, 6 and 7 use to assert exact action sequences. It kept a private `map[string][]byte` and executed each action against it in a method called `exec`. Step 1 of the proposal replaced that map with `veritas.FS`:

```go
// toys3/control/controller_test.go:26-44
// harness drives a Controller by hand. FS actions are answered by veritas.FS
// (no crash semantics: every change is immediately durable). Issued FS
// actions wait in pending until the test completes them.
type harness struct {
	t       *testing.T
	s       *Controller
	fs      *veritas.FS
	pending []Action // issued FS actions, not yet completed
	// fail, if set, decides the error an FS action completes with (OK = run it).
	fail func(Action) ErrCode
}

func newHarness(t *testing.T) *harness {
	return &harness{
		t:  t,
		s:  New(testEnv{}, Config{Dir: root, MaxObjectSize: 64}),
		fs: veritas.NewFS(format.ObjDir(root), format.TmpDir(root)),
	}
}
```

and `exec` became a thin wrapper that applies the test's `fail` hook and otherwise calls `h.fs.Exec(act)` (`toys3/control/controller_test.go:95-108`). Error injection stays in the harness, where it belongs: the veritas never fails, and the error-policy test of Chapter 7 decides per action which ones should.

There was one mechanical consequence. `veritas` imports `control` (it speaks the controller's `Op`, `Status`, `Actuation` and `Feedback`). A test file in package `control` that imports `veritas` would be an import cycle. So `controller_test.go` moved to the external test package, `control_test`, with a dot import so the tests still read as if they were inside the package:

```go
// toys3/control/controller_test.go:1-15
// External test package: veritas imports control, so an internal test could
// not import veritas. The dot import keeps the tests reading as if they were
// inside the package (the use the Go FAQ gives for dot imports).
package control_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	. "manifester/toys3/control"
	"manifester/toys3/format"
	"manifester/toys3/veritas"
)
```

The move had a side benefit: the controller tests can now see only the controller's exported API, the same view the simulator and production have. Every test still passed unchanged, which was the acceptance criterion for this step.

**The payoff is the dependency, not the code saved.** Suppose someone adds an eighth storage command, `FSStat`, and makes the controller issue it. The controller tests drive the controller through `veritas.FS`; the first `FSStat` hits the `panic` above and every test that reaches it fails. The only way to make them pass is to teach `veritas.FS` what `FSStat` means. The spec cannot fall behind the port, because the tests that everyone runs every day depend on it. This is the same argument as the paper's, and the same as Chapter 3's for the closed `Action` set: put the thing you want kept current on the path of something people cannot skip.

The proposal also planned a `veritas.FS`-backed `Executor` for the HTTP handler tests in `prod`, so they would need no disk. That part was not built: `prod_test.go` still runs end to end on a real temp directory (Chapter 9), and the DESIGN.md package table's mention of "HTTP tests" is ahead of the code. Chapter 17 lists it.

The comparison with Chapter 2 §2.2 is worth making explicit. That section rejected "mock the filesystem interface" as an *architecture*, because a synchronous mock cannot express "it happened, and nobody was told", cannot let a scheduler choose interleavings, and cannot be hashed. A veritas used as a fake has none of those problems, because it is not the architecture. The controller is still a sans-I/O state machine; the harness decides when each completion is delivered; the simulator still owns crashes. The veritas only answers the question "what should this command return?", and that question does have a synchronous answer.

## 13a.6 The lockstep runner

The checks themselves share one small, hand-written kit, `toys3/veritas/veritastest`. Its central type says what a conformance check *is*:

```go
// toys3/veritas/veritastest/lockstep.go:15-43
// Check is one conformance check: how to build a fresh implementation paired
// with its veritas (S), how to generate operations (Op), and how to run one
// operation on both and compare.
type Check[S, Op any] struct {
	// Name is the Go type of Op as written in a regression test (for
	// example "control.Actuation"); it heads the printed sequence.
	Name string
	// New returns a fresh implementation and veritas, both empty.
	New func() S
	// Gen returns the i-th operation of a sequence. Draw everything from r.
	Gen func(r *Rand, i int) Op
	// Step applies op to the implementation and to the veritas and returns
	// an error describing any difference in their results.
	Step func(s S, op Op) error
	// Invariant, if set, runs after every Step.
	Invariant func(s S) error
	// Shrink, if set, returns simpler versions of op, simplest first.
	Shrink func(op Op) []Op
	// ShrinkAll, if set, returns simpler versions of a whole sequence,
	// simplest first: changes that only keep a failure if made everywhere
	// at once, like renaming a path in every operation that uses it.
	ShrinkAll func(ops []Op) [][]Op
	// Format returns op as a Go expression.
	Format func(op Op) string
	// Ops is the length of each generated sequence.
	Ops int
	// MaxShrinkRuns caps the sequences executed while shrinking (0: 5000).
	MaxShrinkRuns int
}
```

Put it next to the paper's Figure 3:

```rust
// Bornholt et al., SOSP '21, Figure 3 (abridged)
#[proptest]
fn proptest_index(ops: Vec<IndexOp<u32, u32>>) {
    let mut reference = ReferenceIndex::new();
    let mut implementation = PersistentLSMTIndex::new();
    for op in ops {
        match op {
            Put(key, value) => {
                compare_results!(
                    implementation.put(key, value),
                    reference.put(key, value),
                );
            }
            ...
        }
        check_invariants(&reference, &implementation);
    }
}
```

The correspondence is one to one. `New` is the two `let mut` lines. `Gen` replaces the proptest strategy that produces `ops`. `Step` is the `match` with its `compare_results!`. `Invariant` is `check_invariants`. `Shrink`, `ShrinkAll` and `Format` are what a property-testing framework provides for free and toys3 writes by hand (§13a.9 says why).

The runner is three functions. `Replay` runs a sequence from a fresh pair and reports the first failing operation:

```go
// toys3/veritas/veritastest/lockstep.go:64-92
// Replay runs ops in lockstep on a fresh pair and returns the index of the
// first failing operation and its error, or -1 and nil. A panic in Step or
// Invariant counts as a failure of that operation.
func (c Check[S, Op]) Replay(ops []Op) (index int, err error) {
	s := c.New()
	for i, op := range ops {
		if err := c.step(s, op); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func (c Check[S, Op]) step(s S, op Op) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	if err := c.Step(s, op); err != nil {
		return err
	}
	if c.Invariant != nil {
		if err := c.Invariant(s); err != nil {
			return fmt.Errorf("invariant: %w", err)
		}
	}
	return nil
}
```

`RunSeed` generates seed's sequence, replays it, and on failure shrinks it (`lockstep.go:94-105`). `Run` loops over the seeds an `Options` value chooses and reports the first failure through `t.Errorf` (`lockstep.go:213-248`).

Three design choices in this package are worth noticing.

**It is deterministic and checked by detcheck.** `veritastest` does not import `testing`, `time` or `flag`. It takes a `TB` interface (`Helper`, `Errorf`, `Logf`), which `*testing.T` satisfies, and the caller maps its own flags onto `Options` (`toys3/veritas/veritastest/doc.go:12-14`). So `scripts/ci-determinism.sh`, which derives its package list with `go list ./toys3/...`, picked up both `veritas` and `veritastest` with no change to the script, and detcheck checks them like `control` and `sim`.

**It has its own SplitMix64.** `veritastest.Rand` is the same algorithm as `sim.Rand` (`toys3/veritas/veritastest/rand.go`), copied rather than imported, because `sim`'s tests import `veritastest` and an import the other way would be a cycle. A seed is a number, a sequence is a function of that number, and a failure replays exactly, like a DST seed.

**A panic is a failure, not a crash of the test.** The recover in `step` turns "the controller panicked on this input" into an ordinary failing operation, which the shrinker can then minimize like any other.

The proposal's decision on dependencies is recorded at the top: "hand-written, using the repo's seeded RNG. No new dependency and no dependency audit, failing seeds replay exactly like DST seeds, and the shrinking the paper relies on (§4.3) is simple enough to write." Go's standard `testing/quick` has no shrinking, and adding a third-party property-testing library to a detcheck-gated package would have meant an audit record (Chapter 8) for a few hundred lines of benefit.

## 13a.7 The four conformance checks

With the kit in place, each check is a `Check` value plus a test function.

| | Implementation vs veritas | Where | Backends | Invariants after each op |
|---|---|---|---|---|
| C1 | `control.Controller` vs `veritas.Store` | `toys3/sim/storecheck_test.go` | `veritas.FS`; `StorageModel` with `Disturbances{}` | `obj/` decodes to exactly the veritas's map (current and durable views); `tmp/` empty; no `Trip` |
| C2 | `sim.StorageModel`, no disturbances, vs `veritas.FS` | `toys3/sim/conformance_test.go` | — | every listing and every read agree |
| C3 | `prod.OSExecutor` on `t.TempDir()` vs `veritas.FS` | `toys3/prod/conformance_test.go` | real disk | every listing and every read agree |
| C4 | `sim.Oracle` vs `veritas.Store` (C1's histories) | `toys3/sim/oraclecheck_test.go` | as C1 | the oracle accepts, and its one state is the veritas's map |

```
                    veritas.Store ◄──── C4 ────► sim.Oracle (register model)
                         ▲
                         C1
                         │
                 control.Controller
                         │ Storage port
              ┌──────────┴──────────┐
              ▼                     ▼
         veritas.FS ◄── C2 ──► sim.StorageModel     (no disturbances)
              ▲
              └──────── C3 ──► prod.OSExecutor on a real disk
```

### C1: the controller against `veritas.Store`

C1 is the paper's Figure 3 for toys3. One operation is one request or one restart:

```go
// toys3/sim/storecheck_test.go:47-76
type storeKind uint8

const (
	kGet storeKind = iota
	kPut
	kDelete
	kCopy
	kRestart
)

// storeOp is one C1 operation. A Put's payload is not stored: it is derived
// from the request's ClientReq and Size (storePayload), so a printed
// sequence stays short and every payload is unique.
type storeOp struct {
	Kind     storeKind
	Key, Src string // Key is Dst for a Copy
	Size     int    // Put: payload size; Restart: leftover temp files to plant
}

// Constructors, used by Format and by pasted regression sequences.
func stGet(k string) storeOp         { return storeOp{Kind: kGet, Key: k} }
func stPut(k string, n int) storeOp  { return storeOp{Kind: kPut, Key: k, Size: n} }
func stDelete(k string) storeOp      { return storeOp{Kind: kDelete, Key: k} }
func stCopy(src, dst string) storeOp { return storeOp{Kind: kCopy, Src: src, Key: dst} }
func stRestart() storeOp             { return storeOp{Kind: kRestart} }

// stRestartLeftovers plants n leftover temp files, as a crash between a
// Put's temp write and its rename leaves, then restarts. Recovery must
// remove them and touch nothing else.
func stRestartLeftovers(n int) storeOp { return storeOp{Kind: kRestart, Size: n} }
```

The kinds are listed simplest first, like the paper's `IndexOp` ("we arrange operation alphabets in increasing order of complexity", §4.3), because the shrinker prefers earlier ones. A restart is the paper's `Reboot`: a no-op for the veritas, which is exactly what makes it a test. If recovery damages anything, the next comparison or the invariant will see that the controller's storage no longer matches a veritas that never restarted.

Driving one request means: deliver it, then run every storage command it causes to completion, oldest first, until nothing is pending.

```go
// toys3/sim/storecheck_test.go:162-187
func (p *storePair) deliver(b control.EventBody) (replies []control.Reply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("controller panicked: %v", r)
		}
	}()
	var pending []control.Actuation
	for {
		p.now++
		for _, a := range p.ctl.Step(control.Event{At: p.now, Body: b}) {
			switch a := a.(type) {
			case control.Actuation:
				pending = append(pending, a)
			case control.Reply:
				replies = append(replies, a)
			case control.Trip:
				return replies, fmt.Errorf("tripped: %s", a.Cause)
			}
		}
		if len(pending) == 0 {
			return replies, nil
		}
		b = p.backend.Exec(pending[0])
		pending = pending[1:]
	}
}
```

This is the controller harness of Chapter 6 without the ability to hold completions back: FIFO, synchronous, one in flight. Then `step` compares the single reply with the veritas:

```go
// toys3/sim/storecheck_test.go:238-267
func (p *storePair) step(op storeOp) error {
	if op.Kind == kRestart {
		if err := p.plantLeftovers(op.Size); err != nil {
			return err
		}
		return p.boot() // the veritas has no restarts
	}
	p.next++
	c := p.next
	req := op.request(c)
	wantStatus, wantBody := p.ver.Apply(req)
	if p.oracle != nil {
		p.oracle.Invoke(p.now, 0, control.Request{Client: c, Op: req})
	}
	replies, err := p.deliver(control.Request{Client: c, Op: req})
	if err != nil {
		return err
	}
	if len(replies) != 1 || replies[0].Client != c {
		return fmt.Errorf("want one reply to c%d, got %#v", c, replies)
	}
	got := replies[0]
	if p.oracle != nil {
		p.oracle.Return(p.now, got)
	}
	if got.Status != wantStatus || !bytes.Equal(got.Body, wantBody) {
		return fmt.Errorf("replied %d %s, want %d %s", got.Status, describeBody(got.Body), wantStatus, describeBody(wantBody))
	}
	return nil
}
```

(The `p.oracle` lines are C4's; §13a.14.) After every operation the invariant looks at the storage itself, at quiescence:

```go
// toys3/sim/storecheck_test.go:280-292
// invariant checks the storage at quiescence against the veritas.
func (p *storePair) invariant() error {
	cur := p.backend.Current()
	for _, f := range cur.Files {
		if strings.HasPrefix(f.Path, format.TmpDir(storeRoot)+"/") {
			return fmt.Errorf("tmp/ not empty at quiescence: %s", f.Path)
		}
	}
	if err := p.objMatches("current", cur); err != nil {
		return err
	}
	return p.objMatches("durable", p.backend.Durable())
}
```

`objMatches` unescapes every name under `obj/`, decodes every file with the CRC check, and requires exactly the veritas's keys and payloads (`storecheck_test.go:294-325`). This is the paper's "checking that both store the same key-value mapping", and it is stronger than comparing replies: a bug that writes the right object to the wrong file is caught on the step it happens, not when a later GET stumbles on it.

C1 runs on two backends (`storecheck_test.go:499-505`):

- **`veritas.FS`**, the obvious one. Its durable view is its current view.
- **`StorageModel` with `Disturbances{}`**: no EIO, no tears, no writeback, no crashes, every command applied with an empty plan. This backend has a real durable view, so the "durable" half of the invariant has teeth: if the controller ever acks a Put without fsyncing both the file and the directory, the durable view will not match. §13a.12 shows that this alone catches mutants a and b.

One parameter matters more than it looks: `storeMaxSize = 64`, "small, so the 400 path runs" (`storecheck_test.go:31-34`). §13a.11 explains why that comment is a lesson.

### C2: the storage model against `veritas.FS`

C2 drives `StorageModel` synchronously, with an empty plan for every command:

```go
// toys3/sim/conformance_test.go:10-28
// syncStorage drives a StorageModel synchronously, with no injected failure,
// as a veritastest.FSUnderTest.
type syncStorage struct{ fs *StorageModel }

func (s syncStorage) Exec(a control.Actuation) control.Feedback {
	return s.fs.Land(a, ActuationPlan{}).(control.Feedback)
}

func newSyncStorage() veritastest.FSUnderTest {
	return syncStorage{NewStorageModel(NewRand(1), Disturbances{}, veritastest.FSDirs()...)}
}

// TestC2StorageModelMatchesVeritasFS is conformance check C2: the storage
// model with no disturbances (no faults, no crashes) agrees with veritas.FS on
// every storage command. Flags: -seeds N, -seed S (replay one seed),
// -duration D, as for TestSeeds.
func TestC2StorageModelMatchesVeritasFS(t *testing.T) {
	veritastest.CheckFS(t, newSyncStorage, conformanceOptions("TestC2StorageModelMatchesVeritasFS"))
}
```

The check itself lives in `veritastest`, because C3 uses the same one. Its alphabet is every storage command over five paths in two directories (`/d/obj/a`, `/d/obj/b`, `/d/obj/c`, `/d/tmp/1`, `/d/tmp/2`), few enough that operations collide constantly: renames land on existing files, writes truncate, unlinks and reads hit missing files about as often as present ones (`toys3/veritas/veritastest/fs.go:18-31`). After every command it compares the completions (kind, IO, error code, read data, listed names) and then, as the invariant, the whole visible state: both listings and all five reads (`fs.go:242-275`).

C2 found nothing: 57,000 seeds in a 20-second run. That is the expected result and still worth having. It says the storage model's *happy path* is a filesystem, so every DST failure is about faults and crashes, not about a model that gets `rename` wrong.

### C3: a real disk against `veritas.FS`

C3 runs the same `FSCheck` against `prod.OSExecutor`, the production storage adapter, on a fresh directory under `t.TempDir()`. The adapter needs a small shim that rewrites the alphabet's `/d` prefix to the real directory and waits for the asynchronous completion:

```go
// toys3/prod/conformance_test.go:55-60
// run executes a through OSExecutor and waits for its completion.
func (d diskFS) run(a control.Actuation) control.Feedback {
	ch := make(chan control.EventBody, 1)
	OSExecutor{}.Execute(a, func(e control.EventBody) { ch <- e })
	return (<-ch).(control.Feedback)
}
```

Each lockstep run and each shrink replay gets a new directory (`newDiskFS`, `conformance_test.go:70-93`), so runs never see each other's files.

This is the check that ties the simulator to reality. C2 says `StorageModel` agrees with `veritas.FS`; C3 says the real disk agrees with `veritas.FS`; so on the no-fault, no-crash path the simulator and the real disk agree with each other. Concretely, C2 and C3 pin the POSIX facts the controller and the storage model both rely on: rename over an existing file replaces it, rename or unlink of a missing path is ENOENT, a write truncates an existing file, and directory listings come back in the same order. (That last one matches only because Go's `os.ReadDir` sorts by name; `readdir(3)` itself promises no order.) It also covers the production error mapping of Chapter 17 Part I §2: if `code` ever mapped `ENOENT` from `os.Rename` to `ErrIO`, C3 would fail on the first rename of a missing file, where DST would never notice.

1,742 seeds (150 seconds) on macOS APFS found no disagreement in the alphabet. Building C3 did surface one real difference outside it, case folding, which §13a.13 covers.

### C4: the oracle against `veritas.Store`

C4 is C1 with an oracle attached. §13a.14 is about why it exists.

## 13a.8 Input bias, and when not to bias

A uniform random generator over C1's alphabet would be nearly useless. With four valid keys it would at least collide, but most Gets would miss, most Copies would find no source, and a Put of exactly 65 bytes would be one draw in a thousand. The paper's §4.2 makes the same observation about keys ("a naive implementation of Fig. 3 would generate random keys for Get and Put operations, which would rarely coincide") and its fix is **argument bias**. C1's generator is the toys3 version:

```go
// toys3/sim/storecheck_test.go:356-404
// genStoreOp returns a random operation. Get, Delete and Copy mostly target
// keys already Put (written); sizes cluster at 0, storeMaxSize and
// storeMaxSize+1; a Copy is sometimes onto its own source; one key in 20 is
// invalid; half the restarts first plant one or two leftover temp files, so
// recovery has something to remove (without them its unlink path never runs).
func genStoreOp(r *veritastest.Rand, written *[]string, minSize int) storeOp {
	key := func() string {
		switch k := r.Intn(20); {
		case k == 0:
			return storeInvalidKeys[r.Intn(len(storeInvalidKeys))]
		case k < 16 && len(*written) > 0:
			return (*written)[r.Intn(len(*written))]
		}
		return storeKeys[r.Intn(len(storeKeys))]
	}
	switch k := r.Intn(100); {
	case k < 20:
		return stGet(key())
	case k < 55:
		var n int
		switch r.Intn(6) {
		case 0:
			n = 0
		case 1:
			n = storeMaxSize
		case 2:
			n = storeMaxSize + 1
		case 3:
			n = ValueHeader
		default:
			n = r.Intn(storeMaxSize + 2)
		}
		k := key()
		if format.ValidKey(k) && !slices.Contains(*written, k) {
			*written = append(*written, k)
		}
		return stPut(k, max(n, minSize))
	case k < 70:
		return stDelete(key())
	case k < 95:
		src := key()
		if r.Intn(4) == 0 {
			return stCopy(src, src)
		}
		return stCopy(src, key())
	default:
		return stRestartLeftovers([]int{0, 0, 1, 2}[r.Intn(4)])
	}
}
```

Each bias has a concrete reason, and each is probabilistic, never absolute:

| Bias | Reason | What stays possible |
|---|---|---|
| 15 keys in 20 come from those already Put | otherwise Get, Delete and Copy mostly take the 404 path | keys never written (4 in 20 are drawn from the whole alphabet) |
| Put sizes: 0, 64, 65 and `ValueHeader` each one time in six | the edges of the size limit and of the self-describing value format | any size in `[0, 65]` (the remaining two in six) |
| a quarter of Copies are onto their own source | `Copy(a, a)` reads and rewrites the same file: the one Copy that can go wrong in a special way | Copies between different keys |
| one key in 20 is invalid (empty, or 256 bytes once escaped) | the 400 path for every op kind | — |
| keys include `a/b` | `/` escapes to `%2F`: the one key whose file name differs from the key | — |
| half the restarts plant leftover temps | without crashes, `tmp/` is always empty at a restart (§13a.11) | clean restarts |

The storage generator has its own, smaller set: writes are the most common command because they create what everything else acts on, and sizes cluster at 0, 1 to 16 bytes, and 4,095 to 4,097 bytes, either side of a page (`toys3/veritas/veritastest/fs.go:48-81`). That one is lifted straight from the paper, which biased toward "read/write sizes close to the disk page size, which in our experience are frequent causes of bugs".

**When not to bias.** The paper is just as clear about the limits, and it is the half of §4.2 most worth remembering:

> While elaborate argument selection strategies can often be easily justified ("our production customer traffic follows this distribution so we should mirror it here"), we have found little benefit to them. ... More generally, biasing introduces the risk of baking our assumptions into our tests, when our goal in adopting formal methods is to invalidate exactly such assumptions by checking behaviors we did not consider. Our methodology has settled on trusting default randomness wherever possible, and only introducing bias where we have quantitative evidence that it is beneficial.

The table above follows that rule: every bias names the path it exists to reach, and the coverage pass of §13a.11 is the evidence. The operation mix (20% Get, 35% Put, 15% Delete, 25% Copy, 5% restart) is *not* tuned to anything. There is no attempt to mirror S3 traffic, because nothing says such a mix finds more bugs, and plenty says a mix chosen to look realistic encodes the author's idea of what matters. Notice also what the alphabet leaves out on purpose: upper-case keys. That omission is an assumption baked in, and §13a.13 says exactly which one.

## 13a.9 Shrinking

A failing seed of C1 typically fails somewhere in its first few dozen operations. A sequence of 36 operations with a bug at the end is a reproduction, but not an explanation. The paper's §4.3 describes the standard remedy: "the testing tool repeatedly applies reduction heuristics to simplify the sequence until the reduced version no longer fails", with heuristics like "remove an operation from the sequence" or "shrink an integer argument towards zero". Its example: a first failing sequence of 61 operations (9 crashes, 14 writes, 226 KiB) minimized to 6 operations (1 crash, 2 writes, 2 bytes).

`veritastest` has a hand-written shrinker with three passes, repeated until none makes progress:

```go
// toys3/veritas/veritastest/lockstep.go:107-121
// shrink returns a shorter or simpler sequence that still fails, and its
// error. ops must fail at its last operation. Passes, repeated until none
// makes progress:
//
//  1. drop chunks: halves, then quarters, ... down to single operations;
//  2. replace each operation with the first of its Shrink candidates that
//     still fails, repeatedly;
//  3. replace the whole sequence with the first ShrinkAll candidate that
//     still fails.
//
// After each success the sequence is cut just after its (new) failing
// operation. Every choice is a fixed order, so shrinking is deterministic.
// Any failure counts, not only the original one: the shrunk sequence may
// show a different bug, which is still a bug.
func (c Check[S, Op]) shrink(ops []Op, err error) ([]Op, int, error) {
```

**Pass 1, dropping chunks**, is delta debugging in its simplest form (Zeller and Hildebrandt): try removing the first half, then the second; then each quarter; down to single operations. Whatever can go, goes.

**Pass 2, simplifying one operation**, uses each check's `Shrink`. For C1, `shrinkStoreOp` offers, in order: smaller Put sizes (the minimum, then half-way, then one less), a clean restart instead of one with leftovers, and earlier keys in the alphabet `"a", "b", "c", "a/b", "", storeLongKey` (`toys3/sim/storecheck_test.go:406-439`). "Earlier" is the simplicity order: `"a"` is simpler than `"a/b"` because nothing about it is special.

**Pass 3, rewriting the whole sequence**, is the one property-testing tutorials tend to skip, and it is the reason `ShrinkAll` exists. Pass 2 changes one operation at a time, which is exactly wrong for keys. Suppose the failure is `stPut("a/b", 64), stDelete("a/b"), stGet("a/b")`. Changing the Get alone to `"a"` makes it a Get of a key nobody wrote, which correctly returns 404 on both sides: the failure disappears, so the candidate is rejected. Every single-operation rename breaks the link between operations that share a key. The only rename that can succeed is the one applied *everywhere at once*:

```go
// toys3/sim/storecheck_test.go:448-471
// shrinkStoreAll returns ops with one key renamed to an earlier one
// everywhere, latest key first.
func shrinkStoreAll(ops []storeOp) [][]storeOp {
	var out [][]storeOp
	for i := len(storeAllKeys) - 1; i > 0; i-- {
		for _, to := range storeAllKeys[:i] {
			from, changed := storeAllKeys[i], false
			cand := make([]storeOp, len(ops))
			for j, op := range ops {
				if op.Key == from && op.Kind != kRestart {
					op.Key, changed = to, true
				}
				if op.Src == from && op.Kind == kCopy {
					op.Src, changed = to, true
				}
				cand[j] = op
			}
			if changed {
				out = append(out, cand)
			}
		}
	}
	return out
}
```

The storage check has the same pass for paths and directories (`shrinkFSAll`, `toys3/veritas/veritastest/fs.go:131-146`). Its doc comment in `Check` names the general rule: "changes that only keep a failure if made everywhere at once". Keys and paths are names, and names are only meaningful in relation to each other.

Two more properties matter in practice.

**Shrinking is deterministic.** Every pass tries candidates in a fixed order, and a replay is a pure function of the sequence. `TestShrinkFindsMinimal` checks that shrinking the same seed twice gives the same sequence in the same number of runs (`toys3/veritas/veritastest/veritastest_test.go:40-55`). The paper makes the same point from the other side: "Non-determinism interferes with minimization because the reduction process stops as soon as a test execution does not fail." Everything Chapter 8 did to make a DST seed reproducible is what makes shrinking possible here.

**Any failure counts.** A candidate is accepted if it fails at all, not only if it fails the same way. So the shrunk sequence can describe a different, simpler symptom than the original. That is a feature (the simpler bug is still a bug, and usually the same bug seen more directly) but it means you read the shrunk sequence's error, not the original's. The worked example below shows it happening.

Finally, the output is a **Go literal**, built from the same constructors a regression test uses (`Literal` and `Report`, `lockstep.go:177-198`). A failure is three things: a seed, a command that replays it, and a sequence you can paste.

## 13a.10 A worked shrunk failure

To see the whole pipeline on a bug nobody planned for, plant one. The ShardStore paper's most instructive miss (§8.3, which §13a.11 returns to) was in a cache, so add a cache to the controller: remember the payload of every committed Put, serve Gets from it, and forget to invalidate it on Delete. Concretely, a scratch edit (never committed) to `toys3/control/controller.go`:

- a field `cache map[string][]byte` on `Controller`;
- in `commit`, for a non-delete, `s.cache[r.key] = r.data`;
- in `start`, for a Get, `if d, ok := s.cache[r.key]; ok { return s.finish(r, StatusOK, d) }` before issuing the read.

The cache is in memory, so a restart clears it; only Delete leaves it stale. Then run C1 for the default 200 seeds:

```
$ go test ./toys3/sim -run 'TestC1ControllerMatchesVeritasStore' -seeds=200 -v
=== RUN   TestC1ControllerMatchesVeritasStore
=== RUN   TestC1ControllerMatchesVeritasStore/veritasFS
    storecheck_test.go:533: seed 1: operation 3 of the shrunk sequence failed: replied 200 "", want 404 ""
        (shrunk from 36 operations to 3 in 25 runs)
        replay: go test ./toys3/sim -run 'TestC1ControllerMatchesVeritasStore/veritasFS$' -seed=1 -v
        shrunk sequence:
        []storeOp{
        	stPut("a", 0),
        	stDelete("a"),
        	stGet("a"),
        }
=== RUN   TestC1ControllerMatchesVeritasStore/StorageModel
    storecheck_test.go:533: seed 1: operation 3 of the shrunk sequence failed: replied 200 "", want 404 ""
        (shrunk from 36 operations to 3 in 25 runs)
        replay: go test ./toys3/sim -run 'TestC1ControllerMatchesVeritasStore/StorageModel$' -seed=1 -v
        shrunk sequence:
        []storeOp{
        	stPut("a", 0),
        	stDelete("a"),
        	stGet("a"),
        }
--- FAIL: TestC1ControllerMatchesVeritasStore (0.00s)
    --- FAIL: TestC1ControllerMatchesVeritasStore/veritasFS (0.00s)
    --- FAIL: TestC1ControllerMatchesVeritasStore/StorageModel (0.00s)
FAIL
FAIL	manifester/toys3/sim	0.612s
```

Read the report top to bottom. Seed 1 failed. The shrunk sequence has three operations and the third failed: the controller replied 200 with an empty body where the veritas said 404. Put an empty object, delete it, get it, and the deleted object comes back. That is the whole bug, stated as a test case, found and minimized in well under a second. The `replay:` line is the command that reproduces exactly this, and the literal can go straight into `TestC1Sequences` (`toys3/sim/storecheck_test.go:537-557`), which exists for sequences worth keeping.

**What the shrinker started from.** Seed 1's generated sequence first fails at its 36th operation (a scratch test printed it with `c.Literal(c.Generate(1)[:36])`). It begins

```
stDelete("a/b"),
stPut("a/b", 65),
stPut("a/b", 64),
stDelete("a/b"),
stPut("a/b", 64),
stDelete("a"),
stGet("a/b"),
...
```

and ends

```
stPut("a/b", 12),
stRestart(),
stPut("a/b", 64),
stPut("a/b", 64),
stDelete("a/b"),
stGet("a/b"),
```

with the error `replied 200 value-of-c32 (64 bytes), want 404 ""`. The bias toward written keys is visible: almost every operation hits `a/b`, the first key the sequence happened to Put. And the sequence shows why the bug took 36 operations to surface. It needs a Delete followed by a Get of the same key with no Put or restart in between. Four times before (ops 4, 10, 19 and 27) the sequence deleted `a/b` while the cache still held it, and each time a Put (ops 5, 11 and 31) or a restart (op 24) replaced or wiped the stale entry before the next Get of `a/b`. Ops 35 and 36 are the first Delete-then-Get.

**What each pass contributed.** The same scratch test re-ran the shrinker with passes switched off:

| Shrinker | Shrunk sequence | Runs |
|---|---|---|
| pass 1 only (drop chunks) | `stPut("a/b", 64), stDelete("a/b"), stGet("a/b")` | 14 |
| passes 1 and 2 (no `ShrinkAll`) | `stPut("a/b", 0), stDelete("a/b"), stGet("a/b")` | 33 |
| all three | `stPut("a", 0), stDelete("a"), stGet("a")` | 25 |

Pass 1 did the heavy lifting: 36 operations to 3. Pass 2 shrank the Put's size from 64 to 0, which also changed the error from `value-of-c32 (64 bytes)` to `""`: a different-looking failure, accepted because any failure counts, and a clearer one, because it shows the bug has nothing to do with the payload. Pass 2 could not touch the key, for the reason §13a.9 gave: renaming one of the three operations breaks the failure. Only pass 3 could rename `a/b` to `a` in all three at once. (The full shrinker needed *fewer* runs than passes 1 and 2 alone: once the key is `a`, there are no earlier keys left to try.) Every pass earned its place.

**And DST?** The same planted bug, `go test ./toys3/sim -run TestSeeds -seeds=200`, fails too, which is reassuring. Seed 1 fails first, but with `protocol at step 329: reply to unknown or already answered request c50`: under concurrency the plant's early `finish` inside `start` has a second effect, replying twice, and that is what DST happens to see first. Seed 3 shows the bug itself:

```
seed 3 FAILED linearizable at step 628: no linearization of 2 ops from any of 1 possible start states {k6=71}; window:
  req 78 client 1 DELETE "k6" [152..156 @78983378..81435014] -> 200
  req 83 client 1 GET "k6" [163..164 @82340410..82340410] -> 200 value-of-71
```

That window is about as small as DST windows get, and the oracle's message is exact. But compare what each report asks of you. DST says: at step 628 of seed 3, in a run with crashes and EIO and several clients, these two requests are inconsistent with the carried state `{k6=71}`; read the log to find out why `k6` held 71 and what the server did in between. C1 says: these three operations, from an empty store, on a fault-free disk, give the wrong answer. Both are correct. One is a starting point for an investigation; the other is the end of one.

Remember to revert the plant (`git checkout toys3/control/controller.go`). It was never committed.

## 13a.11 Coverage: the restart that never had anything to clean

A conformance check that passes a million seeds proves nothing about the code it never runs. The paper's §4.2 says it plainly, "these tests can only ever check system states that the test harness is able to reach", and its §8.3 tells the story that motivated its coverage work:

> We have seen one example of a bug that our validation should have caught but did not, and was instead caught during manual code review. That issue involved an earlier code change that had added a new cache to a ShardStore component. Our existing property-based tests had trouble reaching the cache-miss code path in this change because the cache size was configured to be very large in all tests. The new bug was in a change to that cache-miss path, and so was not reached by the property-based tests; after reducing the cache size, the tests automatically found the issue.

toys3 has two such stories, one avoided by design and one found by measurement.

**The one avoided.** `storeMaxSize = 64` is "small, so the 400 path runs". The production default is 64 MiB (`-max-object`, `toys3/cmd/toys3/main.go:29`). Run C1 with that and no Put would ever be rejected for size; the size check, and any off-by-one in it, would be the cache-miss path all over again. DST already has this blind spot (§13a.12, mutant i).

**The one found.** Step 6 of the proposal was a coverage pass: run C1 with coverage of the controller package and list what it never reaches.

```
$ go test ./toys3/sim -run TestC1 -coverpkg=manifester/toys3/control -coverprofile=c1.out
ok  	manifester/toys3/sim	0.715s	coverage: 67.5% of statements in manifester/toys3/control
$ go tool cover -func=c1.out | grep -E 'controller.go|recovery.go'
manifester/toys3/control/controller.go:64:	touches		0.0%
manifester/toys3/control/controller.go:70:	interlocked	0.0%
manifester/toys3/control/controller.go:104:	New		100.0%
manifester/toys3/control/controller.go:109:	Step		80.0%
manifester/toys3/control/controller.go:135:	newIO		100.0%
manifester/toys3/control/controller.go:143:	trip		0.0%
manifester/toys3/control/controller.go:150:	onRequest	90.5%
manifester/toys3/control/controller.go:185:	badRequest	100.0%
manifester/toys3/control/controller.go:191:	permitted	50.0%
manifester/toys3/control/controller.go:205:	startPermitted	60.0%
manifester/toys3/control/controller.go:216:	start		100.0%
manifester/toys3/control/controller.go:229:	writeTmp	100.0%
manifester/toys3/control/controller.go:236:	issue		100.0%
manifester/toys3/control/controller.go:244:	finish		100.0%
manifester/toys3/control/controller.go:256:	commit		100.0%
manifester/toys3/control/controller.go:266:	failTrip	0.0%
manifester/toys3/control/controller.go:272:	onRequestIO	71.4%
manifester/toys3/control/recovery.go:17:	onStarted	85.7%
manifester/toys3/control/recovery.go:30:	onRecoveryDone	81.1%
manifester/toys3/control/recovery.go:88:	fsyncTmpDir	100.0%
manifester/toys3/control/recovery.go:95:	indexIO		75.0%
```

That is the run at `3ff1385`: 78.7% of the statements in `controller.go` and `recovery.go` (the package-wide 67.5% includes `hash.go` and the port's boilerplate). The first run, before commit `35d81ab`, gave 70.8%, and the biggest hole was in `onRecoveryDone`: **recovery's unlink path never ran.** Recovery lists `tmp/`, unlinks each leftover, and fsyncs `tmp/` (Chapter 5). But C1 has no crashes, so every Put finished its rename, and `tmp/` was always empty at a restart. The `kRestart` operation exercised the list and the fsync and nothing in between. A recovery that unlinked the wrong files, or unlinked nothing, would have passed every seed.

That is not hypothetical. Mutant f, "recovery skips temp cleanup" (Chapter 15, §15.10), was invisible to C1 at that point. DST catches it at seed 20 through a long chain (a resurrected temp name aliases a committed object, and a reused temp name writes through it), which needs crashes, writeback and luck. C1 had no way to produce a leftover at all.

The fix was the restart variant you have already seen, `stRestartLeftovers(n)`: before booting, plant `n` durable, torn temp files, exactly what a crash between a Put's temp write and its rename leaves behind (`plantLeftovers`, `toys3/sim/storecheck_test.go:189-208`). The veritas still ignores restarts, so the existing invariant ("`tmp/` empty at quiescence") becomes a check of recovery's cleanup, and the `obj/` invariant checks that cleanup touched nothing else. Half the generated restarts now plant one or two. Coverage rose to 78.7%, and mutant f is now caught by C1 at seed 1, in one operation:

```
seed 1: operation 1 of the shrunk sequence failed: invariant: tmp/ not empty at quiescence: /d/tmp/0000010000000001
(shrunk from 24 operations to 1 in 12 runs)
shrunk sequence:
[]storeOp{
	stRestartLeftovers(1),
}
```

The parallel with the paper is close. There, a test parameter (a huge cache) kept a path unreachable; here, a test property (no crashes) did. In both cases the tests passed for the wrong reason, and in both cases a coverage report, not a failure, was what revealed it.

What C1 still does not reach is unreachable by design, and the coverage report says so plainly:

- `trip` and `failTrip` at 0%, and every error branch in `onRequestIO` and `onRecoveryDone`: C1 is fault-free.
- the corrupt-read branch (`CorruptionDetected`): no bit flips.
- the stale or wrong-type completion branches of `Step` and `onRequestIO`: those come from dead incarnations.
- `touches` and `interlocked` at 0%, `permitted` at 50%: C1 has one request in flight, so the key interlock never has anything to hold back.

DST covers all of these. The two kinds of check are complementary by construction, and the coverage report draws the line between them.

## 13a.12 The mutant table, with a second column

Chapter 15 measured the simulator's teeth with seven mutants, each a durability bug. Step 6 of the proposal added four **functional** mutants: bugs in *what* a request does, not in *how* it survives faults. They are wrappers like the others (`toys3/sim/mutants_test.go`, the `functionalMutants` list), kept out of `TestCatchesBugs` and the golden file so that Chapter 8's behaviour pin is unchanged:

- **g. Copy writes to its source key.** A request rewriter turns `Copy(src, dst)` into `Copy(src, src)`, so the reply is 200 and `dst` is untouched.
- **h. Delete of a missing key replies 200.** The wrapper rewrites the 404 reply to a Delete into a 200.
- **i. The size check is off by one.** The factory builds the real controller with `MaxObjectSize + 1`, so a Put of exactly `MaxObjectSize + 1` bytes is accepted.
- **j. Recovery also unlinks everything in `obj/`.** Before the real recovery's `FSList(tmp)`, the wrapper lists `obj/` and unlinks every committed object.

(In earlier printings of Chapter 15, "g" and "h" were the names of two out-of-tree experiments. Those are now called x and k; see §15.13 and Exercise 2 there.)

`TestMutantTable` (`toys3/sim/mutanttable_test.go`) runs all eleven through DST (seeds 1..500, default invariants) and through C1 on both backends (seeds 1..200), and pins the result. It is the table in DESIGN.md:

| Variant | DST: caught by | DST: first seed | C1 on `veritas.FS`: seed, shrunk ops | C1 on `StorageModel`: seed, shrunk ops |
|---|---|---|---|---|
| a. skip `FSFsyncDir(obj/)` | acked-is-durable | 1 | — | 1, 1 |
| b. skip the temp file's `FSFsync` | obj-never-torn | 1 | — | 1, 1 |
| c. reply 200, then rename and fsync | acked-is-durable | 1 | — | — |
| d. 500 without `Trip` after any post-write error | obj-never-torn | 1 | — | — |
| d2. same, only for a failed `FSFsyncDir(obj/)` | linearizable | 132 | — | — |
| e. skip the CRC check on read | no-corrupt-reads | 18 | — | — |
| f. recovery skips temp cleanup | linearizable | 20 | 1, 1 | 1, 1 |
| g. Copy writes to its source key | linearizable | 1 | 1, 2 | 1, 2 |
| h. Delete of a missing key replies 200 | linearizable | 1 | 1, 1 | 1, 1 |
| i. size check off by one | **not caught in 500 seeds** | — | 1, 1 | 1, 1 |
| j. recovery also unlinks everything in `obj/` | linearizable | 1 | 1, 2 | 1, 2 |

Run it yourself with `go test ./toys3/sim -run TestMutantTable -v`. Each C1 catch logs its shrunk sequence; these are the ones that matter:

```
f   stRestartLeftovers(1)                     invariant: tmp/ not empty at quiescence: /d/tmp/0000010000000001
g   stPut("b", 0), stCopy("b", "a")           invariant: current obj/ holds keys ["b"], veritas ["a" "b"]
h   stDelete("a")                             replied 200 "", want 404 ""
i   stPut("a", 65)                            replied 200 "", want 400 ""
j   stPut("a", 0), stRestart()                invariant: current obj/ holds keys [], veritas ["a"]
a   stPut("a", 0)        (StorageModel only)  invariant: durable obj/ holds keys [], veritas ["a"]
b   stPut("a", 0)        (StorageModel only)  invariant: durable obj/: "a" is torn or corrupt
```

Each is the bug in one line. Look at g: why `"b"` and not `"a"`? Because the shrinker *tried* renaming `b` to `a` everywhere, and `stPut("a", 0), stCopy("a", "a")` is not a failure: Copy onto itself is exactly what the mutant does anyway. The shrunk sequence is minimal in a precise sense: every simpler candidate the shrinker knows passes.

**Why C1 cannot see c, d, d2 and e.** The `why` field of each row says it (`mutanttable_test.go:40-59`):

- c, "reply 200, then rename and fsync": C1 runs every storage command to completion before it compares anything. An early ack is only visible to a crash between the ack and the fsync, or to a concurrent reader. C1 has neither.
- d and d2 need an I/O error; e needs a corrupt read. C1 is fault-free.

These are not weaknesses to fix. They are the definition of the boundary: a conformance check against an exact veritas can only exist where outcomes are exact.

**Why a and b are caught only on `StorageModel`.** `veritas.FS` makes every change durable immediately, so a skipped fsync changes nothing it can see. `StorageModel` with no disturbances keeps durable and volatile state separate even though it never crashes, so C1's durable-view invariant sees on the very first Put that `obj/a` is not durable (a) or is durable but empty (b). The fault-free storage model is still a stricter backend than the veritas, which is why C1 runs on both.

**Why DST never catches i.** The simulator's workload writes values between `ValueHeader` (12) and `MaxValueSize` (40) bytes (`toys3/sim/workload.go:101`, `toys3/sim/config.go:157`). The controller's limit in the simulator is 64. No simulated Put is ever near the limit, so the size check is never exercised and mutant i is behaviourally identical to the real server in every seed. It is the ShardStore cache bug in miniature: a limit the tests never reached. C1 finds it in one operation because its sizes are biased to the edges.

**Why DST catches g, h and j at seed 1, and why C1 is still worth having for them.** All three are blatant functional bugs, and the oracle sees them immediately. But DST's report is a window from a concurrent history, possibly with crashes in it; C1's is `stDelete("a")`. The table's last column is the one to look at: one or two operations, always.

The two kinds of check cover each other's blind spots, almost perfectly. DST misses one functional bug (i) that C1 finds at once; C1 misses four fault bugs (c, d, d2, e) that DST finds at once. Neither would be enough alone.

## 13a.13 What the checks found: case folding, and a Copy gap

The conformance checks found no disagreement in the code they reach. Building them did surface two things that are not bugs in the code as such but are gaps in what the specs assume. Both are open items in DESIGN.md.

### Case folding

`format.EscapeKey` (Chapter 4, §4.6) percent-escapes every byte that is not unreserved, but it passes ASCII letters through unchanged. So keys `K` and `k` become files `obj/K` and `obj/k`. On a case-sensitive filesystem (Linux ext4 and XFS) those are two files. On a case-insensitive one, which is the **macOS APFS default**, they are one file, and the two keys silently alias each other: PUT `K`, then PUT `k`, then GET `K` returns the second value.

`veritas.FS` and `StorageModel` both assume names that differ only in case are different, as POSIX does. C3's random alphabet uses lower-case names only, so it cannot reach the difference. A dedicated test pins it:

```go
// toys3/prod/conformance_test.go:113-137
// TestC3CaseSensitive pins a POSIX assumption the generator's alphabet
// (lower-case names only) cannot reach: names that differ only in case are
// different files. veritas.FS and StorageModel assume so, and format.EscapeKey
// relies on it, passing ASCII letters through unchanged, so keys "K" and "k"
// become obj/K and obj/k. On a case-insensitive filesystem (the macOS APFS
// default; Linux ext4 and xfs are case-sensitive) those are one file, and the
// two keys alias each other. That is a property of the volume, not of
// OSExecutor, so the test skips there rather than teach the veritas to fold
// case.
func TestC3CaseSensitive(t *testing.T) {
	newImpl := newDiskFS(t)
	probe := newImpl().(diskFS)
	if err := os.WriteFile(probe.root+"/obj/x", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(probe.root + "/obj/X"); err == nil {
		t.Skip("filesystem folds case: keys that differ only in case alias each other on this volume")
	}
	veritastest.ReplayFS(t, newImpl, []control.Actuation{
		control.FSWrite{IO: 1, Path: "/d/obj/a", Data: veritastest.Pattern(1, 'a')},
		control.FSWrite{IO: 2, Path: "/d/obj/A", Data: veritastest.Pattern(1, 'b')},
		control.FSRead{IO: 3, Path: "/d/obj/a"},
		control.FSList{IO: 4, Dir: "/d/obj"},
	})
}
```

On the machine this chapter was written on, it reports what you would expect:

```
$ go test ./toys3/prod -run TestC3CaseSensitive -v
=== RUN   TestC3CaseSensitive
    conformance_test.go:129: filesystem folds case: keys that differ only in case alias each other on this volume
--- SKIP: TestC3CaseSensitive (0.01s)
```

The decision recorded in the test's comment is worth understanding. One fix would be to teach `veritas.FS` to fold case on such volumes. That would make the checks pass and the bug permanent: the veritas would be agreeing with the disk about a behaviour the controller must not depend on. The veritas states what the controller *needs* from storage, and a skip on a volume that does not provide it is the honest answer. The real fix is in the format (escape upper-case letters, or reject them), and that changes the on-disk format of every existing object, so it is left open. C1 uses lower-case keys only, for the same reason (`storecheck_test.go:36-38`).

This is exactly the kind of finding the proposal predicted for C3 ("which will probably turn up real differences between the sim and the real disk"): not a bug in the simulator or the adapter, but an assumption that every layer shared and no layer stated.

### Copy has no size check

Neither the controller nor `veritas.Store` checks `MaxObjectSize` on a Copy. The veritas says why: "Copy needs no size check: Src got in through Put." That is true within one configuration. It stops being true if the limit is lowered across a restart: start `cmd/toys3` with `-max-object 1048576`, Put a 1 MiB object, restart with `-max-object 1024`, and a Copy of the old object creates a new one over the limit. The two specs agree with each other, so no conformance check can catch it; it is a gap in both, found by reading them side by side. Whether it is a bug depends on what the limit is for (a request-size limit, which Copy has no request body for, or a storage invariant), which is a product decision rather than a testing one.

## 13a.14 Two specs that check each other: why the oracle stays independent

toys3 now has two executable specs of the object store: the oracle's register model (Chapter 13) and `veritas.Store`. The obvious refactor is to delete one: have the oracle call `veritas.Store` for its sequential step. The proposal's third decision rejects it:

> **Oracle:** stays independent of the veritas, and a test checks that the two agree (C4). They're written differently (the oracle tracks which request wrote each value; the veritas holds bytes), so making one depend on the other would cost more than it saves. Two independent specs that a test keeps in agreement also act as a check on each other.

The difference in representation is real. The oracle's state is a sorted slice of `(key, writer)` pairs, immutable so the WGL search can share it across branches, with a special `unknown` writer for widened keys (§13.4). `veritas.Store` is a mutable `map[string][]byte`. Making the oracle use the veritas would mean either teaching the search to clone maps of byte slices at every branch, or teaching the veritas about writers and `unknown`. Either change makes one of the two specs worse at its own job.

So instead, **C4 makes them agree.** It is C1 with the history also fed to an oracle:

```go
// toys3/sim/oraclecheck_test.go:24-40
// oracleCheck is C4 on storage from newBackend.
func oracleCheck(newBackend func() storeBackend) veritastest.Check[*storePair, storeOp] {
	c := storeCheck(control.NewMachine, newBackend, ValueHeader)
	newPair := c.New
	c.New = func() *storePair {
		p := newPair()
		p.oracle = NewOracle()
		return p
	}
	c.Invariant = func(p *storePair) error {
		if err := p.invariant(); err != nil {
			return err
		}
		return p.oracleAgrees()
	}
	return c
}
```

Each request is an `Invoke` before delivery and a `Return` after the reply (the two `p.oracle` lines in `step`, §13a.7). After every operation, `oracleAgrees` asks the oracle to check the history so far, then reads the oracle's carried estimate, requires it to be exactly one state with no widened keys (a sequential, fault-free history must leave no uncertainty), and compares that state with the veritas's map, translating each payload to its writer with `ValueWriter` (`oraclecheck_test.go:43-86`). Two details follow from the oracle's design:

- **Put sizes start at `ValueHeader`.** The oracle identifies a value by decoding the writer from its first 12 bytes (Chapter 12), so every payload must be a self-describing value. `storeCheck`'s `minSize` parameter exists for this: C4 passes `ValueHeader`, C1 passes 0.
- **A restart is not a crash.** The oracle never sees `CleanRestart`, because nothing is indeterminate across a clean restart: every request has its reply. "Crashes erase nothing" (§13.2) then says the state carries over, and the veritas, which ignores restarts, agrees.

C4 found no disagreement: 89,300 seeds on `veritas.FS` and 85,355 on `StorageModel` in 30 seconds each. That is the expected result, and its value is in what it rules out. Before C4, if the two specs had disagreed about, say, whether `Copy(a, a)` of a missing key is a 404, nothing would have noticed: DST would check the controller against the oracle, C1 would check the controller against the veritas, and the controller could only agree with one of them, so one check would fail and the other would be blamed. Now the specs are checked against each other directly, on every operation of every C1 sequence. If someone changes one spec and not the other, C4 fails with a message naming the state on each side.

This is an old idea in a small form. N-version programming wrote several independent implementations and compared their outputs, with the well-known weakness that independent teams make correlated mistakes (Knight and Leveson, 1986). Two specs written in different styles for different purposes (one a search over sets of states, one a map) are less likely to share a mistake than two of the same shape, and a mistake they do share is at least a mistake in the *spec*, which is the cheapest place to find one.

## 13a.15 How this maps onto the paper

| Paper (Bornholt et al., SOSP '21) | toys3 |
|---|---|
| §3.2 reference model per component, in the implementation language | `veritas.Store`, `veritas.FS`, in Go, about 100 and 160 lines |
| §3.2 the model omits IO errors and resource exhaustion; it fails "in limited ways" | the veritases never return 500 or `ErrIO`; only 400, 404 and ENOENT |
| §3.2 "Mocking": models used as mocks in unit tests keep them current | the controller tests run on `veritas.FS` (§13a.5) |
| §4.1 property-based test: apply each op to both, compare, check invariants | `veritastest.Check` and `Replay` (§13a.6) |
| Fig. 3 `IndexOp` alphabet with `Reclaim` and `Reboot` as model no-ops | `storeOp` with `kRestart` and `stRestartLeftovers` as veritas no-ops |
| §4.1 "an in-memory user-space disk" under the real components | C1's two backends, `veritas.FS` and fault-free `StorageModel` |
| §4.2 argument bias: Gets prefer keys already Put; sizes near the page size | `genStoreOp` and `genFS` (§13a.8) |
| §4.2 trust default randomness unless there is evidence for a bias | every bias names the path it reaches; no traffic mix is mirrored |
| §4.2 coverage metrics to find blind spots | the C1 coverage pass and `stRestartLeftovers` (§13a.11) |
| §8.3 the missed cache bug: a large cache kept the miss path unreached | mutant i, invisible to DST's 40-byte values; `storeMaxSize = 64` |
| §4.3 minimization: drop operations, shrink integers toward zero | `shrink`'s passes 1 and 2 (§13a.9) |
| §4.3 determinism is required for minimization | detcheck-clean `veritastest`, seeded SplitMix64 |
| §4.3 alphabets ordered simplest first, because the tool prefers earlier variants | `kGet` before `kPut` before ... `kRestart`; keys `a` before `a/b` |
| (no equivalent) | pass 3, `ShrinkAll`: renaming a key everywhere at once |
| §4.4 failure injection with a "has failed" flag that relaxes the comparison | **not done**: future work (Chapter 17); the DST oracle covers it |
| §5 crash consistency with a coarse `DirtyReboot` | **not needed**: the storage model and the oracle go further (Chapters 11, 13) |
| (one spec per component) | two independent specs of the store, kept in agreement by C4 |
| (the model is trusted to match the platform) | C2 and C3: the storage model and a real disk both checked against `veritas.FS` |

The last two rows are where toys3 adds something the paper does not do, and both come from toys3's particular situation. It already had an independent spec (the oracle), so C4 was nearly free. And its whole test strategy rests on a simulated disk, so a check that the simulation agrees with a real disk on the easy cases was worth more to toys3 than to a system whose tests run on its own in-memory disk.

The §4.4 row deserves a sentence more. The paper extends conformance checking to injected I/O errors by relaxing the comparison after a failure: a Get that saw an injected error may return no data, but never wrong data. In toys3 that would be a C1 phase 2: let the backend inject `ErrIO`, allow 500s after an injection, never allow a wrong body or a wrong object under `obj/`. The proposal lists it as out of scope because the DST oracle already checks exactly that, with crashes too. It would still be valuable for the same reason C1 is: a three-operation failure instead of a window.

## 13a.16 Running the checks

All of these were run while writing this chapter. The flags are the same as `TestSeeds`'s (Chapter 16, §16.2); in `toys3/sim` the defaults are 200 seeds, and in `toys3/prod`, where every operation touches a real disk, 20.

```sh
go test ./toys3/veritas/...                                    # the veritases and the kit (incl. shrinker tests)
go test ./toys3/sim -run 'TestC[124]' -seeds=2000 -v           # C1, C2, C4 on seeds 1..2000 (~3 s)
go test ./toys3/sim -run TestC1 -duration=5s -v                # C1 for five seconds per backend
go test ./toys3/sim -run 'TestC1ControllerMatchesVeritasStore/veritasFS$' -seed=42 -v   # replay one seed
go test ./toys3/sim -run TestC1Sequences -v                    # the pinned regression sequences
go test ./toys3/sim -run TestC2 -seed=7 -v                     # replay one C2 seed
go test ./toys3/prod -run TestC3 -seeds=50 -v                  # C3 on a real disk, plus the case-folding probe
go test ./toys3/sim -run TestMutantTable -v                    # DST and C1 against all eleven mutants
go test ./toys3/sim -run TestC1 -coverpkg=manifester/toys3/control -coverprofile=c1.out
go tool cover -func=c1.out                                     # what C1 reaches
```

A passing run prints one line per check and backend:

```
$ go test ./toys3/sim -run 'TestC[124]' -seeds=2000 -v
    conformance_test.go:27: 2000 seeds × 100 operations passed
--- PASS: TestC2StorageModelMatchesVeritasFS (0.71s)
    oraclecheck_test.go:92: 2000 seeds × 100 operations passed
    oraclecheck_test.go:92: 2000 seeds × 100 operations passed
--- PASS: TestC4OracleAgreesWithVeritasStore (1.35s)
    --- PASS: TestC4OracleAgreesWithVeritasStore/veritasFS (0.66s)
    --- PASS: TestC4OracleAgreesWithVeritasStore/StorageModel (0.69s)
    storecheck_test.go:533: 2000 seeds × 100 operations passed
    storecheck_test.go:533: 2000 seeds × 100 operations passed
--- PASS: TestC1ControllerMatchesVeritasStore (0.79s)
    --- PASS: TestC1ControllerMatchesVeritasStore/veritasFS (0.38s)
    --- PASS: TestC1ControllerMatchesVeritasStore/StorageModel (0.41s)
--- PASS: TestC1Sequences (0.00s)
PASS
ok  	manifester/toys3/sim	3.483s
```

and `-duration` reports how far it got:

```
$ go test ./toys3/sim -run TestC1 -duration=5s -v
    storecheck_test.go:533: 26024 seeds × 100 operations passed
    storecheck_test.go:533: 24293 seeds × 100 operations passed
--- PASS: TestC1ControllerMatchesVeritasStore (10.00s)
```

About 5,000 sequences of 100 operations per second per backend, on one core. For comparison, the ten-minute DST campaign of Chapter 16 ran about 57 seeds per second per worker: a C1 seed costs roughly a hundredth of a DST seed, because it is shorter and has no queue, no fault planning and (except in C4) no oracle search. They run as part of `go test ./toys3/sim` at the default 200 seeds, and therefore in CI.

A failing check prints its own replay command, as in §13a.10. To keep a failure, paste the shrunk literal into `TestC1Sequences` (for C1 and C4) or into a `veritastest.ReplayFS` call (for C2 and C3; `TestReplayFS` in `toys3/veritas/veritastest/veritastest_test.go` shows the shape). Unlike a DST seed (Chapter 16, §16.7), a pasted sequence does not depend on the RNG: it survives any change to the generator.

## Key takeaways

- A **veritas** is the small, executable, obviously correct version of one component: `veritas.Store` for the object store, `veritas.FS` for the Storage port. It is sequential and fault-free. The word replaces "reference model", because "model" already meant the storage model and the oracle's register model.
- A veritas is also a **test fake**. The controller tests run on `veritas.FS` (which moved them to package `control_test`), so a new storage command cannot be added without updating the spec.
- A **conformance check** runs a seeded random sequence on the implementation and its veritas in **lockstep**, compares every result exactly, and checks invariants after every operation. C1: controller vs `veritas.Store`, on `veritas.FS` and fault-free `StorageModel`. C2: `StorageModel` vs `veritas.FS`. C3: a real disk vs `veritas.FS`. C4: the oracle vs `veritas.Store`.
- **Bias** inputs only where there is a concrete reason (keys already Put, sizes at the limit, Copy onto itself, leftover temps), and keep every bias probabilistic. Do not mirror traffic.
- **Shrinking** drops chunks, simplifies single operations, and renames names everywhere at once (`ShrinkAll`). It is deterministic, any failure counts, and the result is a pasteable Go literal. A planted cache bug went from 36 operations to `stPut("a", 0), stDelete("a"), stGet("a")` in 25 runs.
- **Coverage** found that C1's restarts never had anything to clean, so recovery's unlink path never ran and mutant f was invisible; planting leftover temps fixed both. It is the paper's missed cache bug again, and DST has its own instance: mutant i, never caught because simulated values are at most 40 bytes against a 64-byte limit.
- C1 catches the functional mutants (f to j) in one or two operations and cannot see fault, crash or concurrency bugs (c, d, d2, e) by design; DST is the reverse. They cover each other's blind spots.
- C2 and C3 together tie the simulator to a real disk on the no-fault path. Building C3 surfaced the case-folding gap (keys `K` and `k` alias on macOS's default filesystem), an open item because the fix changes the on-disk format.
- The oracle stays independent of `veritas.Store`, and C4 keeps them in agreement: two specs written differently check each other.

## Exercises

1. **Plant the cache bug yourself.** Make the three-line scratch edit of §13a.10 and run C1. Then make the cache also invalidate on Delete, but not on Copy onto a cached destination. Predict the shrunk sequence before running. Does DST find it, and at which seed? Revert.
2. **Watch a pass fail.** In a scratch test, take `storeCheck(mutCopyToSrc, newVeritasBackend, 0)` (mutant g) and run seed 1 with `ShrinkAll` set to nil. Does the shrunk sequence change? Explain from §13a.12 why `"b"` survives in the full shrinker's output.
3. **A fifth key.** Add `"B"` to `storeKeys`. Run C1 on `veritas.FS` and on `StorageModel`. Both pass. Why? Now write a C1 backend on a real disk (reuse `diskFS` from `prod/conformance_test.go`; you will need a `Current` snapshot of a real directory) and run it on macOS's default volume. Predict the shrunk sequence.
4. **Remove a bias.** Change `genStoreOp` so that Put sizes are uniform in `[0, 65]`. How many seeds does C1 need to catch mutant i now? Then make sizes uniform in `[0, 4096]`. Explain the result in terms of §13a.8 and the paper's warning.
5. **Coverage regression.** Change `genStoreOp` so restarts never plant leftovers, and re-run the coverage command of §13a.11. Which function's coverage drops, and by how much? Run `TestMutantTable/f` and read its failure message.
6. **C1 phase 2 (paper §4.4).** Add a third C1 backend: `StorageModel` with `EIOProb: 0.05`, driven synchronously with planned failures, and a "has failed" flag in `storePair` that relaxes the comparison after the first injected error: a 500 is allowed, but a 200 with a body different from the veritas's is never allowed, and neither is an object under `obj/` that the veritas does not hold *or* held at some earlier point. What do you have to do about `Trip`? Which of mutants d and d2 does your check catch?
7. **Break C4 on purpose.** In a scratch copy, change `veritas.Store`'s Copy so that `Copy(a, a)` on a missing key returns 200. Which checks fail, and which message names the disagreement most directly?
8. **The Copy gap.** Write a C1 sequence that would demonstrate the Copy size gap of §13a.13 if `storePair` supported changing `MaxObjectSize` across a restart. Add that support (a restart operation with a new limit) and decide what the veritas should say. Is your decision about storage, or about requests?

## Further reading

- James Bornholt, Rajeev Joshi, Vytautas Astrauskas, Brendan Cully, Bernhard Kragl, Seth Markle, Kyle Sauri, Drew Schleit, Grant Slatton, Serdar Tasiran, Jacob Van Geffen and Andrew Warfield, *"Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3"*, SOSP 2021 (`papers/s3-shardstore-lightweight-formal-methods.txt` in this repository). §3.2 reference models and mocking, §4 conformance checking, Fig. 3, §8.3 the missed cache bug.
- Koen Claessen and John Hughes, *"QuickCheck: A Lightweight Tool for Random Testing of Haskell Programs"*, ICFP 2000. The origin of property-based testing.
- John Hughes, *"Experiences with QuickCheck: Testing the Hard Stuff and Staying Sane"*, 2016, for stateful (model-based) property testing of real systems, and shrinking sequences of commands.
- Andreas Zeller and Ralf Hildebrandt, *"Simplifying and Isolating Failure-Inducing Input"*, IEEE TSE 2002. Delta debugging, the idea behind the chunk-dropping pass.
- David R. MacIver, Zac Hatfield-Dodds et al., *"Hypothesis: A New Approach to Property-Based Testing"*, JOSS 2019, and MacIver and Donaldson, *"Test-Case Reduction via Test-Case Generation: Insights from the Hypothesis Reducer"*, ECOOP 2020. How a modern shrinker works, and why shrinking the random choices rather than the values generalizes.
- John C. Knight and Nancy G. Leveson, *"An Experimental Evaluation of the Assumption of Independence in Multiversion Programming"*, IEEE TSE 1986. Why independent versions fail together more often than you would hope, and what that means for two specs checking each other.
- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal"*, OSDI 2014. Background for C3: which filesystem behaviours an application may rely on, and which it may not.
