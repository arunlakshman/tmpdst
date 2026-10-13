# Chapter 15: Mutation Testing: Proving the Tests Have Teeth

> *"Who tests the tests? You break the code on purpose and see whether they notice."*

Two hundred passing seeds feel good. A million passing seeds feel better. But a passing run is evidence of only one thing: that the invariants did not fire. It says nothing, on its own, about whether they *could* have fired. A simulator whose plant model never actually loses un-synced data, or whose oracle accepts everything, or whose crash injection is silently switched off, will pass every seed forever.

This chapter is about the discipline that answers "would we have noticed?": **mutation testing**. toys3 ships seven deliberately broken servers in `toys3/sim/mutants_test.go`, each differing from the real server in exactly one protocol step, and a test (`TestCatchesBugs`) that fails unless the simulator catches every one of them. We will look at how the mutants are built (there is real craft in making a bug that is *only* one bug), dissect each one's failing run line by line using logs produced for this book, reproduce the table in `DESIGN.md`, look at an ablation that shows how much a fault model's realism matters, and then talk about the uncomfortable cases: mutants that are silently dead, and mutants that survive.

---

## 15.1 The idea, and where it comes from

Mutation testing is old. The classic statement is DeMillo, Lipton and Sayward's 1978 paper *"Hints on Test Data Selection: Help for the Practicing Programmer"*. The procedure:

1. Take the program under test.
2. Make a small change to it: a **mutant**. Flip a `<` to `<=`, delete a statement, replace a constant.
3. Run the test suite against the mutant.
4. If some test fails, the mutant is **killed**. If every test passes, the mutant **survives**.
5. The fraction of mutants killed (the *mutation score*) measures the suite's power.

Two hypotheses justify it. The **competent programmer hypothesis** says real programs are usually close to correct, so real bugs look like small changes. The **coupling effect** says tests that catch simple faults tend to catch complex ones too. Jia and Harman's 2011 survey in IEEE TSE (*"An Analysis and Survey of the Development of Mutation Testing"*) is the standard map of the field that grew from there.

Classical mutation testing is *syntactic* and *automatic*: a tool generates thousands of mutants by applying operators to the source. toys3 does something different, which is closer to what DST practitioners actually do: **hand-written semantic mutants**, each one a specific, plausible engineering mistake in the durability protocol. "Forgot to fsync the directory." "Acked before renaming." "Kept serving after an fsync error." Each mutant is a hypothesis of the form *"if a developer made this mistake, would the simulator tell us?"*

There are two reasons to prefer hand-written mutants here:

- **Plausibility.** A syntactic tool would mostly generate mutants that crash immediately (nil dereferences), get caught by the server's own unit tests, or are trivially wrong. The durability bugs that matter (missing fsync, wrong ordering, wrong error policy) are policy mistakes, not typos.
- **Explanation.** Each hand-written mutant comes with a story. When it is caught, the failure message should tell that story. If it does not, either the invariant's message is poor or the mutant is caught "by accident" through some unrelated door, and both of those are worth knowing.

## 15.2 How to break a server without touching it

The mutants do not edit `toys3/control/controller.go`. They **wrap** the controller. Every mutant is a `control.Factory` (`toys3/control/machine.go:22-25`) that builds the real `control.Controller` and puts a thin `control.Machine` around it which rewrites either the actions coming out or the events going in. The simulator plugs it in through `Config.Factory` (`toys3/sim/config.go:35-37`), exactly as it would plug in the real server.

Wrapping has three advantages over forking the server code:

- The mutant is guaranteed to differ in **only** the rewritten behaviour. Everything else is the real code, so a later bug fix in the server automatically applies to every mutant.
- The mutants live in a `_test.go` file, so they never ship and are exempt from `detcheck` (`tools/detcheck/detcheck.go:108-110`).
- The pattern teaches you something about the sans-I/O architecture (Chapter 2): because the server's entire interaction with the world is a stream of plain-data Events in and Actions out, you can inject almost any bug by editing those streams.

Three wrapper types cover all seven mutants.

### 15.2.1 skipper: pretend an action ran

```go
// toys3/sim/mutants_test.go:45-77
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
```

Walk through one step. The inner server returns, say, `[FSFsync{io10, tmp/07}]`. The skip predicate matches `FSFsync`. Instead of passing it out, `expand` immediately feeds the inner server the event `fakeOK(a)`:

```go
// toys3/sim/mutants_test.go:17-33
func fakeOK(a control.Action) control.EventBody {
	switch x := a.(type) {
	case control.FSWrite:
		return control.FSWriteDone{IO: x.IO}
	case control.FSFsync:
		return control.FSFsyncDone{IO: x.IO}
	...
	}
	panic(fmt.Sprintf("fakeOK: %T", a))
}
```

An `FSFsyncDone{IO: 10, Err: OK}` (the zero value of `ErrCode` is `OK`). The inner server, believing the fsync succeeded, advances to its next protocol step and returns `[FSRename{io11, tmp/07 → obj/k2}]`. `expand` recurses on that output (a skipped action could produce another skipped action), and the rename, not matching the predicate, goes out. Net effect: one `Step` of the wrapper turns `FSWriteDone{io7}` into `FSRename{io11}`, and the fsync never happens.

You can see this in mutant b's real trace at seed 1:

```
#38     t=6.531543ms   inc=1 deliver     FSWriteDone{io7 OK} => FSRename{io11 /data/tmp/0000000000000007 -> /data/obj/k2}
```

Note the **IO number**. The rename is `io11`, not `io10`. The inner server allocated `io10` for the fsync (`newIO` at `toys3/control/controller.go:135-138`), the wrapper swallowed it, and the numbering carried on. Compare the correct server at the same point in the same seed:

```
#38     t=6.531543ms   inc=1 deliver     FSWriteDone{io7 OK} => FSFsync{io10 /data/tmp/0000000000000007}
#42     t=6.871157ms   inc=1 deliver     FSFsyncDone{io10 OK} => FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2}
```

Gaps in IO numbers in a log are a tell-tale sign of a skipper at work. They are also harmless: IOIDs only need to be unique within an incarnation (`toys3/control/plant.go:22-25`).

**`deferReal`** adds a twist used by mutant c: the skipped action is still issued, but *after* everything else the step produced. Because the skip happens inside `expand`, the inner server has already advanced past it (and may already have emitted its `Reply`) by the time the deferred action is appended. When the real action's completion eventually arrives, the inner server is no longer waiting for that IO, so `Step` treats it as stale and ignores it (`toys3/control/controller.go:127-132`). That is the stale-completion handling from Chapter 5 earning its keep in an unexpected place.

**`truncAfterTrip`** exists because of `deferReal`:

```go
// toys3/sim/mutants_test.go:35-43
// truncAfterTrip drops anything after a Trip (the runtime contract).
func truncAfterTrip(acts []control.Action) []control.Action {
	for i, a := range acts {
		if _, ok := a.(control.Trip); ok {
			return acts[:i+1]
		}
	}
	return acts
}
```

If an expanded step ends with `Trip`, appending deferred actions after it would violate the rule "Trip is always the last action of its Step", and the simulator would report a `protocol` failure (`toys3/sim/simulator.go:200-204`) rather than the bug we are trying to study. A mutant must be broken in exactly one way; tripping the runtime contract would be a second way.

### 15.2.2 noTrip: hide an error, keep going, fail the reply

Mutants d and d2 model a server that replies 500 after an error but does **not** `Trip`. That is hard to express by rewriting actions alone, because the real server, on seeing an error, goes straight to `failTrip` and stops (`toys3/control/controller.go:264-268`). There is no "500 and continue" path to expose. So `noTrip` works on the *inputs*: it hides the error from the inner server, lets the request continue as if nothing happened, and at the end turns the request's eventual 200 into a 500.

```go
// toys3/sim/mutants_test.go:109-119
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
```

The `Step` method (`toys3/sim/mutants_test.go:131-195`) does four things:

1. **Rewrite the error away.** For `FSFsyncDone`, `FSFsyncDirDone`, `FSRenameDone` and `FSUnlinkDone` with `Err == ErrIO`, replace the body with the same struct and `Err: OK` (lines 136-157). `FSWriteDone` and `FSReadDone` are untouched, because the real server never trips on those.
2. **Spare recovery.** If the inner server has not yet emitted `Recovered`, restore the error (lines 158-161). Recovery errors must still trip; otherwise the mutant would also be breaking recovery, which is a different bug.
3. **Follow the doomed request.** If this event was an error (or the next IO of a request that already saw one), and the inner server's response is a single action with an IO, remember that IO as doomed (lines 189-193). The request will march through its remaining steps with the wrapper tracking it.
4. **Fail the ack.** When the inner server finally emits `Committed` for a doomed request, record its client (lines 181-187), and `rewrite` drops the `Committed` telemetry and turns the `Reply` into a 500 (lines 210-229).

Step 4 has a subtle consequence that matters for Chapter 14: the mutant does **not** emit `Committed` for the requests it fails. A 500 is not an ack, and the white-box `AckedIsDurable` invariant must not judge it as one. If the wrapper left the telemetry in, `AckedIsDurable` would fire on a request the client was told had failed, which would be a false positive, and the mutant would be "caught" for the wrong reason.

`dirOnly` (mutant d2) restricts all of this to `FSFsyncDirDone`. Every other event goes through `stepDoomedOnly`:

```go
// toys3/sim/mutants_test.go:197-208
// stepDoomedOnly passes ev through unchanged, but still tracks doomed IOs.
func (m *noTrip) stepDoomedOnly(ev control.Event) []control.Action {
	acts := m.inner.Step(ev)
	...
	return m.rewrite(acts)
}
```

A small inaccuracy: the comment says it "still tracks doomed IOs", but the function never reads or writes `m.doomed`. It tracks `serving` and applies `rewrite`. For d2 this does not matter, because `FSFsyncDir(obj/)` is the *last* protocol step: hiding its error makes the inner server commit in the very same `Step`, so there is never a later IO to follow.

### 15.2.3 eventRewriter: change what the server sees

The simplest wrapper rewrites each event before the inner server sees it:

```go
// toys3/sim/mutants_test.go:276-287
type eventRewriter struct {
	inner   control.Machine
	rewrite func(control.EventBody) control.EventBody
}

func (m *eventRewriter) Step(ev control.Event) []control.Action {
	ev.Body = m.rewrite(ev.Body)
	return m.inner.Step(ev)
}
```

Mutants e (skip CRC) and f (skip temp cleanup) use it. It is the most surgical of the three: it changes one field of one kind of event and nothing else.

### 15.2.4 StateHash: the wrappers are invisible to the trace

Every wrapper implements `StateHash` by delegating to the inner server. The trace hash (Chapter 8) folds in the server's state hash after each step, plus every event and action. So a mutant's trace still changes wherever its *behaviour* changes (different actions, different events), but the wrapper's own bookkeeping (`deferred`, `doomed`, `clients`) is not hashed. That is acceptable for a test-only wrapper whose state is a function of the event stream. It would not be acceptable for production code; see Chapter 8 for why `StateHash` must cover everything that can influence future behaviour.

## 15.3 The seven mutants and the harness

```go
// toys3/sim/mutants_test.go:319-327
var mutants = []mutant{
	{"a-skip-dir-fsync", mutSkipDirFsync},
	{"b-skip-file-fsync", mutSkipFileFsync},
	{"c-ack-before-rename", mutAckBeforeRename},
	{"d-no-halt-on-error", mutNoTripOnError},
	{"d2-no-halt-on-dir-fsync-error", mutNoTripOnDirFsyncError},
	{"e-skip-crc", mutSkipCRC},
	{"f-skip-temp-cleanup", mutSkipTempCleanup},
}
```

Note the labels of d and d2: they still say "halt", while the Go functions behind them say "trip". That is deliberate. When the refactor of Chapter 3a renamed `Halt` to `Trip`, the mutants' Go identifiers followed (`mutNoHaltOnError` became `mutNoTripOnError`, the wrapper type `noHalt` became `noTrip`, the helper `truncAfterHalt` became `truncAfterTrip`), but the *labels* are names of tests: they appear in `go test -run` patterns, in the golden file of Chapter 8 (§8.12), in CI output, and in this book. Renaming a test is an observable change; renaming a function is not. The refactor drew the line there.

In DDD terms the mutants are *decorators on the `Machine` port* (Chapter 3a, §3a.5): the testbench is a customer of the controller, and `Factory` exists so that the customer can substitute a decorated controller. That relationship is what makes the whole chapter possible.

The harness tries seeds in order until one fails:

```go
// toys3/sim/mutants_test.go:331-343
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
```

and `TestCatchesBugs` (`toys3/sim/mutants_test.go:345-365`) runs it twice per mutant: once with `DefaultInvariants` (required to catch within `-mutant-seeds`, default 500), once with `BlackBoxInvariants` (reported for learning, not required). Note `invs` is a *function*: each seed gets fresh invariant instances, as Chapter 14 explained.

Running `go test ./toys3/sim -run TestCatchesBugs -v` while writing this book produced (the `mutants_test.go:356:` / `:373:` prefixes trimmed):

```
=== RUN   TestCatchesBugs/a-skip-dir-fsync
    caught at seed 1 by acked-is-durable (step 46): c6: write of "k2" acked but /data/obj/k2 has no durable entry
    black-box invariants only: caught at seed 1 by linearizable
=== RUN   TestCatchesBugs/b-skip-file-fsync
    caught at seed 1 by obj-never-torn (step 44): durable /data/obj/k0 is torn or corrupt (0 bytes)
    black-box invariants only: caught at seed 2 by liveness
=== RUN   TestCatchesBugs/c-ack-before-rename
    caught at seed 1 by acked-is-durable (step 42): c6: write of "k2" acked but /data/obj/k2 has no durable entry
    black-box invariants only: caught at seed 1 by linearizable
=== RUN   TestCatchesBugs/d-no-halt-on-error
    caught at seed 1 by obj-never-torn (step 241): durable /data/obj/k2 is torn or corrupt (0 bytes)
    black-box invariants only: caught at seed 20 by liveness
=== RUN   TestCatchesBugs/d2-no-halt-on-dir-fsync-error
    caught at seed 132 by linearizable (step 380): no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
          req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
    black-box invariants only: caught at seed 132 by linearizable
=== RUN   TestCatchesBugs/e-skip-crc
    caught at seed 18 by no-corrupt-reads (step 657): 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
    black-box invariants only: caught at seed 18 by no-corrupt-reads
=== RUN   TestCatchesBugs/f-skip-temp-cleanup
    caught at seed 20 by linearizable (step 1750): no linearization of 1 ops from any of 1 possible start states {k5=221}; window:
          req 248 client 3 GET "k5" [471..475 @255595778..256780566] -> 200 value-of-231
    black-box invariants only: caught at seed 20 by linearizable
--- PASS: TestCatchesBugs (2.25s)
```

This matches the table in `DESIGN.md` exactly, row for row:

| Variant | Caught by (all invariants) | First seed | Black-box only |
|---|---|---|---|
| a. skip `FSFsyncDir(obj/)` | acked-is-durable | 1 | linearizable, seed 1 |
| b. skip the temp file's `FSFsync` | obj-never-torn | 1 | liveness (500s in Settling), seed 2 |
| c. reply 200, then rename and fsync | acked-is-durable | 1 | linearizable, seed 1 |
| d. 500 without `Trip` after any post-write error | obj-never-torn | 1 | liveness, seed 20 |
| d2. same, only for a failed `FSFsyncDir(obj/)` | linearizable | 132 | linearizable, seed 132 |
| e. skip the CRC check on read | no-corrupt-reads | 18 | no-corrupt-reads, seed 18 |
| f. recovery skips temp cleanup | linearizable | 20 | linearizable, seed 20 |

Everything that follows uses logs produced by replaying these seeds with the event log on. (The mutant factories live in a `_test.go` file and are not importable, so the logs were produced by a small out-of-tree program with a verbatim copy of `mutants_test.go`'s wrappers, calling `sim.Run` with `Config.Log = true`. Exercise 1 shows how to get the same logs inside the repo.)

## 15.4 Mutant a: skip `FSFsyncDir(obj/)`

```go
// toys3/sim/mutants_test.go:86-89
// (a) Rename is never made durable: no FSFsyncDir(obj/).
func mutSkipDirFsync(env control.Env, cfg control.Config) control.Machine {
	return &skipper{inner: control.New(env, cfg), skip: isObjFsyncDir(cfg.Dir)}
}
```

**The mistake.** "The file is fsynced and renamed; the rename is atomic; done." Rename is atomic with respect to *visibility*, not durability. The new directory entry lives in the kernel's cache until the directory is fsynced (Chapter 4).

**The catch.** Seed 1, step 46:

```
#44     t=7.233120ms   inc=1 fs-land     land FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} -> FSRenameDone{io12 OK}
#45     t=7.305169ms   inc=1 deliver     FSFsyncDone{io11 OK} => FSRename{io13 /data/tmp/0000000000000009 -> /data/obj/k0}
#46     t=7.377477ms   inc=1 deliver     FSRenameDone{io12 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io15 /data/obj/k2}
#46     t=7.377477ms   inc=1 crash       FAILURE acked-is-durable: c6: write of "k2" acked but /data/obj/k2 has no durable entry
```

On the step where the rename completes, the skipper swallows `FSFsyncDir{io14}`, feeds back a fake OK, and the inner server commits. `AckedIsDurable` asks the PlantModel for `obj/k2`'s durable entry and finds none: the rename only changed `dirent.cur` (`toys3/sim/plantmodel.go:354-362`). No crash was needed. (The `FSRead{io15}` in the same step is a queued GET on `k2` that becomes runnable as soon as the PUT finishes.)

Notice the timestamps: up to step 44 they are identical to the correct server's log. The same seed drives the same RNG through the same sequence of draws until the first point where the mutant behaves differently, which here is the swallowed fsync. This is one of the most useful properties of DST for debugging mutants and real bugs alike: **diff the log of the broken run against the log of the fixed run, same seed, and the first differing line is where behaviour diverged.**

**Black-box.** Seed 1 by `linearizable`. Without the white-box check, the oracle needs a crash that reverts the un-synced `obj/` entry and a later read that sees the old value (or 404). With every PUT in the run exposed, that happens almost immediately.

## 15.5 Mutant b: skip the temp file's `FSFsync`

```go
// toys3/sim/mutants_test.go:91-97
// (b) The temp file's bytes are never fsynced before the rename.
func mutSkipFileFsync(env control.Env, cfg control.Config) control.Machine {
	return &skipper{inner: control.New(env, cfg), skip: func(a control.Action) bool {
		_, ok := a.(control.FSFsync)
		return ok
	}}
}
```

**The mistake.** "The directory fsync makes the rename durable, so we're good." The directory fsync makes the *name* durable. The *content* behind the name needs its own fsync. On a real filesystem this is the classic "zero-length file after crash" bug that bit many applications on ext4 with delayed allocation.

**The catch.** Seed 1, step 44, from Chapter 14:

```
#44     t=7.233120ms   inc=1 fs-land     land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k0 is torn or corrupt (0 bytes)
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k2 is torn or corrupt (0 bytes)
```

The failure fires the instant the directory fsync *lands*, before the server is told. Both renamed entries are now durable and both point at inodes whose durable content is still empty. Two failures in one step from one invariant.

**Black-box: the interesting one.** The oracle *cannot* catch b. Follow the chain: a crash happens; the adversary keeps the durable directory entry but the inode's content reverts to its durable version (empty) or a torn prefix; recovery leaves `obj/` alone; a GET reads the file, `DecodeObject` fails, the server emits `CorruptionDetected` and replies **500**. To the oracle a 500 GET is `Failed`, and failed reads constrain nothing (`toys3/sim/oracle.go:285-288`). The key is effectively "unknown". No contradiction can ever be derived.

What catches it is `Liveness`'s rule that an op invoked in Settling never gets a 500. Seed 2:

```
FAIL liveness step 1719: 500 with no fault injected (Settling): req 284 client 3 GET "k7" [527..529 @303457778..303910931] -> 500
```

The GET was invoked at 303.46 ms, after Chaos ended at 300 ms. The run's stats line says `corruptdet=34`: the server had been tripping over damaged objects and replying 500 all through Chaos, which is legal under the oracle's rules, until a quiet-tail request made it illegal. Why seed 2 and not seed 1? Seed 1 happens not to end with a damaged key being read in the tail. This is the lesson `DESIGN.md` records:

> (b) is invisible to the linearizability oracle. The CRC turns a lost object into a 500, and a 500 promises nothing. Only a ground-truth check, or "no 500s once faults stop", catches it.

There is an irony worth savouring. The CRC check, a *safety* feature, is what makes b invisible to the oracle: without it, the server would serve the torn bytes with a 200 and `NoCorruptReads` would fire immediately. Safety nets can hide falls.

## 15.6 Mutant c: reply 200, then rename and fsync

```go
// toys3/sim/mutants_test.go:99-107
// (c) Reply 200 first, then rename and fsync the directory (ack before
// durable, and even before visible).
func mutAckBeforeRename(env control.Env, cfg control.Config) control.Machine {
	obj := isObjFsyncDir(cfg.Dir)
	return &skipper{inner: control.New(env, cfg), deferReal: true, skip: func(a control.Action) bool {
		_, ren := a.(control.FSRename)
		return ren || obj(a)
	}}
}
```

**The mistake.** "The data is safely fsynced in the temp file; we can ack now and finish the bookkeeping in the background." This is the shape of every "optimistic ack" optimization.

**The catch.** Seed 1, step 42:

```
#40     t=6.676498ms   inc=1 fs-land     land FSFsync{io10 /data/tmp/0000000000000007} -> FSFsyncDone{io10 OK}
#41     t=6.789500ms   inc=1 deliver     FSWriteDone{io9 OK} => FSFsync{io11 /data/tmp/0000000000000009}
#42     t=6.871157ms   inc=1 deliver     FSFsyncDone{io10 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io14 /data/obj/k2} FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} FSFsyncDir{io13 /data/obj}
#42     t=6.871157ms   inc=1 crash       FAILURE acked-is-durable: c6: write of "k2" acked but /data/obj/k2 has no durable entry
```

Look at the action list on step 42. That one line is a complete picture of `deferReal`: the skipper fed fake OKs for the rename (`io12`) and the directory fsync (`io13`), the inner server committed and replied, the queued GET on `k2` started (`FSRead{io14}`), and *then* the two real actions were appended. Worse than a; the GET at `io14` will be *issued before the rename* and may land before it, reading the old value (or ENOENT) after the client has been told the PUT succeeded. That would be a linearizability violation even without a crash. The comment's phrase "ack before durable, and even before visible" is precise.

When `FSRenameDone{io12}` and `FSFsyncDirDone{io13}` eventually arrive, the inner server has forgotten those IOs (it finished the request), so they are ignored as stale.

## 15.7 Mutant d: 500 without Trip after any post-write error

```go
// toys3/sim/mutants_test.go:121-123
func mutNoTripOnError(env control.Env, cfg control.Config) control.Machine {
	return &noTrip{inner: control.New(env, cfg)}
}
```

**The mistake.** "We reported the error. Crashing the whole server for one failed request is an overreaction." Chapter 7 makes the full case for fail-stop. Mutant d is that argument made executable.

**The catch.** Seed 1, step 241. This run has *no crashes at all* (`crashes=0` in its stats line):

```
#232    t=53.087606ms  inc=1 fs-land     land FSFsync{io78 /data/tmp/000000000000004c} -> FSFsyncDone{io78 EIO}
#233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => FSRename{io80 /data/tmp/000000000000004c -> /data/obj/k2}
#234    t=53.582705ms  inc=1 fs-land     land FSRename{io79 /data/tmp/000000000000004b -> /data/obj/k0} -> FSRenameDone{io79 OK}
#235    t=53.616264ms  inc=1 deliver     FSRenameDone{io79 OK} => FSFsyncDir{io81 /data/obj}
#236    t=54.179815ms  inc=1 fs-land     land FSFsyncDir{io81 /data/obj} -> FSFsyncDirDone{io81 EIO}
#237    t=54.207944ms  inc=1 deliver     FSFsyncDirDone{io81 EIO} => Reply{c28 500}
#238    t=54.292701ms  inc=1 fs-land     land FSRename{io80 /data/tmp/000000000000004c -> /data/obj/k2} -> FSRenameDone{io80 OK}
#239    t=54.674448ms  inc=1 deliver     FSRenameDone{io80 OK} => FSFsyncDir{io82 /data/obj}
#241    t=55.831528ms  inc=1 fs-land     land FSFsyncDir{io82 /data/obj} -> FSFsyncDirDone{io82 OK}
#241    t=55.831528ms  inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k2 is torn or corrupt (0 bytes)
```

Compare this with the correct server at the same seed. In `seed1.log`, step 233 reads `FSFsyncDone{io78 EIO} => Reply{c30 500} Trip{fsync temp: EIO}` and the server crashes on the spot. Here, the wrapper hides the EIO, and the inner server carries on to rename `tmp/…4c` into `obj/k2` (step 233). Now the PlantModel's fsyncgate model bites: the failed fsync *poisoned* the inode (`toys3/sim/plantmodel.go:330-335`, `FsyncGate: true` in `BaseConfig`), so its dirty pages are marked clean but will never reach the disk. When the next directory fsync lands at step 241, `obj/k2` durably names an inode whose durable content is empty. `obj-never-torn` fires.

This is *exactly* the PostgreSQL fsyncgate story from Chapter 7: retrying or continuing after a failed fsync is unsafe because the kernel may have dropped the error state along with the data. The mutant did not need a crash to be caught because the white-box invariant can see the durable view directly. Notice too, at step 237, a different request (`c28`) whose own `FSFsyncDir` failed got its 200 turned into a 500, also without a Trip; that error alone would not have tripped `obj-never-torn`, because the file content was fine.

**Black-box.** Seed 20 by `liveness`:

```
FAIL liveness step 2403: 500 with no fault injected (Settling): req 331 client 2 GET "k31" [648..651 @329389038..330313878] -> 500
```

Same mechanism as b: content that was never durable is lost at a crash, the CRC catches it, and the key reads back as 500 forever. It takes until seed 20 because the black-box path needs an fsync EIO on a PUT, a crash before that key is overwritten, and a quiet-tail GET of that exact key.

## 15.8 Mutant d2: only for a failed `FSFsyncDir(obj/)`

```go
// toys3/sim/mutants_test.go:125-129
// (d2) Like (d), but only for a failed FSFsyncDir(obj/): the rename is
// visible, not durable, and the server keeps serving it.
func mutNoTripOnDirFsyncError(env control.Env, cfg control.Config) control.Machine {
	return &noTrip{inner: control.New(env, cfg), dirOnly: true}
}
```

Chapter 7 dissects this run in full, because it is *the* argument for tripping. The short version, from seed 132:

```
#317    ... land FSUnlink{io68 /data/obj/k1} -> FSUnlinkDone{io68 OK}
#323    ... land FSFsyncDir{io70 /data/obj} -> FSFsyncDirDone{io70 EIO}
#324    ... deliver FSFsyncDirDone{io70 EIO} => Reply{c45 500} FSUnlink{io71 /data/obj/k1}
#327    ... deliver FSUnlinkDone{io71 ENOENT} => Reply{c47 404}
#339    ... deliver FSReadDone{io73 ENOENT nil} => Reply{c49 404} FSRead{io75 /data/obj/k1}
#343    ... crash CRASH (trip: fsync temp: EIO): kept=1 lost=2 torn=0, dropped 1 queued entries
#365    ... deliver FSReadDone{io5 OK obj(value-of-26)} => Reply{c55 200 value-of-26}
#380    ... FAILURE linearizable: no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
  req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
```

DELETE `c45` unlinked `k1` visibly; its directory fsync failed; the mutant replied 500 and *kept serving*. DELETE `c47` and GET `c49` both observed `k1` absent. Then an unrelated `Trip` crashed the server, the adversary reverted the un-synced unlink, and after the restart GET `c55` saw `value-of-26`, which had been observed as gone. The oracle's carried state after the crash window was `{}` (k1 absent, forced by the 404s) or `{k1=52}` (the abandoned PUT `c52` might have applied). Neither allows `value-of-26`.

Two things make d2 special in the table:

- **No white-box invariant helps.** The mutant drops the `Committed` for the doomed DELETE, so `AckedIsDurable` never looks. The `obj/` directory is clean in the durable view (the object file is intact; it was just un-deleted). Only the black-box oracle can see the problem, because the problem *is* a client-visible history.
- **It takes 132 seeds.** It needs an EIO on precisely a directory fsync of `obj/` (one of several FS ops per mutation, at 0.2 to 3% each depending on the seed's swarm draw), then a read that observes the un-durable change, then a crash before anything else makes `obj/` durable (any later successful `FSFsyncDir(obj/)` by another request would persist the change and heal it), then a read after restart. That is a long conjunction, and it is exactly the kind of bug that a real system would ship with.

## 15.9 Mutant e: skip the CRC check on read

```go
// toys3/sim/mutants_test.go:289-301
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
```

The rewrite is clever. Rather than editing `DecodeObject` (which lives in the real server), it **re-encodes** any read result whose magic and length look right, recomputing the CRC over whatever payload bytes arrived. The inner server then "verifies" a CRC that is guaranteed to match. Files with a bad magic or a wrong length still fail, so this mutant removes *only* the checksum check, not the structural checks: one protocol step.

**The catch.** Seed 18, step 657:

```
#656    t=153.982439ms inc=7 fs-land     land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657    ...                                FAILURE no-corrupt-reads: 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
#657    ...                                FAILURE linearizable: GET req 98 client 0 GET "k3" ... -> 200 garbage(13 bytes) returned bytes no PUT wrote
```

`raw(29 bytes)` is a 16-byte header plus a 13-byte payload, with one bit flipped by `PlantModel.read`'s `ReadCorruptProb` coin (`toys3/sim/plantmodel.go:398-402`). The correct server at the same seed and step emits `CorruptionDetected{"k3"}` and replies 500 (Chapter 14 shows both lines). The mutant serves `garbage(13 bytes)` with a 200.

Why seed 18 and not seed 1? Because seed 1 has `readcorrupt=0`: at 0.5% per read with a few hundred reads per seed, many seeds see no bit flip at all, and the bit flip must also land on a GET (not a COPY whose garbage then has to be read back) and on an object rather than a `ENOENT`. And as Chapter 14 noted, with `ReadCorruptProb = 0` mutant e survives 1,000 seeds: the protocol never puts a damaged file in `obj/`, so the check is unreachable. A mutation that survives only because its code is unreachable is a strong hint that the fault model is missing something.

## 15.10 Mutant f: recovery skips temp cleanup

```go
// toys3/sim/mutants_test.go:303-312
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
```

If the recovery listing of `tmp/` comes back empty, recovery skips straight to `FSFsyncDir(tmp)` and `Recovered` (`toys3/control/recovery.go:40-49`). Leftover temp files accumulate across incarnations.

**The mistake.** "Leftover temp files are just garbage. They waste disk, but nobody reads `tmp/`." The trouble is that something does *write* `tmp/`, with names that repeat.

This is the most beautiful failure in the set, so let us follow it from the log of seed 20 (1,750 steps, 13 crashes). The key facts:

- **Temp names repeat across incarnations.** `TmpPath` names a temp file after the IOID of its `FSWrite`, in 16 hex digits (`toys3/format/layout.go:73-79`), and IOIDs restart from 1 in every incarnation (`toys3/control/controller.go:92`, `155-158`). The comment on `TmpPath` states the safety argument: names "never collide" *because* "recovery clears tmp/ before a new incarnation writes anything". The mutant breaks the premise.
- **FSWrite on an existing name reuses the inode.** `PlantModel.write` creates a new inode only if the name is absent; otherwise it truncates and overwrites the existing inode (`toys3/sim/plantmodel.go:308-317`). That is the semantics of `open(O_WRONLY|O_CREAT|O_TRUNC)` (which is what `prod.writeFile` does, `toys3/prod/executor.go:76-86`).
- **A rename leaves the old name un-durably removed.** The server fsyncs `obj/` after a rename but never fsyncs `tmp/`. So after `rename(tmp/X → obj/k)`, the `tmp/X` entry's *current* version is absent but its *durable* version may still be the inode, if the temp's name had become durable (via a crash that kept it, or background writeback).

Now the story. `tmp/0000000000000004` is a leftover from a much earlier incarnation; it is already in the recovery listing of incarnation 9 (step 1184) and stays there, because this mutant never removes anything. Its name is durable.

**Incarnation 12.** PUT `c221` on `k5` is the second request to start after recovery, so its `FSWrite` gets IO 4, and its temp path is `tmp/0000000000000004`. The name exists, so the write goes into the **leftover's inode** (call it I):

```
#1472   inc=12 deliver  FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSWrite{io3 /data/tmp/0000000000000003 obj(value-of-220)} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-221)} ...
#1473   inc=12 fs-land  land FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-221)} -> FSWriteDone{io4 OK}
#1480   inc=12 fs-land  land FSFsync{io7 /data/tmp/0000000000000004} -> FSFsyncDone{io7 OK}
#1489   inc=12 fs-land  land FSRename{io11 /data/tmp/0000000000000004 -> /data/obj/k5} -> FSRenameDone{io11 OK}
#1495   inc=12 fs-land  land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#1497   inc=12 deliver  FSFsyncDirDone{io14 OK} => Emit{Committed{c221 "k5" crc=e23c4709}} Reply{c221 200}
```

`Recovered{temps=0}` on the first line is the mutant's fingerprint: there were ten leftovers in the listing, and none were removed. After step 1497, `obj/k5` durably names inode I with durable content `value-of-221`. The PUT is correct and durable; `AckedIsDurable` is satisfied. But the directory entry `tmp/0000000000000004` is now `{cur: absent, durable: I}`, because `tmp/` was never fsynced after the rename.

**The crash.** An injected crash at step 1518:

```
#1518   inc=12 crash    CRASH (injected crash): kept=0 lost=3 torn=0, dropped 3 queued entries
```

`PlantModel.Crash` resolves every un-synced directory entry change with a coin (`toys3/sim/plantmodel.go:471-488`). For `tmp/…04` the coin says *revert*: `cur = durable = I`. Now **two names point at inode I**: `obj/k5` and `tmp/0000000000000004`. A hard link has been conjured out of a crash.

```
   after crash:   obj/k5 ──────┐
                               ├──► inode I: "TOY3" | crc | len | value-of-221
                  tmp/…04 ─────┘
```

**Incarnation 13.** Recovery (still skipping cleanup) leaves `tmp/…04` in place. PUT `c231` on `k25` is again the request that gets IO 4:

```
#1534   inc=13 deliver  FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSRead{io3 /data/obj/k1} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-231)} ...
#1537   inc=13 fs-land  land FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-231)} -> FSWriteDone{io4 OK}
#1546   inc=13 fs-land  land FSRename{io10 /data/tmp/0000000000000004 -> /data/obj/k25} -> FSRenameDone{io10 OK}
```

The `FSWrite` truncates and overwrites **inode I**. Through its other name, `obj/k5` now contains `value-of-231`. Then the rename moves the `tmp/` name onto `obj/k25`, and `k25` is correct too. Nobody touched `k5`, but its content changed.

**The observation.**

```
#1669   inc=13 deliver  Request{c248 GET "k5"} => FSRead{io52 /data/obj/k5}
#1678   inc=13 fs-land  land FSRead{io52 /data/obj/k5} -> FSReadDone{io52 OK obj(value-of-231)}
#1679   inc=13 deliver  FSReadDone{io52 OK obj(value-of-231)} => Reply{c248 200 value-of-231}
...
#1750   inc=13 crash    FAILURE linearizable: no linearization of 1 ops from any of 1 possible start states {k5=221}; window:
  req 248 client 3 GET "k5" [471..475 @255595778..256780566] -> 200 value-of-231
```

The oracle's carried state says, with certainty, that `k5 = value-of-221` (acked by `c221`, never overwritten). The GET returned a perfectly valid value (`ValueWriter` accepts it, and it really was written by PUT `c231`, so `NoCorruptReads` stays silent), but it is **the value of a different key**. Only the linearizability model, which knows that request 231 was a PUT on `k25`, can say this is impossible. The failure is reported at step 1750, the next quiescent point (a `Trip` crash), 71 steps after the bad reply.

**The guard that proves the mutant is alive.** Every seed's stats line under this mutant shows `tempsrm=0`. Under the correct server, seed 1 reports `tempsrm=7`. If someone accidentally made the rewrite a no-op (say, by matching on the wrong event type), `tempsrm` would go back to non-zero, and the trace hash would equal the correct server's. That is the subject of §15.12.

**And the correct server?** The same aliasing happens to it, and it survives. In the correct server's seed 1 log, PUT `c23`'s temp `tmp/0000000000000031` is renamed onto `obj/k1` at step 160, and after the crash at step 233 the incarnation-2 recovery listing contains `0000000000000031` again: the resurrected alias. Recovery unlinks it (unlinking one name of a two-name inode removes only that name), and no new write can reach it. Recovery's temp cleanup is not housekeeping; it is what keeps resurrected hard links from being written through. Chapter 11 discusses the modelling choice behind this, and Chapter 5 the recovery protocol.

## 15.11 Ablation: how realistic must the world be?

`DESIGN.md` records a striking observation about mutant f:

> (f) took seed 429 with neither background writeback nor swarm, seed 36 with writeback alone, and seed 20 with both.

This book re-ran it, adding the fourth combination:

| Configuration | Background writeback | Swarm (per-seed variation) | First catching seed |
|---|---|---|---|
| `BaseConfig`, `WritebackProb = 0` | off | off | **429** |
| `BaseConfig` | on (5%) | off | **36** |
| `DefaultConfig`, `WritebackProb = 0` | off | on | **24** |
| `DefaultConfig` | on | on | **20** |

(Searched with seeds 1..1000; all four claims reproduce.)

Why does each ingredient help?

- **Background writeback** (`toys3/sim/plantmodel.go:199-201`, `195-232`) makes temp *names* durable on their own, the way a journal commit would. Without it, a temp name becomes durable only if a crash happens to keep it, which then makes it a leftover to begin with; the alias in §15.10 needs a durable temp name *and then* a later rename and a crash that reverts the rename's removal. Writeback makes the first condition common.
- **Swarm testing** (`toys3/sim/config.go:137-145`) varies the number of keys per seed from 1 to 32. Mutant f's damage is slow: it needs an object to be written through an alias and then read *before* any PUT to that key overwrites it. With `BaseConfig`'s 3 keys, every key is overwritten constantly, healing the damage before anyone looks. Seed 20 has `keys=32`: most objects sit untouched long enough for a GET to catch the corruption. Chapter 12 explains swarm testing in general.

The lesson, in `DESIGN.md`'s words: *whether a bug gets caught depends on how realistic the fault model is and how varied the workload is.* A twenty-fold difference in time-to-catch on a bug this simple should make you nervous about every bug you have *not* written a mutant for. It is also why mutation testing is the right tool for *evaluating changes to the simulator itself*: if you change the fault model, rerun the mutants and compare first-catch seeds. A change that makes them all slower made the simulator weaker.

## 15.12 When the mutant is wrong

Mutation testing has its own failure modes, and they are sneaky because they look like success or like an uninteresting result.

**A dead mutant.** The mutant's rewrite never fires: a type switch on the wrong event, a path comparison that never matches (for example `isObjFsyncDir` comparing against the wrong root), a predicate on `Err == control.ErrIO` when the event in question carries `ErrNotFound`. A dead mutant is behaviourally identical to the real server, so it is never caught, and `TestCatchesBugs` reports "not caught in 500 seeds". That at least is loud. The quieter version is a mutant that fires only in a rare path, so it is caught eventually, but for a reason unrelated to the bug it claims to model.

**A mutant that breaks two things.** The mutant fires, but it also violates some unrelated contract: it appends actions after a `Trip`, it emits a `Committed` for a request it fails, it hides recovery errors as well as request errors. It gets caught, quickly, and the table looks great, but the invariant that caught it is not the one that would catch the real-world bug. The care taken in `noTrip` (restoring recovery errors, dropping `Committed`) and in `skipper` (`truncAfterTrip`) is exactly about avoiding this.

**The mutant f story.** The brief for this book recalls that mutant f was at first *not caught*, and that the cause turned out to be a bug in the mutant itself rather than a weakness of the simulator. The repository cannot show that earlier version: `toys3/sim/mutants_test.go` was added in a single commit (`210c1fa`) in its final form, so the broken mutant exists only in that recollection, and this book will not invent its details. What the repository *does* preserve is the ablation above, which shows that even the *correct* mutant f needs 429 seeds under a less realistic model. A buggy mutant and a weak simulator produce the same symptom ("not caught"), and you cannot tell which you have without checking that the mutant is alive.

So, the guards. In rough order of cost:

1. **One protocol step per mutant.** Each wrapper changes one kind of event or action. If you find yourself writing a mutant that changes two, write two mutants.
2. **A liveness stat.** Pick a counter the mutant must move and check it. For f, `tempsrm=0` across every seed (under the correct server it is positive in nearly every seed with a crash). For a skipper, count how many actions it swallowed. For `noTrip`, count doomed requests. A mutant whose counter stays at zero over 200 seeds is dead.
3. **A trace-hash diff against the real server.** Run seed 1 with the real factory and with the mutant. If the trace hashes are equal, the mutant never changed anything observable. For this book: correct server seed 1 is `trace=1ca97eb77a9044e1`; mutant f at seed 1 passes (not caught) but its trace is `f9a6442332345f03`, with `tempsrm=0`; the "g" mutant below is `c4f18946dc209c4e`. Both alive. This could be an automated test (see exercise 3).
4. **Check *what* caught it.** `TestCatchesBugs` logs the invariant and the message. If mutant c were caught by `panic` or `protocol`, that would be a mutant bug, not a success.

## 15.13 Surviving mutants: equivalent, or weak tests?

A mutant that survives is either an **equivalent mutant** (it changes the code but not the observable behaviour, so no test *could* kill it) or evidence of a **weak test suite** (it is a real bug, and the invariants or the fault model cannot see it). Distinguishing the two is the famous hard problem of mutation testing; there is no algorithm for it in general.

Here is a concrete one. While writing this book, a mutant "g" was tried in an out-of-tree harness: skip recovery's `FSFsyncDir(tmp)` (a `skipper` whose predicate matches `FSFsyncDir` on `format.TmpDir(cfg.Dir)`). Recovery unlinks the leftovers but never makes the unlinks durable. Run against seeds 1..2000 with the default invariants: **not caught**.

Is that a weak test, or is g equivalent? Reason it through with the PlantModel's model:

- After recovery's unlinks, each leftover `tmp/X` is `{cur: absent, durable: I}`. If the server later writes `tmp/X` in this incarnation (IO X), `PlantModel.write` sees the name absent in the *current* view and creates a **fresh** inode J (`plantmodel.go:308-314`). There is no aliasing within the incarnation.
- If a crash happens, `tmp/X` may revert to I (the old leftover, possibly an alias of a committed object). The next recovery lists and unlinks it again *before* any write is issued, because requests queue behind recovery.
- So the un-synced unlinks can resurrect garbage, but garbage is always cleaned before it can be written through.

Under this model, g appears to be an **equivalent mutant**: the recovery fsync of `tmp/` is belt-and-braces. That is itself a finding (Chapter 17 lists it among the surprises), and it raises a design question rather than answering one: is the fsync there because a *real* filesystem has some behaviour the PlantModel lacks (say, an `fsync` of a directory being needed before a create in the same directory is ordered after the unlink), or is it simply cheap insurance? Either answer is defensible. What is not defensible is not knowing.

And a white-box invariant *can* kill g: Chapter 14's sketched `TmpCleanAfterRecovery` ("when `Recovered` is emitted, nothing under `tmp/` is durable") fires immediately. Whether you want that invariant is the same design question in another form: invariants define what the system promises, and a promise nobody depends on is a cost.

## 15.14 Mutation testing as a habit

Some practical habits that fall out of this chapter:

- **Every new invariant gets a mutant it alone catches.** Otherwise you cannot tell whether the invariant works. Chapter 14's exercises do this for `TmpCleanAfterRecovery`.
- **Every change to the fault model reruns the mutants**, and you compare first-catch seeds before and after. `TestCatchesBugs` is cheap (2.25 s here).
- **Every real bug you fix becomes a mutant.** Reintroduce it as a wrapper; confirm the simulator catches it; keep it in the list. That is how the table grows from seven rows to seventy.
- **Record the black-box column.** It tells you which bugs a real user would see, and how soon.
- **Treat survivors as research questions**, not as failures to hide. g taught us something about the recovery protocol.

## Key takeaways

- Mutation testing measures whether your tests *could* fail. In DST it is the only good answer to "would we have noticed?".
- toys3's mutants are hand-written, semantic, one-protocol-step bugs, implemented as wrappers around the real server that rewrite its events or actions. Three wrappers (`skipper`, `noTrip`, `eventRewriter`) cover all seven.
- A good mutant is broken in exactly one way: `truncAfterTrip`, restoring recovery errors, and dropping `Committed` for doomed requests all exist to keep it that way.
- Every row of the `DESIGN.md` table reproduces. White-box invariants catch most mutants at seed 1 and without a crash; black-box invariants catch all of them eventually, b and d only through liveness, d2 and f only through linearizability.
- Mutant f's 20-seed catch depends on both background writeback and swarm testing; without either it takes 429 seeds. Fault-model realism and workload variety are first-class concerns.
- A mutant can be dead or wrong. Guard with one-step discipline, liveness counters (`tempsrm=0`), trace-hash diffs against the real server, and by checking *which* invariant caught it.
- A surviving mutant is either equivalent or a gap. The recovery `FSFsyncDir(tmp)` appears to be equivalent under the current PlantModel model.

## Exercises

1. **Get the logs in-repo.** Add a temporary test to `toys3/sim/mutants_test.go`:

   ```go
   func TestMutantLog(t *testing.T) {
       cfg := DefaultConfig(132)
       cfg.Factory = mutNoTripOnDirFsyncError
       cfg.Log = true
       r := Run(cfg)
       t.Log(FormatLog(r.Log))
   }
   ```

   Run `go test ./toys3/sim -run TestMutantLog -v | less` and find steps 317 to 380. Do the same for mutant f at seed 20 and follow `tmp/0000000000000004` with `grep`.

2. **Reply before the directory fsync only.** Add to the `mutants` list:

   ```go
   {"h-ack-before-dir-fsync", func(env control.Env, cfg control.Config) control.Machine {
       return &skipper{inner: control.New(env, cfg), deferReal: true, skip: isObjFsyncDir(cfg.Dir)}
   }},
   ```

   Predict, before running: which invariant catches it, at which seed, and does the black-box suite catch it at seed 1 like c, or later? Unlike c, the rename has happened before the ack, so the value is *visible*. Run `go test ./toys3/sim -run 'TestCatchesBugs/h-ack-before-dir-fsync' -v`.

3. **Automate the liveness guard.** Write `TestMutantsAreAlive`: for each mutant, run seed 1 with the real factory and with the mutant, and fail if the two `TraceHash` values are equal. Then deliberately break one mutant (change the condition in `mutSkipTempCleanup` from `d.Err == control.OK` to `d.Err == control.ErrIO`, so it only rewrites failed listings, whose `Names` are already empty) and confirm the new test fails while `TestCatchesBugs` reports "not caught".

4. **Skip the CRC only for COPY.** Write a mutant that re-encodes `FSReadDone` only when the read belongs to a COPY. (Hint: an `eventRewriter` cannot tell GET reads from COPY reads by the completion alone. Write a wrapper that also sees the inner server's actions: remember every pending `control.Request` in arrival order; because requests on one key are serialized, an `FSRead` of `obj/<key>` belongs to the oldest unfinished request that reads `<key>` (a GET of it, or a COPY with it as source). Record the IOs that belong to COPYs, and re-encode only their `FSReadDone`.) Predict the catching invariant: the corrupted payload is now written to `dst` as a *valid* object with a fresh CRC. Is it `no-corrupt-reads`, `linearizable`, or both? At what seed?

5. **Write directly to obj/.** Write a mutant that rewrites the PUT path to skip the temp file entirely: `FSWrite` to the final `obj/` path, `FSFsync`, `FSFsyncDir(obj/)`, reply. (Hint: rewrite the `FSWrite`'s `Path` to the object path, swallow the `FSRename` with a fake OK, and keep the rest.) Predict which invariant fires first. Think about a crash between `FSWrite` landing and `FSFsync` landing: what does `obj/k` contain in the durable view if the key already existed?

6. **Allow concurrent same-key GETs.** The server serializes *all* requests on a key, including GET after GET (`toys3/control/controller.go:63-72`). Write a mutant (this one cannot be a wrapper; copy `controller.go` into a test-only package or add a test-only field) where `interlocked` returns false when both requests are GETs. Predict: does any invariant catch it? If not, is it an equivalent mutant? Then try the stronger version where a GET never conflicts with anything, and reason about a GET that overtakes an earlier PUT on the same key.

7. **Re-run the ablation.** Write a test that copies `firstCatch` but builds configs with `BaseConfig(seed)` and `cfg.Disturbances.WritebackProb = 0`, and confirm mutant f's first catch is seed 429. Then try `QuiesceEvery` halved and doubled, and record how the first-catch seed of d2 moves. Explain the direction.

8. **Equivalent or not?** Make the case, in writing, for or against the claim in §15.13 that skipping recovery's `FSFsyncDir(tmp)` is an equivalent mutant. Then change the PlantModel in some realistic way that would make it *not* equivalent (for example, make `PlantModel.write` to a name whose current entry is absent but whose durable entry is present reuse the durable inode, as a buggy filesystem might) and see whether the default invariants now catch g. Revert afterwards.

## Further reading

- Richard A. DeMillo, Richard J. Lipton and Frederick G. Sayward, *"Hints on Test Data Selection: Help for the Practicing Programmer"*, IEEE Computer, 1978. The origin of mutation analysis, the competent programmer hypothesis and the coupling effect.
- Yue Jia and Mark Harman, *"An Analysis and Survey of the Development of Mutation Testing"*, IEEE Transactions on Software Engineering, 2011. The standard survey, including the equivalent mutant problem.
- Goran Petrović and Marko Ivanković, *"State of Mutation Testing at Google"*, ICSE-SEIP 2018. How mutation testing is used at scale, with a focus on surfacing only useful mutants to reviewers.
- Alex Groce, Chaoqiang Zhang, Eric Eide, Yang Chen and John Regehr, *"Swarm Testing"*, ISSTA 2012. The idea behind `DefaultConfig`'s per-seed variation, and the reason mutant f is caught at seed 20 rather than 429.
- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014, and the TigerBeetle VOPR documentation, for how simulation-first teams validate that their simulators actually find bugs.
- Craig Ringer's 2018 pgsql-hackers thread on PostgreSQL's handling of `fsync()` errors ("fsyncgate"), and Jonathan Corbet's LWN article *"PostgreSQL's fsync() surprise"* (2018), the real-world story behind mutant d.
