# Chapter 7: Errors, Fail-Stop and the Safety Trip

> *"The danger is not the 500 itself (a 500 promises nothing) but what later reads observe."*
> (`toys3/control/doc.go:61-62`)

Chapter 6 followed the happy paths and the 404s. This chapter is about every other branch of `onRequestIO` and `onRecoveryDone`: what the server does when the filesystem says `EIO`. It is the most important chapter in the book for anyone who builds storage, because the error policy is where intuition fails most often, and where DST earns its keep most visibly.

The chapter has three parts. First, the policy: a table of every error the server can see, and for each, whether it replies 500 or replies 500 *and trips*. Second, the reasons: the principle that "a read must never see a value a crash can erase", and the Linux "fsyncgate" behaviour that makes `fsync` errors unrecoverable in place. Third, the evidence: two deliberately broken servers (mutants **d** and **d2**) and the exact simulator runs that catch them, dissected step by step.

---

## 7.1 Two kinds of failure response

When an I/O operation fails, a server has roughly three options:

1. **Retry.** Try the operation again. Tempting, and for some operations (a read) harmless. For `fsync`, as we will see, catastrophically wrong.
2. **Report and continue.** Reply with an error to the client whose request failed, and keep serving everyone else. This is what nearly every server does by default.
3. **Fail-stop.** Reply with an error (if you can), then stop the process. Let a supervisor restart it. On restart, rebuild all in-memory state from durable state.

toys3 never retries. It uses (2) for a small set of errors where nothing visible changed, and (3) for everything else. The `Trip` action is how the controller asks for fail-stop:

```go
// toys3/control/action.go:21-26
// Trip is a safety trip: it asks the runtime to kill this incarnation and
// restart it (fail-stop), after executing the actions that precede it in the
// same slice. Use it when in-memory state can no longer be trusted, e.g. after
// an fsync error. Cause says why. In simulation it is an ordinary crash; in
// production the process exits and a supervisor restarts it.
type Trip struct{ Cause string }
```

And inside the server, every trip goes through one function:

```go
// toys3/control/controller.go:140-146
// trip fail-stops: nothing after it in this incarnation is trusted. The
// runtime executes the actions before it (here: the 500, if any) and then
// kills the incarnation.
func (s *Controller) trip(cause string, before ...Action) []Action {
	s.mode = modeTripped
	return append(before, Trip{Cause: cause})
}
```

`trip` sets `modeTripped` and returns `before...` followed by `Trip`. From then on `Step` returns `nil` for every event (`controller.go:110-112`). The request-level wrapper adds the 500:

```go
// toys3/control/controller.go:264-268
// failTrip replies 500 to r and trips. Used for every error after which
// obj/ may hold a change that is visible but not durable (see package doc).
func (s *Controller) failTrip(r *request, what string, err ErrCode) []Action {
	return s.trip(what+": "+err.String(), Reply{Client: r.client, Status: StatusServerError})
}
```

Note what `failTrip` does *not* do: it does not remove `r` from `reqs`, and it does not call `startPermitted`. There is no point. The incarnation is over; `reqs` will be discarded with the rest of memory.

Fail-stop is an old and respected idea. Candea and Fox's *Crash-Only Software* (HotOS 2003) argues that if recovery from a crash is the only well-tested path, you should make it the *only* path: stop means crash, start means recover. toys3 is crash-only in exactly that sense. There is no "graceful shutdown" in the controller, there is no in-place error recovery, and `Started` always runs the full recovery protocol of Chapter 5. A `Trip` costs a restart and a recovery scan of `tmp/`, which in production is a few milliseconds and in simulation is a few microseconds.

### Why the word "trip"

Until the refactor of Chapter 3a this action was called `Halt{Reason}`. The behaviour is unchanged; the new name, `Trip{Cause}`, comes from safety engineering, and it carries the whole argument of this chapter in one word.

In a process plant, a **safety-instrumented system** watches for conditions under which normal control can no longer be trusted to keep the process safe: a pressure beyond its limit, a sensor that disagrees with its twin, a lost heartbeat. When one occurs, it **trips**: it drives the process to a predefined safe state (close the feed valve, open the vent, drop the control rods; a nuclear reactor's SCRAM is the canonical trip) and holds it there. Three properties define a trip, and toys3's `Trip` has all three:

1. **It is triggered by a loss of trust, not by a failed operation as such.** A trip does not mean "something went wrong"; it means "the controller's model of the process it controls (for toys3, storage) can no longer be relied on". toys3 trips after an fsync error not because the fsync failed (a failed read does not trip), but because after it the controller's belief about what is durable is unknowable (fsyncgate, §7.5), and after a failed rename or dir fsync because a change may be visible without being durable.
2. **It drives to a safe state, and the safe state is "stopped".** No partial service, no "degraded mode". `modeTripped` ignores every event (`toys3/control/controller.go:110-112`).
3. **Recovery from a trip is a deliberate restart, not a retry.** A tripped plant is restarted through its normal startup sequence, which re-establishes known conditions before control resumes. A tripped toys3 incarnation is replaced by a new one, whose recovery re-establishes a known `tmp/` and whose state is rebuilt from the disk, the only thing that can be trusted.

"Halt" said only the second of these. It also collided with the simulator's own word for stopping a run after an invariant failure (`Simulator.halted`, which the refactor deliberately left alone: a simulator stopping is not the controller's safety action). And a trip is distinct from a **crash**: a crash is a power-loss *disturbance* that happens *to* the controller, a trip is an *action* the controller takes. They have the same effect on the disk, and the simulator uses the same code path for both (§7.6), but the vocabulary keeps them apart, and so do the statistics: `crashes=` counts both, `trips=` counts the controller's own.

## 7.2 The policy table

`toys3/control/doc.go:59-84` states the whole policy. Here it is with the code that implements each row:

| Error | Response | Code |
|---|---|---|
| `FSRead` (GET, COPY src): ENOENT | 404 | `controller.go:284-285` |
| `FSRead`: EIO | 500 | `controller.go:286-287` |
| `FSRead`: OK but header/length/CRC bad | 500 + `CorruptionDetected` telemetry, no Trip | `controller.go:289-292` |
| `FSWrite(tmp)`: any error | 500 | `controller.go:304-308` |
| `FSFsync(tmp)`: any error | 500 + Trip ("fsync temp") | `controller.go:316-318` |
| `FSRename(tmp→obj)`: any error | 500 + Trip ("rename") | `controller.go:326-328` |
| `FSUnlink(obj)` (DELETE): ENOENT | 404 | `controller.go:339-340` |
| `FSUnlink(obj)`: EIO | 500 + Trip ("unlink") | `controller.go:341-342` |
| `FSFsyncDir(obj)`: any error | 500 + Trip ("fsync obj dir") | `controller.go:350-352` |
| recovery `FSList(tmp)`: any error | Trip | `recovery.go:37-39` |
| recovery `FSUnlink(tmp/x)`: EIO | Trip (ENOENT is fine) | `recovery.go:58-64` |
| recovery `FSFsyncDir(tmp)`: any error | Trip | `recovery.go:77-79` |

The rule that generates this table is stated in one sentence at `toys3/control/doc.go:67-68`:

> So the rule is: any error at or after the first step that can touch obj/ trips, and errors before it are a plain 500.

Plus two refinements: `fsync` errors trip regardless of where they are (fsyncgate, §7.5), and a corrupt object is a plain 500 because the damage is already durable (§7.6). Let us justify each row.

### Reads: plain 500 (or 404)

A failed read changed nothing. The object file is still whatever it was. The client gets a 500, which in the oracle's model is a "Failed" read and "constrains nothing" (`toys3/sim/oracle.go:285-288`). The server keeps serving. There is no reason to trip: the server's in-memory state does not depend on the read's outcome, and the next GET will simply try again.

### FSWrite of the temp file: plain 500

A failed write may have left anything in the temp file: nothing, an empty file, or a prefix of the data (the StorageModel chooses among these, `toys3/sim/storagemodel.go:299-308`). But the temp file is private. No GET reads `tmp/`, no COPY reads `tmp/`, and `obj/` is untouched. The request is finished with a 500, the garbage temp file stays where it is, and the *next recovery* deletes it (`controller.go:304-307`: "Only a temp file was touched; obj/ is unchanged. The garbage temp file is removed by the next recovery.").

One consequence worth noting: garbage temp files accumulate in `tmp/` until the next restart. In the simulator, restarts are frequent. In production, a long-running server that hits many write errors would grow `tmp/` without bound. Chapter 17 lists this among the limitations.

### FSRename into obj/: 500 + Trip

Here is the first row where `obj/` may have changed. On EIO, the rename "may or may not have happened" (`toys3/control/storage.go:117-118`). The StorageModel models this literally: with probability 1/2 a failed rename is applied anyway (`storagemodel.go:352-367`).

Suppose the server replied 500 and kept serving. Two things can go wrong.

First, if the rename *did* apply, the new object is visible: the next GET on that key (which runs as soon as this request finishes, by per-key FIFO) reads the new value. But the rename is not durable; nobody has fsynced `obj/`. A crash can revert it. The client then sees the value appear and disappear.

Second, if the rename did *not* apply, the temp file is still there with its fsynced content. Nothing visible changed. This half is harmless.

The server cannot tell which case it is in. It could try to find out (stat `obj/k`, compare checksums), but any such probe would itself be an I/O operation that can fail, and would still leave the problem that the visible state is not durable. Fail-stop sidesteps the whole question: after a crash and recovery, the visible state *is* the durable state, whatever the adversary decided.

### FSUnlink in DELETE: ENOENT is 404, EIO is 500 + Trip

ENOENT means the key was absent and nothing changed: 404, exactly as in Chapter 6. EIO means the unlink "may or may not have applied" (the StorageModel again applies it with probability 1/2, `storagemodel.go:412-421`). If it applied, the key is visibly gone but not durably gone. A later GET would see 404, and a crash could bring the object back. Same pattern as the rename; same response.

### FSFsyncDir of obj/: 500 + Trip

This one is subtle because the rename (or unlink) *succeeded*. It is visible. Only the directory fsync failed. So the state is: the change is definitely visible, and definitely not known to be durable. If the server kept serving, every subsequent read of that key would observe a change that a crash can undo. This is precisely the bug from §1.1 of Chapter 1, and precisely mutant **d2**, which §7.8 dissects.

There is a second reason, which applies to every fsync: after an fsync error you cannot trust that a *later* fsync will mean anything (§7.5). So even "retry the directory fsync until it succeeds" is not safe on Linux.

### FSFsync of the temp file: 500 + Trip

This row is the one that does not follow from "touches obj/". At the time the temp file's fsync fails, `obj/` has not changed. The temp file is private. Why trip?

The package doc answers with one word, "fsyncgate" (`toys3/control/doc.go:73`), and the comment in the code says the same (`controller.go:317`). The reasoning is about what the failure *means*: an fsync error is the kernel telling you that some writeback to this file failed, and on Linux it may also be telling you that dirty data has been dropped. The safe, conservative response to "my storage stack lost data I gave it" is to stop trusting anything in memory and restart from what is durable. That is a policy decision about the *environment*, not a consequence of the toys3 protocol.

It is worth being precise about what the simulator can and cannot show here. In the StorageModel, a failed fsync poisons only that one inode (`storagemodel.go:331-336`), and the poisoned temp file would never be renamed if the server simply replied 500 and dropped the request. So in the *model*, a plain 500 on temp-fsync failure is arguably safe, and the Trip here is belt and braces. Exercise 4 asks you to test that hypothesis by building a mutant. What the simulator *does* show, decisively, is that **continuing the protocol** after a temp-fsync failure is a bug: that is mutant **d** in §7.9, and it is caught at seed 1.

### Recovery errors: Trip, retry from scratch on the next boot

```go
// toys3/control/recovery.go:28-30
// onRecoveryDone advances recovery. Any error trips: recovery must finish
// before serving, and restarting retries it from scratch.
```

Recovery has three steps (list `tmp/`, unlink each leftover, fsync `tmp/`; Chapter 5). If any fails, the server trips without replying to anyone: requests queued during recovery have not been started, and they will be abandoned by the crash. The next boot starts recovery again. There is one tolerated error: an unlink that returns ENOENT ("already gone: fine", `recovery.go:61`), which can happen if an earlier boot's unlink had applied visibly but its completion was lost in a crash.

Why not retry inside the incarnation? Because "restart" *is* the retry, and it is the one path that is already exercised constantly. You can see this in the seed-1 trace from the preface, which trips in recovery twice:

```
#251    t=58.892693ms  inc=2 fs-land     land FSUnlink{io2 /data/tmp/0000000000000031} -> FSUnlinkDone{io2 EIO}
#252    t=59.063797ms  inc=2 deliver     FSUnlinkDone{io2 EIO} => Trip{recovery: unlink temp: EIO}
#252    t=59.063797ms  inc=2 crash       CRASH (trip: recovery: unlink temp: EIO): kept=1 lost=1 torn=0, dropped 0 queued entries
#253    t=60.075667ms  inc=3 restart     boot
#254    t=60.075667ms  inc=3 deliver     Started => FSList{io1 /data/tmp}
...
#261    t=60.885632ms  inc=3 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000031 000000000000004c]}
```

Incarnation 2 failed to unlink `tmp/…31` and tripped. Incarnation 3 found `…31` still there (the failed unlink had not applied, or had applied and been reverted by the crash) and, this time, removed it.

### Corrupt object: 500 + CorruptionDetected, no Trip

```go
// toys3/control/controller.go:289-292
		payload, valid := format.DecodeObject(d.Data)
		if !valid {
			return s.finish(r, StatusServerError, nil, Emit{Telemetry: CorruptionDetected{Key: readKey}})
		}
```

A read that returns bytes failing the magic, length or CRC check means either the stored file is damaged or the read itself was corrupted in transit. Neither is fixed by restarting: a damaged stored file is *already durable*, and a transient read corruption will most likely not recur on the next read. The doc says it in one line: "Corrupt object files are a plain 500 plus a CorruptionDetected fact, not a Trip: the file is already durable, restarting would not fix it" (`toys3/control/doc.go:82-84`).

Here is the correct server handling an injected bit flip, from seed 18:

```
#656    t=153.982439ms inc=7 fs-land     land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657    t=154.136154ms inc=7 deliver     FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptionDetected{"k3"}} Reply{c98 500}
```

`raw(29 bytes)` is how the log renders bytes that do not decode as an object (`toys3/sim/log.go:29-44`): a 16-byte header plus a 13-byte payload, with one bit flipped by the StorageModel (`storagemodel.go:399-403`). The server emits the fact and replies 500. The run passes. Mutant **e** in Chapter 15 runs the same seed without the CRC check and serves the garbage with a 200.

## 7.3 The principle: a read must never see a value a crash can erase

Underneath the table is one principle. The package doc states it in the negative:

```
// toys3/control/doc.go:61-68
// The danger is not the 500 itself (a 500 promises nothing) but what later
// reads observe. If a failed mutation may have changed obj/ visibly but not
// durably, a later GET can observe the change and a crash can then undo it:
// the client saw a value appear and vanish, which no linearization explains
// ("crashes erase nothing"). The per-key lock only protects reads while the
// mutation is in flight; once it has replied, the next GET on that key runs.
```

Two points deserve emphasis.

**"A 500 promises nothing" is true of the reply, not of the system.** The client that received the 500 learns nothing about whether its mutation happened. The oracle encodes this: a Failed mutation "takes effect at most once, at some instant in [Invoke, Return], or never" (`toys3/sim/oracle.go:29-31`). But *other* clients are entitled to a consistent view. If a GET observes the mutation, the oracle concludes "it took effect", and from then on it must stay in effect until something else changes the key.

**Crashes erase nothing.** This is the oracle's model of durability (`toys3/sim/oracle.go:32-35`):

```go
//   - Crashes erase nothing. So an acked mutation that disappears after a
//     restart, or an unacked one that a read observed and that later
//     vanishes, has no valid linearization and fails the check. (The latter
//     is why the server should Trip after an fsync error.)
```

The oracle does not model crashes as events that change the key-value state at all. A crash only does two things in the oracle: it turns every pending request into an Abandoned one with a return time equal to the crash time (`oracle.go:107-118`), and it ends a window. So from the oracle's point of view, a correct server must make its *visible* state survive crashes exactly, except for the effects of requests whose outcome was never determined (Failed or Abandoned) *and that nobody observed*.

That is a strong requirement, and it is the right one: it is what "durable" means to a client. Trip is how the server meets it in the error cases. After a Trip, the crash resolves every un-synced change one way or the other, and *then* the next incarnation serves reads. No read ever observes the in-between.

Why the per-key lock is not enough: while the failed mutation is in flight, per-key FIFO stops any GET on that key from reading. But "in flight" ends at the 500. The very next GET in the queue starts in the same step (`finish` calls `startPermitted`, Chapter 6). If the server did not trip, that GET would read the un-durable state immediately.

## 7.4 A worked example, by hand

Before the real trace, here is the smallest history that shows the problem, which is also an oracle unit test:

```go
// toys3/sim/oracle_test.go:142-156
func TestOracleRejectsObservedThenVanished(t *testing.T) {
	// A 500 PUT that a read observed must not disappear later: crashes
	// erase nothing (this is why the server trips after an fsync error).
	h := newHist()
	h.inv(1, put("a", 1))
	h.ret(1, control.StatusOK, nil)
	h.inv(2, put("a", 2))
	h.ret(2, control.StatusServerError, nil)
	h.inv(3, control.Get{Key: "a"})
	h.ret(3, control.StatusOK, val(2))
	h.crash()
	h.inv(4, control.Get{Key: "a"})
	h.ret(4, control.StatusOK, val(1))
	h.check(t, false)
}
```

```
time ──►
c1  PUT a=1  ├──200──┤
c2  PUT a=2                ├──500──┤                 (may or may not apply, within its interval)
c3  GET a                              ├──200 v2──┤  (so c2 applied, before c3's read)
                                                      ⚡ crash
c4  GET a                                                  ├──200 v1──┤   ✗ v2 vanished
```

The oracle tries every linearization. c1 must be linearized (Definite). c2 is optional. c3 read `v2`, so c2 must be linearized before c3. Now the state is `a=2`. The crash does nothing to the model. c4 reads `v1`: there is no op after c3 that could write `v1` (c1 is already linearized and ops take effect at most once). No linearization exists. The history is rejected.

The partner test, `TestOracleFailedMutationMayOrMayNotApply` (`oracle_test.go:109-140`), shows the cases that *are* fine: if nobody read `v2` before the crash, then after the crash reading either `v1` or `v2` is acceptable, because c2's 500 left its outcome open.

## 7.5 fsyncgate

In March 2018, Craig Ringer reported on the PostgreSQL hackers mailing list that PostgreSQL's handling of `fsync` failures on Linux could silently lose data. The thread became known as "fsyncgate". The issue, in simplified form:

1. An application writes data with `write()`. The data sits in the page cache as dirty pages.
2. The kernel tries to write the dirty pages back to the device, in the background or because the application called `fsync()`. The device reports an error.
3. On Linux (with the common filesystems at the time), the kernel **marks those pages clean anyway**, records the error, and reports it to the next `fsync()` caller. The data that failed to reach the disk may then be evicted from the page cache. It is gone.
4. PostgreSQL's checkpointer treated an `fsync` failure as transient: it logged the error and retried the checkpoint later. The retry's `fsync()` returned **success**, because there were no longer any dirty pages to write and the error had already been reported once.
5. PostgreSQL concluded the checkpoint was durable and discarded the WAL that would have been needed to redo the lost writes.

The lesson that the PostgreSQL project (and others) drew is that **an fsync failure cannot be retried in place**. After a failed `fsync`, the in-memory view of the file (the page cache, and any application state that assumes it) no longer corresponds to anything on disk, and no later `fsync` can tell you otherwise. PostgreSQL's response was to treat `fsync` failure as a PANIC by default (crash, then recover from WAL), exposed as the `data_sync_retry` setting, which defaults to off. That is fail-stop.

Rebello et al.'s *"Can Applications Recover from fsync Failures?"* (USENIX ATC 2020) studied the question systematically across file systems (ext4, XFS, Btrfs) and applications, and found that the behaviour after an `fsync` failure varies by file system and mode, that many widely used applications handled it incorrectly, and that the only generally safe application strategy is not to trust the page cache after the failure.

### How the StorageModel models fsyncgate

```go
// toys3/sim/storagemodel.go:45-48
	// FsyncGate models Linux after a failed fsync: the dirty data is marked
	// clean, later fsyncs of that file return OK, and the data still never
	// reaches disk. Only a fresh FSWrite clears it.
	FsyncGate bool
```

`BaseConfig` turns it on (`toys3/sim/config.go:169`). The mechanism is a `poisoned` flag on each inode (`storagemodel.go:66-71`):

```go
// toys3/sim/storagemodel.go:326-344
func (fs *StorageModel) fsync(path string, fail bool) control.ErrCode {
	in, ok := fs.lookup(path)
	if !ok {
		return control.ErrNotFound
	}
	if fail {
		if fs.faults.FsyncGate && in.dirty {
			in.poisoned = true // the kernel drops the dirty pages' error state
		}
		return control.ErrIO
	}
	if in.poisoned {
		return control.OK // fsyncgate: the lie
	}
	in.durable = in.volatile
	in.dirty = false
	fs.durGen++
	return control.OK
}
```

A failed fsync of a dirty inode poisons it. Every later fsync of a poisoned inode returns OK **without making anything durable**. Reads still see the volatile content (the page cache is still there, for now). At a crash, a poisoned inode's volatile content is discarded: `case in.poisoned: rep.Lost++` (`storagemodel.go:503-504`) leaves `durable` alone and then sets `volatile = durable`. Background writeback never picks a poisoned inode either (`storagemodel.go:222`). Only a fresh `FSWrite` clears the poison (`storagemodel.go:319`). `TestStorageModelFsyncGateLies` (`toys3/sim/storagemodel_test.go:157-171`) pins the behaviour: fail an fsync, retry it (OK), crash, and the data is gone.

This is the StorageModel being deliberately adversarial (Chapter 11). Real file systems differ in details; the model takes the worst plausible behaviour so that a server correct against the model is correct against the kernels that behave this way.

## 7.6 How Trip works in the two runtimes

### In the simulator: an ordinary crash, immediately

```go
// toys3/sim/simulator.go:200-207
		case control.Trip:
			if i != len(acts)-1 {
				s.fail(Failure{Invariant: "protocol", Msg: "Trip is not the last action of its step"})
				return
			}
			s.res.Stats.Trips++
			s.crash("trip: " + x.Cause)
			return
```

When the simulator executes a step's actions in order (Chapter 10) and reaches a `Trip`, it first checks the contract that `Trip` is the **last** action of its step. The controller guarantees this by construction (`trip` appends it last, and `control/doc.go:81-82` states it: "Trip is always the last action of its Step; the 500 Reply (if any) precedes it"), and a server that violated it would fail the "protocol" invariant. The reason for the contract: anything after a `Trip` would be executed by a runtime that has already decided the incarnation is dead. The mutant wrappers in `mutants_test.go` must respect it too; `truncAfterTrip` (`mutants_test.go:35-43`) exists so that a wrapper which appends deferred actions after its inner server's output never places anything after a `Trip`.

Then `crash` runs, which is exactly the same function as an injected power loss (`simulator.go:276-307`): the StorageModel resolves all volatile state adversarially, every queued entry for the dead incarnation is dropped (FS actions that have not landed never happen; completions that have not been delivered are lost), every outstanding client request is marked Abandoned in the oracle, and a restart is scheduled after a random delay. Because the 500 `Reply` precedes the `Trip` in the action list, it has already been delivered to the oracle and the workload by the time the crash happens. The client whose request caused the trip sees a definite 500; everyone else in flight sees "abandoned".

"A Trip is a power loss" is a modelling choice worth pausing on (`toys3/DESIGN.md:179-183`: "A crash, including `Trip`, is a power loss"). A real process exit is gentler than a power loss: the page cache survives a process exit. Modelling Trip as a power loss is strictly more adversarial, which is the safe direction. It also means a server that trips after an fsync error is tested against the worst case: everything un-synced may vanish, *and* the next incarnation starts immediately.

### In production: the process exits

```go
// toys3/prod/loop.go:91-93
		case control.Trip:
			l.logf("toys3: TRIP: %s", a.Cause)
			return fmt.Errorf("%w: %s", ErrTripped, a.Cause)
```

`Loop.dispatch` executes actions in order. The 500 `Reply` before the `Trip` is delivered to its waiting HTTP handler; then `dispatch` returns `ErrTripped`, `Run` returns, and `stopped` is closed (`loop.go:61`). Any HTTP request still waiting gets `ErrStopped` from `Submit` and a 503 from the handler (`http.go:63-67`). `Submit` has a careful special case so that a 500 dispatched just before the Trip is not lost in the race with `stopped` (`loop.go:148-154`: "a Reply dispatched just before a Trip still counts").

The binary turns `ErrTripped` into a process exit:

```go
// toys3/cmd/toys3/main.go:48-56
	go func() {
		err := loop.Run(ctx)
		if errors.Is(err, prod.ErrTripped) {
			// Fail-stop: in-memory state is untrusted. A supervisor restarts us
			// and recovery runs on boot.
			log.Fatalf("toys3: exiting: %v", err)
		}
		srv.Close()
	}()
```

`log.Fatalf` exits with status 1. A supervisor (systemd, Kubernetes, a shell loop) restarts the process, and `Started` runs recovery. `TestTripStopsLoop` (`toys3/prod/prod_test.go:121-130`) checks the in-process half of this with an executor that fails every file fsync: the PUT gets 500, `Run` returns `ErrTripped`, and a subsequent GET gets 503.

Note that production's trip does *not* lose the page cache, and does not undo "visible but not durable" changes. If the machine stays up, a rename that applied before the trip is still visible after the restart. That is fine: the crash-only argument does not need the trip to *erase* anything. It needs the next incarnation to start from a state that the oracle considers possible, and "the failed mutation took effect" is one of the possible outcomes of a 500. What matters is that no read observed the in-between state *and then saw it reverted*; after a real restart without power loss, nothing gets reverted. Chapter 9 discusses this asymmetry between simulation and production.

## 7.7 TestErrorPolicy: the table as a test

The unit test that pins the policy runs each failing operation through the hand-driven harness (Chapter 6, §6.11) with a `fail` function that makes every action of one type return EIO:

```go
// toys3/control/controller_test.go:361-369
// failKind makes every action of type T fail with ErrIO.
func failKind[T Action]() func(Action) ErrCode {
	return func(a Action) ErrCode {
		if _, ok := a.(T); ok {
			return ErrIO
		}
		return OK
	}
}
```

```go
// toys3/control/controller_test.go:371-408 (cases)
		{"write tmp: plain 500", failKind[FSWrite](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; reply c1 500"},
		{"fsync tmp: trip", failKind[FSFsync](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; reply c1 500; trip"},
		{"rename: trip", failKind[FSRename](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/k; reply c1 500; trip"},
		{"fsyncdir: trip", failKind[FSFsyncDir](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; reply c1 500; trip"},
		{"read: plain 500", failKind[FSRead](), Get{Key: "k"},
			"read /d/obj/k; reply c1 500"},
		{"unlink: trip", failKind[FSUnlink](), Delete{Key: "k"},
			"unlink /d/obj/k; reply c1 500; trip"},
```

Each case pre-populates `obj/k` with `"old"`, runs one operation, and checks the exact action string. Then:

```go
// toys3/control/controller_test.go:398-405
			if strings.HasSuffix(tc.want, "trip") {
				// Tripped: everything afterwards is ignored.
				h.fail = nil
				expect(t, "after trip", h.step(Request{Client: 2, Op: Get{Key: "k"}}), "")
			} else {
				h.fail = nil
				expect(t, "still serving", h.do(2, Get{Key: "k"}), "read /d/obj/k; reply c2 200 old")
			}
```

For tripping cases, a further request produces nothing (the server is in `modeTripped`). For non-tripping cases, the server keeps serving and the old value is intact.

Two observations about what this test can and cannot show:

- It shows the *policy* is implemented as written. It cannot show the policy is *right*. Note the `fsyncdir: trip` case: in the harness, the failed fsyncdir happens after a rename that already changed the harness's filesystem (`veritas.FS`, a plain map underneath), so `obj/k` holds the new value. If the policy were "plain 500" here, the follow-up GET in the test would return `v`, and the test would be just as happy to assert that. Nothing in the harness has a notion of "this is visible but a crash would undo it": the harness's filesystem has no crash semantics (`controller_test.go:21-23`). This is Exercise 1 of Chapter 1 made concrete.
- It shows each error in isolation. It cannot show interactions: an EIO on one request followed by a crash caused by an EIO on another. That is the simulator's job.

## 7.8 Mutant d2: the vanishing read, dissected

Now the main event. Mutant **d2** is the bug from Chapter 1, §1.1, implemented as a wrapper around the real server:

```go
// toys3/sim/mutants_test.go:125-129
// (d2) Like (d), but only for a failed FSFsyncDir(obj/): the rename is
// visible, not durable, and the server keeps serving it.
func mutNoTripOnDirFsyncError(env control.Env, cfg control.Config) control.Machine {
	return &noTrip{inner: control.New(env, cfg), dirOnly: true}
}
```

### How the noTrip wrapper works

(A naming note: the mutant's Go identifiers say "trip" (`mutNoTripOnDirFsyncError`, `noTrip`), but its *label*, `d2-no-halt-on-dir-fsync-error`, still says "halt". The labels are test names that appear in the golden file of Chapter 8 and in every quoted output in this book, so the refactor kept them verbatim.)

A mutant must be "exactly one protocol step" wrong (`mutants_test.go:13-15`). The cleanest way to turn "500 + Trip" into "500, keep serving" without editing `controller.go` is to **hide the error from the inner server and fix up the reply on the way out**:

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

`Step` (`mutants_test.go:131-195`) does the following:

1. If `dirOnly` and the event is not an `FSFsyncDirDone`, pass it through untouched (`stepDoomedOnly`, which only watches for `Recovered`). So d2 changes nothing except directory-fsync errors.
2. For an `FSFsyncDone`, `FSFsyncDirDone`, `FSRenameDone` or `FSUnlinkDone` carrying `ErrIO`, rewrite the event to the same completion with `OK` and note `isErr`.
3. If the inner server has not yet emitted `Recovered`, restore the error (`restoreErr`): recovery errors still trip. The mutant is about the serving path only.
4. Step the inner server with the (possibly rewritten) event.
5. If the event was an error (or belongs to a request already doomed), the request is now *doomed*: the inner server believes all is well and continues the protocol. If the inner server's output contains a `Committed` telemetry, record the client as doomed; `rewrite` will drop that telemetry and turn the matching `Reply` from 200 into 500. Otherwise, if the inner server issued exactly one action with an IOID (the next protocol step), remember that IOID in `doomed` so the next completion is recognized as part of a doomed request.

For d2, a failed `FSFsyncDir(obj)` is always the last protocol step, so the inner server immediately commits, the wrapper drops the `Committed` telemetry, and replies 500. The net effect: exactly the correct server's behaviour, except that the `Trip` is missing. Because the `Committed` telemetry is dropped, the white-box `AckedIsDurable` invariant (Chapter 14) has nothing to check, and the bug must be caught by what clients see.

### The run

`TestCatchesBugs` reports:

```
=== RUN   TestCatchesBugs/d2-no-halt-on-dir-fsync-error
    mutants_test.go:356: caught at seed 132 by linearizable (step 380): no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
          req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
    mutants_test.go:361: black-box invariants only: caught at seed 132 by linearizable
```

Seed 132 has 2 keys and 4 clients, crash probability 0.001, EIO probability 0.03. Seeds 1 to 131 all pass with this mutant: you need a directory fsync to fail, then a read of the affected key *before* the next crash, then a crash that reverts the change, then a read after the restart. Here is the relevant part of the log, from the step where the background story begins.

**The setup: key k1 holds c26's value, durably.** Earlier in the run, `c26 PUT "k1" value-of-26` committed (step 207). At step 303, `c42 COPY "k1" -> "k1"` rewrote it with the same value. At step 316, `c44 COPY "k1" -> "k0"` commits, and `startPermitted` starts the two requests that were queued behind it:

```
#316    t=73.245743ms  inc=2 deliver     FSFsyncDirDone{io67 OK} => Emit{Committed{c44 "k0" crc=4fb8a072}} Reply{c44 200} FSUnlink{io68 /data/obj/k1} FSRead{io69 /data/obj/k0}
```

`FSUnlink{io68 /data/obj/k1}` is `c45 DELETE "k1"` (queued behind the COPY because the COPY held `k1` as its source). `FSRead{io69 /data/obj/k0}` is `c46 GET "k0"`.

**The fault: the directory fsync of c45's DELETE fails.**

```
#317    t=73.944060ms  inc=2 fs-land     land FSUnlink{io68 /data/obj/k1} -> FSUnlinkDone{io68 OK}
#319    t=74.190407ms  inc=2 deliver     FSUnlinkDone{io68 OK} => FSFsyncDir{io70 /data/obj}
#320    t=74.516910ms  inc=2 fs-land     land FSRead{io69 /data/obj/k0} -> FSReadDone{io69 OK obj(value-of-26)}
#321    t=74.578078ms  inc=2 deliver     FSReadDone{io69 OK obj(value-of-26)} => Reply{c46 200 value-of-26}
#323    t=75.173243ms  inc=2 fs-land     land FSFsyncDir{io70 /data/obj} -> FSFsyncDirDone{io70 EIO}
#324    t=75.326928ms  inc=2 deliver     FSFsyncDirDone{io70 EIO} => Reply{c45 500} FSUnlink{io71 /data/obj/k1}
```

At step 317 the unlink lands: in the StorageModel, the directory entry `obj/k1` now has `cur = 0` (gone) but `durable` still points at the inode holding c26's value (Chapter 11). At step 323 the directory fsync fails, so nothing becomes durable (`storagemodel.go:375-377`).

At step 324, compare what the correct server would do with what the mutant did. The correct server would return `Reply{c45 500} Trip{fsync obj dir: EIO}` and crash. The mutant fed the inner server `FSFsyncDirDone{io70 OK}`; the inner server committed c45 (`Emit{Committed{c45 delete "k1"}}`, `Reply{c45 200}`) and started the next request on `k1`, `c47 DELETE "k1"` (`FSUnlink{io71}`). The wrapper dropped the `Committed` and turned the 200 into a 500. The log shows the result: `Reply{c45 500} FSUnlink{io71 /data/obj/k1}`.

**The observation: two clients see k1 as absent.**

```
#326    t=75.912893ms  inc=2 fs-land     land FSUnlink{io71 /data/obj/k1} -> FSUnlinkDone{io71 ENOENT}
#327    t=76.250319ms  inc=2 deliver     FSUnlinkDone{io71 ENOENT} => Reply{c47 404}
...
#332    t=76.747670ms  inc=2 deliver     Request{c49 GET "k1"} => FSRead{io73 /data/obj/k1}
...
#338    t=77.373037ms  inc=2 fs-land     land FSRead{io73 /data/obj/k1} -> FSReadDone{io73 ENOENT nil}
#339    t=77.733508ms  inc=2 deliver     FSReadDone{io73 ENOENT nil} => Reply{c49 404} FSRead{io75 /data/obj/k1}
```

`c47 DELETE "k1"` gets 404: the key is (visibly) absent. `c49 GET "k1"` gets 404 too. In the same step, `c51 GET "k1"` starts (`FSRead{io75}`). These are Definite outcomes. As far as the oracle is concerned, `k1` is now absent, which forces the conclusion that c45's failed DELETE *did* take effect, at some instant before c47's unlink.

**The crash: an unrelated fsync error, a correct Trip.**

```
#329    t=76.251998ms  inc=2 deliver     Request{c48 PUT "k0" value-of-48} => FSWrite{io72 /data/tmp/0000000000000048 obj(value-of-48)}
...
#340    t=78.043936ms  inc=2 fs-land     land FSFsync{io74 /data/tmp/0000000000000048} -> FSFsyncDone{io74 EIO}
#342    t=78.134914ms  inc=2 deliver     Request{c52 PUT "k1" value-of-52}
#343    t=78.234970ms  inc=2 deliver     FSFsyncDone{io74 EIO} => Reply{c48 500} Trip{fsync temp: EIO}
#343    t=78.234970ms  inc=2 crash       CRASH (trip: fsync temp: EIO): kept=1 lost=2 torn=0, dropped 1 queued entries
```

`c48 PUT "k0"` hits an fsync error on its temp file. d2 only swallows *directory* fsync errors, so this one reaches the inner server unchanged, and the inner server trips correctly. The simulator crashes the incarnation. The StorageModel resolves every un-synced change independently (`storagemodel.go:469-530`): it reports one change kept and two lost. One of the lost changes is the un-durable unlink of `obj/k1`, which reverts to its durable version: the entry points at c26's inode again. (The other two changes concern c48's temp file: its directory entry was kept, which is why the next recovery finds `tmp/…48`, and its content was poisoned by the failed fsync, which counts as lost.)

At the moment of the crash, three of the four clients are mid-request:

- `c50 PUT "k0"` (queued behind c48),
- `c51 GET "k1"` (its read `io75` was in flight; its completion is dropped),
- `c52 PUT "k1"` (arrived at step 342, queued behind c51, never started).

All three become Abandoned in the oracle (`oracle.go:107-118`). Abandoned reads constrain nothing. Abandoned mutations "take effect at most once, at some instant in [Invoke, crash], or never". The oracle does not know that c52 never started; it only knows c52 was invoked and never answered.

**The first window check: two possible states for k1.** A crash leaves no op pending, which is a quiescent point, so the `Linearizable` invariant checks the window at once (Chapter 13). On key `k1`, the window contains (among others): c45 DELETE → 500 (optional), c47 DELETE → 404 (required: `k1` absent at its linearization point), c49 GET → 404 (required: absent), c51 GET (abandoned, ignored), c52 PUT (abandoned, optional). The only linearizations consistent with c47 and c49 apply c45. Then c52 either applied (after c49) or did not. The possible end states for `k1` are therefore:

```
{}        k1 absent   (c45 applied, c52 never)
{k1=52}   k1 = value-of-52   (c45 applied, c52 applied at some instant before the crash)
```

That is the "2 possible start states {} {k1=52}" in the failure message. The window passes: the history up to the crash is linearizable. The bug is not yet visible.

**The restart and the impossible read.**

```
#345    t=81.760773ms  inc=3 restart     boot
#346    t=81.760773ms  inc=3 deliver     Started => FSList{io1 /data/tmp}
...
#356    t=82.642556ms  inc=3 deliver     Request{c55 GET "k1"}
...
#360    t=83.890513ms  inc=3 deliver     FSFsyncDirDone{io3 OK} => Emit{Recovered{temps=1}} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-53)} FSRead{io5 /data/obj/k1}
#361    t=84.250319ms  inc=0 quiesce     quiesce
#363    t=84.978169ms  inc=3 fs-land     land FSRead{io5 /data/obj/k1} -> FSReadDone{io5 OK obj(value-of-26)}
#365    t=84.991187ms  inc=3 deliver     FSReadDone{io5 OK obj(value-of-26)} => Reply{c55 200 value-of-26}
```

Incarnation 3 recovers (removing c48's leftover temp file), then serves `c55 GET "k1"`, which reads `obj/k1` and finds c26's value, resurrected by the crash. It replies `200 value-of-26`. The server is behaving perfectly well *in incarnation 3*; the damage was done in incarnation 2.

**The report, at the next quiescent point.** c55 replied at step 365, but other requests (c53, c54, c56 on `k0`) were still outstanding, and the oracle only checks complete windows. The quiesce marker at step 361 stopped new requests; at step 380 the last outstanding one, `c56 DELETE "k0"`, replied, the workload became idle, and the check ran:

```
#380    t=91.269593ms  inc=3 deliver     FSFsyncDirDone{io11 OK} => Emit{Committed{c56 delete "k0"}} Reply{c56 200}
#380    t=91.269593ms  inc=3 crash       FAILURE linearizable: no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
  req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
```

(The failure line is rendered with `Kind: KindCrash`, so it reads "crash"; it is not a crash. `fail` uses that kind for log entries, `simulator.go:371-373`.)

The `k1` component of the new window contains one op: `req 55 ... GET "k1" -> 200 value-of-26`, with logical invoke/return sequence numbers 102 and 104 and virtual times 82.64 ms and 84.99 ms. From either possible start state, `{}` or `{k1=52}`, a GET returning `value-of-26` is impossible: nothing in the window writes value-of-26, and neither start state contains it. No linearization. The run fails.

In one picture:

```
            inc 2                                                        inc 3
c26  PUT k1=26 ──200 (long before)
c45  DELETE k1    ├─unlink lands (visible, not durable)─ fsyncdir EIO ─500┤   (mutant: no Trip)
c47  DELETE k1                                                 ├─404┤          k1 absent
c49  GET k1                                                          ├─404┤    k1 absent
c52  PUT k1                                                              ├──── abandoned
                                                                    ⚡ crash (trip from c48's fsync)
                                                                       obj/k1 unlink reverted
c55  GET k1                                                                       ├─200 v26┤ ✗
oracle: after the crash k1 ∈ { absent, v52 }.  v26 is impossible.
```

### Why it took 132 seeds

Each ingredient has to line up: an EIO specifically on an `FSFsyncDir(obj)` (one of several FS operations per request, each failing with probability between 0.002 and 0.03 depending on the seed's swarm draw); a *later* definite read or 404 of the affected key in the same incarnation (so the oracle is forced to conclude the change happened); a crash before any successful directory fsync makes the change durable (any later successful `FSFsyncDir(obj)` for *any* key would make *all* current `obj/` entries durable, `storagemodel.go:378-382`, healing the bug); the crash resolving that particular entry as "reverted" (probability 1/2); and a read of the key after the restart. In seed 132 the crash came from an unrelated Trip only about 3 ms of virtual time after the fault, before another directory fsync could heal it. The mutant is real, it is black-box visible, and it is *rare*. That is exactly the profile of a bug that ships.

Note also which invariants did *not* fire. `AckedIsDurable` is silent because the wrapper dropped c45's `Committed` telemetry (there was no ack). `ObjNeverTorn` is silent because every durable file under `obj/` is a valid object. `NoCorruptReads` is silent because every read returned bytes that some PUT really wrote. Only the oracle, reasoning about the *history*, sees the problem. That is why the d2 row in `DESIGN.md`'s table says "linearizable (a read saw the value, then a crash erased it)" for both columns.

## 7.9 Mutant d: continuing after any error

Mutant **d** is d2 without the `dirOnly` restriction: it hides `FSFsync`, `FSRename`, `FSUnlink` and `FSFsyncDir` errors (`mutants_test.go:121-123`). It is caught much faster, at seed 1, and by a different invariant:

```
=== RUN   TestCatchesBugs/d-no-halt-on-error
    mutants_test.go:356: caught at seed 1 by obj-never-torn (step 241): durable /data/obj/k2 is torn or corrupt (0 bytes)
    mutants_test.go:361: black-box invariants only: caught at seed 20 by liveness
```

Here is the end of seed 1 with mutant d. Compare it with the correct server's seed-1 trace at the same steps (shown in §7.2's recovery example and in Chapter 6): until step 232 the two runs are identical, because the same seed draws the same random numbers as long as the server behaves the same.

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

Step by step:

- **#232.** The fsync of `tmp/…4c` (a PUT to `k2` by client c30; IO 0x4c = 76 is its write) fails. With `FsyncGate` on and the inode dirty, the StorageModel **poisons** the inode. The temp file's durable content is still nil: it was never successfully synced.
- **#233.** The correct server would reply `500` and `Trip{fsync temp: EIO}` here; in the correct seed-1 run, this is exactly what happens at step 233 (`Reply{c30 500} Trip{fsync temp: EIO}`). The mutant hides the error, so the inner server proceeds to the next step: `FSRename{io80 tmp/…4c -> obj/k2}`.
- **#236–237.** Meanwhile, a different PUT (c28, to `k0`) has its *directory* fsync fail. The mutant hides that too; the inner server commits c28, and the wrapper drops the telemetry and rewrites the reply to 500.
- **#238.** c30's rename lands. `obj/k2` now names the poisoned inode.
- **#239–241.** The inner server issues `FSFsyncDir(obj)`, and this time it succeeds. Every current entry in `obj/` becomes durable, including `obj/k2 → poisoned inode`. The durable content of that inode is nil.

At step 241, without any crash, the durable view of the filesystem contains `obj/k2` pointing at an empty file. `ObjNeverTorn` (Chapter 14) checks the durable view whenever it may have changed (`toys3/sim/invariant.go:168-186`), decodes every file under `obj/`, and fails on the empty one. The failure is reported at the `fs-land` step, before the server even sees the completion and before it would have emitted c30's (hidden-error) `Committed`.

This is fsyncgate end to end. The mutant behaves exactly like the PostgreSQL checkpointer of 2018: treat the fsync error as something you can carry on from, and the data you thought you had made durable is not there.

**The black-box view.** With only client-visible invariants (`BlackBoxInvariants`, `invariant.go:70-72`), mutant d survives until seed 20, where `Liveness` fails:

```
FAIL liveness step 2403: 500 with no fault injected (Settling): req 331 client 2 GET "k31" [648..651 @329389038..330313878] -> 500
```

The mechanism is the same corruption, seen from the outside. Some object under `obj/` ended up durably empty or torn. After a crash, every GET of that key reads the damaged file, fails the CRC check, and replies 500 with a `CorruptionDetected` telemetry (the stats line for this run shows `corruptdet=3`). The linearizability oracle cannot object: a 500 GET is a Failed read, and Failed reads constrain nothing. Only the liveness rule "no request issued in Settling gets a 500" (`invariant.go:98-104`) turns "permanently unreadable object" into a failure: once faults stop, a correct server has no reason to fail. This is the general lesson of Chapter 14: **a checksum turns silent data loss into a visible error, and you still need an invariant that treats that visible error as a bug.**

## 7.10 What the policy costs

Fail-stop is not free. Every Trip is a restart, every restart is a recovery, and every request in flight at the time is abandoned. In the 200-seed run in the preface, the server tripped 673 times. In production, a disk that returns a burst of errors would make toys3 restart repeatedly. That is the intended behaviour: a disk returning EIO on fsync is a disk that is losing data, and a crash loop is a loud, honest signal. The alternative, a server that keeps serving and quietly returns values that later vanish, is worse in every way that matters for storage.

What toys3 does not do is distinguish *transient* from *persistent* failures, rate-limit restarts, or fence a node that keeps failing. In a replicated system, a fail-stopped node is replaced by its peers; a single-node toy has no peers. Chapter 17 lists this.

## Key takeaways

- toys3 never retries. Errors before anything visible can change are a plain 500; errors at or after the first step that can change `obj/`, and every `fsync` error, are 500 + `Trip`. Recovery errors trip without a reply. A corrupt object is a 500 plus a `CorruptionDetected` telemetry, without a trip.
- The principle is "a read must never see a value a crash can erase". The oracle encodes it as "crashes erase nothing": a failed mutation may or may not take effect, but once observed, it must stay.
- Per-key FIFO protects reads only while the mutation is in flight. The moment it replies, the next request on the key runs. Trip is what prevents that request from observing visible-but-not-durable state.
- After an `fsync` error, the page cache cannot be trusted and a later `fsync` may lie (fsyncgate). The StorageModel models this with poisoned inodes.
- `Trip` must be the last action of its step. In simulation it is a power-loss crash; in production the process exits and a supervisor restarts it.
- Mutant d2 (no Trip on directory-fsync error) needs a precise conjunction of fault, read, crash and resolution, and is caught at seed 132 by the linearizability oracle alone. Mutant d (no Trip on any error) is caught at seed 1 by `ObjNeverTorn`: continuing after a poisoned fsync puts an empty file durably into `obj/`.

## Exercises

1. **Read the d2 failure yourself.** Run `go test ./toys3/sim -run 'TestCatchesBugs/d2' -v`. Then, in a scratch module that copies `mutNoTripOnDirFsyncError` and its helpers out of `toys3/sim/mutants_test.go` (they only use exported `control` and `sim` APIs), run seed 132 with `cfg.Log = true` and print `sim.FormatLog(r.Log)`. Find steps 317, 323, 339, 343 and 365 and confirm the story of §7.8.

2. **Heal the bug by accident.** In d2's run, the bug is only visible because no successful `FSFsyncDir(obj)` happened between step 323 and the crash at 343. Predict what happens to the first failing seed if you make the StorageModel's background writeback much more aggressive (`cfg.Disturbances.WritebackProb = 0.5` in the scratch program). Will d2 be caught earlier, later, or never? Run `firstCatch`-style search over seeds 1–500 and check.

3. **Plain 500 on rename error.** Write a mutant that replies 500 without tripping *only* on `FSRenameDone` errors (copy `noTrip` and add a `renameOnly` flag analogous to `dirOnly`). Predict: which invariant catches it, and is it black-box visible? Remember the StorageModel applies a failed rename with probability 1/2.

4. **Is the temp-fsync Trip necessary?** §7.2 argued that tripping on a temp-file fsync error is conservative policy rather than something the protocol strictly needs, because the poisoned temp file never reaches `obj/` if the request simply ends with a 500. Test the hypothesis. You cannot do it with an event-rewriting wrapper (the inner server trips as soon as it sees the error). Instead, copy `controller.go` into a scratch package, change `stepFsyncTmp`'s error branch to `return s.finish(r, StatusServerError, nil)`, and run several thousand seeds with `DefaultInvariants()`. If nothing fails, write a paragraph on whether you would keep the Trip anyway, and why.

5. **fsyncgate off.** Set `cfg.Disturbances.FsyncGate = false` and re-run mutant d for seeds 1–500. Does it still get caught at seed 1? By which invariant? Explain the difference in terms of `storagemodel.go:326-344`.

6. **The protocol check.** Write a mutant whose `Step` appends a harmless `Emit{Telemetry: control.Recovered{}}` *after* any `Trip` its inner server returns. Run one seed in which a Trip occurs (seed 1 has nine). Which invariant name appears in the failure, and at which step? Why is it important that this is checked by the simulator rather than trusted?

7. **Production asymmetry.** In production, a Trip does not lose the page cache. Construct a sequence of HTTP requests and injected executor failures (in the style of `failFsync` in `toys3/prod/prod_test.go:110-119`) in which the in-process `Loop` trips after a rename has applied, and show that after `startServer` runs again on the same directory, the new value is visible. Argue that this is consistent with the oracle's model.

## Further reading

- Craig Ringer et al., the "Linux fsync() error reporting" / "fsyncgate" thread on the pgsql-hackers mailing list (March–April 2018).
- Jonathan Corbet, *"PostgreSQL's fsync() surprise"*, LWN.net, April 2018. A clear summary of the fsyncgate problem and the kernel's side of it.
- Anthony Rebello, Yuvraj Patel, Ramnatthan Alagappan, Andrea C. Arpaci-Dusseau and Remzi H. Arpaci-Dusseau, *"Can Applications Recover from fsync Failures?"*, USENIX ATC 2020.
- George Candea and Armando Fox, *"Crash-Only Software"*, HotOS 2003. The argument for making crash-and-recover the only shutdown and error-recovery path.
- Ding Yuan et al., *"Simple Testing Can Prevent Most Critical Failures"*, OSDI 2014. Most catastrophic failures in the systems studied came from mishandled, explicitly signalled errors.
