# Chapter 17: Limitations and Where to Go Next

> *"All models are wrong, but some are useful."* (George Box)
>
> *"…and a simulator is a model you have agreed to trust. Know where you agreed."*

Sixteen chapters have argued that toys3's simulator is good at what it does. This chapter is the counterweight. It has three parts:

1. **What the model does not cover**: the faults, operations and environments that no seed can ever produce, and what each omission means for the confidence you can draw from a passing campaign.
2. **Discrepancies and surprises**: places where the documentation and the code disagree, where a comment is slightly off, or where running the code revealed something nobody had written down. These were found while writing this book. None is a correctness bug in the server; several are worth fixing anyway.
3. **A roadmap**: concrete extensions, each with a design sketch and an exercise, ordered roughly from "an afternoon" to "a new project". The last one connects toys3 back to the system it is a rehearsal for.

---

## Part I: What the model does not cover

A useful way to frame this is to ask, for every component of the real system, "which of its behaviours can a seed produce?" Anything the simulator cannot produce is untested by DST, no matter how many seeds pass.

```
                 real system                         what the simulator models
  ┌────────────────────────────────────────┐   ┌──────────────────────────────────────┐
  │ HTTP clients, TCP, net/http            │   │ (nothing: requests appear as Events) │
  │ prod.Handler, prod.Loop, goroutines    │   │ (nothing)                            │
  │ core.Server.Step                       │   │ core.Server.Step  ◄── the real code  │
  │ prod.OSExecutor → os.* → syscalls      │   │ FakeFS (a model of POSIX + crashes)  │
  │ kernel page cache, journaling FS, disk │   │ FakeFS durable/volatile versions     │
  │ wall clock, monotonic clock            │   │ virtual clock (Event.At)             │
  │ process supervisor, restarts           │   │ KindRestart after RestartDelay       │
  └────────────────────────────────────────┘   └──────────────────────────────────────┘
```

Only one box on the left is executed as-is. Everything else is either modelled or absent.

### 1. The network and the HTTP layer

The simulator injects `core.Request` events directly (`toys3/sim/simulator.go:248-252`) and consumes `core.Reply` actions directly (`toys3/sim/simulator.go:228-237`). There is no connection, no request body that arrives in pieces, no client that disconnects mid-request, no reply that is written but never received, and no retry.

Consequences:

- **`prod.Handler` and `prod.ParseRequest` are tested only by `toys3/prod/prod_test.go`**, a deterministic but hand-written end-to-end test on a real temp directory. Key flattening (`/bucket/key` → `bucket/key`, `toys3/prod/http.go:110-117`), copy-source parsing, `MaxBytesReader`, and the status mapping are outside DST.
- **The client model is too kind.** A real client that gets a timeout *retries*. A retried PUT is a second PUT with the same value; a retried DELETE that gets 404 because the first attempt actually succeeded is a classic source of "phantom failure" reports. toys3's oracle has no notion of a client retry, so none of this is exercised. Section III.2 sketches how to add it.
- **Lost replies are only modelled via crashes.** A request whose reply was lost in the network, while the server stayed up, would be an `Abandoned` op that is not associated with a crash. The oracle's `Abandoned` outcome would handle it fine; nothing produces it.

### 2. The production runtime

`prod.Loop` (`toys3/prod/loop.go`) and `prod.OSExecutor` (`toys3/prod/executor.go`) are not executed by the simulator. The design accepts that by keeping them thin (`loop.md` rule 10), but "thin" is not "empty". Things DST cannot see:

- **Error mapping.** `code` (`toys3/prod/executor.go:102-112`) maps `fs.ErrNotExist` to `ErrNotFound` and everything else to `ErrIO`. If someone changed it to map `ENOENT` from `os.Rename` to `ErrIO` (or the reverse), every seed would still pass.
- **The Submit/Reply handshake.** `Loop.Submit` (`toys3/prod/loop.go:121-154`) has a three-way select and a special case for "a Reply dispatched just before a Halt still counts". It is tested by `TestHaltStopsLoop`, not by simulation.
- **Backpressure.** The inbox is a bounded channel (`NewLoop`, `inboxSize`); a full inbox blocks HTTP handlers and executor callbacks. The simulator has no inbox at all. `docs/flow-control.md` describes the much more careful design intended for the bigger system; toys3 does not attempt it.
- **`PrepareDir`** (`toys3/prod/executor.go:117-124`) creates `obj/` and `tmp/` with `MkdirAll` and fsyncs `root`. It does not fsync `root`'s parent, so on a brand-new data directory the directory `root` itself may not be durable after the first power cut. The FakeFS starts with both directories already durable (`NewFakeFS`, `toys3/sim/fakefs.go:119-133`) and has no mkdir, so this is outside the model.

### 3. Filesystem operations and faults that do not exist in the FakeFS

The FakeFS is unusually adversarial for a teaching tool (Chapter 11), but it has a fixed vocabulary:

- **No mkdir, rmdir, or nested directories.** Directories are fixed at construction (`toys3/sim/fakefs.go:96-97`). A design that sharded objects into subdirectories (`obj/ab/abcdef`) would need directory creation durability (fsync the parent after mkdir), which is a classic source of bugs and is not modelled.
- **No partial reads or short writes that report success.** `FSRead` returns the whole file or an error. `FSWrite` writes all of `Data` or fails (a failed write may leave a prefix, `toys3/sim/fakefs.go:288-297`, but a *successful* write is always complete). Real `write(2)` can return a short count; Go's `File.Write` loops until done or error, so for toys3's production path this is mostly fine, but a lower-level implementation would need it.
- **Torn writes are per file, at crash time, and only as a prefix.** `Crash` cuts dirty content to a random strict prefix (`toys3/sim/fakefs.go:495-497`). Real disks can tear at sector granularity in the *middle* of a file, reorder sector writes within a file, or persist a later sector and not an earlier one. Because toys3 writes whole files and checks a CRC, prefix tearing is enough to exercise its defences; a log-structured design that appends to one file would need richer tearing.
- **No interleaved tearing across files beyond independence.** Each un-synced change is resolved independently (`rng.Intn(2)` per dirent and per inode). Real journaling filesystems impose *orderings* (for example, ext4 in `data=ordered` mode writes data blocks before the metadata that references them), so some combinations the FakeFS produces cannot happen on ext4, and conversely the FakeFS never produces a few correlated failures (a whole journal transaction lost at once) that can.
- **No metadata operations.** No `stat`, no sizes, no mtimes, no permissions, no `fsync` returning data about the device. No hard links created on purpose (they only appear as a crash artifact).
- **No ENOSPC, EDQUOT, EROFS.** Every error is `ErrIO` or `ErrNotFound` (`toys3/core/types.go:31-35`). "Disk full" is a common and important production failure with its own semantics (a write that fails partway; a rename that cannot allocate a directory block). In production, `code` would map ENOSPC to `ErrIO`, which the server handles; the simulator just never produces the case where *every* write fails for a long time.
- **No slow disk.** Latency is uniform in `[20µs, 1.5ms]` plus a uniform reply delay (`toys3/sim/config.go:165-166`). There is no two-speed latency (mostly fast, a heavy tail), no stall, no "fsync takes 30 seconds". `docs/lessons.md` §9 argues that correlated slowness finds bugs that independent latency never does. toys3 has no timeouts, so slowness cannot currently *break* it, but it would change which interleavings are likely.
- **No reads that see stale data.** Reads always see the volatile (current) version. A filesystem bug or a cache-coherence problem that made a read return an older version is not modelled (and would not be the server's fault).

### 4. Time

The server never looks at `Event.At`. It has no timers, timeouts, leases or TTLs. So the simulator's clock is only used to order events and to make logs readable. Clock skew, clocks going backwards (`loop.md`'s design has a node-local clock and a UTC clock, `docs/lessons.md` §2) and timer races are all untested because there is nothing to test. The moment you add a timeout (for example, to give up on a stalled fsync), this changes.

### 5. Processes and nodes

- **One process.** There is no second process touching the data directory, and no locking against one. Running two toys3 binaries on the same `-dir` would be a disaster; nothing prevents it.
- **One node.** No replication, no consensus, no partitions. This is the biggest gap between toys3 and the system it is a rehearsal for, and section III.3 sketches the first step across it.
- **Restart is immediate and certain.** A crash is always followed by a restart within `RestartDelay` (`toys3/sim/simulator.go:304-305`). There is no "machine stays down for an hour", no "machine comes back with a different disk" (FoundationDB's simulator swaps drives), no "restart loops forever because recovery always hits an error". The last one is interesting: a persistent EIO during recovery would make toys3 halt on every boot (by design, `toys3/core/server.go:181-239`), and the Liveness invariant would then fail at the end of the run. Because faults stop in QuietTail, the simulator never produces that persistent case.

### 6. Buggify

`core.Env` includes `Buggify` (`toys3/core/env.go:18-26`), and the simulator's `env` implements it with a 5% chance during Chaos (`toys3/sim/simulator.go:407`). But **the server has no buggify sites**: `grep -rn 'Maybe(' toys3` finds only the interface and the two implementations. The server takes no rare-but-legal paths on purpose. All of the exploration comes from the environment (FS latency, EIO, crash timing, tie-breaks). Section III.1 is about changing that.

### 7. The oracle's own approximations

- **Widening.** When a component would start from more than 4,096 possible states, `widenStates` (`toys3/sim/oracle.go:492-511`) replaces the keys involved with `unknown`, and any subsequent read of an unknown key is accepted. This is sound (never a false failure) and not complete (a real bug can slip through a widened window). The 15-second campaign in Chapter 16 widened nine times in 7,240 seeds.
- **The checker budget.** A single window search is capped at 2^20 memo entries (`toys3/sim/oracle.go:556-559`). Exceeding it is reported as a failure ("checker budget exceeded… shrink QuiesceEvery"), which is the right choice (loud, not silent), but it means oracle performance can masquerade as a correctness failure if windows grow.
- **Read-only ops on failure constrain nothing.** A GET that got a 500 or was abandoned is dropped from the window (`toys3/sim/oracle.go:318-321`). That is correct, but it means the server could return a wrong value *alongside* a 500 and nothing would notice (the Body of a 500 is never checked). The server never does that; the oracle would not catch it if it did.

### 8. Liveness is end-of-run only, and only for the last incarnation

`Liveness.AtEnd` (`toys3/sim/invariant.go:191-211`) checks that the server is alive, that *the final incarnation* emitted `Recovered`, and that no op is still `Pending`. It does not check that every earlier boot recovered (most did not: they crashed or halted during recovery, which is legitimate), and it does not bound *how long* a request may wait. A server that took 59 ms of the 60 ms QuietTail to answer a request would pass. A bounded-latency liveness check ("every request issued in QuietTail replies within X ms of virtual time") would be a cheap and meaningful strengthening.

### 9. FakeFS versus ext4: in which direction does the model err?

It errs in both directions, and you should know which is which.

**More adversarial than a typical Linux journaling filesystem:**

- A cross-directory rename can be resolved *independently* in its two directories at a crash: both names survive, or neither (`TestFakeFSRenameWithoutDirFsyncMayVanish`, `toys3/sim/fakefs_test.go:67-91`). On ext4, a rename is one journal transaction; its two halves commit together. The "resurrected temp file aliases a committed object" scenario behind mutant f relies on this independence (plus background writeback making the temp name durable first). Defending against it costs nothing for a correct server, and it is legal POSIX behaviour, so it is a good default.
- Every un-synced change is a coin flip at every crash, with no ordering between them.
- `FsyncGate` is always on in `BaseConfig` (`toys3/sim/config.go:167`). Linux behaved this way for years; newer kernels report the error more reliably, but the safe assumption remains "after an fsync error, the page cache is unknowable".

**Less adversarial:**

- No mid-file tearing, no sector reordering (see §3).
- No metadata corruption, no lost directories, no `fsck` moving files to `lost+found`.
- No persistent device failure. A seed's EIOs are independent per operation.
- No "fsync returned success but the disk lied" (a drive with a volatile write cache and no barrier support). The FakeFS's fsync, when it says OK and the inode is not poisoned, really is durable.

The rule of thumb: the FakeFS is strict about **what POSIX permits** and lenient about **what hardware does**. That is a reasonable place to be for an application-level protocol, and a poor place to be for a filesystem or a database that does its own block management.

---

## Part II: Discrepancies and surprises

These were found by reading every file and running the code while writing this book. Each item says what the documentation claims, what the code does, and whether it matters. **None of these was fixed**; the book's rule was not to modify code. They are good first patches.

### 1. The server never uses its `Env`

*Claim.* `DESIGN.md:31-32`: "Randomness and buggify come from the injected `Env`." `core.Env` is documented as "the only way nondeterminism enters the core" (`toys3/core/env.go:3-4`).

*Code.* `core.Server` stores `env` (`toys3/core/server.go:88`) and never reads it. No call to `Uint64`, `Intn` or `Maybe` exists in `toys3/core`. The test harness even says so: "a fixed core.Env (the server does not draw randomness today)" (`toys3/core/server_test.go:12`).

*Does it matter?* Not for correctness. It matters for expectations: the simulator's `env.Maybe` draws from the shared RNG, so the *first* buggify site anyone adds will change RNG consumption and therefore reshuffle every seed's universe (Chapter 16's bisecting caveat). Expect every trace hash to change on that commit.

### 2. `loop.md` and the code disagree on the queue order

*Claim.* `loop.md:88-89`: "a priority queue ordered by `(time, seq, rngTie)`".

*Code.* `Queue.less` orders by `(At, Tie, Seq)` (`toys3/sim/queue.go:125-134`): the random tie-break comes *before* the insertion sequence. `DESIGN.md:80` agrees with the code.

*Does it matter?* Yes, conceptually, and the code is right. With `(time, seq, tie)` the tie-break would never be used (seq is unique), and same-instant events would always be processed in insertion order, which is exactly the exploration the random tie is supposed to add. `loop.md` should be updated. (The bigger system's design, `docs/decisions.md` decision C, refines this further: random across lanes, FIFO within a lane.)

### 3. Liveness says "every boot", checks "the last boot"

*Claim.* `invariant.go:76-77` ("every boot reached core.Recovered") and `DESIGN.md:146` ("every boot recovers").

*Code.* `Liveness.AtEnd` accepts the run if a `Recovered` fact exists with `f.Inc == v.Inc()`, i.e. for the final incarnation only (`toys3/sim/invariant.go:196-204`).

*Does it matter?* The code is the right check: earlier incarnations are allowed to crash or halt mid-recovery (recovery halts on any error, by design). The docs should say "the final boot recovered". As noted in Part I §8, there is room for a stronger *bounded* liveness check.

### 4. `HistOp.Invoke` is not "when the Request was delivered"

*Claim.* `toys3/sim/oracle.go:27`: `Invoke core.Time // when the Request was delivered`.

*Code.* `Oracle.Invoke` is called from `Simulator.wake` (`toys3/sim/simulator.go:250`) at the moment the client *issues* the request. The delivery is scheduled at the same virtual instant (`At: s.now`, `simulator.go:252`), but it is a separate queue entry with its own random `Tie`, so other same-instant entries can be processed in between, and a crash can even drop the delivery entirely (the op then becomes `Abandoned` without the server ever seeing it).

*Does it matter?* No: an earlier invocation point only makes the window wider, which is conservative for a linearizability check. But the comment is wrong in a way that will confuse someone reading a log, because the `[InvSeq..RetSeq @Invoke..Return]` in a failure message is not when the server saw the request. Mutant d2's failure (Chapter 7) includes an abandoned PUT (c52) that was never even started by the server; the oracle still lists `{k1=52}` as a possible start state, which is correct precisely because the oracle cannot know whether delivery happened.

### 5. Failures are logged as crashes

*Code.* `Simulator.fail` appends a `LogEntry` with `Kind: KindCrash` and a note starting `FAILURE` (`toys3/sim/simulator.go:370-373`). So every failing log ends with a line like:

```
#380    t=91.269593ms  inc=3 crash       FAILURE linearizable: no linearization of 1 ops ...
```

No crash happened at step 380. *Does it matter?* Cosmetic, but it trips up readers grepping for `crash`. A dedicated `KindFailure` (or printing the note with no kind) would be clearer.

### 6. Recovery's `FSFsyncDir(tmp)` does not appear to be load-bearing

*Claim.* The package doc lists it as a recovery step "even if nothing was removed" (`toys3/core/doc.go:102-104`), and the server does it (`fsyncTmpDir`, `toys3/core/server.go:241-246`).

*Experiment.* A mutant that skips only `FSFsyncDir(<dir>/tmp)` (built with the same `skipper` wrapper as mutant a, feeding the inner server a fake OK) was run against seeds 1..2000 of `DefaultConfig` with all invariants. **No seed failed.**

*Why?* After a crash, every directory entry has `cur == durable` (Chapter 11). Recovery's unlinks make `cur = 0` for each leftover temp name, not durable. If the tmp fsync is skipped and another crash happens, some of those names come back; the next recovery unlinks them again. Within an incarnation, a new `FSWrite` to a temp name whose entry is `cur = 0` creates a *fresh* inode (`toys3/sim/fakefs.go:298-304`), so a resurrected old inode can never be overwritten through it. Temp names are only read by recovery. So the fsync's durability is never observed.

*Does it matter?* It is a belt-and-braces step that costs one fsync per boot, and keeping it is defensible: it means "tmp/ is durably clean when serving starts" is an invariant you can reason about, and it would become necessary if temp names were ever reused in a way that reached a stale inode. But it is a nice example of what DST can tell you that review cannot: **this step is not justified by any failure the model can produce.** Either the model is missing a failure (possible), or the step is optional. Write that down in the design doc either way.

### 7. "The first failure stops the run", but several can be reported at one step

*Claim.* `Simulator.fail`: "fail records f … and stops the run: after the first violation the rest of the history is noise" (`toys3/sim/simulator.go:362-363`).

*Code.* `fail` sets `halted`, which stops the main loop *after* the current step. But `afterStep` iterates over all invariants for that step (`toys3/sim/simulator.go:348-352`), and each may add failures. Mutant e at seed 18 reports two failures at step 657: `no-corrupt-reads` and `linearizable`. Mutant b at seed 1 reports two `obj-never-torn` failures at step 44 (one per torn file).

*Does it matter?* No, and it is arguably a feature: two invariants agreeing about one step is useful information. Only the sweep and the campaign runner show just `Failures[0]`.

### 8. `readcorrupt` exceeds `corruptobj`

*Observation.* The 200-seed aggregate has `readcorrupt=71 corruptobj=70`; the 15-second campaign has 2,318 versus 2,294.

*Explanation.* `ReadCorruptions` counts at *land* time (`toys3/sim/fakefs.go:388-391`), `CorruptObjects` at *delivery* time, when the server emits the fact (`toys3/sim/simulator.go:194-195`). A crash between the two drops the completion (`Queue.DropIncarnation`), so the server never sees the corrupt bytes. Not a bug; a concrete demonstration of the "landed but never told" window from Chapter 10.

### 9. The "dropped: dead incarnation" branch is defensive

*Code.* `Simulator.handle` checks `e.Inc != s.inc || !s.alive` for `KindDeliver` and `KindFSLand` and logs "dropped: dead incarnation" (`toys3/sim/simulator.go:128-131, 134-137`).

*Observation.* `crash` calls `q.DropIncarnation(s.inc)` immediately (`simulator.go:280`), which removes *every* `KindDeliver` and `KindFSLand` entry of that incarnation. Entries for the new incarnation are only scheduled after `restart` increments `s.inc` and sets `alive`. So by the time a dead incarnation's entry could be popped, it has already been removed. The branch never fires in the current code (no seed's log in this book contains the note). It is a sensible belt-and-braces check; it would start to matter if someone made `DropIncarnation` lazy (drop on pop, as `queue.go:7-8` describes) for performance.

### 10. Faults planned in Chaos can land in QuietTail; writeback never stops

*Code.* `FakeFS.Plan` decides `Fail` at *issue* time, gated on `!fs.quiet` (`toys3/sim/fakefs.go:138`). An action issued at 299.9 ms with `Fail = true` lands, as an EIO, after `KindQuiet` at 300 ms. If it is an fsync, the server halts, and a crash happens in QuietTail. `WritebackProb` is not gated on `quiet` at all (`toys3/sim/fakefs.go:189-191`). `Crash` resolution, of course, still applies to any crash.

*Does it matter?* The Liveness "no 500 in QuietTail" rule only looks at ops *invoked* during QuietTail (`toys3/sim/invariant.go:178-187`), and those ops' FS actions are planned in quiet mode, so they never fail. The rule is therefore sound. But "no new faults or crashes" in the `KindQuiet` log note (`simulator.go:162`) is slightly too strong: one more Halt, and its crash, can still happen early in the tail. Worth a sentence in `DESIGN.md`.

### 11. Reads serialize with reads

*Code.* A request's "keys touched" make no distinction between reading and writing (`touches`, `conflicts`, `toys3/core/server.go:69-76`). Two GETs on the same key are in the same FIFO queue and run one after the other. You can see it at the start of seed 1: `c1 GET k2` and `c4 COPY k2 -> k0` both touch `k2`, so the COPY waits for the GET.

*Does it matter?* It is a performance choice, not a correctness one, and it makes the server easier to reason about. A reader/writer queue would be a good exercise; the oracle needs no change (reads that overlap are allowed to see either value).

### 12. The CI audit step checks an empty set

*Code.* `scripts/ci-determinism.sh:16-28` lists every third-party module in the dependencies of the deterministic packages and requires an audit record for each. Running it today prints the header and nothing else, because none of `./core/... ./sim/... ./toys3/core/... ./toys3/sim/...` imports a third-party module. `go.mod` requires only `golang.org/x/tools`, used by `tools/detcheck`. Meanwhile `tools/detcheck/allow.txt:28` allow-lists `github.com/twmb/franz-go/pkg/kmsg` and `docs/dep-audits/` holds its audit record, but nothing imports it yet (`core/kafka.go` mentions kmsg only in comments).

*Does it matter?* No; the mechanism is in place for the day the Kafka engine imports kmsg. It does mean the audit gate has never been exercised end to end in this repository.

### 13. A fourth data point for the mutant f ablation

*Claim.* `DESIGN.md:205-207`: mutant f "took seed 429 with neither background writeback nor swarm, seed 36 with writeback alone, and seed 20 with both."

*Experiment.* All three were reproduced exactly (base config without writeback: 429; base with writeback: 36; swarm with writeback, i.e. `DefaultConfig`: 20). The missing corner of the 2×2, **swarm without writeback, is 24**.

| | no writeback | writeback |
|---|---|---|
| fixed config (`BaseConfig`) | 429 | 36 |
| swarm (`DefaultConfig`) | 24 | 20 |

*Reading.* Each ingredient alone gives most of the improvement, and they partly overlap: both make "a temp name becomes durable before it is renamed away" more likely to coincide with "the key is not rewritten for a long time". Swarm does it by sometimes choosing 16 or 32 keys; writeback does it by making the temp name durable without needing a lucky crash. It is a nice illustration that **realism of the fault model and diversity of the workload are partially substitutable**, and that neither is a substitute for having both.

### 14. The story of the mutant that did not bite is not in git

The brief for this book mentions that mutant f was at first *not caught*, because of a bug in the mutant itself. The repository history cannot confirm or show this: `toys3/sim/mutants_test.go` first appears, in its final form, in commit `210c1fa`. What the repository does preserve is the general lesson, and a cheap way to defend against it: **a mutant's effect should be visible in the stats**. Mutant f's runs report `tempsrm=0` in every seed (its whole effect is to hide leftovers from recovery, `toys3/sim/mutants_test.go:316-324`), while the correct server removes about five temp files per seed. If a mutant's stats line is indistinguishable from the correct server's, suspect the mutant before you trust the simulator. Chapter 15 turns this into a checklist.

### 15. `KindCrash` is never scheduled

*Code.* `EntryKind` has a `KindCrash` ("kill the server now", `toys3/sim/queue.go:18`), and `handle` processes it (`toys3/sim/simulator.go:147-150`). But no code path ever schedules a `KindCrash` entry: injected crashes happen inline in `maybeCrash` (called after every delivery and every landing, `simulator.go:144, 215`), and Halts crash inline in `deliver`. The kind's only live use is as the `Kind` of log entries written by `crash` and `fail`.

*Does it matter?* No. It is a hook waiting for a feature: scheduled crashes at chosen virtual times, which would make it easy to write targeted regression tests ("crash exactly between this rename and that fsync"). Appendix F's last exercise asks you to build it.

### 16. The queue's doc says "dropped on Pop"; the code drops eagerly

*Claim.* `toys3/sim/queue.go:6-8`: "entries for a dead incarnation are dropped on Pop".

*Code.* `DropIncarnation` (`queue.go:102-120`) removes them eagerly at crash time and re-heapifies. `Pop` does no filtering. The defensive check in `handle` (item 9) is what *would* implement drop-on-pop.

*Does it matter?* Only to a reader. Either strategy is deterministic. Eager dropping is O(n) per crash; lazy dropping would be O(1) per crash and a cheap check per pop. Fix the comment.

---

## Part III: Roadmap

Each item has a motivation, a design sketch grounded in the existing code, and an exercise. They are roughly ordered by effort.

### III.1 Add buggify sites (an afternoon)

**Motivation.** The environment explores timing and faults; buggify lets the *code* explore its own rare-but-legal choices. FoundationDB's experience is that buggify finds bugs in the code's assumptions about itself ("this queue is never empty here", "this always happens before that").

**Sketch.** Candidate sites in `core/server.go`, each a legal variation:

- **`startRunnable` order** (`toys3/core/server.go:313-321`). It starts runnable requests in arrival order. Any order is legal (they touch disjoint keys). With `s.env.Maybe("start-runnable-reverse")`, iterate backwards. This checks that nothing depends on the start order of independent requests.
- **Split a step's actions.** Instead of returning the next FS action immediately, the server could "forget" to start a runnable request in this step and start it on the next event. Legal (it is just slower), and it explores more interleavings. Careful: there must *be* a next event, or the request starves. The Liveness invariant will tell you if you got that wrong.
- **Re-read after write.** After `commit`, optionally issue an `FSRead` of the object and verify its CRC before replying. Legal, slower, and a good way to give the read-corruption fault more chances to interact with writes.

Remember Part II §1: the first site changes every seed's universe. Re-record the mutants' first-catch seeds in `DESIGN.md` after adding it.

**Exercise.** Add the `startRunnable` site. Run `go test ./toys3/sim`. Did any trace hash change? (Yes: every seed.) Did any seed fail? Then add a deliberately *illegal* buggify (start a request that is *not* at the head of its queues) and confirm which invariant catches it.

### III.2 Simulate the network and client retries (a few days)

**Motivation.** Part I §1. The most common production surprise for an object store is not a power cut; it is a client that timed out and retried.

**Sketch.**

1. Add a *client-side* timeout to the workload: if no reply arrives within T of virtual time, the client marks its request `Abandoned` in the oracle (a new kind of abandonment, not tied to a crash) and issues a retry of the same operation with a *new* `ClientReq`. The server still has the old request in flight; its eventual reply must be dropped by the simulator (it goes to a client that no longer waits). This needs a "reply to an abandoned request" path in `Simulator.reply`, which today treats it as a protocol failure (`toys3/sim/simulator.go:229-233`).
2. Make replies lossy: with some probability, a `Reply` action is executed by the server but never delivered (the client then times out and retries).
3. Model the retry correctly in the oracle. A retried PUT is a *new* op with the same value bytes. Today `MakeValue(req, size)` derives the value from the request id (`toys3/sim/workload.go:163-176`), so a retry would write different bytes; to model a real retry you would keep the *original* writer id in the value. The oracle's model (`Value.Writer`) then has two ops that can produce the same writer id, which is fine: the model only records which bytes are present.
4. Watch what happens to DELETE. A retried DELETE whose first attempt succeeded gets 404. Under the current oracle, a definite 404 means "the key was absent at some instant in the window", which is true. But a *client* that treats that 404 as a failure has a bug. That is the beginning of idempotency design: request ids that the server remembers, or conditional operations (`If-Match`).

**Exercise.** Implement step 1 only (timeouts and retries, no lost replies). Predict whether any existing invariant fires on the correct server. Then implement lost replies and repeat.

### III.3 Two nodes: primary-backup replication (a week or more)

**Motivation.** This is the step from "storage" to "distributed storage", and it is where DST pays for itself many times over.

**Sketch.**

- Two `core.Server` instances, each with its own FakeFS (two disks), identified by node id. The simulator's queue entries gain a target node (the bigger system's skeleton already has this: `sim.Target` with a `Node core.NodeRef`, `sim/scheduler.go:18-22`).
- A new action/event pair for node-to-node messages: `Send{To, Msg}` and `Received{From, Msg}`. The simulator delivers them with latency, drops, duplicates and reordering; a partition is a period during which messages between two nodes are dropped (or, better, held and re-timed on heal, `docs/lessons.md` §6).
- Protocol: the primary performs the local durable write, sends the object to the backup, and replies 200 only after the backup acknowledges *its* durable write. The backup's write uses the same temp → fsync → rename → fsync-dir dance.
- Failover: when the primary crashes, the backup becomes primary. Now you need epochs (an incarnation number in every message, `docs/lessons.md` §4) so a resurrected old primary cannot accept writes. That is where the first real bugs will be.
- The oracle does not change at all. It checks the client-visible history against one sequential register per key, regardless of how many nodes there are. That is the beauty of a black-box linearizability oracle.

**Exercise.** Before writing code, list five bugs you expect to make (for example: "the primary acks after its own fsync but before the backup's", "the backup applies a write from a deposed primary"). For each, write the mutant you would use to prove the simulator catches it. Then build the system and run the mutants.

### III.4 Coverage-guided seeds and "sometimes" assertions (days)

**Motivation.** `TestCoverage` is a binary check over a fixed 200 seeds (Chapter 16). The DST literature goes further: record which interesting events each seed reached, and prefer to explore *from* seeds that reached rare events.

**Sketch.**

- Replace the coverage counters with named **sometimes-assertions** emitted as facts: `Sometimes("crash during rename")`, `Sometimes("recovery found an alias of a committed object")`, `Sometimes("COPY src == dst")`. The bigger system's `core.Assertions` interface (`core/env.go`: `Always`, `Sometimes`, `Reachable`, `Unreachable`) is exactly this shape.
- The campaign runner aggregates them and prints the ones that never fired. A sometimes-assertion that never fires is a gap in the fault model or the workload, and fixing it is the highest-value work in a DST project.
- Coverage-guided selection: keep a corpus of seeds that first reached each assertion; spend part of the campaign budget re-running *variations* of those seeds. Variation needs the next item.

**Exercise.** Add three sometimes-assertions as new `core.Fact`s emitted from the simulator (not the server), aggregate them in `Stats`, and print them in the summary line. Which one fires least often in 1,000 seeds?

### III.5 Multiverse: reseeding after N draws (days)

**Motivation.** `docs/lessons.md` §3 describes it: run a seed to an interesting point (draw number N), then *fork the universe* by reseeding the RNG and continuing, many times. This explores the neighbourhood of a rare state without having to find it again from scratch.

**Sketch.** `sim.Rand` already counts draws (`Calls()`, `toys3/sim/rand.go:59-61`). Add `Rand.Reseed(x)` and a `Config.ForkAt (draws uint64, newSeed uint64)`. When `rng.Calls()` reaches `ForkAt.draws`, reseed. A run is then named by a pair `(seed, (draws, newSeed))`, still fully reproducible. The campaign runner, seeing a seed reach a rare sometimes-assertion at draw N, spawns forks at N−k for a range of k.

**Exercise.** Implement `ForkAt` and confirm that a fork at a draw count larger than the run's `RandCalls` reproduces the original trace hash exactly, and that a fork at draw 1 is a completely different run.

### III.6 Swarm over fault families (an afternoon)

**Motivation.** `DefaultConfig` varies four knobs per seed (keys, clients, crash rate, EIO rate). `docs/lessons.md` §9 argues for going further: per seed, switch whole fault families *off*, because "everything a little bit on" means families crowd each other out.

**Sketch.** In `DefaultConfig`, with a fixed number of draws from the swarm stream (so the run's own stream is untouched), choose a random subset of {EIO, torn writes, read corruption, writeback, FsyncGate, crashes} to disable, and a random subset of the op alphabet to disable (`OpMix` weights to zero; at least one mutation and GET must remain). Re-run the mutation table and the ablation from Part II §13.

**Exercise.** Predict which mutant's first-catch seed gets *worse* when fault families can be switched off, and which gets better. Run it.

### III.7 ENOSPC and a persistent fault mode (an afternoon)

**Sketch.** Add `ErrNoSpace` to `core.ErrCode` (it is plain data; adding a value is cheap). Give the FakeFS a capacity in bytes and make `write` fail with `ErrNoSpace` (possibly after a prefix) when it would exceed it. Decide the server's policy: is ENOSPC on a temp write a plain 500 (yes: only a temp changed), and does it change anything about recovery? Add a fault mode in which the disk stays full for a long stretch of Chaos, so that recovery itself sees ENOSPC… except recovery only unlinks and fsyncs, which do not allocate. Is that true on your real filesystem? (Unlinking can require journal space.)

**Exercise.** Implement capacity, run the campaign, and add a QuietTail rule: during the tail the disk has space, so no 500s. Is that rule still sound?

### III.8 A LIST operation (days)

**Motivation.** S3's ListObjects is where many object stores' consistency stories get complicated. It is a multi-key read.

**Sketch.** `core.List{Prefix}` → `FSList(obj)` → reply with the unescaped names (`UnescapeKey`, `toys3/core/format.go:72-91`, exists today and is unused by the server). Concurrency: a LIST touches *every* key, so under the current per-key FIFO it would serialize with everything. The oracle needs a new op type whose result is a *set* of keys, linearized at one instant; it no longer decomposes per key (a LIST links every key into one component), which will stress `maxStartStates` and widening.

**Exercise.** Before implementing, decide: should LIST be linearizable, or only "eventually consistent"? Write the oracle rule for each choice, and a mutant that each rule would catch and the other would not.

### III.9 Large objects and streaming (a week)

**Sketch.** Today a PUT's whole payload is one `FSWrite`. For large objects you would write in chunks (`FSWriteAt{Offset}`), fsync once, then rename. The FakeFS then needs chunk-granular tearing (Part I §3), and the CRC needs to become incremental or per-chunk. GET would stream chunks into the reply, which means the core needs a way to send a partial reply, which means the HTTP layer becomes part of the protocol, which means Part I §1 becomes urgent. This is a good exercise in discovering how one feature pulls a whole layer into the deterministic core.

### III.10 Apply it to Manifester (a new project)

The top-level `core/`, `sim/` and `sys/` directories are interfaces for a WarpStream-like Kafka engine, written *before* toys3, and toys3 was the rehearsal. What carries over directly:

| toys3 | Manifester skeleton | Notes |
|---|---|---|
| `core.Step(Event) []Action` | `core.StateMachine` | Same shape. |
| `core.Env{Rand, Buggify}` | `core.Env{Rand, Buggify, Assertions}` (`core/env.go`) | The bigger Env adds `NewID`, `MaybeP`, `Fault`, and record-and-continue assertions. |
| `sim.Queue (At, Tie, Seq)` | `sim.Scheduler` with lanes (`sim/scheduler.go`) | Lanes give FIFO within a connection and random ties across lanes (decision C). |
| `Incarnation` + `DropIncarnation` | `Scheduler.DropNode(core.NodeRef)` | Same idea, per node. |
| `FakeFS` (one engine) | `sim.Engine`s (object store, control plane, network) | Each engine owns state and schedules completions on the one queue (`docs/lessons.md` §0). |
| `Invariant.AfterStep(View)` | `sim.Invariant.Observe(Timeline, now)` (`sim/roles.go`) | Facts become an append-only timeline with cursors. |
| `Workload` (plain struct) | `sim.Workload`, itself a sans-I/O `StateMachine` | The Kafka client model must be a state machine because `kgo` cannot run in the simulator. |
| `Oracle` (register per key) | reference model of acked offsets | Different model, same discipline: decide what a client may observe, check every observation. |
| mutants | — | Write them from day one. |

The lessons that transfer best are not the interfaces but the habits: decide what each error *proves* (`docs/lessons.md` §1's NotApplied / MaybeApplied / Applied is the generalization of toys3's "500 promises nothing"), separate *landing* from *delivery* for every external effect, check a ground-truth invariant eagerly *and* a black-box invariant at quiescence, and never trust a simulator until a mutant has bitten.

**Exercise.** Pick one Manifester flow from `loop.md` (the produce path: `ProduceReq → S3Put → MetaCommit → Reply`). Write its toys3-style error table (Chapter 7): for each step, what does each error prove, and is it a plain error reply, a retry, or a Halt? Then write the mutant list.

## Key takeaways

- A passing campaign covers only what the simulator can produce. In toys3 that excludes the network, the production runtime, mkdir, ENOSPC, slow disks, mid-file tearing, timers, multiple processes and multiple nodes.
- The FakeFS errs toward strict POSIX (more adversarial than ext4 about rename and ordering) and toward kind hardware (no lying caches, no persistent failures). Know which side a bug you care about falls on.
- Running the code found sixteen discrepancies or surprises, none a server bug, several worth fixing: the unused `Env`, `loop.md`'s queue order, liveness wording, the `Invoke` comment, failure log kinds, an fsync with no observable justification, faults that land after Quiet.
- The cheapest high-value extensions are buggify sites, sometimes-assertions and swarm over fault families. The most valuable expensive one is a second node, for which the oracle needs no change at all.

## Exercises

1. **Fix a discrepancy.** Pick items 2, 3, 4 or 5 from Part II and make the documentation or code consistent. For item 5, make sure `TestDeterminismCanary` still passes (logging must not change the trace).
2. **Justify or remove the tmp fsync.** Reproduce Part II §6 yourself: write mutant g (skip `FSFsyncDir` of `tmp/`) in a copy of `mutants_test.go`, run it for 2,000 seeds, and confirm it survives. Then either (a) find a change to the FakeFS or the server's temp naming under which it *is* caught, or (b) write a paragraph for `DESIGN.md` explaining why it is safe to remove. Both are good outcomes.
3. **Bounded liveness.** Strengthen `Liveness` so that every request invoked in QuietTail must reply within 10 ms of virtual time. Run 1,000 seeds on the correct server. If anything fails, is it the server or the bound?
4. **Measure the ext4 gap.** Change `FakeFS.Crash` so that a rename's two directory halves are resolved *together* (both kept or both reverted), as a journaling filesystem would. Re-run mutant f. Is it still caught? At which seed? What does that tell you about whether toys3's defence against f matters on ext4?
5. **Persistent EIO.** Add a config flag that, for one stretch of Chaos, makes every `FSList` fail. Watch the server halt on every boot. What does the Liveness invariant report if the stretch extends into QuietTail? Is the server's behaviour (halt forever) the right one? What would you want instead?

## Further reading

- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications"*, OSDI 2014. The ALICE tool and the systematic study of which update protocols survive which filesystem behaviours; the best guide to Part I §3 and §9.
- Anthony Rebello et al., *"Can Applications Recover from fsync Failures?"*, USENIX ATC 2020. What real filesystems do after an fsync error, measured.
- Jonathan Corbet, *"PostgreSQL's fsync() surprise"*, LWN.net, 2018. The fsyncgate story.
- Dan Luu, *"Files are hard"* (2015). A tour of how hard the Part I §3 list really is.
- Alex Groce et al., *"Swarm Testing"*, ISSTA 2012.
- The Antithesis documentation on Sometimes assertions and on exploring from interesting states (the "multiverse" idea of III.5).
- George Candea and Armando Fox, *"Crash-Only Software"*, HotOS 2003. The philosophical basis for toys3's Halt-and-recover policy, and for item 5 above.
- `docs/lessons.md` and `docs/decisions.md` in this repository: the design notes for the system toys3 rehearses.
