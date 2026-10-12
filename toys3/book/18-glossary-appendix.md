# Chapter 18: Glossary and Appendix

This chapter is reference material. The glossary defines every term the book uses with a specific meaning, with pointers into the code. The appendices collect the tables you will want open while working: commands, a file index with key symbols and line numbers, the invariant and mutation tables cross-referenced to chapters, the error policy, the `Stats` fields, and the log format.

Line numbers refer to commit `210c1fa`.

---

## Glossary

**Abandoned.** The `Outcome` of a request that was outstanding when the server crashed or halted (`toys3/sim/oracle.go:19`). The client never learned what happened. For the oracle, an abandoned mutation may take effect at one instant in `[Invoke, crash]` or never; an abandoned read constrains nothing. Set by `Oracle.Crash` (`oracle.go:142-152`). Counted in `Stats.Indetermined`. Chapters 10, 13.

**Acked-is-durable.** Invariant (b), checked eagerly by `AckedIsDurable` (`toys3/sim/invariant.go:64, 115-141`): on the step a `Committed` fact is emitted, the key's file in the FakeFS *durable* view must exist and carry the committed checksum (or be absent, for a delete). Catches a missing fsync without waiting for a crash. Chapter 14.

**Action.** An output of `Step`: a request for the runtime to do something (`toys3/core/action.go:7`). A closed set: `Reply`, seven filesystem actions, `Halt`, `Emit`. Plain data. Chapter 3.

**Adversary.** The role the FakeFS plays at a crash: each un-synced change is resolved independently by the RNG, as if chosen by someone trying to break the server (`FakeFS.Crash`, `toys3/sim/fakefs.go:458-519`). Chapter 11.

**Alias (resurrected temp).** A temp file name that a crash brings back as a second directory entry for the inode of a committed object, because the temp name was durable (often via writeback) and the rename's removal of it was not. Harmless if recovery unlinks it; catastrophic if a later write truncates it (mutant f). Chapters 11, 15.

**Allow-list.** `tools/detcheck/allow.txt`: the exact import paths the deterministic packages may import. Fail-closed: anything not listed is an error. A trailing `/...` allows subpackages. Chapter 8.

**Arrival order / seq.** Each accepted request gets `seq` from `Server.nextSeq` (`toys3/core/server.go:285-287`). Every per-key FIFO queue is ordered by it, which is why COPY's two-queue membership cannot deadlock. Chapter 6.

**BaseConfig.** The fixed simulation configuration (`toys3/sim/config.go:146-173`): 300 ms Chaos, 60 ms QuietTail, 4 clients, 3 keys, the default fault probabilities. `DefaultConfig` is `BaseConfig` plus swarm variation. Chapter 12.

**Black-box invariant.** An invariant that judges only what clients observed, never the FakeFS's internals: `NoCorruptReads`, `Linearizable`, `Liveness` (`BlackBoxInvariants`, `toys3/sim/invariant.go:51-53`). Contrast white-box. Chapters 14, 15.

**Budget (oracle).** The cap on memo entries in one WGL search, 2^20 by default (`toys3/sim/oracle.go:556-559`). Exceeding it is reported as a `linearizable` failure suggesting a smaller `QuiesceEvery`. Chapter 13.

**Buggify.** FoundationDB-style cooperative fault injection: code asks `env.Maybe(site)` whether to take a rare-but-legal path (`toys3/core/env.go:18-26`). The simulator says yes 5% of the time during Chaos (`toys3/sim/simulator.go:407`); production always says no. toys3's server has no buggify sites today. Chapters 3, 17.

**Canary (determinism).** `TestDeterminismCanary` (`toys3/sim/sim_test.go:24-42`): runs seeds 1–12 twice and compares trace hash, rand calls and steps; checks that logging does not perturb; checks that adjacent seeds differ. Run by CI. Chapters 8, 16.

**Chaos.** The first phase of a run (`Config.Chaos`, 300 ms by default) during which faults and crashes are injected. Ends at the `KindQuiet` entry. Chapter 10.

**Checksum.** CRC-32 (IEEE) of an object's payload, stored in the header (`core.Checksum`, `toys3/core/format.go:20`). Also carried in `Committed.Sum`. Chapter 4.

**ClientReq.** A request id assigned by the runtime and echoed on the reply (`toys3/core/types.go:17-20`). In simulation, also the identity embedded in every value a PUT writes. Logged as `c17`. Chapter 3.

**Committed.** The fact emitted immediately before the 200 reply to a successful PUT, COPY or DELETE, at the moment the server believes the mutation is durable (`toys3/core/fact.go:9-19`). Checked by acked-is-durable. Chapter 3.

**Completion.** A `...Done` event (`FSWriteDone`, etc.) that reports the result of a filesystem action, matched to it by `IOID`. Chapter 3.

**Component.** In the oracle, a connected set of keys in one window: keys are linked if one op touches both (COPY) or a carried group contains both. Each component is searched independently (`Oracle.check`, `toys3/sim/oracle.go:396-487`). Chapter 13.

**Correlation ID.** `loop.md` rule 8: every request carries an id so late or duplicate completions can be recognized. In toys3: `IOID` for FS actions, `ClientReq` for client requests. Chapter 2.

**CorruptObject.** The fact emitted when an object file fails its header, length or CRC check on read (`toys3/core/fact.go:27-32`). The reply is 500, not Halt. Counted in `Stats.CorruptObjects`. Chapters 7, 14.

**Crash.** A simulated power loss of the server: FakeFS resolves volatile state, every queued entry of the dead incarnation is dropped, outstanding requests are abandoned, and a restart is scheduled (`Simulator.crash`, `toys3/sim/simulator.go:275-306`). A `Halt` is the same kind of crash. Chapter 10.

**CrashReport.** What `FakeFS.Crash` returns: counts of kept, lost and torn changes (`toys3/sim/fakefs.go:453-455`). Logged on the crash line. Chapter 11.

**Current view.** What reads see now: `dirent.cur` pointing at `inode.volatile` (`FakeFS.Current`, `toys3/sim/fakefs.go:542`). Chapter 11.

**DefaultConfig.** `BaseConfig` plus per-seed swarm choices of key count, client count, crash rate and EIO rate drawn from a stream separate from the run's (`toys3/sim/config.go:135-143`). What every test and campaign uses. Chapter 12.

**Definite.** The `Outcome` of a request that got 200, 400 or 404 (`toys3/sim/oracle.go:17`): it happened exactly as replied, at one instant in `[Invoke, Return]`. Chapter 13.

**Deliver.** `KindDeliver`: a queue entry that hands an event to the current server incarnation's `Step` (`toys3/sim/queue.go:15`). Distinct from fs-land. Chapter 10.

**detcheck.** The go/analysis checker in `tools/detcheck/detcheck.go` that enforces the determinism rules on `core/`, `sim/`, `toys3/core`, `toys3/sim`: import allow-list; no goroutines, channels, select, map range, reflect map iteration, or package-level variables (except error sentinels and `_`). Suppress with `//detcheck:ok <reason>`. Chapter 8.

**Dirent.** A directory entry in the FakeFS with two versions: `cur` (visible) and `durable` (survives a crash) (`toys3/sim/fakefs.go:67-71`). 0 means absent. Chapter 11.

**Dirty.** An inode whose volatile content differs from its durable content (`toys3/sim/fakefs.go:63`). Only dirty inodes can lose or tear content at a crash. Chapter 11.

**Durable view.** What would survive a crash in which the adversary loses everything volatile: durable entries pointing at durable content (`FakeFS.Durable`, `toys3/sim/fakefs.go:536-539`). Invariants (b) and (e) check it. Chapter 11.

**DurableGen.** A counter bumped whenever durable state may have changed (`toys3/sim/fakefs.go:448-450`), so `ObjNeverTorn` can skip steps where nothing durable changed. Chapter 14.

**EIO / ErrIO.** The catch-all I/O error code: "the operation may or may not have taken effect" (`toys3/core/types.go:34`). Injected per operation with `EIOProb`. Chapters 4, 7, 11.

**Emit.** The action that records a `Fact` (`toys3/core/action.go:83-85`). No effect on behaviour. Chapter 3.

**Entry.** One item in the simulator's queue: time, tie, seq, kind, incarnation and a payload (`toys3/sim/queue.go:47-59`). Plain data. Chapter 10.

**Env.** The interface through which nondeterminism enters the core: `Rand` and `Buggify` (`toys3/core/env.go:5-8`). Time is *not* in it; it arrives on each event. Chapter 3.

**Event.** An input to `Step`: `At` plus a `Body` from a closed set (`toys3/core/event.go:5-12`). Chapter 3.

**Fact.** A structured observation the server emits for invariants and tracing: `Committed`, `Recovered`, `CorruptObject` (`toys3/core/fact.go`). Chapter 3.

**Factory.** `func(env Env, cfg Config) Machine`: builds a fresh server for one boot (`toys3/core/machine.go:22-25`). Taking a factory is what lets the simulator run mutants. Chapters 3, 15.

**Fail-stop.** The policy of halting (and recovering from disk) when in-memory state can no longer be trusted, rather than continuing to serve. Implemented with `Halt`. Chapter 7.

**Failed.** The `Outcome` of a request that got 500 (`toys3/sim/oracle.go:18`): a mutation may or may not have taken effect. Chapter 13.

**FakeFS.** The simulator's in-memory filesystem with crash semantics (`toys3/sim/fakefs.go:98-108`). Chapter 11.

**Fold.** The mixing function for the running trace hash (`toys3/sim/hash.go:7-11`). Chapter 8.

**fs-land.** `KindFSLand`: the queue entry at which an FS action's effect is applied to the FakeFS (`toys3/sim/queue.go:16`). Its completion is delivered later, after `FSPlan.Reply`. A crash in between gives "it happened, but nobody was told". Chapter 10.

**FSPlan.** The fault decision and latencies for an FS action, drawn when the action is *issued* (`toys3/sim/fakefs.go:46-52`), so RNG consumption follows issue order. Chapter 11.

**FsyncGate.** A FakeFS mode modelling Linux after a failed fsync: the inode is "poisoned", later fsyncs return OK, and the data never reaches disk until a fresh write (`toys3/sim/fakefs.go:40-43, 315-333`). Named after the 2018 PostgreSQL incident. Chapters 7, 11.

**Group.** In the oracle, a set of keys whose possible states are carried jointly between windows (`toys3/sim/oracle.go:347-352`). Usually one key per group; COPY or correlated crash outcomes can tie keys together. Chapter 13.

**Halt.** The action by which the server asks to be killed and restarted (`toys3/core/action.go:75-79`). Always the last action of its step. In simulation it is a crash; in production the process exits. Chapter 7.

**Incarnation.** One boot of the server, numbered from 1 (`toys3/sim/queue.go:5-9`). Every server-bound queue entry carries one; a crash drops all entries of the dead incarnation. Chapter 10.

**Indetermined.** `Stats.Indetermined`: requests outstanding at a crash, i.e. abandoned (`toys3/sim/simulator.go:282-283`). Chapter 16.

**Inode.** A file's content in the FakeFS, independent of its names, with `durable`, `volatile`, `dirty` and `poisoned` (`toys3/sim/fakefs.go:59-65`). Two names can point at one inode. Chapter 11.

**Invariant.** A check run after every simulator step and once at the end (`toys3/sim/invariant.go:37-41`). Five by default. Chapter 14.

**IOID.** Correlates a filesystem action with its completion; unique within one incarnation, assigned by `Server.newIO` (`toys3/core/types.go:22-25`, `server.go:155-158`). Also names the temp file. Logged as `io42`. Chapter 3.

**Linearizable.** A history is linearizable if every operation can be assigned a single instant within its invocation-to-response interval such that the operations, executed sequentially in that order, produce the observed results. Invariant (a); checked by the `Oracle` (`toys3/sim/oracle.go`). Chapter 13.

**Liveness.** Invariant (d): every request issued in QuietTail never gets a 500 (checked every step), and at the end the server is alive, its final incarnation recovered, and no op is pending (`toys3/sim/invariant.go:176-211`). Chapter 14.

**LogEntry.** One line of the event log (`toys3/sim/config.go:183-192`), rendered by `LogEntry.String` (`toys3/sim/describe.go:118-137`). Appendix F.

**Machine.** A hashable state machine: `StateMachine` + `Hashable` (`toys3/core/machine.go:16-20`). What a runtime drives. Chapter 3.

**Mutant.** A deliberately broken variant of the server, built by wrapping the real one so that exactly one protocol step is wrong (`toys3/sim/mutants_test.go`). Chapter 15.

**Oracle.** The component that records the client-visible history and checks it for linearizability against a sequential key-value model (`toys3/sim/oracle.go:53-88`). Chapter 13.

**Phase.** The server's lifecycle stage: booting, recovering, serving, halted (`toys3/core/server.go:11-19`). Chapter 5.

**Poisoned.** See FsyncGate.

**QuiesceEvery.** The interval at which clients pause until every request has replied, creating quiescent points that bound oracle windows (`toys3/sim/workload.go:30-34`). 8 ms by default. Chapter 12.

**Quiescent point.** A moment with no op pending. The oracle checks one window at each (and at each crash, which abandons everything pending). Chapter 13.

**QuietTail.** The phase after Chaos (`Config.QuietTail`, 60 ms by default): no new faults planned, no injected crashes, clients keep issuing, and liveness is judged. Ends at `KindTailEnd`. Chapter 10.

**Rand.** The run's single seeded random stream, SplitMix64 with a call counter (`toys3/sim/rand.go:10-61`). Chapter 8.

**Recovered.** The fact emitted once per boot when recovery finishes, carrying the number of temp files removed (`toys3/core/fact.go:21-25`). Chapter 5.

**Recovery.** On `Started`: list `tmp/`, unlink every leftover, fsync `tmp/`, emit `Recovered`, then start queued requests (`toys3/core/server.go:170-246`). Any error halts. Chapter 5.

**Sans-I/O.** The design pattern in which protocol logic consumes inputs and produces outputs as data, and never performs I/O itself. The `Step(Event) []Action` shape. Chapter 2.

**Seed.** The single number from which a run is derived: the run's RNG seed, and (XORed with a constant) the swarm configuration. Chapters 1, 12, 16.

**Self-describing value.** A PUT payload whose first 12 bytes encode the writer's `ClientReq` and length, and whose remaining bytes are a pure function of those (`MakeValue`, `toys3/sim/workload.go:157-176`). Any bytes read back name their writer, or are rejected as garbage by `ValueWriter`. Chapter 12.

**Skipper.** The mutant wrapper that hides selected actions from the runtime and feeds the inner server an immediate OK completion instead (`toys3/sim/mutants_test.go:44-74`). Chapter 15.

**StateHash.** FNV-1a over the server's entire state in a fixed field order (`toys3/core/server.go:481-508`). Folded into the trace hash after every delivery. Chapter 8.

**Step.** (1) The server's single entry point, `Step(Event) []Action`. (2) One iteration of the simulator's main loop: one queue entry popped and handled (`Simulator.step`). Context disambiguates.

**Swarm testing.** Varying the test configuration per seed (which features and faults are on, how many keys, etc.) rather than running every seed with one "balanced" configuration. Chapter 12.

**Tie.** A random number drawn when an entry is scheduled (`toys3/sim/queue.go:72-81`), used to order entries with the same `At`. Chapter 10.

**Torn write.** A file that a crash leaves holding a strict prefix of its un-synced content (`TornWriteProb`, `toys3/sim/fakefs.go:495-497`). Chapters 4, 11.

**Trace hash.** The running hash of a run: step number, time, entry kind and incarnation, every event and action, the server's `StateHash`, and the FakeFS hash, folded after every step (`toys3/sim/simulator.go:94-96, 172-183`). Two runs of one seed must agree. Chapter 8.

**Unknown.** The oracle's widened value for a key: "present or absent, with any value" (`toys3/sim/oracle.go:298-302`). Any read is accepted and fixes the value. Chapter 13.

**Volatile.** The version of a file's content that reads see now (`inode.volatile`). Becomes durable only by fsync, writeback, or luck at a crash. Chapter 11.

**WGL.** Wing–Gong–Lowe: a depth-first search for a linearization, with Lowe's memoization of (set of linearized ops, model state). `search.dfs` (`toys3/sim/oracle.go:580-639`). Chapter 13.

**White-box invariant.** An invariant that inspects ground truth the clients cannot see, here the FakeFS's durable view: `AckedIsDurable`, `ObjNeverTorn`. Chapter 14.

**Widening.** Replacing a too-large set of possible states with one state in which disagreeing keys are `unknown` (`widenStates`, `toys3/sim/oracle.go:489-511`). Sound, not complete. Counted in `Stats.Widenings`. Chapter 13.

**Window.** The ops between two quiescent points, checked together by the oracle. Chapter 13.

**Writeback.** The FakeFS's model of the kernel flushing one random un-synced change on its own (`WritebackProb`, `FakeFS.writeback`, `toys3/sim/fakefs.go:195-232`). Chapter 11.

---

## Appendix A: Command cheat sheet

All commands are run from the repository root. Each was run while writing this book.

### Tests

```sh
go test ./toys3/core -v                                  # server unit tests (exact action sequences)
go test ./toys3/core -run TestErrorPolicy -v             # the error table, one subtest per row
go test ./toys3/prod -v                                  # end-to-end HTTP on a temp dir; Halt stops the loop
go test ./toys3/sim                                      # everything in the simulator package (~5 s)
go test ./toys3/sim -run TestSeeds -seed=132 -v          # replay one seed; event log printed on failure
go test ./toys3/sim -run 'TestSeeds$' -seeds=500 -v      # seeds 1..500, aggregate stats line
go test ./toys3/sim -run TestSeeds -duration=2m -v       # seeds 1,2,... for two minutes
go test ./toys3/sim -run TestDeterminismCanary -v        # same seed twice; log does not perturb
go test ./toys3/sim -run TestCoverage -v                 # every fault family fires in 200 seeds
go test ./toys3/sim -run TestCorruptObjectUnreachableWithoutBitFlips
go test ./toys3/sim -run TestCatchesBugs -v              # all seven mutants, all + black-box invariants
go test ./toys3/sim -run 'TestCatchesBugs/d2' -v         # one mutant
go test ./toys3/sim -run TestCatchesBugs -mutant-seeds=2000 -v
go test ./toys3/sim -run 'TestOracle' -v                 # hand-built histories
go test ./toys3/sim -run 'TestFakeFS' -v                 # crash semantics of the fake FS
go test ./toys3/sim -run 'TestQueue' -v                  # queue ordering and dropping
go test ./tools/detcheck                                 # the analyzer's own test
```

### Campaigns

```sh
go run ./toys3/cmd/toys3sim -duration 10m -check-determinism
go run ./toys3/cmd/toys3sim -duration 15s -workers 2 -check-determinism
go run ./toys3/cmd/toys3sim -duration 30m -start-seed 1000000 -det-every 5 -max-failures 5
```

### Determinism gate

```sh
scripts/ci-determinism.sh                                    # detcheck + audits + canaries
go run ./tools/detcheck ./toys3/core/... ./toys3/sim/...     # silent and exit 0 when clean
go run ./tools/detcheck ./toys3/prod/...                     # shows what detcheck rejects (expected)
scripts/audit-dep.sh github.com/foo/bar@v1.2.3               # audit a third-party module (network)
```

### Running the real server

```sh
go run ./toys3/cmd/toys3 -dir /tmp/toys3-data -addr 127.0.0.1:8080
curl -X PUT --data-binary 'hello world' http://127.0.0.1:8080/photos/cat.jpg       # 200
curl -i http://127.0.0.1:8080/photos/cat.jpg                                       # 200 hello world
curl -X PUT -H 'x-amz-copy-source: /photos/cat.jpg' http://127.0.0.1:8080/backup/cat.jpg   # 200
curl -X DELETE http://127.0.0.1:8080/photos/cat.jpg                                # 200
curl -i http://127.0.0.1:8080/photos/cat.jpg                                       # 404
curl -i -X POST http://127.0.0.1:8080/photos/cat.jpg                               # 405
od -A d -t x1 -c '/tmp/toys3-data/obj/backup%2Fcat%2Ejpg'                          # header + payload
```

### Bisecting and shrinking

```sh
git bisect start <bad> <good>
git bisect run sh -c 'go test ./toys3/sim -run TestSeeds -seed=4711 >/dev/null'
```

---

## Appendix B: File index

### `toys3/core` (deterministic, detcheck-clean)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Package doc: on-disk layout, durable write protocol, error table, concurrency, recovery | the whole file is documentation (1-110) |
| `types.go` | Basic types | `Time` 5, `Duration` 8, `ClientReq` 20, `IOID` 25, `ErrCode` 29 (`OK`, `ErrNotFound`, `ErrIO` 31-35), `Status` 50 (200/400/404/500, 52-57) |
| `event.go` | Inputs to `Step` | `Event` 5, `EventBody` 12, `Started` 18, `Request` 25, `Op` 32, `Put` 35, `Get` 41, `Delete` 44, `Copy` 48, `FSWriteDone` 58, `FSFsyncDone` 64, `FSRenameDone` 71, `FSFsyncDirDone` 77, `FSReadDone` 84, `FSUnlinkDone` 91, `FSListDone` 97 |
| `action.go` | Outputs of `Step` | `Action` 7, `Reply` 13, `FSWrite` 29, `FSFsync` 36, `FSRename` 44, `FSFsyncDir` 50, `FSRead` 56, `FSUnlink` 62, `FSList` 68, `Halt` 79, `Emit` 85 |
| `fact.go` | Observations for invariants | `Fact` 7, `Committed` 14, `Recovered` 23, `CorruptObject` 30 |
| `env.go` | Injected nondeterminism | `Env` 5, `Rand` 12, `Site` 19, `Buggify` 24 |
| `machine.go` | Shapes runtimes drive | `StateMachine` 5, `Hashable` 12, `Machine` 17, `Factory` 25, `NewMachine` 28 |
| `format.go` | On-disk format and paths | `Magic`/`HeaderSize`/`MaxNameLen` 11-17, `Checksum` 20, `EncodeObject` 23, `DecodeObject` 35, `EscapeKey` 55, `UnescapeKey` 72, `ValidKey` 99, `ObjDir` 104, `TmpDir` 107, `ObjPath` 110, `TmpPath` 115 |
| `server.go` | The state machine | `Config` 4, `phase` 12, `recStep` 22, `opKind` 31, `reqStep` 41, `request` 56, `touches` 69, `conflicts` 74, `Server` 87, `New` 104, `Step` 109, `ioOf` 135, `newIO` 155, `halt` 163, `onStarted` 170, `onRecoveryDone` 183, `fsyncTmpDir` 241, `onRequest` 259, `canRun` 299, `startRunnable` 313, `start` 324, `writeTmp` 337, `issue` 344, `finish` 365, `commit` 377, `failHalt` 387, `onRequestIO` 393, `StateHash` 483, `hasher` 512 |
| `server_test.go` | Hand-driven harness with exact expected action strings | `harness` 22, `describe` 167, `TestHappyPath` 222, `TestCommittedSum` 238, `TestNotFound` 252, `TestBadRequest` 263, `TestSameKeySerialized` 273, `TestDifferentKeysConcurrent` 291, `TestCopyHoldsBothKeys` 298, `TestCopySameKey` 313, `TestRecovery` 322, `TestRecoveryEmptyAndErrors` 343, `TestCorruptObject` 360, `TestErrorPolicy` 382, `TestStaleIOIgnored` 421, `TestStateHash` 445 |

### `toys3/sim` (deterministic, detcheck-clean)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Main loop in pseudocode | (1-34) |
| `rand.go` | Seeded RNG | `Rand` 10, `NewRand` 16, `Uint64` 19, `Intn` 30, `Float64` 46, `Chance` 49, `Between` 52, `Calls` 61 |
| `hash.go` | Hash helpers | `Fold` 7, `hashString` 14, `hashBytes` 24 |
| `queue.go` | Global priority queue | `Incarnation` 9, `EntryKind` 12 (kinds 14-23), `Entry` 48, `Queue` 65, `Schedule` 72, `Pop` 84, `DropIncarnation` 102, `less` 125 |
| `config.go` | Run configuration and results | `Config` 10, `Result` 45, `Stats` 59, `Stats.Add` 81, `Stats.String` 104, `statusIndex` 114, `DefaultConfig` 135, `BaseConfig` 146, `Failure` 176, `LogEntry` 184, `Run` 196 |
| `simulator.go` | The main loop | `Simulator` 11, `New` 41, `Run` 82, `handle` 125, `deliver` 170, `callStep` 219, `reply` 228, `wake` 240, `wakeParked` 255, `maybeCrash` 265, `crash` 275, `restart` 309, `build` 327, `afterStep` 338, `fail` 364, View methods 385-394, `env` 400 |
| `fakefs.go` | Adversarial filesystem | `FSFaults` 13, `FSPlan` 48, `InodeID` 57, `inode` 60, `dirent` 68, `fdir` 75, `FakeFS` 98, `NewFakeFS` 119, `Plan` 136, `Land` 155, `writeback` 196, `split` 238, `lookup` 251, `setEntry` 264, `write` 279, `fsync` 315, `rename` 335, `fsyncDir` 359, `read` 376, `unlink` 396, `list` 413, `names` 433, `DurableGen` 450, `CrashReport` 453, `Crash` 458, `SetQuiet` 522, `Snapshot` 532, `Durable` 539, `Current` 542, `DurableFile` 558, `CurrentFile` 567, `Hash` 593 |
| `workload.go` | Simulated clients | `OpMix` 11, `WorkloadConfig` 18, `Workload` 50, `NewWorkload` 59, `Think` 86, `Issue` 93, `Complete` 124, `Crash` 136, `Idle` 148, `ValueHeader` 159, `MakeValue` 163, `ValueWriter` 181 |
| `oracle.go` | History and linearizability | `Outcome` 13, `HistOp` 23, `History` 39, `Value` 48, `Oracle` 74, `Invoke` 110, `Return` 118, `Crash` 142, `CheckQuiescent` 176, `CheckFinal` 185, `modelState` 200, `wop` 239, `apply` 247, `unknown` 302, `window` 307, `search` 337, `group` 349, `opKeys` 355, `maxStartStates` 390, `check` 396, `widenStates` 492, `factor` 517, `(*Oracle).search` 555, `dfs` 580, `memoKey` 641, `describeOp` 688 |
| `invariant.go` | The five invariants | `TimedFact` 12, `View` 20, `Invariant` 37, `DefaultInvariants` 44, `BlackBoxInvariants` 51, `NoCorruptReads` 58/88, `AckedIsDurable` 64/114, `ObjNeverTorn` 70/144, `Linearizable` 74/165, `Liveness` 81/175 |
| `describe.go` | Log rendering and hashing | `DescribeOp` 11, `describePayload` 29, `DescribeEvent` 46, `DescribeAction` 71, `DescribeFact` 102, `LogEntry.String` 118, `FormatLog` 140, `fmtTime` 150, `hashOp` 155, `hashEvent` 170, `hashAction` 199 |
| `sim_test.go` | Drivers and canaries | flags 11-15, `repro` 17, `TestDeterminismCanary` 24, `TestSeeds` 46, `TestCoverage` 86, `TestCorruptObjectUnreachableWithoutBitFlips` 124 |
| `fakefs_test.go` | FakeFS semantics | `TestFakeFSUnsyncedWriteMayVanish` 43, `TestFakeFSRenameWithoutDirFsyncMayVanish` 67, `TestFakeFSFullySyncedSurvives` 93, `TestFakeFSTornPrefix` 115, `TestFakeFSEIOMayOrMayNotApply` 132, `TestFakeFSFsyncGateLies` 155, `TestFakeFSNeverAliasesHandedOutSlices` 171 |
| `oracle_test.go` | Hand-built histories | `TestOracleAcceptsLinearizableConcurrency` 53, `TestOracleRejectsReadNotYetPossible` 72, `TestOracleRejectsStaleRead` 85, `TestOracleRejectsAckedThenLost` 96, `TestOracleFailedMutationMayOrMayNotApply` 109, `TestOracleRejectsObservedThenVanished` 142, `TestOracleRejectsGarbageAndMustRespectRealTime` 158, `TestOracleCarriesUncertaintyAcrossWindows` 173, `TestOracleWidensInsteadOfGivingUp` 193 |
| `queue_test.go` | Queue | `TestQueueOrdersByTimeThenTieThenSeq` 9, `TestQueueTieBreakDependsOnSeed` 34, `TestQueueClampsPastAndDropsIncarnation` 68 |
| `mutants_test.go` | Broken servers | `fakeOK` 18, `truncAfterHalt` 35, `skipper` 48, `isObjFsyncDir` 78, `mutSkipDirFsync` 86, `mutSkipFileFsync` 91, `mutAckBeforeRename` 100, `noHalt` 112, `mutNoHaltOnError` 120, `mutNoHaltOnDirFsyncError` 126, `noHalt.Step` 130, `rewrite` 210, `eventRewriter` 289, `mutSkipCRC` 303, `mutSkipTempCleanup` 316, `mutants` 331, `mutantSeeds` 341, `firstCatch` 345, `TestCatchesBugs` 361 |

### `toys3/prod` (production; not checked by detcheck)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Runtime diagram | (1-15) |
| `env.go` | Production `Env`: real randomness, buggify off | `Env` 10 |
| `executor.go` | Real `os` calls | `Executor` 17, `OSExecutor` 33, `Execute` 36, `run` 41, `writeFile` 75, `syncPath` 88, `code` 102, `PrepareDir` 117 |
| `loop.go` | The event loop | `ErrHalted` 15, `ErrStopped` 18, `Loop` 21, `NewLoop` 39, `Run` 60, `dispatch` 78, `post` 103, `Submit` 121 |
| `http.go` | S3 subset over HTTP | `CopySourceHeader` 16, `ErrMethodNotAllowed` 19, `Handler` 32, `ServeHTTP` 39, `ParseRequest` 81, `splitKey` 110 |
| `prod_test.go` | End-to-end tests | `startServer` 26, `TestEndToEnd` 71, `failFsync` 111, `TestHaltStopsLoop` 121 |

### `toys3/cmd`

| File | Purpose | Key symbols (line) |
|---|---|---|
| `toys3/main.go` | Server binary: `-dir`, `-addr`, `-max-object`; exits non-zero on Halt | `main` 26 |
| `toys3sim/main.go` | Parallel campaign runner | flags 31-36, shared state 39-48, `record` 53, workers 63-90, `progress` 92, exit 118-125 |

### Supporting files

| File | Purpose |
|---|---|
| `toys3/DESIGN.md` | The design document; invariant table 141-147, mutation table 188-196 |
| `loop.md` | The 15 DST design rules |
| `tools/detcheck/detcheck.go` | The analyzer; test-main skip 95-99, file loop 106-184, `suppressions` 189 |
| `tools/detcheck/allow.txt` | Import allow-list |
| `tools/detcheck/testdata/src/a/a.go` | Analyzer test cases (`// want` comments) |
| `scripts/ci-determinism.sh` | CI gate; SIGPIPE-safe canary detection 30-39 |
| `.github/workflows/determinism.yml` | vet, test, gate |

---

## Appendix C: Invariants and mutants, cross-referenced

### Invariants (`toys3/DESIGN.md:141-147`, `toys3/sim/invariant.go`)

| | Invariant | Type | Implementation | When checked | Chapters |
|---|---|---|---|---|---|
| a | Every 200 GET is linearizable against a per-key register model (COPY reads src and writes dst atomically) | black-box | `Linearizable` → `Oracle.CheckQuiescent` / `CheckFinal` | every quiescent point (incl. crashes) and end | 13, 14 |
| b | Acked mutations survive crashes unless superseded | black-box (via a) **and** white-box (`AckedIsDurable`) | oracle ("crashes erase nothing"); `AckedIsDurable` on the `Committed` step | quiescent points; eagerly every step | 13, 14 |
| c | No torn or corrupt bytes returned with 200 | black-box | `NoCorruptReads` | every step | 12, 14 |
| d | Every request replies while alive; the final boot recovers; no 500 for a request issued in QuietTail | black-box | `Liveness` | 500s every step; rest at end | 14 |
| e | Every file under `obj/` in the durable view is a complete object | white-box | `ObjNeverTorn` | whenever `DurableGen` changes | 11, 14 |

### Mutants (`toys3/DESIGN.md:188-196`, re-run for this book; all matched)

| Mutant | Broken step | Wrapper | Caught by (all invariants) | First seed | Black-box only | Chapters |
|---|---|---|---|---|---|---|
| a `a-skip-dir-fsync` | no `FSFsyncDir(obj/)` | `skipper` | acked-is-durable (step 46) | 1 | linearizable, 1 | 4, 15 |
| b `b-skip-file-fsync` | no `FSFsync(tmp)` | `skipper` | obj-never-torn (step 44) | 1 | liveness, 2 | 4, 15 |
| c `c-ack-before-rename` | reply 200, then rename and fsync | `skipper` with `deferReal` | acked-is-durable (step 42) | 1 | linearizable, 1 | 4, 15 |
| d `d-no-halt-on-error` | 500 without Halt after any post-write error | `noHalt` | obj-never-torn (step 241) | 1 | liveness, 20 | 7, 15 |
| d2 `d2-no-halt-on-dir-fsync-error` | same, only for failed `FSFsyncDir(obj/)` | `noHalt{dirOnly}` | linearizable (step 380) | 132 | linearizable, 132 | 7, 15 |
| e `e-skip-crc` | CRC not verified on read | `eventRewriter` | no-corrupt-reads (step 657) | 18 | no-corrupt-reads, 18 | 4, 14, 15 |
| f `f-skip-temp-cleanup` | recovery ignores leftover temps | `eventRewriter` | linearizable (step 1750) | 20 | linearizable, 20 | 5, 11, 15 |

Ablation for f (first catching seed; Chapter 17 Part II §13): fixed config without writeback 429, fixed with writeback 36, swarm without writeback 24, swarm with writeback 20. Mutant e without read corruption: not caught in 1,000 seeds.

---

## Appendix D: The error policy

From `toys3/core/doc.go:68-82` and `toys3/core/server.go:393-477`; verified by `TestErrorPolicy` (`toys3/core/server_test.go:382-419`) and `TestRecoveryEmptyAndErrors`.

| Step | ENOENT | EIO | Why | Code |
|---|---|---|---|---|
| `FSRead` (GET) | 404 | 500 | Nothing changed. | `server.go:395-418` |
| `FSRead` (COPY src) | 404 | 500 | Nothing changed; dst untouched. | same |
| Read decodes badly | — | 500 + `CorruptObject` fact, no Halt | File already durable; restart would not fix it. | `server.go:410-413` |
| `FSWrite(tmp)` | 500 | 500 | Only a temp file changed; next recovery deletes it. | `server.go:420-430` |
| `FSFsync(tmp)` | 500 + Halt | 500 + Halt | fsyncgate: page cache untrustworthy. | `server.go:432-440` |
| `FSRename(tmp→obj)` | 500 + Halt | 500 + Halt | May have applied, visibly but not durably. | `server.go:442-450` |
| `FSUnlink(obj)` (DELETE) | 404 | 500 + Halt | May have applied, visibly but not durably. | `server.go:452-464` |
| `FSFsyncDir(obj)` | 500 + Halt | 500 + Halt | fsyncgate, and the rename/unlink above is visible but not durable. | `server.go:466-474` |
| Recovery `FSList(tmp)` | Halt | Halt | Recovery must finish before serving; next boot retries. | `server.go:185-192` |
| Recovery `FSUnlink(tmp/x)` | ignored (already gone) | Halt | | `server.go:206-217` |
| Recovery `FSFsyncDir(tmp)` | Halt | Halt | | `server.go:225-232` |

Rule of thumb: **any error at or after the first step that can touch `obj/` halts; errors before it are a plain 500.** `Halt` is always the last action of its step, preceded by the 500 reply if any; after it the server ignores every event (`server.go:110-112`). Bad requests (invalid key, oversize PUT, unknown op) get 400 immediately and never enter the queue (`server.go:259-284`).

Production mapping: `fs.ErrNotExist` → `ErrNotFound`; any other error → `ErrIO`, logged (`toys3/prod/executor.go:102-112`). A Halt makes `Loop.Run` return `ErrHalted`, and `cmd/toys3` exits via `log.Fatalf` for a supervisor to restart (`toys3/cmd/toys3/main.go:48-56`).

---

## Appendix E: `Stats` fields reference

Defined at `toys3/sim/config.go:59-78`, rendered by `Stats.String` (`config.go:104-111`) in this order.

| Printed | Field | Incremented at | Aggregation | Meaning |
|---|---|---|---|---|
| `reqs` | `Requests` | `simulator.go:249` (`wake`) | sum | Requests issued by clients |
| `200` `400` `404` `500` | `ByStatus[0..3]` | `simulator.go:235` (`reply`) | sum | Replies by status; any other status counts as 500 |
| `indet` | `Indetermined` | `simulator.go:283` (`crash`) | sum | Requests abandoned by a crash |
| `fsops` | `FSOps` ← `FSCounters.Landed` | `fakefs.go:156`; copied `simulator.go:114` | sum | FS actions that landed |
| `eio` | `EIOs` ← `FSCounters.EIOs` | `fakefs.go:186-188` | sum | Landed actions completed with ErrIO |
| `crashes` | `Crashes` ← `Result.Crashes` | `simulator.go:298`; copied 121 | sum | All crashes, including Halts |
| `halts` | `Halts` | `simulator.go:204` | sum | Crashes requested by the server |
| `midput` | `CrashesMidPut` | `simulator.go:292-294` | sum | Crashes with a PUT or COPY outstanding |
| `torn` | `TornFiles` | `simulator.go:277` | sum | Files cut to a prefix by a crash |
| `lost` | `LostChanges` | `simulator.go:278` | sum | Un-synced changes discarded (incl. poisoned inodes) |
| `kept` | `KeptChanges` | `simulator.go:279` | sum | Un-synced changes a crash kept anyway |
| `readcorrupt` | `ReadCorruptions` ← `FSCounters` | `fakefs.go:388-391`; copied `simulator.go:116` | sum | Reads returned with a flipped bit (counted at land) |
| `corruptobj` | `CorruptObjects` | `simulator.go:194-195` | sum | `CorruptObject` facts (counted at delivery) |
| `tempsrm` | `TempsRemoved` | `simulator.go:196-197` | sum | Temp files removed by recovery |
| `windows` | `Windows` ← `OracleStats` | `oracle.go:402`; copied `simulator.go:118` | sum | Linearizability windows checked |
| `maxwin` | `MaxWindowOps` | `oracle.go:403` | **max** | Ops in the largest window |
| `widened` | `Widenings` | `oracle.go:465, 480` | sum | Times the oracle widened keys to unknown |

Identities that hold for the correct server: `200 + 400 + 404 + 500 + indet == reqs`; `corruptobj ≤ readcorrupt`; `halts ≤ crashes`; `midput ≤ crashes`.

Not printed: `FSCounters.Writebacks` (`fakefs.go:115`), `OracleStats.MaxVisited` and `MaxStates` (`oracle.go:94-95`). They are available programmatically.

---

## Appendix F: Queue entry kinds and the log format

### `EntryKind` (`toys3/sim/queue.go:14-23`)

| Kind | Printed | `Inc` set? | Scheduled by | Handled by | Logged? |
|---|---|---|---|---|---|
| `KindDeliver` | `deliver` | yes | `wake` (Request), `restart` (Started), fs-land (completion) | `deliver` → `Step` | yes: event `=>` actions |
| `KindFSLand` | `fs-land` | yes | `deliver` for each FS action | `FakeFS.Land`, schedules the completion | yes (Log mode): `land A -> E` |
| `KindClientWake` | `client-wake` | no | `New`, `reply`, `wakeParked` | `wake`: issue or park | no |
| `KindCrash` | `crash` | no | not scheduled in the current code; crashes are performed directly by `maybeCrash` and `Halt` | `crash` if alive | crash lines are written by `crash` itself |
| `KindRestart` | `restart` | no | `New` (t=0), `crash` | `restart` | yes: `boot` |
| `KindQuiesce` | `quiesce` | no | `New`, `afterStep` | sets `quiescing` | yes |
| `KindQuiet` | `quiet` | no | `New` at `Chaos` | stops faults and buggify | yes |
| `KindTailEnd` | `tail-end` | no | `New` at `Chaos+QuietTail` | clients stop | yes |

(No code path schedules a `KindCrash` entry today; the kind exists, and is handled, for a future "crash at time T" feature. Its other use is as the `Kind` of log entries written by `crash` and, confusingly, by `fail`.)

Queue order: `(At, Tie, Seq)` ascending (`queue.go:125-134`). Scheduling in the past clamps to now (`queue.go:73-75`). `DropIncarnation` removes `KindDeliver` and `KindFSLand` entries of one incarnation (`queue.go:102-120`).

### Log line format (`toys3/sim/describe.go:118-137`)

```
#<step:-6> t=<fmtTime:-12> inc=<n> <kind:-11> [<DescribeEvent>] [=> <DescribeAction> ...] [<note>]
```

`fmtTime` prints `<ms>.<6-digit remainder>ms` (nanosecond resolution). Examples from seed 1:

```
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#44     t=7.233120ms   inc=1 fs-land     land FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} -> FSRenameDone{io12 OK}
#233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Halt{fsync temp: EIO}
#233    t=53.112270ms  inc=1 crash       CRASH (halt: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#235    t=55.879722ms  inc=2 restart     boot
#1257   t=300.000000ms inc=0 quiet       chaos over: no more faults or crashes
```

Rendering vocabulary (`describe.go`):

| Text | Meaning |
|---|---|
| `c17` | `ClientReq` 17 |
| `io42` | `IOID` 42 of the current incarnation |
| `value-of-26` | bytes exactly equal to `MakeValue(26, len)` |
| `obj(value-of-26)` | an object file (valid header and CRC) whose payload is `value-of-26` |
| `obj(N bytes)` | a valid object file whose payload is not a workload value |
| `raw(N bytes)` | bytes that are neither a valid object nor a value (torn, bit-flipped) |
| `garbage(N bytes)` | in a history op: a body no PUT wrote |
| `nil` | no data (e.g. `FSReadDone` with ENOENT) |
| `Committed{c6 "k2" crc=9b35c638}` | the commit fact, with the payload CRC in hex |
| `Committed{c56 delete "k0"}` | a delete's commit fact |
| `[102..104 @82642556..84991187]` | in a history op: `InvSeq..RetSeq` (logical) and `Invoke..Return` (virtual ns) |

## Key takeaways

- The glossary terms with the most precise and least obvious meanings are: *landed* vs *delivered*, *durable* vs *current/volatile*, *abandoned* vs *failed* vs *definite*, *incarnation*, and *widening*. If those five are sharp, the rest of the book reads easily.
- The appendices are designed to be kept open: A for commands, B for "where is that function", D when reasoning about errors, E when reading a campaign summary, F when reading a log.

## Exercises

1. Without looking at the glossary, write one-sentence definitions of: fs-land, quiescent point, poisoned inode, widening, black-box invariant. Then compare.
2. Pick any five line references in Appendix B and check them with your editor. If any has drifted, you are on a newer commit; use `git log -p --follow` on the file to see what moved.
3. Using Appendix E, compute the identities for the 15-second campaign in Chapter 16. Do they hold?
4. Using Appendix F, write a `grep` or `awk` one-liner that extracts, from a seed's log, every crash line together with the line immediately before it. What fraction of crashes in seed 1 immediately follow an `fs-land`? Why is that fraction high? (Hint: where does `maybeCrash` get called?)
5. Add a `KindCrash` scheduler: a `Config.CrashAt []core.Duration` that schedules `KindCrash` entries at fixed virtual times. Use it to write a test that crashes exactly once, at a time you choose, during a PUT.

## Further reading

- Maurice Herlihy and Jeannette Wing, *"Linearizability: A Correctness Condition for Concurrent Objects"*, ACM TOPLAS 1990.
- Jeannette Wing and Chun Gong, *"Testing and Verifying Concurrent Objects"*, Journal of Parallel and Distributed Computing, 1993.
- Gavin Lowe, *"Testing for Linearizability"*, Concurrency and Computation: Practice and Experience, 2017.
- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal"*, OSDI 2014.
- Guy Steele, Doug Lea and Christine Flood, *"Fast Splittable Pseudorandom Number Generators"*, OOPSLA 2014 (SplitMix64).
- Daniel Lemire, *"Fast Random Integer Generation in an Interval"*, ACM TOMACS 2019 (the bounded `Intn`).
