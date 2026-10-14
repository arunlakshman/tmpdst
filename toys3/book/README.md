# toys3: Learning Deterministic Simulation Testing, Chapter by Chapter

This directory is a book about one small program and the simulator that tests it.

**toys3** is a single-node, S3-flavoured object server (PUT, GET, DELETE, COPY; one file per object; fsync before every 200) built as a pure `Step(Event) []Action` state machine. **toys3/sim** is a deterministic simulator that runs that exact server against an adversarial in-memory filesystem, crashes it thousands of times per second, records every client-visible operation, and checks the history for linearizability and durability. Everything is driven by one seeded random number generator, so every failure is reproducible from a single number.

The book walks through all of it: the ideas, the code (quoted with `path:line` references against commit `10777a2`), and real artifacts produced by running the code: event logs, failing seeds and invariant messages.

---

## Table of contents

| # | Chapter | What it covers |
|---|---|---|
| 0 | [Preface](00-preface.md) | What you will learn, how to read the book, the repository map, and a five-minute tour that runs the server's tests, a replayed seed, 200 seeds, the mutation suite and the CI gate. |
| 1 | [Why DST](01-why-dst.md) | The class of bugs that unit, integration and chaos tests miss (illustrated by a real "observed, then vanished" bug), the FoundationDB story, the three ingredients (one thread, simulated I/O and time, one seeded RNG), what DST is not, and how toys3 assembles the pieces. |
| 2 | [The architecture: Step(Event) []Action](02-architecture.md) | Why I/O inside logic makes simulation impossible, why mocks and synchronous interfaces fall short, the sans-I/O pattern, why Events and Actions are plain data, and all fifteen rules of `loop.md` mapped one by one onto toys3. |
| 3 | [The vocabulary](03-vocabulary.md) | Every type in `toys3/control`: times, IDs, error codes, statuses, every Event, every Action, the Storage port (`Actuation` and `Feedback`), every kind of `Telemetry`, the `Env`/`Buggify` seam (and the fact that the server does not use it yet), and the `Machine`/`Factory` contracts, with real log renderings. |
| 3a | [The domain model](03a-domain-model.md) | toys3 through domain-driven design and control theory: subdomains, the six bounded contexts and the context map (shared kernels, conformist adapters, the `GroundTruth` anti-corruption layer), why the `Controller` is the one aggregate and the per-key queue is its key interlock, telemetry as domain events, ports and adapters, the feedback-loop block diagram mapped onto the code (controller, storage (the textbook *plant*, and why toys3 does not use that word), actuation, feedback, dead time, feedback delay, disturbances, trip, interlock, permissive, modes, settling), and where the metaphor was deliberately not applied. |
| 4 | [The durability problem](04-durability.md) | How bytes actually become durable on POSIX: page cache, file vs directory fsync, atomic rename, torn writes. Why the temp → fsync → rename → fsync-dir dance is the minimal correct protocol. The on-disk object format (`toys3/format`) byte by byte (with a real hex dump), key escaping, temp-file naming and the on-disk layout. |
| 5 | [The controller, part 1: operating modes, recovery, queuing](05-server-lifecycle.md) | The controller's operating modes (startup, recovering, auto, tripped), its struct field by field, `Step` dispatch and `Feedback.ID()`, stale-completion handling, the recovery protocol line by line, why requests queue during startup and recovery, and real recovery traces including a resurrected temp file and a recovery that trips and retries. |
| 6 | [The controller, part 2: the key interlock and the four protocols](06-server-protocols.md) | Implicit per-key queues as a key interlock with a start permissive, why a single arrival order makes deadlock impossible, COPY holding two keys, and a step-by-step walk through PUT, GET, DELETE and COPY in `onRequestIO`, with the unit tests and a real trace as worked examples. |
| 7 | [Errors, fail-stop and the safety trip](07-errors-fail-stop.md) | The 500 vs 500+Trip table and the rule behind it ("a read must never see a value a crash can erase"), fsyncgate and how it is modelled, and a full dissection of mutant d2 at seed 132, plus mutant d and how the no-trip wrapper works. |
| 8 | [State hashing and determinism](08-hashing-determinism.md) | `StateHash`, FNV-1a, the running trace hash and what is folded into it, the determinism canary, SplitMix64 and Lemire's bounded integers, Go's determinism hazards, the `detcheck` analyzer and its allow-list, and two instructive tooling bugs: generated test mains and a SIGPIPE-silenced CI canary., and the trace hash as a refactoring safety net: the golden test, the frozen telemetry hash key, and the commit-by-commit proof that the DDD refactor changed no behaviour. |
| 9 | [The production runtime](09-production-runtime.md) | How the same controller runs for real: `OSExecutor`, the single-owner `Loop`, `Submit`, the S3-subset HTTP handler, `main.go` wiring, Trip as process exit, what DST does not cover here, and a real `curl` session with a hex dump of the stored object. |
| 10 | [The simulator](10-simulator.md) | The global queue ordered by `(At, Tie, Seq)`, virtual time, incarnations, the land/deliver split (dead time and feedback delay: "it happened but nobody was told"), crash and restart, parking and quiescence, run phases (Chaos and Settling), and the main loop line by line with real log excerpts. |
| 11 | [The storage model](11-fake-filesystem.md) | `StorageModel` and its `Disturbances`: Inodes with durable and volatile content, directory entries with durable and current versions, every operation's normal and EIO behaviour, background writeback, read corruption, fsyncgate, the three-phase crash resolution, and worked crash scenarios including a real resurrected-temp alias and how it corrupts data when recovery is broken. |
| 12 | [Workloads and swarm testing](12-workloads-swarm.md) | Clients, the op mix, self-describing values that name their writer, quiescent points, `BaseConfig` vs per-seed swarm variation in `DefaultConfig`, and measured ablations showing how fault-model realism and workload variety change how fast a bug is caught. |
| 13 | [The oracle: linearizability](13-oracle.md) | Linearizability from first principles, outcomes (definite, failed, abandoned), "crashes erase nothing", the sequential register model, the memoized Wing–Gong–Lowe search line by line (and why its memo is sound), per-key decomposition, carried uncertainty and widening, every oracle unit test worked by hand, real failure messages decoded, and the oracle read as a set-membership observer over the register model. |
| 13a | [The veritas: executable truth and conformance checks](13a-veritas.md) | The veritas (why the word replaces "reference model"), `veritas.Store` and `veritas.FS`, the veritas as the controller tests' fake, conformance checks C1 to C4 and the lockstep runner, input bias and when not to bias, shrinking (including whole-sequence renaming) with a worked shrunk failure, the coverage pass that made recovery's cleanup reachable, the mutant table's C1 column, case folding on macOS, the Copy size gap, why the oracle stays independent, and a section-by-section map onto the ShardStore paper (SOSP 2021). |
| 14 | [Invariants](14-invariants.md) | The five invariants line by line, white-box vs black-box checking, the protocol/panic/max-steps checks, coverage tests as invariants about the test itself, and how to write a new invariant. |
| 15 | [Mutation testing](15-mutation-testing.md) | Proving the tests have teeth: the wrapper techniques used to build mutants, every mutant dissected with its real failing output, the full story of mutant f, ablations, the functional mutants g to j and the conformance-check column, surviving mutants, and how to tell a weak test from an equivalent mutant. |
| 16 | [Running campaigns](16-campaigns.md) | `TestSeeds`, `toys3sim`, the summary line field by field, reproducing and logging any seed, running the conformance checks with `-seeds`, `-seed` and `-duration`, determinism double-runs, CI, and the results of a 10-minute campaign. |
| 17 | [Limitations and where to go next](17-limitations.md) | What the model does not cover, the documentation/code discrepancies and surprises found while writing this book, and a roadmap of extensions (buggify sites, a network layer, replication, coverage-guided seeds, applying it all to the Manifester skeleton). |
| 18 | [Glossary and appendix](18-glossary-appendix.md) | Glossary, command cheat sheet, file index with key symbols, invariant and mutation tables, the error policy table, and reference tables for stats and log formats., and an old-name → new-name table for the refactor. |

---

## Suggested reading paths

**The full path (recommended for a first read).** Read 0 → 18 in order (3a comes after 3, 13a after 13). Each chapter uses only ideas introduced earlier, and the exercises build on each other: by Chapter 15 you will be writing your own mutants and predicting which invariant catches them.

**The "I want to build a simulator" path.** 0, 1, 2, 3, 3a, then 10 → 16 (including 13a), then 17. Skim 4 to 7 when a chapter refers back to the durability protocol or the error policy. This path is for readers who already understand crash consistency and want the simulator machinery: queue, storage model, workload, oracle, invariants, mutation testing, campaigns.

**The "I care about storage correctness" path.** 0, 1, 4, 5, 6, 7, 11, 13a, 14, 15. This is the path for someone who wants to know *why* the durability protocol is what it is, and how each step was shown to be necessary by a mutant the simulator catches.

**The "I have one evening" path.** 1 (why), 2 (the shape), 7 (one bug, end to end), 13 §§ on outcomes and the WGL search, 15 (all the bugs). Then run the commands in the preface.

## Before you start

From the repository root:

```sh
go test ./toys3/control ./toys3/format ./toys3/prod  # controller, format and production runtime tests
go test ./toys3/sim -run TestSeeds -seed=1 -v         # replay one seed
go test ./toys3/sim -run TestCatchesBugs -v           # watch seven broken servers get caught
go test ./toys3/sim -run TestGolden -v                # the behaviour pin (Chapter 8, §8.12)
go test ./toys3/sim -run 'TestC[124]' -v              # conformance checks C1, C2, C4 (Chapter 13a)
```

Line references in this book are against commit `10777a2`. If they have drifted, search for the function name. The first edition was written against `210c1fa`; commits `9b65410..a4018d5` then refactored toys3 with domain-driven design and a control-theory vocabulary without changing any behaviour (every trace hash in the book is unchanged); `56b0d2e` later renamed the control-theory word *plant* to *storage*, and `10777a2` fixed comment drift, both golden-guarded. Chapter 3a explains the new names, and Chapter 18's appendix maps every old name to its new one. Commits `0f46dfe..3ff1385` then added the `veritas` packages and the conformance checks; Chapter 13a covers them, and its line references are against `3ff1385`. The rest of the book was updated where those commits changed a fact (the controller tests' harness, the mutant table, the test commands) but its line references still point at `10777a2`. The book itself does not change any code outside `toys3/book/`.
