# toys3: Learning Deterministic Simulation Testing, Chapter by Chapter

This directory is a book about one small program and the simulator that tests it.

**toys3** is a single-node, S3-flavoured object server (PUT, GET, DELETE, COPY; one file per object; fsync before every 200) built as a pure `Step(Event) []Action` state machine. **toys3/sim** is a deterministic simulator that runs that exact server against an adversarial in-memory filesystem, crashes it thousands of times per second, records every client-visible operation, and checks the history for linearizability and durability. Everything is driven by one seeded random number generator, so every failure is reproducible from a single number.

The book walks through all of it: the ideas, the code (quoted with `path:line` references against commit `210c1fa`), and real artifacts produced by running the code: event logs, failing seeds and invariant messages.

---

## Table of contents

| # | Chapter | What it covers |
|---|---|---|
| 0 | [Preface](00-preface.md) | What you will learn, how to read the book, the repository map, and a five-minute tour that runs the server's tests, a replayed seed, 200 seeds, the mutation suite and the CI gate. |
| 1 | [Why DST](01-why-dst.md) | The class of bugs that unit, integration and chaos tests miss (illustrated by a real "observed, then vanished" bug), the FoundationDB story, the three ingredients (one thread, simulated I/O and time, one seeded RNG), what DST is not, and how toys3 assembles the pieces. |
| 2 | [The architecture: Step(Event) []Action](02-architecture.md) | Why I/O inside logic makes simulation impossible, why mocks and synchronous interfaces fall short, the sans-I/O pattern, why Events and Actions are plain data, and all fifteen rules of `loop.md` mapped one by one onto toys3. |
| 3 | [The vocabulary](03-vocabulary.md) | Every type in `toys3/core`: times, IDs, error codes, statuses, every Event, every Action, every Fact, the `Env`/`Buggify` seam (and the fact that the server does not use it yet), and the `Machine`/`Factory` contracts, with real log renderings. |
| 4 | [The durability problem](04-durability.md) | How bytes actually become durable on POSIX: page cache, file vs directory fsync, atomic rename, torn writes. Why the temp → fsync → rename → fsync-dir dance is the minimal correct protocol. The on-disk object format byte by byte (with a real hex dump), key escaping, temp-file naming and the on-disk layout. |
| 5 | [The server, part 1: lifecycle, recovery, queuing](05-server-lifecycle.md) | The server's phases, its struct field by field, `Step` dispatch, stale-completion handling, the recovery protocol line by line, why requests queue during boot and recovery, and real recovery traces including a resurrected temp file and a recovery that halts and retries. |
| 6 | [The server, part 2: per-key FIFO and the four protocols](06-server-protocols.md) | Implicit per-key queues, why a single arrival order makes deadlock impossible, COPY holding two keys, and a step-by-step walk through PUT, GET, DELETE and COPY in `onRequestIO`, with the unit tests and a real trace as worked examples. |
| 7 | [Errors and fail-stop](07-errors-fail-stop.md) | The 500 vs 500+Halt table and the rule behind it ("a read must never see a value a crash can erase"), fsyncgate and how it is modelled, and a full dissection of mutant d2 at seed 132, plus mutant d and how the no-halt wrapper works. |
| 8 | [State hashing and determinism](08-hashing-determinism.md) | `StateHash`, FNV-1a, the running trace hash and what is folded into it, the determinism canary, SplitMix64 and Lemire's bounded integers, Go's determinism hazards, the `detcheck` analyzer and its allow-list, and two instructive tooling bugs: generated test mains and a SIGPIPE-silenced CI canary. |
| 9 | [The production runtime](09-production-runtime.md) | How the same core runs for real: `OSExecutor`, the single-owner `Loop`, `Submit`, the S3-subset HTTP handler, `main.go` wiring, Halt as process exit, what DST does not cover here, and a real `curl` session with a hex dump of the stored object. |
| 10 | [The simulator](10-simulator.md) | The global queue ordered by `(At, Tie, Seq)`, virtual time, incarnations, the land/deliver split ("it happened but nobody was told"), crash and restart, parking and quiescence, run phases, and the main loop line by line with real log excerpts. |
| 11 | [The fake filesystem](11-fake-filesystem.md) | Inodes with durable and volatile content, directory entries with durable and current versions, every operation's normal and EIO behaviour, background writeback, read corruption, fsyncgate, the three-phase crash resolution, and worked crash scenarios including a real resurrected-temp alias and how it corrupts data when recovery is broken. |
| 12 | [Workloads and swarm testing](12-workloads-swarm.md) | Clients, the op mix, self-describing values that name their writer, quiescent points, `BaseConfig` vs per-seed swarm variation in `DefaultConfig`, and measured ablations showing how fault-model realism and workload variety change how fast a bug is caught. |
| 13 | [The oracle: linearizability](13-oracle.md) | Linearizability from first principles, outcomes (definite, failed, abandoned), "crashes erase nothing", the sequential model, the memoized Wing–Gong–Lowe search line by line (and why its memo is sound), per-key decomposition, carried uncertainty and widening, every oracle unit test worked by hand, and real failure messages decoded. |
| 14 | [Invariants](14-invariants.md) | The five invariants line by line, white-box vs black-box checking, the protocol/panic/max-steps checks, coverage tests as invariants about the test itself, and how to write a new invariant. |
| 15 | [Mutation testing](15-mutation-testing.md) | Proving the tests have teeth: the wrapper techniques used to build mutants, every mutant dissected with its real failing output, the full story of mutant f, ablations, surviving mutants, and how to tell a weak test from an equivalent mutant. |
| 16 | [Running campaigns](16-campaigns.md) | `TestSeeds`, `toys3sim`, the summary line field by field, reproducing and logging any seed, determinism double-runs, CI, and the results of a 10-minute campaign. |
| 17 | [Limitations and where to go next](17-limitations.md) | What the model does not cover, the documentation/code discrepancies and surprises found while writing this book, and a roadmap of extensions (buggify sites, a network layer, replication, coverage-guided seeds, applying it all to the Manifester skeleton). |
| 18 | [Glossary and appendix](18-glossary-appendix.md) | Glossary, command cheat sheet, file index with key symbols, invariant and mutation tables, the error policy table, and reference tables for stats and log formats. |

---

## Suggested reading paths

**The full path (recommended for a first read).** Read 0 → 18 in order. Each chapter uses only ideas introduced earlier, and the exercises build on each other: by Chapter 15 you will be writing your own mutants and predicting which invariant catches them.

**The "I want to build a simulator" path.** 0, 1, 2, 3, then 10 → 16, then 17. Skim 4 to 7 when a chapter refers back to the durability protocol or the error policy. This path is for readers who already understand crash consistency and want the simulator machinery: queue, fake filesystem, workload, oracle, invariants, mutation testing, campaigns.

**The "I care about storage correctness" path.** 0, 1, 4, 5, 6, 7, 11, 14, 15. This is the path for someone who wants to know *why* the durability protocol is what it is, and how each step was shown to be necessary by a mutant the simulator catches.

**The "I have one evening" path.** 1 (why), 2 (the shape), 7 (one bug, end to end), 13 §§ on outcomes and the WGL search, 15 (all the bugs). Then run the commands in the preface.

## Before you start

From the repository root:

```sh
go test ./toys3/core ./toys3/prod             # server and production runtime tests
go test ./toys3/sim -run TestSeeds -seed=1 -v # replay one seed
go test ./toys3/sim -run TestCatchesBugs -v   # watch seven broken servers get caught
```

Line references in this book are against commit `210c1fa`. If they have drifted, search for the function name. The book itself does not change any code outside `toys3/book/`.
