# Chapter 0: Preface

> *"Every serious test of a storage system is, in the end, a test of what happens when the power goes out at the worst possible moment. The only question is whether you get to choose the moment."*

This book is about a small program and a big idea.

The small program is **toys3**: a single-node, S3-flavoured object server that supports exactly four operations (PUT, GET, DELETE, COPY), stores every object as one file on a local disk, and promises that once it has said "200 OK" to a mutation, that mutation survives a power cut. It is about 530 lines of state machine (`toys3/core/server.go`), 120 lines of on-disk format (`toys3/core/format.go`), and a few hundred lines of production glue (`toys3/prod`). You could read the whole server in an afternoon.

The big idea is **deterministic simulation testing** (DST): the practice of running your real system code inside a simulated world (simulated disk, simulated clock, simulated clients, simulated crashes) that is driven by a single seeded random number generator. Same seed, same universe, same bug, every time. The simulator in `toys3/sim` is much larger than the server it tests (roughly 2,850 lines of non-test Go, about 3,850 with its tests, against roughly 1,100 lines for all of `toys3/core` including its long package documentation), and that ratio is not an accident. It is the whole point.

The reason toys3 exists is stated plainly in its own design document:

> Being a good S3 is not the point. The point is to have a small system with real durability bugs to make, so that a deterministic simulator can catch them.
> (`toys3/DESIGN.md:5-6`)

So this is a book about **learning DST by taking apart a complete, working example**. Every chapter is grounded in code that exists in this repository, quoted with `path:line` references, and in artifacts (event logs, failing seeds, invariant messages) that were produced by actually running that code while the book was being written.

---

## 0.1 Who this book is for

You are an experienced engineer. You have written concurrent code, you have debugged a flaky test at 2 a.m., and you have a healthy suspicion of anything that says "durable". You may have heard of FoundationDB's simulator or TigerBeetle's VOPR and wondered what it would take to build one. You may already know Go well, or you may not; the Go used here is plain and the book explains the few idioms that matter (type switches over closed interfaces, slices as immutable values, `map` iteration order).

You do *not* need to know anything about linearizability checking, fsync semantics, or the Wing–Gong–Lowe algorithm in advance. Each is built up from first principles when it is needed.

You do need to be willing to run things. The single most effective way to learn DST is to break a working system on purpose and watch the simulator catch you. Every chapter ends with exercises that ask you to do exactly that.

## 0.2 What you will learn

By the end of the book you should be able to:

1. **Explain why DST finds bugs that unit tests, integration tests and even randomized stress tests do not**, and what the three ingredients are (a single thread of control, simulated I/O, and one seeded source of randomness) (Chapter 1).
2. **Design a component as a pure `Step(Event) []Action` state machine**, the "sans-I/O" shape that makes simulation possible, and explain each rule in `loop.md` (Chapters 2 and 3).
3. **Reason precisely about how bytes become durable on a POSIX filesystem**: page cache, `fsync` of a file versus its directory, atomic `rename`, torn writes, and why the temp-file → fsync → rename → fsync-dir sequence is the minimum correct protocol (Chapter 4).
4. **Read and modify the toys3 server** (`toys3/core/server.go`) with confidence, including its recovery protocol, its per-key FIFO queues, and its error policy (Chapters 5 to 7).
5. **Understand how determinism is enforced and verified**: state hashing, trace hashes, the `detcheck` static analyzer and its allow-list, and two instructive bugs in the tooling itself (Chapter 8).
6. **See how the same core runs in production** with real goroutines, real `os` calls and real HTTP, and why the production shell contains no business logic (Chapter 9).
7. **Build a simulator**: a priority queue ordered by `(At, Tie, Seq)`, virtual time, server incarnations, crash and restart, and the separation between an I/O *landing* and its completion being *delivered* (Chapter 10).
8. **Build an adversarial fake filesystem** with durable and volatile versions of every inode and directory entry, crash resolution, EIO semantics, background writeback, read corruption, and the Linux "fsyncgate" behaviour (Chapter 11).
9. **Design workloads that make bugs visible**: self-describing values, quiescent points, and swarm testing (Chapter 12).
10. **Check linearizability** of a history containing crashes, 500s and abandoned requests, using a memoized Wing–Gong–Lowe search with per-key decomposition and carried uncertainty (Chapter 13).
11. **Write invariants that have teeth**, and tell black-box from white-box checks (Chapter 14).
12. **Prove your tests have teeth with mutation testing** and interpret ablation results (Chapter 15).
13. **Run and read DST campaigns**, reproduce any failing seed with one command, and wire determinism checks into CI (Chapter 16).
14. **Know the limits** of this model and have a concrete roadmap of extensions: network faults, replication, buggify sites, coverage-guided seed selection (Chapter 17).

## 0.3 How to read this book

The chapters are written to be read in order, but they are also meant to be read *with the code open*. Each code excerpt carries its location, for example:

```go
// toys3/core/server.go:109-132
func (s *Server) Step(ev Event) []Action {
	if s.phase == phaseHalted {
		return nil
	}
	...
```

Line numbers refer to the tree at commit `210c1fa` ("toys3: deterministic simulator, fake FS, oracle, and DST campaigns"). If you are reading a later version and the numbers have drifted, search for the function name.

There are three reading paths, described in more detail in `README.md`:

- **The full path** (recommended): 0 → 18 in order. The book is built so that each chapter uses only ideas introduced earlier.
- **The "I want to build a simulator" path**: 1, 2, 3, 10, 11, 12, 13, 14, 15, 16. Skim 4 to 7 for context when a chapter refers back.
- **The "I care about storage correctness" path**: 1, 4, 5, 6, 7, 11, 14, 15. This is the path for someone who wants to understand *why* the durability protocol is what it is and how each step was proven necessary.

Each chapter ends with three fixed sections:

- **Key takeaways**: the handful of ideas you should be able to explain to a colleague without notes.
- **Exercises**: concrete tasks in this repository. Most are of the form "make this change, predict what the simulator will report, run it, compare". Prediction is the important part; writing your guess down before you run the command is how intuition gets calibrated.
- **Further reading**: papers, talks and posts. Only sources the author is confident exist are listed.

## 0.4 A map of the repository

The repository is a Go module named `manifester` (see `go.mod`). It contains two projects that share tooling:

```
tmpdst/                         module "manifester", Go 1.24
├── loop.md                     the DST design rules (15 instructions). Read it.
├── docs/
│   ├── lessons.md              lessons distilled from the Moonpool sim book
│   ├── decisions.md            decisions log for the (future) Kafka engine
│   ├── flow-control.md         inbox priority and backpressure design
│   └── dep-audits/             audit records for third-party deps of core/sim
├── core/  sim/  sys/           interface skeleton for "Manifester", a future
│                               WarpStream-like Kafka engine (interfaces only)
├── tools/detcheck/             static analyzer enforcing determinism rules
├── scripts/
│   ├── ci-determinism.sh       CI gate: detcheck + audits + canaries
│   └── audit-dep.sh            audit a third-party module before allowing it
├── .github/workflows/determinism.yml
└── toys3/                      ◄── the subject of this book
    ├── DESIGN.md               the design document (207 lines; read it twice)
    ├── core/                   pure state machine + on-disk format
    │   ├── doc.go              package doc: layout, protocol, error table
    │   ├── types.go            Time, Duration, ClientReq, IOID, ErrCode, Status
    │   ├── event.go            Event and every EventBody
    │   ├── action.go           every Action
    │   ├── fact.go             Committed, Recovered, CorruptObject
    │   ├── env.go              Env = Rand + Buggify
    │   ├── machine.go          StateMachine, Hashable, Machine, Factory
    │   ├── format.go           object header, CRC, key escaping, paths
    │   ├── server.go           THE SERVER (Step and everything it calls)
    │   └── server_test.go      hand-driven harness, exact action sequences
    ├── sim/                    the deterministic simulator
    │   ├── doc.go              main loop in pseudocode
    │   ├── rand.go             SplitMix64 + Lemire bounded ints
    │   ├── hash.go             Fold, FNV helpers
    │   ├── queue.go            Entry, EntryKind, Queue (binary heap)
    │   ├── config.go           Config, Stats, DefaultConfig (swarm), Result
    │   ├── simulator.go        the main loop, crash, restart, env
    │   ├── fakefs.go           adversarial in-memory filesystem
    │   ├── workload.go         clients, op mix, self-describing values
    │   ├── oracle.go           history recording + WGL linearizability checker
    │   ├── invariant.go        the five invariants
    │   ├── describe.go         log rendering and event/action hashing
    │   ├── sim_test.go         canary, TestSeeds, coverage, CRC reachability
    │   ├── fakefs_test.go      crash-semantics tests of the fake FS
    │   ├── oracle_test.go      hand-built histories, accepted and rejected
    │   ├── queue_test.go       queue ordering and incarnation dropping
    │   └── mutants_test.go     seven deliberately broken servers
    ├── prod/                   production runtime (not deterministic)
    │   ├── doc.go  env.go  executor.go  loop.go  http.go  prod_test.go
    ├── cmd/toys3/main.go       the server binary (-dir, -addr, -max-object)
    └── cmd/toys3sim/main.go    the parallel DST campaign runner
```

The top-level `core/`, `sim/` and `sys/` directories are the *interface skeleton* of a larger project (a Kafka-compatible engine in the style of WarpStream, called Manifester). They contain contracts and no implementations, and they are not the subject of this book. They matter here for one reason: toys3 was built as a **rehearsal**. Every design rule it follows comes from `loop.md` and `docs/lessons.md`, which were written for the bigger system. toys3 is the place where those rules were tried end to end on something small enough to finish. Chapter 17 discusses what carries over.

## 0.5 Five-minute tour: run it

Before reading further, run these from the repository root. They are all cheap.

```sh
# 1. The server's own unit tests: exact action sequences for every protocol.
go test ./toys3/core -v -run TestHappyPath

# 2. Replay one simulation seed. It prints the seed's stats line.
go test ./toys3/sim -run TestSeeds -seed=1 -v

# 3. Run 200 seeds against the correct server.
go test ./toys3/sim -run 'TestSeeds$' -seeds=200 -v

# 4. Watch the simulator catch seven deliberately broken servers.
go test ./toys3/sim -run TestCatchesBugs -v

# 5. The CI determinism gate: static analysis, audits, canaries.
scripts/ci-determinism.sh
```

When this book was written, step 2 printed:

```
=== RUN   TestSeeds
    sim_test.go:52: seed 1: steps=1628 crashes=10 trace=1ca97eb77a9044e1
        reqs=225 200=154 400=3 404=36 500=7 indet=25 fsops=527 eio=10 crashes=10 halts=9 midput=8 torn=1 lost=9 kept=12 readcorrupt=0 corruptobj=0 tempsrm=7 windows=30 maxwin=13 widened=0
--- PASS: TestSeeds (0.00s)
```

Read that line slowly. In 360 milliseconds of *simulated* time (and a few milliseconds of real time), seed 1 issued 225 requests from 4 clients against 3 keys, executed 527 filesystem operations of which 10 failed with EIO, crashed the server 10 times (9 of which were the server deliberately halting itself after an I/O error), caught 8 of those crashes in the middle of a PUT or COPY, tore 1 file, lost 9 un-synced changes and kept 12, cleaned up 7 temp files during recovery, and checked 30 windows of history for linearizability, the largest containing 13 operations. And the trace hash `1ca97eb77a9044e1` will be identical on your machine, because nothing in that run depended on anything except the number 1.

Step 3 printed, for 200 seeds:

```
reqs=47270 200=33446 400=887 404=6842 500=1035 indet=5060 fsops=113656 eio=1061 crashes=1691 halts=673 midput=1473 torn=87 lost=1539 kept=1350 readcorrupt=71 corruptobj=70 tempsrm=1006 windows=6708 maxwin=23 widened=0
--- PASS: TestSeeds (1.74s)
```

1,691 crashes in under two seconds. That is the economics of DST in one line: a real power-cut test rig might manage a few crashes per minute.

Step 4 printed (abridged):

```
=== RUN   TestCatchesBugs/a-skip-dir-fsync
    caught at seed 1 by acked-is-durable (step 46): c6: write of "k2" acked but /data/obj/k2 has no durable entry
=== RUN   TestCatchesBugs/d2-no-halt-on-dir-fsync-error
    caught at seed 132 by linearizable (step 380): no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
      req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
=== RUN   TestCatchesBugs/f-skip-temp-cleanup
    caught at seed 20 by linearizable (step 1750): no linearization of 1 ops from any of 1 possible start states {k5=221}; window:
      req 248 client 3 GET "k5" [471..475 @255595778..256780566] -> 200 value-of-231
```

By Chapter 15 you will be able to read each of those messages and tell the full story of the bug behind it: which protocol step was skipped, which crash exposed it, and why no sequential execution of the client's operations could have produced what the client saw.

## 0.6 Conventions

- **"The server"** means `core.Server` in `toys3/core/server.go`. **"The simulator"** means `sim.Simulator` in `toys3/sim/simulator.go`. **"The FakeFS"** means `sim.FakeFS`.
- **Event / Action / Fact** with a capital letter mean the Go interfaces in `toys3/core` (`core.Event`, `core.Action`, `core.Fact`).
- **Incarnation** means one boot of the server, from `Started` until a crash or `Halt`. Incarnations are numbered from 1 (`toys3/sim/queue.go:5-9`).
- Times in logs are virtual. `t=53.112270ms` means 53,112,270 simulated nanoseconds since the start of the run.
- In event logs, `c17` is client request 17 (a `core.ClientReq`), `io42` is filesystem operation 42 *within the current incarnation* (a `core.IOID`), and `value-of-26` means "the exact bytes written by PUT request 26" (Chapter 12 explains how the simulator can tell).
- ASCII diagrams use `──►` for data flow and `⚡` for a crash.

## 0.7 A note on honesty

Every claim in this book about what the code does was checked against the code. Where the documentation (`DESIGN.md`, package docs, comments) says something slightly different from what the code does, the book says so and shows both. A list of those discrepancies, and a couple of surprises found while writing (for example, one recovery step that no mutant can show to be necessary), is collected in Chapter 17 and the appendix. Finding those is itself a DST lesson: the simulator is the specification that actually runs.

## Key takeaways

- toys3 is a deliberately small, deliberately *honest* target: it has real durability requirements, so it can have real durability bugs.
- The simulator is bigger than the server. That is normal for DST and is where the leverage is.
- Everything is reproducible from a seed. A failing seed is a complete bug report.
- The book is grounded in the actual code at commit `210c1fa` and in artifacts produced by running it.

## Exercises

1. Run the five commands in §0.5. Confirm that `go test ./toys3/sim -run TestSeeds -seed=1 -v` prints `trace=1ca97eb77a9044e1` on your machine. If it does not, stop: either the code has changed or determinism is broken, and Chapter 8 will help you find out which.
2. Open `toys3/DESIGN.md` and `loop.md` side by side. For each of the 15 instructions in `loop.md`, write one sentence about where toys3 implements it (a file name is enough). Keep the list; Chapter 2 walks through the same exercise and you can compare.
3. Count lines: `wc -l toys3/core/*.go toys3/sim/*.go`. Compute the ratio of simulator-plus-tests to server. Then guess what that ratio is for FoundationDB or TigerBeetle, and look it up after reading Chapter 1's further reading.
4. Run `go test ./toys3/sim -run TestSeeds -seed=2 -v` and `-seed=3`. Compare the stats lines. Which numbers vary most between seeds? Hypothesize why before reading Chapter 12 on swarm testing.

## Further reading

- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014. The talk that introduced most of the industry to FoundationDB's simulator.
- Jingyu Zhou et al., *"FoundationDB: A Distributed Unbundled Transactional Key Value Store"*, SIGMOD 2021. Section on simulation testing.
- TigerBeetle's documentation and talks on the **VOPR** (Viewstamped Operation Replicator), their deterministic cluster simulator, and their "TigerStyle" engineering notes.
- Pierre Zemb, *The Moonpool Sim Book* (https://pierrez.github.io/moonpool/), the source of `docs/lessons.md` in this repo.
- `loop.md` and `toys3/DESIGN.md` in this repository: short, dense, and the specification this book unpacks.
