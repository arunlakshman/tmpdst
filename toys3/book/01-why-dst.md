# Chapter 1: Why Deterministic Simulation Testing

> *"If you can't reproduce it, you can't fix it. If you can't make it happen, you can't reproduce it."*

This chapter is about the problem, not the solution. Before we look at a single line of toys3, we need a sharp picture of the class of bugs DST exists to catch, why the ordinary testing toolbox misses them, and what the minimum set of ingredients is that turns "a test that sometimes fails" into "a seed that always fails".

---

## 1.1 A bug you cannot write a unit test for

Here is a story that is entirely possible with a plausible-looking version of toys3. Read it as a sequence of events on a real machine.

1. A client sends `PUT /b/k` with value `v2`. The key already holds `v1`.
2. The server writes `v2` to a temporary file, fsyncs it, and renames it over `obj/b%2Fk`.
3. The server calls `fsync` on the `obj/` directory to make the rename durable. The disk controller hiccups and `fsync` returns `EIO`.
4. The server replies `500 Internal Server Error` to the client. That is fine: a 500 promises nothing.
5. The server keeps running. Why not? It reported the error honestly.
6. Another client sends `GET /b/k`. The rename is visible in the kernel's directory cache, so the server reads `v2` and replies `200 v2`.
7. The machine loses power.
8. On reboot, the directory entry change was never written to disk. `obj/b%2Fk` points at `v1` again.
9. A third client sends `GET /b/k` and receives `200 v1`.

Look at what the clients saw, in real-time order: `PUT v2 → 500`, then `GET → v2`, then (after the outage) `GET → v1`. No one deleted or overwrote `v2`. A value appeared, was observed, and then vanished. There is no sequential execution of these operations that explains this history. The system is not linearizable, and the bug is not in any one line of code: every individual step above is "reasonable". The bug is in a *policy* (step 5: keep serving after an `fsync` error) interacting with a *fault* (step 3) and a *crash* (step 7) at a *specific time* (after step 6, before any later `fsync` happened to persist the directory).

This is not a hypothetical shape. It is mutant **d2** in `toys3/sim/mutants_test.go:125-129`, and the simulator finds it at seed 132. Chapter 7 dissects the exact run. For now, ask yourself: how would you write a test that catches this?

- A **unit test** of the error path would check "on `fsync` EIO, reply 500". It would pass. The bug is not in the reply.
- An **integration test** against a real disk cannot make `fsync` return EIO on demand, and cannot cut power between step 6 and step 8.
- A **fault-injection test** with a mocked filesystem could return EIO from `fsync`, but a mock that returns EIO usually has no notion of "the rename is visible but not durable", and no notion of a crash that resolves the difference.
- A **stress test** running for hours on real hardware might hit a real EIO once a year and would almost never pair it with a power cut at the right moment. If it did, you would have no way to replay it.

What you need is a world in which:

- the filesystem *models* the gap between "visible" and "durable",
- faults like EIO and crashes are *injected deliberately* and *often*,
- the client-visible history is *recorded* and *checked against a model*,
- and the whole thing is *replayable*, so that when it fails you can watch it fail again, step by step, with logging turned on.

That world is a deterministic simulator.

## 1.2 The bugs that hide

It helps to name the families of bugs that DST is particularly good at. They share a property: **they need a particular interleaving of independent events to manifest**, and the space of interleavings is astronomically large.

| Family | Example in a storage server | What it needs to manifest |
|---|---|---|
| Crash consistency | Ack before the data is durable; rename not fsynced | A crash between the ack and the next unrelated fsync |
| Error handling | Continue after an fsync error | A rare I/O error *and* a later crash |
| Recovery | Leftover temp files not cleaned | A crash mid-write, a restart, and a later write that reuses state |
| Concurrency | Two requests on one key interleave | Two requests arriving within microseconds, completions in an unlucky order |
| Lost completions | An operation applied but the caller was never told | A crash between "effect landed" and "completion delivered" |
| Liveness | A request never gets a reply | A queue that waits on an event that will never come |

An empirical study of production failures in distributed data-intensive systems (Yuan et al., OSDI 2014, *"Simple Testing Can Prevent Most Critical Failures"*) found that a large majority of catastrophic failures were caused by incorrect handling of non-fatal errors that were explicitly signalled in the code. The error paths existed; they were simply wrong, and they were almost never executed in testing. DST's first contribution is brutal and simple: **it executes error paths constantly**. In the 200-seed run from the preface, the simulator injected 1,061 EIOs and 1,691 crashes in under two seconds of wall time.

## 1.3 Why randomized testing alone is not enough

You could imagine getting most of the way with "chaos" testing: run the real server on a real (or FUSE-wrapped) filesystem, inject random faults, kill the process at random, and check the results. This is valuable, and tools like Jepsen have found an enormous number of real bugs this way. But it has three structural weaknesses that DST removes.

**1. It is not reproducible.** Real threads are scheduled by the operating system. Real timers fire on real clocks. Real disks have real latency. Two runs with the same "random seed" diverge within microseconds because the seed controls only *your* random choices, not the kernel's. When a chaos test finds a bug, you get a log and a prayer. In DST, you get a seed. `go test ./toys3/sim -run TestSeeds -seed=132 -v` replays the *exact* universe, event for event.

**2. It is slow.** A real crash-and-reboot cycle takes seconds. A real fsync takes milliseconds. A simulated crash takes microseconds, and simulated time jumps directly to the next scheduled event: if nothing happens for 5 ms of virtual time, the simulator does not wait 5 ms; it advances the clock. The toys3 simulator runs roughly a hundred seeds per second per core (each seed covering 360 ms of virtual time, with ten-ish crashes) on a laptop that was busy with other work. That is roughly a thousand simulated crashes per second per core.

**3. It explores poorly.** Real systems under chaos tend to settle into a narrow band of behaviours: the same latency distribution, the same thread interleavings, the same handful of fault patterns. A simulator controls *every* source of nondeterminism and can deliberately vary each one per seed: how many keys, how many clients, how often to crash, how likely an EIO is, which of two same-instant events goes first. Chapter 12 shows that one mutant took **429 seeds** to catch with a fixed configuration and a less realistic filesystem, and **20 seeds** with per-seed variation (swarm testing) and background writeback.

## 1.4 The FoundationDB story

The idea is old. Deterministic replay has been used in debugging for decades. But the modern practice of building an entire distributed database *around* a deterministic simulator is usually traced to FoundationDB.

The FoundationDB team (as described in Will Wilson's 2014 Strange Loop talk and in the 2021 SIGMOD paper) made a decision very early: before writing the database, they wrote a simulator and a language extension (Flow, an actor-model extension to C++) that made every source of nondeterminism (network, disk, time, randomness) go through interfaces that the simulator could replace. The entire cluster ran in one thread, with one seeded random number generator, against simulated networks and disks that dropped, delayed, reordered and corrupted messages, and simulated machines that rebooted, lost power and swapped hard drives. They ran this simulation continuously, on thousands of cores, for years.

The results were remarkable enough to become folklore: a database whose correctness reputation came primarily from simulation rather than from field experience, and a team that reported finding bugs in the simulator faster than the real world could produce them. Their key cultural practices map directly onto things you will see in toys3:

- **Run the real code.** The simulator does not test a model of the database; it tests the database. In toys3, the simulator drives `control.Controller` itself (`toys3/sim/simulator.go:310-326` builds it through `cfg.Factory`).
- **Make the world hostile.** Faults are not rare in simulation; they are common. In toys3, `BaseConfig` gives every filesystem operation a 1% chance of EIO and every un-synced file a 30% chance of being torn at a crash (`toys3/sim/config.go:162-170`).
- **Buggify.** FoundationDB's code contains `BUGGIFY` points: places where, in simulation only, the code deliberately takes a rare-but-legal path (a smaller buffer, an extra delay, an early timeout). toys3 has the interface for this (`toys3/control/env.go:18-26`) but, as Chapter 3 points out, no call sites yet.
- **Check invariants continuously.** Not just at the end of a run, but after every step, so the failure is reported at the step that caused it. toys3 does exactly this (`toys3/sim/simulator.go:339-354`).

## 1.5 The three ingredients

Strip DST down and you find three ingredients. Everything else is engineering around them.

### Ingredient 1: one thread of control

All "concurrent" activity (many clients, many in-flight disk operations, a crashing server) is represented as **events in a single priority queue**, processed one at a time by one goroutine. There are no real threads whose interleaving the OS decides.

This does not mean the system under test cannot be concurrent. toys3's server has many requests in flight at once and many filesystem operations outstanding at once. But the *concurrency is in the data*, not in the threads: an in-flight operation is a record in a slice, waiting for a completion event. The simulator decides, deterministically, which completion arrives next.

```
         real threads (nondeterministic)             one event queue (deterministic)

  goroutine A ─┬─ write ─── fsync ───► ...       t=3.1ms  deliver FSWriteDone{io7}
  goroutine B ─┼── read ─► ...                    t=3.1ms  deliver Request{c9 GET k}
  goroutine C ─┴──── rename ─────► ...            t=3.4ms  fs-land  FSRename{io8}
     (OS scheduler picks the order)               t=3.4ms  ⚡ crash
                                                  (queue order picks; ties by seeded RNG)
```

### Ingredient 2: simulated I/O and time

Every interaction with the outside world (disk, network, clock) goes through an interface the simulator can implement. In toys3 this interface is not a Go interface like `type FS interface { Write(...) }`. It is the **vocabulary of Actions and Events**: the server returns `FSWrite{IO: 7, Path: ..., Data: ...}` as a plain value and later receives `FSWriteDone{IO: 7, Err: OK}` as a plain value. Who actually performs the write (the real `os` package in production, the `PlantModel` in simulation) is invisible to the server. Chapter 2 explains why this "sans-I/O" shape is more powerful than a mocked interface.

Time is simulated the same way: the server never calls `time.Now()`. Every event arrives stamped with a time (`control.Event.At`, `toys3/control/event.go:5-8`), and in simulation that time is the virtual clock.

### Ingredient 3: one seeded source of randomness

Every random choice in a run (how long an fsync takes to land and to be reported, whether it fails, whether a crash happens now, which client issues what, which of two same-instant events goes first, how a crash resolves each un-synced change) is drawn from **one** random number generator, seeded from **one** number. In toys3 that generator is `sim.Rand`, a SplitMix64 stream (`toys3/sim/rand.go:10-26`), and it counts its own draws (`Calls()`), which turns "the runs diverged" into "the runs diverged after draw 1,234".

```
seed ──► Rand ──┬──► queue tie-breaks         (Queue.Schedule)
                ├──► FS dead time, feedback   (PlantModel.Plan)
                │    delay, EIO plan
                ├──► crash coin                (Simulator.maybeCrash)
                ├──► crash resolution          (PlantModel.Crash)
                ├──► writeback, read bit-flips (PlantModel.Land / read)
                ├──► client think time, ops    (Workload)
                └──► server Env                (env.Uint64 / Intn / Maybe)
```

If the three ingredients hold, then **the run is a pure function of the seed** (and the code). That single property is what makes everything else in this book possible:

- A failing seed is a complete bug report.
- You can add logging to a failing run *without changing it* (toys3 checks this: `TestDeterminismCanary` asserts that turning on the event log does not change the trace hash, `toys3/sim/sim_test.go:32-37`).
- You can bisect a code change against a fixed seed.
- You can verify determinism mechanically by running a seed twice and comparing hashes (Chapter 8).

## 1.6 What DST is not

It is worth being precise about the limits, because over-claiming is the fastest way to lose trust in a test suite.

- **DST does not test your real I/O layer.** The production executor (`toys3/prod/executor.go`) is not exercised by the simulator. If `OSExecutor` mapped `ENOENT` to `ErrIO` by mistake, no seed would notice. That is why the production layer must be *thin* (loop.md, rule 10) and separately tested (`toys3/prod/prod_test.go`).
- **DST tests against a model of the world, not the world.** If the PlantModel is more forgiving than ext4, bugs that ext4 would expose go unnoticed. If it is *less* forgiving than ext4 (and toys3's PlantModel is deliberately less forgiving in some respects, see Chapter 11), you may defend against failures that real systems do not exhibit. That is usually the right trade.
- **DST finds bugs; it does not prove their absence.** A million passing seeds is strong evidence, not proof. Mutation testing (Chapter 15) is how you measure how strong.
- **DST needs invariants.** A simulator without a good oracle just generates very fast, very reproducible runs that nobody checks. Half of the value in toys3 is in `oracle.go` and `invariant.go`.

## 1.7 How toys3 puts it together

Here is the whole picture, which the rest of the book fills in.

```
                        ┌──────────────────────── sim.Simulator (one goroutine) ─────────────────────────┐
                        │                                                                                 │
  seed ──► sim.Rand ──► │  Queue (At, Tie, Seq)                                                           │
                        │   │ pop                                                                         │
                        │   ▼                                                                             │
                        │  KindClientWake ──► Workload.Issue ──► Oracle.Invoke ──► schedule Deliver(Req)  │
                        │  KindDeliver ─────► control.Controller.Step(Event) ──► []Action                 │
                        │                        │ FS*   ──► PlantModel.Plan ──► schedule FSLand          │
                        │                        │ Reply ──► Oracle.Return, Workload.Complete             │
                        │                        │ Emit  ──► telemetry timeline                           │
                        │                        │ Trip  ──► crash                                        │
                        │  KindFSLand ──────► PlantModel.Land ──► schedule Deliver(...Done)               │
                        │  KindCrash/Trip ──► PlantModel.Crash, Queue.DropIncarnation, Oracle.Crash       │
                        │  KindRestart ─────► Factory(env, cfg) ──► schedule Deliver(Started)             │
                        │                                                                                 │
                        │  after every step: trace = Fold(trace, StateHash, plant hash, ...)              │
                        │                    invariants: no-corrupt-reads, acked-is-durable,              │
                        │                                obj-never-torn, linearizable, liveness           │
                        └─────────────────────────────────────────────────────────────────────────────────┘
```

The same `control.Controller` runs in production inside `prod.Loop` (Chapter 9), with real goroutines executing real `os` calls and an HTTP handler feeding requests in. The server cannot tell the difference, because the only things it ever sees are Events.

If you have a background in control engineering, this picture will look familiar, and it is meant to: the server is a *controller* in a feedback loop around a *plant* (the disk), the simulator is a *software-in-the-loop testbench* that replaces the real plant with a *plant model* and injects *disturbances*, and the invariants are *monitors*. Chapter 3a develops that reading carefully, and the code uses its names.

## 1.8 Why a toy?

One more question deserves an answer: why build a *toy* S3 rather than apply DST directly to the real target (the Kafka engine whose skeleton lives in `core/`, `sim/` and `sys/`)?

Because DST has a learning curve, and the steep part of the curve is not the simulator; it is **learning to see the bugs**. You have to learn what "acked implies durable" really means on a POSIX filesystem, why a 500 is not a free pass, why a read can be a bug, how to write a linearizability checker that tolerates crashes, and how to tell whether your invariants actually catch anything. All of that is easier to learn on a system with four operations and one directory than on a distributed log with partitions, leases and an object store. toys3 is the flight simulator for the flight simulator.

## Key takeaways

- The bugs DST targets need a *specific interleaving* of faults, crashes and concurrent operations. They are nearly impossible to hit, and impossible to replay, with real threads and real I/O.
- The three ingredients are: one thread of control, simulated I/O and time, and one seeded RNG. Together they make a run a pure function of its seed.
- DST is fast because simulated time jumps, and thorough because the simulator controls and varies every source of nondeterminism.
- DST tests the real code against a model of the world. Its value is bounded by the fidelity of that model and the strength of its invariants.
- toys3 is a deliberately small target on which to learn all of the above end to end.

## Exercises

1. **Write the d2 story as a test you could not write.** Using only `toys3/control/controller_test.go`'s harness (an in-memory map with no crash semantics), try to write a test that exposes the "observed then vanished" bug from §1.1. Explain precisely which part of the scenario the harness cannot express. (Hint: look at what `harness.exec` does for `FSFsyncDir`, `toys3/control/controller_test.go:100-101`.)
2. **Count the randomness.** Run `go test ./toys3/sim -run TestSeeds -seed=1 -v` and note the trace hash. Now read `toys3/sim/simulator.go` and list every call to `s.rng` or to something that draws from it. For each, write down what would happen to determinism if that call used `math/rand` instead.
3. **Simulated time.** In `toys3/sim/config.go:148-175`, Chaos lasts 300 ms and Settling 60 ms of virtual time. Time a 200-seed run (`time go test ./toys3/sim -run 'TestSeeds$' -seeds=200`). Compute the ratio of simulated time to wall time. What dominates the wall time? (Hint: Chapter 13.)
4. **Stress test thought experiment.** Suppose you ran the real `cmd/toys3` binary on a real disk, with a script that issues random requests and `kill -9`s the process every few seconds. Which of the seven mutants in `toys3/sim/mutants_test.go:319-327` could that rig ever catch? Which could it never catch, and why? (Hint: `kill -9` is not a power loss. The page cache survives a process kill.)

## Further reading

- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014.
- Jingyu Zhou et al., *"FoundationDB: A Distributed Unbundled Transactional Key Value Store"*, SIGMOD 2021.
- Ding Yuan et al., *"Simple Testing Can Prevent Most Critical Failures: An Analysis of Production Failures in Distributed Data-Intensive Systems"*, OSDI 2014.
- Kyle Kingsbury, the Jepsen analyses (https://jepsen.io). The best public catalogue of what goes wrong when you *do* test real systems under faults.
- TigerBeetle, the VOPR and "TigerStyle" (in the TigerBeetle repository's docs). A modern, open-source example of a system designed around a deterministic simulator from day one.
- Antithesis (https://antithesis.com), which applies deterministic simulation to unmodified software by running it inside a deterministic hypervisor. A useful contrast with the "sans-I/O" approach this book takes.
- Phil Eaton, *"What's the big deal about Deterministic Simulation Testing?"* (2024 blog post). A short, readable overview.
