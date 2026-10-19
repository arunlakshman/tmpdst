# Blog series outline: Write your own deterministic simulation test in Go

**Goal:** after reading Parts 1–3, a reader can build a working DST harness for their own system from scratch.

**Format:** a hands-on tutorial. Each part:
- adds code the reader types and runs;
- ends with a working checkpoint (`go test` passes or finds a planted bug);
- ends with exercises.

**Running example:** `minikv`, a toy durable key-value store (PUT and GET of whole values as files, with fsync).
- It is a distilled toys3, small enough that the whole harness is about 600 lines.
- Companion repo: one tag per part (`part-1`, `part-2`, `part-3`).

Each post is 3,000–4,000 words.

---

## Part 1: The shape of a simulatable system
*Checkpoint: one seed runs the server against a perfect fake disk, and running it twice prints the same trace hash.*

1. **What DST is, in one page**
   - The whole system runs in one goroutine against simulated I/O, driven by one seed.
   - The same seed always produces the same run.
   - Why this finds the bugs unit tests can't: interleavings of crashes and I/O.
2. **Rule 1: business logic never does I/O**
   - Write the `Step(Event) []Action` interface.
   - Define `minikv`'s events and actions (Request, Write, Fsync, Rename, FsyncDir, Read, Reply) as plain structs with IO IDs.
3. **Write the server state machine**
   - The PUT protocol: tmp → fsync → rename → fsync dir → reply.
   - GET.
   - Explain each step of the protocol and why it is needed.
4. **Rule 2: inject every source of nondeterminism**
   - A table of Go traps and their fixes: `time.Now`, map iteration, `math/rand`, goroutines, `select`.
   - Write a 15-line SplitMix64 RNG.
5. **Write the simulator core**
   - A priority queue ordered by `(time, tie, seq)`.
   - A virtual clock.
   - The main loop: pop → Step → execute actions → schedule completions with random latency.
6. **A perfect fake disk**
   - A map of path → bytes.
   - Every operation succeeds after a random delay.
7. **Prove determinism**
   - Fold every event and action into a trace hash.
   - Run the same seed twice and compare the two hashes.
   - Demonstrate a leak by deliberately ranging over a map.
8. **Exercises**
   - Add DELETE.
   - Add a lint that bans the `time` import.

## Part 2: Faults: a disk that crashes like a real one
*Checkpoint: thousands of seeds run with crashes, torn writes and EIO; the reader watches recovery work and can replay any seed.*

1. **What fsync actually promises**
   - Page cache vs disk.
   - File fsync vs directory fsync.
   - Rename atomicity.
   - One diagram: a PUT timeline with crash points marked.
2. **Model durable vs volatile state**
   - Rewrite the fake disk so each file and each directory entry has a durable version and a volatile version.
   - fsync promotes the volatile version to durable.
3. **Crash = power loss**
   - Drop all in-flight events for the dead incarnation.
   - Resolve each un-synced change with the RNG: kept, lost or torn prefix.
   - Restart the server with a fresh instance.
4. **Recovery in the server**
   - On `Started`, list `tmp/` and delete the leftovers.
   - Add a CRC header so torn files are detectable.
5. **Error injection**
   - EIO on any operation, with "may or may not have applied" semantics.
   - Why fsync errors require fail-stop: fsyncgate.
6. **A workload generator**
   - N clients, a small key set for contention, self-describing values (each value names its writer).
7. **Phases**
   - Chaos, then settling (no new faults), then drain.
8. **Making failures reproducible**
   - `go test -run TestSim -seed=N -v` prints the full event log.
   - Show how to read the log.
9. **Exercises**
   - Add per-seed random configuration (swarm testing).
   - Measure how often a crash lands between rename and fsync-dir.

## Part 3: Checking correctness and proving the checks work
*Checkpoint: a campaign runs 100k seeds green, and every planted bug is caught within a few hundred seeds.*

1. **Invariants vs an oracle**
   - Invariants are white-box: they check what the server believes against the fake disk's ground truth.
   - An oracle is black-box: it checks only what clients saw.
2. **Write three invariants**
   - acked ⇒ durable.
   - No torn object is ever visible.
   - Liveness: every request is answered once faults stop.
   - Check them after every step, not only at the end.
3. **Write a linearizability checker**
   - The history of invoke and return events.
   - A sequential model (a map).
   - WGL search, explained by building it in about 80 lines.
   - Handling 500s and crashes as "maybe applied".
   - Quiescent windows keep the search small.
4. **Mutation testing: prove the harness has teeth**
   - Wrap the server to inject bugs: skip the dir fsync, ack before the rename, don't fail-stop, skip the CRC check.
   - Table of results: which invariant caught each mutant, and at which seed.
   - The rule: a check that never failed hasn't been tested.
5. **Running campaigns**
   - Parallel seeds across cores.
   - A double-run determinism check on every Nth seed.
   - Reading a summary line.
   - Keeping failing seeds as regression tests.
6. **Using DST as a refactoring safety net**
   - Golden trace hashes for fixed seeds.
7. **Taking it to your own system: a checklist**
   - Find the I/O boundary.
   - Define events and actions.
   - Build a fake for each external system, with honest semantics.
   - Write invariants and an oracle.
   - Plant mutants.
   - Run campaigns.
8. **Limits of simulation**
   - A simulation is only as honest as its fault model.
   - Example: Trip-after-restart, where the page cache survives a process exit.
9. **Exercises**
   - Add a "process exit, page cache survives" fault.
   - Add network faults.

---

**Series mechanics**
- Every part opens with "what you'll build" and ends with "checkpoint" and "exercises".
- Code is complete and runnable at each tag.
- Readers who want depth can go to the book in `toys3/book/`:
  - Part 1 → ch 2–3a and 8.
  - Part 2 → ch 4 and 10–12.
  - Part 3 → ch 13–16.
