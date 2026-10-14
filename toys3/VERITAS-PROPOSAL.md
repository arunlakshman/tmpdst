# Proposal: veritas specs and conformance checks for toys3

Inspired by Bornholt et al., *Using Lightweight Formal Methods to Validate a
Key-Value Storage Node in Amazon S3* (SOSP '21), §3.2 and §4
(`papers/s3-shardstore-lightweight-formal-methods.pdf`).

Decisions:

- **Name:** "veritas" replaces "reference model" everywhere. A veritas is the
  small, executable, obviously correct version of one component. ("Model" is
  already overloaded: `sim.StorageModel`, the oracle's register model.)
- **Generator and shrinker:** hand-written, using the repo's seeded RNG. No new
  dependency and no dependency audit, failing seeds replay exactly like DST
  seeds, and the shrinking the paper relies on (§4.3) is simple enough to
  write.
- **Oracle:** stays independent of the veritas, and a test checks that the two
  agree (C4). They're written differently (the oracle tracks which request
  wrote each value; the veritas holds bytes), so making one depend on the other
  would cost more than it saves. Two independent specs that a test keeps in
  agreement also act as a check on each other.

## Why

toys3 already checks concurrency and crashes more thoroughly than the paper.
What it lacks:

1. **No small executable spec per component.** The only spec is the oracle's
   `refState`, buried in the linearizability search. Storage has none. The
   nearest thing is a private fake filesystem in `control/controller_test.go`
   (`harness.exec`).
2. **No step-by-step check of each component against its spec.** Every failure
   comes through the full DST run, as a concurrent history with crashes in it.
3. **Nothing checks the simulator against a real disk.** DST is only as
   trustworthy as `sim.StorageModel`, and nothing checks it against a real
   disk, even on the plain no-fault, no-crash path.

## Part 1: The veritas package

A new package, `toys3/veritas`, with no I/O and checked by detcheck.

**V1. `veritas.Store`: the object store** (about 50 lines)

- A `map[string][]byte` with `Put`, `Get`, `Delete` and `Copy`, each returning
  `(control.Status, body)`.
- It encodes the rules the controller should follow:
  - an invalid key or a payload over `MaxObjectSize` gets 400 and changes
    nothing;
  - Get, Delete or Copy of a missing key gets 404;
  - Copy onto the same key works.
- No crashes, no 500s, no concurrency: sequential and fault-free, as in the
  paper.

**V2. `veritas.FS`: the storage port** (about 80 lines)

- `Exec(control.Actuation) control.Feedback` over a `map[path][]byte`. Every
  change is immediately durable and there are no faults.
- It replaces the private `harness.exec` in `controller_test.go`.

**The veritas also serves as a test fake:**

- `controller_test.go` runs on `veritas.FS`.
- `prod` gets a `veritas.FS`-backed `Executor`, so the HTTP handler tests need
  no disk.

The payoff: anyone who adds a new storage command (say an `FSStat`) has to add
it to `veritas.FS`, or the controller tests stop compiling. The spec stays
current because the tests depend on it.

## Part 2: Conformance checks

Each check runs a random sequence of operations against both the
implementation and its veritas in lockstep. After every operation it compares
the results and checks invariants.

**C1. `control.Controller` matches `veritas.Store`** (the paper's Fig. 3 test
for toys3)

- **Operations**, listed simplest first so the shrinker prefers them: `Get`,
  `Put`, `Delete`, `Copy`, `CleanRestart`.
  - `CleanRestart` starts a new controller incarnation on the same storage.
  - The veritas ignores it, so the check proves recovery damages nothing.
- **Driving it:** send one request, run every pending storage command to
  completion, then compare the reply's status and body with the veritas.
- **Invariants after each operation:**
  - decoding every file in `obj/` gives exactly the `veritas.Store` map;
  - `tmp/` is empty once nothing is in flight;
  - no `Trip`.
- **Two storage backends:**
  - `veritas.FS`;
  - `sim.StorageModel` with no faults configured (`Disturbances{}`).
- **Input bias, only where there's a concrete reason:**
  - Get, Delete and Copy mostly target keys that were already Put;
  - payload sizes cluster around 0, `MaxObjectSize` and `MaxObjectSize+1`;
  - Copy sometimes uses the same key for source and destination;
  - a few invalid keys are mixed in.

**C2. `sim.StorageModel` with no faults matches `veritas.FS`**

- Random sequences of storage commands (write, fsync, rename, fsync-dir, read,
  unlink, list) over a small set of paths, so they collide often.
- Compare each completion's error code, the data returned by reads, and the
  names returned by list.

**C3. `prod.OSExecutor` on `t.TempDir()` matches `veritas.FS`**

- The same generator as C2, run against a real disk in a `prod` test.
- It ties the simulator's assumptions to real POSIX behaviour:
  - rename over an existing file;
  - ENOENT for rename or unlink of a missing path;
  - write truncating an existing file;
  - list order.
- C2 and C3 together show that the sim and the real disk agree on the
  no-fault, no-crash path.

**C4. The oracle agrees with `veritas.Store`**

- Feed each C1 run's history into `sim.Oracle` as a sequential history.
- The oracle must accept it, and its final state must equal the
  `veritas.Store` map, compared by decoding which request wrote each value.
- If the two specs ever disagree on 404s, Copy, or anything else, this test
  fails.

**Shared test helper: `veritas/veritastest`**

- A seeded generator for each set of operations, using the sim RNG.
- A lockstep runner that takes the implementation, its veritas, the operations
  and the invariants.
- A shrinker that drops chunks of operations, then single operations, then
  shrinks sizes toward zero.
- On failure, it prints the seed, the replay command, and the shrunk sequence
  as a Go literal you can paste into a regression test.
- It reuses the existing `-seeds`, `-seed` and `-duration` flags.

## Part 3: Keeping the checks honest

- **Mutants.** Add functional bugs that DST catches slowly or not at all:
  - Copy writes to the source key;
  - Delete of a missing key returns 200;
  - an off-by-one in the `MaxObjectSize` check;
  - recovery deletes from `obj/`.

  Add a column to the "Bugs DST catches" table showing which of C1–C4 catches
  each mutant and how long its shrunk sequence is.
- **Coverage (paper §4.2).** Run C1 with `-coverprofile` on `control` and list
  the branches it never reaches. Keep `MaxObjectSize` small so the 400 path
  actually runs; the paper's missed cache bug is the warning here.
- **Docs.** In DESIGN.md:
  - add `veritas/` to the package table;
  - add `veritas` to the vocabulary table ("the executable truth for one
    component; also its test fake");
  - add a "Conformance checks" section next to "Invariants".

## Part 4: Update the book (`toys3/book/`)

- **New chapter `13a-veritas.md`** (between `13-oracle.md` and
  `14-invariants.md`), covering:
  - what a veritas is and why the word replaces "reference model";
  - V1 and V2, and using a veritas as a test fake;
  - the conformance checks C1–C4, how the lockstep runner works, input bias,
    and shrinking, with one worked shrunk failure;
  - how this relates to the paper, and why the oracle stays independent.
- **Edit existing chapters:**
  - `02-architecture.md`: add `veritas` to the package map.
  - `03-vocabulary.md` and `18-glossary-appendix.md`: add *veritas*,
    *conformance check*, *lockstep* and *shrinking*.
  - `11-fake-filesystem.md`: note that C2 and C3 check `StorageModel` against
    `veritas.FS` and a real disk.
  - `13-oracle.md`: point to C4 (the oracle and `veritas.Store` agree).
  - `15-mutation-testing.md`: add the new mutants and the conformance-check
    column.
  - `16-campaigns.md`: how to run the conformance checks with `-seeds`, `-seed`
    and `-duration`.
  - `17-limitations.md`: what conformance checks don't cover (faults,
    concurrency, crashes; the fault-tolerant comparison of paper §4.4 is
    future work).
  - `README.md`: add the new chapter to the table of contents.

## Out of scope for this phase

- **Paper §4.4, failure injection with a "has failed" flag that loosens the
  comparison:** a natural C1 phase 2 (EIO allowed, wrong bytes never allowed).
  The DST oracle already covers it.
- **Paper §5, crash consistency:** toys3's storage model plus the oracle
  already go further than the paper's coarse `DirtyReboot`.
- **Renaming `sim.StorageModel` and the oracle's `refState`,** which still use
  the overloaded word "model": a separate rename commit if you want it.

## Order of work

1. V2 `veritas.FS`, and switch `controller_test.go` to it. This is a pure
   refactor and the existing tests must still pass.
2. The `veritastest` helper (generator, lockstep runner, shrinker) and C2.
3. C3, which will probably turn up real differences between the sim and the
   real disk.
4. V1 `veritas.Store` and C1.
5. C4.
6. The mutants column, the coverage pass and the DESIGN.md update.
7. The book update (Part 4).

Each step is one commit, and `go test ./toys3/...` and
`scripts/ci-determinism.sh` must pass after each.
