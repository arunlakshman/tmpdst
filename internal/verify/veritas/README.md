# veritas: the answer key for the outside world

This package holds small, correct models of the things our code talks to. We
call each model a **spec**. Real code and simulated code are compared with the
specs, to prove that they behave the same.

Words used here:

- **spec**: a short program that shows the correct behavior. It has no faults
  and it runs one operation at a time. The paper below calls it a
  "reference model".
- **oracle**: a checker we trust to say if a result is correct. A spec is the
  oracle for the code that is compared with it.
- **subject**: the real or simulated code under test.
- **seed**: a number that fixes the random operations. The same seed always
  gives the same test.
- **shrink**: make a failing case smaller, so that a person can read it.
- **planted bug**: a known bug that a test adds on purpose, to prove that the
  suite can catch it.

## Where this design comes from

This package follows the ideas of one paper:

> James Bornholt, Rajeev Joshi, Vytautas Astrauskas, Brendan Cully, Bernhard
> Kragl, Seth Markle, Kyle Sauri, Drew Schleit, Grant Slatton, Serdar Tasiran,
> Jacob Van Geffen and Andrew Warfield. *Using Lightweight Formal Methods to
> Validate a Key-Value Storage Node in Amazon S3.* SOSP 2021.
> https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3
> (DOI 10.1145/3477132.3483540)

Read the paper once. It is 15 pages. This
section tells you what we took from it, what we changed and what we did not do.

### The problem in the paper

Amazon S3 built a new storage node called ShardStore. It has more than 40,000
lines of Rust. It changes every week. A full proof of such a system is too
expensive, and the proof would be out of date after the next change. So the
authors chose "lightweight formal methods". They do not prove the code correct.
They use tools that run by themselves, that normal engineers can use, and that
keep working while the code and the specification change.

They found 16 bugs before production, including crash and concurrency bugs
that normal tests had missed. The team that owns the code now maintains the
checks. That is the real result: the checks stayed alive.

### The idea we took

1. **Write the specification as a small program, in the same language as the
   code.** The paper calls it an executable reference model. The model for
   their log-structured merge tree is a hash map, and all models together are
   about 1% of the implementation (450 lines, against 44,048). Because it is
   normal code, engineers can read it and change it without learning a new
   language. Our three specs are about 400 lines of Go in total
   (`objectstore.go`, `controlplane.go`, `partition.go`). `ObjectStore` is a
   map. `Partition` is a slice.
2. **Check that the real code agrees with the model.** The paper uses
   property-based testing: make a random list of operations, run each one on
   the model and on the implementation, compare the results, then check some
   invariants. We do the same in `veritastest.SpecComparator` (the `Step` and
   `Invariant` functions).
3. **Split "correct" into separate properties and check each with the right
   tool.** The paper has three: normal behavior, crash behavior and concurrent
   behavior. We have four checks, C1 to C4, one for each kind of component (see
   below).
4. **Make the failing case small.** The paper reports one failure that began
   with 61 operations and ended with 6 after automatic minimization. We have
   our own shrinker in `veritastest`. It removes chunks of operations and
   replaces each operation with a simpler one. The result is printed as a Go
   list, so you can paste it into a test.
5. **Choose random arguments with care.** If keys are fully random, a Get
   almost never finds a key that a Put wrote. The paper biases Get toward keys
   that were Put earlier, and toward corner cases. It also warns that too much
   bias hides the cases you did not think of, so it keeps the default
   randomness and adds bias only when there is evidence. Our `generator`
   profiles do the same: they change how often things happen (small key pools,
   many replays, many invalid requests), and every case stays possible.
6. **Watch the quality of the generator.** The paper uses code coverage to find
   parts that the tests never reach. We count the interesting cases that each
   generator makes (a replay, a conflict, an invalid commit, a lookup that is
   cut by `MaxBytes`, and so on) and the test fails if a count is too low.
7. **Keep the system deterministic**, so that a failure can be replayed. The
   paper removes random behavior, for example the random order of a Rust hash
   map. We have the same rule: a spec never lets the order of a Go map decide
   a result.
8. **Reuse the model as a test fake.** The paper uses its index model as a mock
   in unit tests. Our adapters in `vsys` run production code on top of the
   specs, and the simulator uses the specs as its fake object store and fake
   control plane.
9. **Run longer to find more.** The paper calls this "pay-as-you-go": the same
   checks run quickly on a laptop and for a long time before a release. We have
   `MANIFESTER_SEEDS` and `MANIFESTER_CAMPAIGN` for this.
10. **A passing check is not a proof.** The paper gives an example: a cache was
    so large in all tests that the cache-miss path was never reached, and a bug
    there was missed. Our broker tests use very small segment limits
    (`TestConformanceBrokerSmallSegments`) so that every rule for cutting a
    segment is used.

### What we changed or added

- **Planted bugs.** The paper reports the bugs that its checks found in real
  code. We also add known bugs on purpose (`veritastest/plantedbug`), and every
  suite must catch all of them. A suite that cannot catch a planted bug is not
  trusted.
- **Four checks, with two independent judges.** C1 to C4 below. The Kafka
  oracle is a second checker that never imports this package, so one mistake
  cannot hide in both.
- **A whole-system simulator.** Our project also runs the full system in a
  deterministic simulator (`internal/verify/sim`). The paper mostly tests one
  component at a time, on an in-memory disk. The specs here serve both uses.
- **Plain-language rules for the team.** The code is maintained by engineers who
  read English as a second language, so names and comments are simple. See
  `veritastest/README.md` for how to add a new spec.

### What we did not do (yet)

- **No proofs.** Like the paper, we can miss bugs. A green test means "no bug
  found with these seeds", not "no bug".
- **No failure injection in the comparison yet.** The paper relaxes the check
  after an injected disk failure, with a simple "has failed" flag, because the
  model does not know which part of an operation was done. Our C2 runs with all
  faults off. The design for faults is in section 8 of
  `docs/veritas-conformance-plan.md`.
- **No crash-state checks.** The paper checks crash consistency with a
  "Dependency" object for every write. Our brokers keep no state, and the
  control plane is rebuilt from its log, so we did not need this first.
- **No concurrency model checking.** The paper uses Loom and Shuttle to check
  that the code is linearizable. We do not. The `vsys` adapters use a mutex, and
  the simulator runs one step at a time.
- **No proof of the specs themselves.** The paper starts to prove properties of
  its models with the Prusti tool. We check our specs with an independent naive
  model and with invariants (see below).

## The three specs

| Spec | File | What it models | Real code it checks |
| --- | --- | --- | --- |
| `ObjectStore` | `objectstore.go` | S3: put, get a byte range, delete, list | the S3 adapter and the simulated object store |
| `ControlPlane` | `controlplane.go` | the control plane: commit a segment, lookup, describe topics | the applier, the memstore, the gRPC client and the simulated control plane |
| `Partition` | `partition.go` | one Kafka partition: append, read, high watermark | the broker (through the control plane and object store specs) |

The rules of each spec are written in the comment above its type. Read that
comment first. It is the shortest way to learn what the real code must do.

## The four checks

| Check | What is tested | Compared with | Real or simulated |
| --- | --- | --- | --- |
| C1 | broker answers: produce, fetch, ListOffsets | `Partition` | broker code is real; storage and control plane are specs |
| C2 | simulated object store and control plane, faults off | `ObjectStore`, `ControlPlane` | engines are simulated |
| C3 | S3 adapter and control-plane stores | `ObjectStore`, `ControlPlane` | adapters are real code |
| C4 | the Kafka oracle in `sim/kafka` | `Partition`, `ControlPlane` | oracle is simulation-side code |

The Kafka oracle never imports this package. This keeps the two checkers
independent. If both used the same code, one mistake could hide in both.

## Run the tests

From the repository root:

```bash
# The specs and the shared test framework
go test ./internal/verify/veritas/... -count=1

# C1, C2, C3 (C3 for S3 needs MANIFESTER_S3_ENDPOINT; it skips if it is not set)
go test ./internal/broker ./internal/verify/sim/engines \
        ./internal/sys/s3store ./internal/controlplane/... -count=1

# C4
go test ./internal/verify/sim/kafka -run C4 -count=1
```

## Control how many random tests run

| Variable | Meaning |
| --- | --- |
| `MANIFESTER_SEEDS=200` | run seeds 1 to 200 in every conformance test |
| `MANIFESTER_SEED=17` | run only seed 17 (use it to replay a failure) |
| `MANIFESTER_CAMPAIGN=30s` | keep running new seeds until 30 seconds pass |

A bad value stops the test with a message that names the variable. Without any
variable, each test uses its own small default, so a normal run stays fast.

## When a test fails

The failure message gives four things:

1. the seed,
2. what was different (the subject answer and the spec answer),
3. a command to replay that one seed,
4. the failing sequence, made as small as possible, as a Go list that you can
   paste into a regression test.

Run the replay command, then read the shrunk sequence from the top. The last
operation is the one that failed.

## Where things live

| Path | Purpose |
| --- | --- |
| `veritastest/` | `SpecComparator`: compares a subject with a spec after every step and shrinks a failing sequence |
| `veritastest/generator/` | random operations, in named profiles, with coverage tests |
| `veritastest/observation/` | one common result shape, so results can be compared as values |
| `veritastest/suites/` | the shared tests for each spec; they accept any subject |
| `veritastest/plantedbug/` | known bugs that every suite must catch |
| `veritastest/runcontrol/` | reads the three variables above |
| `vsys/` | adapters that let production code run on top of the specs |

To add a new spec, follow `veritastest/README.md`.

## Rules for this package

- No random order. Never let the order of a Go map decide a result.
- No clock, no network, no goroutines, no files. A spec is plain data.
- Keep a spec short enough that a reader can see that it is correct. If a spec
  grows hard to read, split the rule, do not add more code.
- Write names and comments in short, simple English.
