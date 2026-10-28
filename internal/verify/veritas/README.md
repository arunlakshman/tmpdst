# veritas: the answer key for the outside world

This package holds small, correct models of the things our code talks to. We
call each model a **spec**. Real code and simulated code are compared with the
specs, to prove that they behave the same.

Words used here:

- **spec**: a short program that shows the correct behavior. It has no faults
  and it runs one operation at a time.
- **oracle**: a checker we trust to say if a result is correct. A spec is the
  oracle for the code that is compared with it.
- **subject**: the real or simulated code under test.
- **seed**: a number that fixes the random operations. The same seed always
  gives the same test.
- **planted bug**: a known bug that a test adds on purpose, to prove that the
  suite can catch it.

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
