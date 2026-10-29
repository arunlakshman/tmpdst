# veritas: the answer key for the outside world

This package holds small, correct models of the things our code talks to. We
call each model a **spec**. Real code and simulated code are compared with the
specs, to prove that they behave the same.

Terms: a **spec** is a short, fault-free program that shows the correct
behavior. An **oracle** is a checker we trust to say if a result is correct.
A **subject** is the code under test. A **seed** fixes the random operations,
so the same seed gives the same test. A **planted bug** is a known bug that a
test adds on purpose, to prove that the suite can catch it.

## Where this design comes from

This package follows one paper: James Bornholt et al., *Using Lightweight
Formal Methods to Validate a Key-Value Storage Node in Amazon S3*, SOSP 2021.
https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3

**What we took**

- A spec is a small program in the same language as the code (the paper calls
  it a reference model). The paper's models are about 1% of the implementation.
  Our three specs are about 400 lines of Go.
- Property-based testing: make random operations, run each on the spec and on
  the subject, compare the results, check invariants (`veritastest.SpecChecker`).
- Split "correct" into separate checks, each with its own tool (C1 to C4 below).
- Shrink a failing sequence to a small one. The paper cut one failure from 61
  operations to 6.
- Bias the random arguments only a little, and keep every case possible
  (`generator` profiles). Count the interesting cases that a generator makes.
- Keep everything deterministic, so a failure can be replayed.
- Use the spec as a test fake (`vsys`, and the simulator).
- Run longer to find more (`MANIFESTER_SEEDS`, `MANIFESTER_CAMPAIGN`).
- A pass is not a proof. Use small limits so that every code path is reached.

**What we added:** planted bugs that every suite must catch, a second checker
(the Kafka oracle) that never imports this package, and a whole-system
simulator.

**What we did not do yet:** failure injection in the comparison (see section 8
of `docs/veritas-conformance-plan.md`), crash-state checks, concurrency model
checking, and proofs of the specs themselves. Like the paper, we can miss bugs.

## The three specs

| Spec | File | What it models | Real code it checks |
| --- | --- | --- | --- |
| `ObjectStore` | `objectstore.go` | S3: put, get a byte range, delete, list | S3 adapter, simulated object store |
| `ControlPlane` | `controlplane.go` | commit a segment, lookup, describe topics | applier, memstore, gRPC client, simulated control plane |
| `Partition` | `partition.go` | one Kafka partition: append, read | the broker |

The rules of each spec are in the comment above its type. Read that first.

## The four checks

| Check | What is tested | Compared with | Real or simulated |
| --- | --- | --- | --- |
| C1 | broker answers: produce, fetch, ListOffsets | `Partition` | broker is real; storage and control plane are specs |
| C2 | simulated object store and control plane, faults off | `ObjectStore`, `ControlPlane` | simulated |
| C3 | S3 adapter and control-plane stores | `ObjectStore`, `ControlPlane` | real |
| C4 | the Kafka oracle in `sim/kafka` | `Partition`, `ControlPlane` | simulation side |

## Run the tests

```bash
go test ./internal/verify/veritas/... -count=1            # specs and framework
go test ./internal/broker ./internal/verify/sim/engines \
        ./internal/sys/s3store ./internal/controlplane/... -count=1   # C1, C2, C3
go test ./internal/verify/sim/kafka -run C4 -count=1      # C4
```

The S3 test in C3 needs `MANIFESTER_S3_ENDPOINT`; without it, it skips.

| Variable | Meaning |
| --- | --- |
| `MANIFESTER_SEEDS=200` | run seeds 1 to 200 |
| `MANIFESTER_SEED=17` | run only seed 17 (replay a failure) |
| `MANIFESTER_CAMPAIGN=30s` | keep running new seeds for 30 seconds |

A failure prints the seed, the difference, a replay command, and the shortest
failing sequence as a Go list. To add a new spec, follow `veritastest/README.md`.

## Where things live

| Path | Purpose |
| --- | --- |
| `veritastest/` | `SpecChecker`: compares after every step and shrinks |
| `veritastest/generator/` | random operations in named profiles |
| `veritastest/observation/` | one result shape, so results compare as values |
| `veritastest/suites/` | shared tests for each spec; they accept any subject |
| `veritastest/plantedbug/` | known bugs that every suite must catch |
| `veritastest/runcontrol/` | reads the variables above |
| `vsys/` | adapters that run production code on top of the specs |

A spec never lets the order of a Go map decide a result, and it uses no clock,
network, goroutines or files.
