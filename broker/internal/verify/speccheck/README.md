# speccheck: the tools that compare the broker with its specs

A **spec** is a small, correct model of a thing our code talks to. Real code
and simulated code are compared with the specs, to prove that they behave the
same. The specs are not in this folder. Each one lives with the thing it
describes (see the table below). This folder holds the tools: the suites, the
planted bugs, the bridge from broker calls to the control plane spec, and the
adapters that run production code on top of the specs.

Terms: a **spec** is a short, fault-free program that shows the correct
behavior. An **oracle** is a checker we trust to say if a result is correct.
A **subject** is the code under test. A **seed** fixes the random operations,
so the same seed gives the same test. An **injected bug** is a known bug that a
test adds on purpose, to prove that the suite can catch it. The framework calls
it a **planted bug** (`spectest.Bug`); in this folder the package is still
named `injectedbug`.

## Where this design comes from

This package follows one paper: James Bornholt et al., *Using Lightweight
Formal Methods to Validate a Key-Value Storage Node in Amazon S3*, SOSP 2021.
https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3

**What we took**

- A spec is a small program in the same language as the code (the paper calls
  it a reference model). The paper's models are about 1% of the implementation.
  Our specs are small.
- Property-based testing: make random operations, run each on the spec and on
  the subject, compare the results, check invariants (`spectest.Lockstep`).
- Split "correct" into separate checks, each with its own tool (C1 to C4 below).
- Shrink a failing sequence to a small one (rapid does it). The paper cut one
  failure from 61 operations to 6.
- Bias the random arguments only a little, and keep every case possible
  (`conformance` profiles). Count the interesting cases that a generator makes
  (`coverage_test.go`).
- Keep everything deterministic, so a failure can be replayed: a failure prints
  the rapid seed and the command that runs it again.
- Use the spec as a test fake (`specsystem`, and the simulator).
- Run longer to find more (`MDS_SEEDS`, `MDS_CAMPAIGN`).
- A pass is not a proof. Use small limits so that every code path is reached.

**What we added:** planted bugs that every suite must catch, a second checker
(the Kafka oracle) that never imports a spec, and a whole-system
simulator.

**What we did not do yet:** failure injection in the comparison (see section 8
of `docs/speccheck-conformance-plan.md`), crash-state checks, concurrency model
checking, and proofs of the specs themselves. Like the paper, we can miss bugs.

## The three specs

| Spec | Where | What it models | Real code it checks |
| --- | --- | --- | --- |
| `objectstore.Spec` | `shared/objectstore/spec.go` | S3: put, get a byte range, delete, list | S3 adapter, simulated object store |
| `spec.ControlPlane` | `controlplane/spec` | topics, segments (commit, find), consumer groups | the control plane, the simulated control plane |
| `spec.Broker` | `broker/spec/broker.go` | one Kafka partition: append, read | the broker |

There is one control plane spec in the repo. The broker kernel speaks
`core.IOCall`; `specbridge` converts a call to a `contract.Operation`, asks the
spec, and converts the answer back. It has no rule of its own.

The rules of each spec are in the comment above its type. Read that first.

## The checks (C2 was deleted in T1.15)

| Check | What is tested | Compared with | Real or simulated |
| --- | --- | --- | --- |
| C1 | broker answers: produce, fetch, ListOffsets | `spec.Broker` | broker is real; storage and control plane are specs |
| C2 | deleted: the simulated object store and control plane are `dst/check/faulty` around the spec itself, so there is nothing to compare | - | - |
| C3 | S3 adapter (control-plane stores: `controlplane/test/conformance`) | `objectstore.Spec`, `spec.ControlPlane` | real |
| C4 | the Kafka oracle in `internal/verify/simulator/kafka` | `spec.Broker`, `spec.ControlPlane` | simulation side |

## Run the tests

```bash
go test ./spec ./internal/verify/speccheck/... -count=1     # broker spec and suites
(cd ../controlplane && go test ./spec -count=1)             # control plane spec
go test ./internal/broker ./internal/verify/speccheck/s3conform -count=1   # C1, C3
go test ./internal/verify/simulator/fakeservices/... -count=1   # fault tests of the simulated services
go test ./internal/verify/simulator/kafka -run C4 -count=1      # C4
```

The S3 test in C3 needs `MDS_S3_ENDPOINT`; without it, it skips.

Every suite is a `spectest.Lockstep` (package `dst/check/spectest`, on rapid).
The variables are read by the one parser in `dst/campaign` (`campaign.ReadEnv`),
with the prefix `MDS` (`conformance.EnvPrefix`):

| Variable | Meaning |
| --- | --- |
| `MDS_SEEDS=200` | run 200 checks (a check is one random list of steps) |
| `MDS_SEED=17` | run only the rapid seed 17 (replay a failure) |
| `MDS_CAMPAIGN=30s` | keep running new seeds (1, 2, 3, ...) for 30 seconds |

`MDS_SEED` is the strongest, then `MDS_CAMPAIGN`, then `MDS_SEEDS`. Without a
variable, each test runs the number of checks that its code sets. A failure
prints the seed, the difference, the command that runs it again, and the
shortest failing list of steps as Go code. To add a new spec, add a Lockstep
builder to `conformance/` like `ObjectStoreLockstep` (generator, subject
interface, result type, `Equal`), and a planted bug or two in `injectedbug/`.

## Where things live

| Path | Purpose |
| --- | --- |
| `conformance/` | the suites (`spectest.Lockstep`) for each spec: generators and profiles, result values, subjects, the naive control plane model |
| `injectedbug/` | planted bugs that every suite must catch (`spectest.RequireCaught`) |
| `s3conform/` | check C3: the S3 adapter against the object store spec |
| `specbridge/` | the control plane spec behind `core.IOCall` |
| `specsystem/` | adapters that run production code on top of the specs |
| `../../../spec/` (`broker/spec`) | the broker spec and its naive-model test |

A spec never lets the order of a Go map decide a result, and it uses no clock,
network, goroutines or files.
