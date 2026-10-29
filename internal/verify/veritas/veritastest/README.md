# How to add a new spec and its tests in 30 minutes

This guide is for engineers who add a new spec to `internal/verify/veritas`
and want it checked against a real or simulated component.

Words used here:

- **spec**: a short program that shows the correct behavior. It has no faults
  and it runs one operation at a time.
- **oracle**: a checker we trust to say if a result is correct. A spec is the
  oracle for the code that is compared with it.
- **subject**: the real or simulated code that we compare with the spec.
- **planted bug**: a known bug that a test adds on purpose, to prove that the
  suite can catch it.
- **shrink**: make a failing case smaller.

## The packages

`veritastest` (this directory) has `SpecChecker`, `Run` and `Rand`. It
compares a subject with a spec after every step and shrinks a failing
sequence.

These packages sit under `veritastest/`:

| Package | What it does |
| --- | --- |
| `veritastest/generator` | Makes random operations. An `InputProfile` says how often each kind of operation appears. |
| `veritastest/observation` | Turns a result from the spec, a simulated engine or a real adapter into one common shape, so the results can be compared. |
| `veritastest/plantedbug` | Wraps a subject and adds a known bug. |
| `veritastest/runcontrol` | Reads the environment variables that choose the seeds, and builds the replay command. |

## Steps

### 1. Write the spec (about 10 minutes)

Add one file in `internal/verify/veritas`, for example `myspec.go`.

- Keep plain data. Do not use goroutines, time, files or the network.
- Do not use map iteration order for any result. Sort the keys first.
- Add a `StateHash` method. It lets a test compare the whole state with one
  number.
- Write the rules in the doc comment of the type, in short sentences. A reader
  must be able to check the code against the comment.
- Add unit tests for each rule.

Then write a second, naive model made of plain maps or slices, from the rules
in the doc comment only. Do not copy the spec code. Compare the spec with the
model on random operations. See `controlplane_model_test.go` and
`partition_model_test.go` in `internal/verify/veritas`. This checks the spec
itself: nobody else checks the answer key.

### 2. Write a generator profile and a coverage test (about 5 minutes)

Put the operation generator in `veritastest/generator`. Add an `InputProfile` that
says how often each kind of operation appears. A generator that never makes a
replay, a conflict or an invalid request proves little.

Write a coverage test. It runs many seeds on the spec alone and counts each
interesting result. It fails if a count is zero. The existing example is
`TestControlPlaneCoverage` in `veritastest/generator/generator_test.go`.

A generator takes all random values from the `*veritastest.Rand` it receives.
Then the same seed always gives the same sequence.

### 3. Write the suite (about 5 minutes)

Build a `veritastest.SpecChecker`. Set these fields:

- `New`: a fresh subject and a fresh spec, both empty.
- `Gen`: your generator.
- `Step`: apply one operation to both sides. Convert both results with
  `veritastest/observation` and return an error that says what is different.
  Write the error as a full sentence with the numbers in it.
- `Invariant` (optional): a rule that must hold after every step, for example
  `StateHash` equals the state hash of the subject.
- `Shrink` (optional): simpler versions of one operation.
- `Format`: the operation as Go source text, so a failure can be pasted into a
  regression test.
- `Ops`: the number of operations in one sequence.

Then call `veritastest.Run(t, comparator, options)`.

### 4. Add planted bugs (about 5 minutes)

Use `veritastest/plantedbug` to wrap the subject with a known bug, for
example a wrong offset or a lost write. Write a test that runs the suite on the
wrapped subject and checks that it fails. A suite that cannot catch a planted
bug does not protect anything.

### 5. Set the options with runcontrol (about 2 minutes)

Do not write `Seeds` and `Seed` by hand. Call
`runcontrol.Options(t, defaultSeeds)`. It returns the `veritastest.Options` and
sets the replay command. Pass the result to `veritastest.Run`.

| Variable | Meaning |
| --- | --- |
| `MANIFESTER_SEEDS` | The number of seeds to run (1 to this number). |
| `MANIFESTER_SEED` | Run only this one seed. |
| `MANIFESTER_CAMPAIGN` | Run seeds until this time has passed, for example `30s` or `5m`. |

A wrong value stops the test with a message. It does not use a default.

### 6. Replay a failing seed

A failure prints the seed, the shrunk sequence and a replay line. Run the line
inside the package directory:

```
MANIFESTER_SEED=17 go test -run '^TestMySuite$' -count=1 -v .
```

Use the top-level test name. The shrunk sequence is Go source text. Paste it
into a normal test to keep the case forever.

## The four checks: real and simulated parts

| Check | Real component | Simulated component or spec |
| --- | --- | --- |
| C1 broker vs Partition | The broker state machine (`internal/broker`). | The object store and the control plane are the specs `veritas.ObjectStore` and `veritas.ControlPlane`. The reference is `veritas.Partition`. |
| C2 simulated engines vs specs | None. | The simulated engines (`sim/engines`) run with all faults off. The reference is the spec. |
| C3 adapters vs specs | The S3 adapter and the control-plane stores (in-memory store, gRPC client, applier on MemKV and on Pebble). The S3 test needs `MANIFESTER_S3_ENDPOINT`. | The reference is the spec. |
| C4 Kafka oracle vs specs | None. | The oracle in `sim/kafka` is checked with histories built from `Partition` and `ControlPlane`. |

C3 is the only check that runs the real code in `internal/sys` and the real
control-plane stores.
