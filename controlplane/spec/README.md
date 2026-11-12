# spec: the reference model of the control plane

`spec.ControlPlane` is a small program that gives the correct answer to every
control plane operation (topics, segments, consumer groups). It keeps plain maps
and slices, runs one operation at a time, and has no faults. It is a small
part of the code it checks. The idea is from the paper "Using Lightweight Formal
Methods to Validate a Key-Value Storage Node in Amazon S3" (Bornholt et al.,
SOSP 2021).

## The files

- `spec.go`: the `Model` interface, `ControlPlane`, and the rules for topics
  and segments.
- `groups.go`: the rules for the logged half of consumer groups (`GroupInstall`,
  `GroupAssign`, `GroupRemove`, `OffsetCommit`, `OffsetFetch`, describe, list).
- `rebalance.go`: `spec.Rebalance`, the model of the classic group protocol
  (join, sync, heartbeat, leave, timers, role changes). It holds a
  `ControlPlane` as its `Store` for the logged half. The rules are in
  `docs/rebalance.md`.
- `spec_test.go`, `rebalance_test.go`: small examples of each rule.

## The independence rule

The model shares types with the code (`contract`), and never logic.
It imports none of `domain`, `metadb`, `engine` or `replica`, and no program of
the product imports it: `go list -deps ./cmd/mdscontrolplane` does not list it.
A test that compared the code with itself would prove nothing. Only tests and
the simulator import the model. The simulator reaches it through one file,
`internal/verify/simulator/spec.go`.

The model is deterministic: no clock, network, goroutines or files, and no map
order reaches an answer. It is in the determinism gate (`make determinism`).

```go
m := spec.New()
done := m.Apply(contract.CreateTopic{Name: "t", Partitions: 2}) // spec.Model
```

## The tests that use the model

All of them are in `test/conformance` (the conformance tests). All but
`TestGRPCRebalance` (a fixed script) use
[rapid](https://pkg.go.dev/pgregory.net/rapid) (a test-only dependency). Each
rapid test of the metadata kind gives `Run` a target: a few functions (`Exec`, and as many of `Reboot`,
`DirtyReboot`, `Checkpoint` and `Inject` as the target has). The harness is in
`harness_test.go`, the operations that rapid draws are in `ops_test.go`. The
rebalance tests have their own world and steps (`rebalance_test.go`,
`grpc_rebalance_test.go`), because that model also has time and calls that wait.

| Test | What it checks |
|---|---|
| `TestMetadataMemory`, `TestMetadataPebble` | metadb, on the memory engine and on Pebble, answers like the model. A reboot writes a checkpoint and restores from it (memory), or closes and opens the engine again (Pebble). |
| `TestReplica` | The real replica in one process (in-memory log and object store). Clean restarts, crashes and checkpoints in the middle, with and without log trim, with and without injected storage failures. |
| `TestGRPC` | The real gRPC stack: client, server, replica, on a trimmed log. Crashes, checkpoints and injected failures, with no clean restart (the replica test has it). The wire API has only some of the group operations, so the target leaves out the others (`Target.Lacks`). |
| `TestRebalanceMachine` | The rebalance machine (`domain/rebalance`) against `spec.Rebalance`, with timers, failed writes, role changes and abandoned calls. |
| `TestGRPCRebalance` | The real gRPC group handlers (the node hosts the machine) against `spec.Rebalance`, in a fixed script. |
| `TestDouble*` | Two known faults of the environment: a crash that loses answered writes, and "no data" after a failure. Each test wraps the replica target with the fault and checks that `Run` FAILS. |

The bugs in the code itself are not tests of this kind. They are the mutants of
`ops/mutants.txt` (`make mutants`): each plants one real bug in a copy of the
tree and a named test must fail. The group and topic mutants are killed by
`TestMetadataMemory`.

How a test works: rapid draws a list of operations. Each one goes to the model
and to the target, and the answers must be equal. After each answer some
invariants are checked (runs of a partition have no gap, topics are sorted).
Background operations (`checkpoint`, `reboot`, `dirtyReboot`) do nothing in the
model, so a restart must not change any answer. After a restart the whole state
is compared with the model, and every earlier commit is sent again: it must be a
replay with the same ranges.

The operations are in a list from simple to complex (reads, commit, create
topic, group operations, background operations). The shrinker prefers the first
ones, so a failure is shrunk to simple operations. We use rapid's shrinker and
nothing of our own. Arguments come from small sets, and only a few choices lean
on purpose. They are listed at the top of `ops_test.go`. Each operation lists
the answers it aims at (`want`), and `Run` counts the answers of all checks. If
an aimed-at answer was not seen, `Run` makes more rounds of checks (at most 20
more), and fails if it is still missing. So every rule is reached, and a mutant
does not survive by luck. A fixed seed or fewer than 10 checks turns the
counting off.

### Failure injection

`Inject` makes the next call to the storage fail: an append that stores nothing,
an append that stores and then fails (the answer is lost), a failed log read, a
failed checkpoint upload. After an injected failure the flag `hasFailed` is set.
Then an operation may answer with an error, and the test asks again until it
gets a real answer. The real answer must equal the model. An error is allowed, a
wrong answer never is. The paper also allows "no data". Our log does not lose
data, so we do not.

Because the test sends an operation again, this test uses only operations that
are safe to send again: no group install, assign or remove, and no create topic
with `FailIfExists`.

### What the paper has that we leave out

Concurrency and interleavings are not tested here. The simulator
(`internal/verify/simulator`, `make sim`) does that. The object store has no
model: its rules are a plain table in `test/s3store/conform_test.go`.

## More checks, and replaying a failure

Rapid runs 100 checks by default. Some slow tests run fewer. A number that you
give with a flag is used for all of them.

```sh
make spec-long                                     # 2000 checks of each test (timeout 60 min)
go test ./test/conformance -run TestReplica \
    -rapid.checks=500 -rapid.steps=40              # more checks, longer lists
```

When a check fails, rapid shrinks it and prints the shrunk list of actions with
the drawn values, and the seed and a fail file:

```
To reproduce, specify -run="TestReplica/trim=true,failures=false" -rapid.failfile="testdata/rapid/.../x.fail" (or -rapid.seed=123)
```

Run the same test with `-rapid.failfile=...` or `-rapid.seed=...` to see it again.
Rapid also replays the files it finds in `testdata/rapid/<test>/` at the start of
that test, so keep a fail file as a regression test if you want. Use
`-rapid.nofailfile` to stop rapid from writing the file. With `-rapid.v` you
see more detail. A test with a real clock and goroutines (the replica tests) can be
slightly different on each run, so a replay may need more than one try.
