# Integration test gaps

Two tests are missing from `test/integration`. This note is the context to pick
them up later. Nothing here is built yet.

## What runs today

`make it` runs the integration tests. Only the backends run in Docker, started by
testcontainers from `internal/testinfra`:

- DynamoDB Local (`amazon/dynamodb-local:3.1.0`, in memory)
- PostgreSQL (`postgres:17.11-alpine`)
- SeaweedFS for S3 (`chrislusf/seaweedfs:4.48`)

The control plane is the real `mdscontrolplane` binary, run as local child
processes (not in a container). A real gRPC client talks to it. The client in
`test/integration/client_test.go` has the broker's failover rule: it goes to the
next node when a node is down or is not the writer, and a retried commit keeps its
segment key.

Each test runs on DynamoDB and on PostgreSQL:

| Test | What it does |
|---|---|
| `TestStoreAndLookup` | One node. Create a topic, commit a segment, find it. |
| `TestRestartFromCheckpoint` | One node. 30 commits, log trimmed, restart, state comes back from the S3 checkpoint. |
| `TestThreeNodesTakeover` | Three nodes, 3 writers. SIGKILL the writer once. Every acknowledged commit must be on both surviving nodes. |

The gRPC calls used are `CreateTopic`, `CommitSegment`, `FindSegments`,
`DescribeTopics` and `GetStatus`.

## Gap 1: consumer group calls

No test in `test/integration` calls `JoinGroup`, `SyncGroup`, `Heartbeat`,
`LeaveGroup`, `OffsetCommit`, `OffsetFetch`, `DescribeGroups` or `ListGroups`.

These calls are tested in other places, but never with the real binary on a real
backend:

- `test/conformance/grpc_rebalance_test.go`: a real node over gRPC against the
  rebalance model, but with no real log store.
- `internal/verify/simulator`: consumers through takeovers, with a fake log.

So nothing proves that a consumer group works on DynamoDB, or that it survives the
death of a real process.

What to add:

- A few consumers join one group and sync. Check that they get one generation, one
  leader, and the assignment the leader gave.
- Heartbeats answer OK while the group is stable.
- Commit offsets, then fetch them and get the same values.
- A consumer leaves or goes silent. The others are told to join again, and a new
  generation forms.
- Kill the writer. The consumers find the new node and join again, and the
  acknowledged offsets are still there.

## Gap 2: failover under load

`TestThreeNodesTakeover` stops soon after about 250 acknowledged commits (100
before the kill, 150 after). It has one failover, and the killed node never comes
back. It has no reads during the run, and it checks commit answers only at the end.

What to add, as `TestFailoverUnderLoad` or similar:

- A small load, not a stress test: a few thousand requests in total, from a few
  concurrent clients. Start near 5000, and keep the number in one constant.
- A mix of commits and reads.
- 3 or 4 failovers: SIGKILL the writer about every 1000 acknowledged commits, then
  start it again as a standby, so it has to catch up.
- Check each answer as it comes back: the offsets and counts are right, and a retry
  with the same segment key gets the same answer.
- Check that the writer epoch went up for each failover, so that a run with no real
  failover cannot pass.
- At the end, every live node holds exactly what was acknowledged. The helper
  `partitionsMatch` in `controlplane_test.go` already does this.

Performance numbers and a long run are not goals.

## Notes for whoever picks this up

- Reuse `startNode`, `newClient`, `newBackend`, `rankArgs` and `trimArgs`.
- DynamoDB is the backend that matters for these tests. Running them on PostgreSQL
  too is cheap (`stores` in `controlplane_test.go`), but it adds run time.
- The speed on DynamoDB Local is not measured. Measure it first, then set the
  request count and the timeout. `make it` has a 15 minute limit for all packages.
- Today `make it` runs everything. A make target for one test would help while
  working on these.
- The test client needs group helpers. `client_test.go` has only topic and segment
  helpers today. Its `retryable` rule should work for group calls, but check it: a
  group answer of `ERR_CODE_UNAVAILABLE` means try the next node.
- Decide what a client may see during a takeover. A write that fails may have been
  applied (`MaybeApplied`), so the checks must allow that, as the takeover test does
  by retrying with the same key.
- DynamoDB Local is not real DynamoDB. It has no throttling and no real network
  faults, so these tests do not cover those.
