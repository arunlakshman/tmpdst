# Sim log engine (CP-DSM-8)

`kafka-log` runs control-plane **replica processes** (`internal/controlplane/statemachine`) plus the
faulting Dynamo loglet (`internal/verify/simulator/fakeservices/fakelogstore`). Brokers still emit `ControlPlaneCall`.
`internal/verify/simulator/fakeservices/fakelogservice` is a thin bridge: boundary faults (503, timeout, lost
response, outage) and `ControlPlaneDeliver` / `ControlPlaneReply` routing to `cp-*` nodes (one
failover hop when a standby refuses). `ControlPlaneRealState` is reference replay of durable
logstore slots (`statemachine.ReplayPosition`). The speccheck package remains the independent oracle
(decision 21). Replica apply, roles, gap-fill, and checkpoints live only in
`internal/controlplane/statemachine`.

Run with `go run ./cmd/mdssimulator -scenario kafka-log -seeds 300` (`kafka.ConfigLog`).
The speccheck-backed preset (`-scenario kafka`) is unchanged.

## Topology

```
 broker ControlPlaneCall --> fakelogservice bridge (boundary faults)
                      |
                   ControlPlaneDeliver to cp-0 / cp-1 (core.Machine)
                      |
                   Offer / fold / LogAppend|LogRead|LogTrim|Obj*
                      |
                   logstore engine (ControlPlaneRealState) + fakeobjectstore + fakenetwork
                      |
                   ControlPlaneReply --> bridge --> ControlPlaneDone to broker
```

Two CP processes (rank 0 preferred appender, rank 1 standby). Three fault
families are all live: kernel attrition can kill or stop a `cp-*` process (a
full reboot from the factory), `StallProb` / `CrashProb` stall or soft-crash the
active replica in process (`control_plane.go` passes the swarmed values, `swarmLog` scales
them), and the bridge and logstore Put fault coins stay live in `swarmLog`.
Two rules in the state machine make this safe:

- A lookup or describe is "fresh" (`reads.go`): the replica reads the log tail
  after the read arrived and answers only when it has applied that tail. A
  standby that served its old fold made a consumer see an end offset below an
  acked one ("fetch out of range"). A replica that is not ready, crashes, or
  hits a trimmed slot refuses its waiting reads, so the bridge fails over.
- A checkpoint ends with a CRC-32. The sim flips a bit in some object reads
  (`GetCorruptProb`), and a restored checkpoint with a changed byte silently
  lost offsets after a process restart. `CheckSnapshot` is run before
  `Restore`; on a bad CRC the replica reads the object again.
- Only a node that has applied the last tail it saw writes a checkpoint, and a
  failed checkpoint read is retried (never catch up from slot 0 on a guess), so
  a lagging node cannot overwrite a newer checkpoint after the log was trimmed. A MaybeApplied commit Put
retries the same slot (conditional append); abandoning the slot used to wedge
`drain` so later commits never applied. `ConfigLog` also raises bridge
`CallerTimeout`, shortens per-Put logstore timeout, and extends Settling/Settle
under stacked retries. Log knobs stay on `simulator.LogParams`. Mutants use
`LogParams.Bug` / `statemachine.Config.InjectedBug` (and `LogStoreFaults.Bug` for trim-delete).

## Mutants (`internal/verify/simulator/kafka/logmutants_test.go`)

| mutant | where | caught by |
|---|---|---|
| `InjectedBugWriterOffsets` | state machine commit path | `kafka: acked range is committed in the control plane` |
| `InjectedBugTrimDelete` | logstore trim | `logcp-sim: a Put never wins a slot below the trim floor` |
| `InjectedBugPerReplicaWindow` | state machine apply | `logcp-sim: a replica's state equals the replay of the log at its slot` |
| `InjectedBugGapSkip` | state machine gap-fill | same digest property |

C2-style conformance for the state machine path is `go test ./internal/controlplane/statemachine -run Lockstep`.

## Reachables

Campaign expect-reachables use the `logcp-sim:` property names (`logReachables`
in `kafka/config.go`): gap fill, same-slot Put, window replay, trim, standby
took over, failback, replica stalled, replica crashed, restore from a checkpoint.
Not required in the default campaign: `idempotent replay after takeover` (too rare
in 300 seeds) and `replica hit a trimmed slot`.
