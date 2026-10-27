# Sim log engine (CP-DSM-8)

`kafka-log` runs control-plane **replica processes** (`controlplane/machine`) plus the
faulting Dynamo loglet (`sim/engines/logstore`). Brokers still emit `CPCall`.
`sim/engines/logengine` is a thin bridge: boundary faults (503, timeout, lost
response, outage) and `CPOffer` / `CPReply` routing to `cp-*` nodes (one
failover hop when a standby refuses). `CPTruth` is reference replay of durable
logstore slots (`machine.ReplaySlot`). Veritas remains the independent oracle
(decision 21). Replica apply, roles, hole-fill, and checkpoints live only in
`controlplane/machine`.

Run with `go run ./cmd/manisim -scenario kafka-log -seeds 300` (`kafka.ConfigLog`).
The veritas-backed preset (`-scenario kafka`) is unchanged.

## Topology

```
 broker CPCall --> logengine bridge (boundary faults)
                      |
                   CPOffer to cp-0 / cp-1 (core.Machine)
                      |
                   Offer / fold / LogAppend|LogRead|LogTrim|Obj*
                      |
                   logstore engine (CPTruth) + objstore + netsim
                      |
                   CPReply --> bridge --> CPDone to broker
```

Two CP processes (rank 0 preferred appender, rank 1 standby). CP processes are
`NoAttrition` (in-process stall/crash follow-up). Bridge and logstore Put
fault coins are currently zeroed in `swarmLog` so CommitSegment retry budget
matches the old in-engine put loop; re-enable under a green stacked-fault pass.
Log knobs stay on `sim.LogParams`. Mutants use `LogParams.Bug` /
`machine.Config.Bug` (and `LogStoreFaults.Bug` for trim-delete).

## Mutants (`sim/kafka/logmutants_test.go`)

| mutant | where | caught by |
|---|---|---|
| `BugWriterOffsets` | machine commit path | `kafka: acked range is committed in the control plane` |
| `BugTrimDelete` | logstore trim | `logcp-sim: a Put never wins a slot below the trim floor` |
| `BugPerReplicaWindow` | machine apply | `logcp-sim: a replica's state equals the replay of the log at its slot` |
| `BugHoleSkip` | machine hole-fill | same digest property |

C2-style conformance for the machine path is `go test ./controlplane/machine -run Lockstep`.

## Reachables

Campaign expect-reachables still use the `logcp-sim:` property names (hole fill,
takeover, trim, restore, …). Coverage under attrition and fault swarm is part of
the CP-DSM-8 live lanes.
