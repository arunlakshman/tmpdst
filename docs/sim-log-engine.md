# Sim log engine (stage 3)

`sim/engines/logengine` simulates the log-backed control plane (`docs/log-cp-service.md`) so the whole-system swarm
(brokers, Kafka clients, S3, network, attrition) runs against log semantics. Run it with
`go run ./cmd/manisim -scenario kafka-log -seeds 300` (`kafka.ConfigLog`); the veritas-backed preset (`-scenario kafka`) is
unchanged. Decision 21 holds: the state machine (`state.go`) shares no code with veritas, `controlplane/applier` or `controlplane/logcp`;
veritas remains the oracle through the Kafka invariants and the C2 conformance test. The extractable tree is `controlplane/` (decision 56); the DSM machine lands under `controlplane/machine` (decision 55).

## Model

```
 Execute (CPCall) -- boundary faults: outage, 503, timeout, lost response (as cpengine)
        |
   route: lowest-rank active replica that is up and not stalled, else 503      reads: any live replica,
        |                                                                       wait until applied >= tail
   queue -> flush: <= K entries in flight, <= GroupMax commands per entry
        |
   Put(slot, entry) -- latency, may be refused (not applied) or applied with a lost answer (same slot repeated)
        |                lands in order of its own latency, so slot n+1 can land before n (hole)
   slot taken (other writer, filler, tombstone) -> answer "lost" -> the SAME entry goes to a later slot
        |
   log: slots (empty | entry | tombstone), if-absent writes only
        |
   2 replicas (rank 0 active, rank 1 standby): tail read each tick (standby at half rate, ReadFail), apply in slot
   order, hole filler (no-op after HoleAfter), takeover (no other writer for TakeoverAfter), failback, stall, crash,
   restore from the latest checkpoint, checkpoint every CkptEvery slots, tombstone trim KeepSlots below it
```

Offsets are assigned only by apply (`applyCmd`), a commit is answered only when its replica applies the slot that holds
it, and the idempotency window is `WindowSlots` after the first slot, evaluated at `TrimStride` boundaries (a function of
the slot). Slot counts are scaled down (checkpoint 24, stride 16, keep 8) so a 45 s run crosses them; the default window
is longer than a run, because a replay after the window is a new commit by design (decision 41). `LogParams` is in
`sim/engine.go`; `swarmLog` (`sim/kafka/config.go`) swarms K, group size, HoleAfter, TakeoverAfter, the fault rates, and a
trim-heavy profile (checkpoint every 4-16 slots, keep 2-4).

## Properties

Always (in the engine, on top of the Kafka oracle: no overlap, dense offsets, idempotent per key, acked data durable,
reads after an ack):

| property | what it says |
|---|---|
| a replica's state equals the replay of the log at its slot | every replica's digest equals the reference replay (`ref`, also the `CPTruth`) at the slot it applied; checked at every apply and tick |
| a key committed twice gets its original ranges | shadow map of first answers, dropped when the reference forgets the key |
| no empty slot below the trim floor / a Put never wins a slot below the trim floor | trim must leave tombstones |
| a checkpoint is never below the trim floor | a restore can always replay |

Reachable: lost slot retried at a later slot; hole filled by a no-op; Put not answered, same slot repeated; standby took
over; failback; replayed key answered from the window; replay of a key whose slot was trimmed; idempotent replay after
takeover; replica stalled; replica crashed; replica restored from a checkpoint; log trimmed below a checkpoint; several
appends in flight (Sometimes).

## Mutants (`sim/kafka/logmutants_test.go`, each caught within 6 seeds)

| mutant | caught by |
|---|---|
| `BugWriterOffsets`: the writer picks bases, apply uses them (a retried key leaves a gap) | `kafka: committed offsets are contiguous` and the consumer invariants |
| `BugTrimDelete`: trimmed slots are emptied, not tombstoned (a stale writer wins an old slot) | `a Put never wins a slot below the trim floor`, `no empty slot below the trim floor`, digests, lost acked data |
| `BugPerReplicaWindow`: each replica forgets keys at its own pace | `a replica's state equals the replay of the log at its slot` (window 40) |
| `BugHoleSkip`: a replica passes an empty slot without a no-op | the same property, plus lost acked data |

Engine-level tests (`engine_test.go`) drive each mechanism by hand: lost slot, hole, takeover with replay after it,
failback, trim with replay and crash/restore, lossy Puts (40% lost, 30% refused, 30 commits all exactly once), the
boundary faults, determinism of the trace.

## Results

Whole system, 3 brokers, default `kafka-log` preset: 300 seeds 0 failing (7.5 M steps), 2000 seeds 0 failing (50.7 M
steps, no coverage gap, determinism check on every tenth seed). C2 (`TestConformanceLogEngine`, 200 seeds x 120 ops):
fault-free, the engine answers like the veritas spec. The sim moves 6 partitions per segment, so the 4096 cap never binds
there; `TestLogStoreTakesTheDefaultCap` runs 128 producers over 256 partitions and commits a 100-partition segment, which the
old store would refuse (101 items).

## Limits

Two replicas, one log, no real DynamoDB behaviour (throttling, item size, TTL), no index (`R#` rows) and no
checkpoint-to-S3 failure; those stay in `controlplane/logcp` tests. Latencies are the CP fault menu, not the fakedb model. A third
replica and a log-wide outage (all replicas down) are not modelled. The window is never exhausted in the default preset.
