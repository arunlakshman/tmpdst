# Design A prototype: the control plane over a command log (X2, X5, commit model)

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Status: PROTOTYPE and measurements (2026-10-20), not a decision. Idea A of `delos-logdrive-ideas.md`, built under
`cp/logsm`. It is reachable only from tests: no `-store` kind, no change to `cmd/manicp`, no trace hash changed.
Labels: **measured** (this machine, Apple M1, Go 1.24), **modelled** (fakedb with injected latency), **computed**,
**assumption**. All AWS numbers (X1, X3, X4) are still open: **the PutItem latency in section 4 is an assumption.**

## 1. What was built

| piece | where | notes |
|---|---|---|
| `Log` seam | `cp/logsm/log.go` | `Append(slot, entry)` if-absent (returns the winner's entry when lost), `Read(from, limit)`, `CheckTail`. A written slot never changes. |
| `MemLog`, `DynLog` | `memlog.go`, `dynlog.go` | `DynLog`: `LOG/<20-digit slot>`, one `PutItem` with `attribute_not_exists(pk)` and `ALL_OLD` on failure, consistent `Query` for Read, descending `Limit 1` for CheckTail. Runs on `cp/dynamo/fakedb`. |
| entry codec | `codec.go` | Command only: key, batches (topic, partition, records, loc), with a per-entry string table. 150 partitions = 2,970 B, 50 = about 1 KB. Decode 8.7 us. |
| `Replica` | `replica.go` | A `veritas.ControlPlane` plus `applied`. Applies slots strictly in order through `Exec` (the oracle is not copied). Read-ahead buffer, shared and paced tail read ("bus stand"), `SyncTo`, `Await`, fresh `Read` (CheckTail, apply, answer from memory), `Local` (stale). |
| `Committer` | `committer.go` | K appends in flight on consecutive slots. A slot lost to another writer is retried at a later slot. An unknown outcome is retried at the SAME slot (conditional, so safe): it either lands or shows who owns the slot (entry ID). Answer only after this replica applied the slot. |
| hole-filler | `Replica.Tick/SyncTo` | A slot empty for `HoleAfter` (default 250 ms) while later slots exist gets a no-op via the same conditional append. The original writer loses and retries elsewhere; apply skips the no-op. |
| `Checkpoint` | `checkpoint.go` | Slot, state hash, ends (grouped by topic, delta coded), sorted 16-byte key hashes. No restore into veritas. |
| `Store` | `store.go` | `cp.Store` over a replica and committer, for the conformance suite only. |
| small additions elsewhere | `fakedb`, `internal/verify/veritas/snapshot.go` | fakedb `PutItem` (conditional, `ALL_OLD`, `AfterPut` lost-answer hook); veritas `Ends()` and `SegmentKeys()` (read-only). |

Commands in the log: CommitSegment, CreateTopic (otherwise `DescribeTopics` would diverge between replicas) and no-op.
The state machine is unchanged `veritas.ControlPlane`: first slot of a key wins, a later equal command is answered from
state, a different payload is `ErrConflict`, an invalid command consumes a slot and answers `ErrInvalid`.

## 2. How to run

```
go test ./cp/logsm                                  # unit, X2 (300 seeds), X5, determinism of the harness
LOGSM_SEEDS=100000 go test ./cp/logsm -run X2Seeds -v      # X2 at scale (16 s)
LOGSM_SEED=123 go test ./cp/logsm -run TestX2Seed -v       # replay one seed
go test ./cp/logsm -run X5 -v                              # checkpoint size and load time
go test ./cp/logsm -run Backlog -v                         # one-minute backlog replay
go test ./cp/logsm -run xxx -bench 'Apply|Decode' -benchtime 20000x
LOGSM_MODEL=1 go test ./cp/logsm -run TestModel -v -timeout 30m     # section 4 (5 minutes, real sleeps)
go test ./cp/cptest -run LogSM                              # conformance C3 against veritas
```

## 3. X2 and X5 results

### X2: three replicas, random interleavings, faults (PASS)

`sim_test.go`: real replicas, committers and hole-fillers; goroutines parked at every log call and wait, and a seeded
stream picks who runs, so one seed is one interleaving (determinism test: same seed twice gives the same trace hash).
Per seed: 3 replicas, 1 to 3 writers each (K=1..3), 8 commands per writer from 8 shared keys (duplicates, payload
variants that conflict, invalid commands, CreateTopic), delays of 0 to 2 steps at every log call, and per log call 5%
lost write, 5% lost answer (landed, error returned), 5% read error, and 2% writer death after taking a slot (a hole).
Final checks: every replica applied the whole log with the same `StateHash`; against an independent model (not
veritas) per-partition runs are dense from 0 with no overlap and equal to the model; every answer a writer saw
agrees with the model (replay equals the first answer, `ErrConflict` only for a differing payload, `ErrInvalid` only
for an invalid command). Also: `cptest` conformance C3 (`TestConformanceLogSMMem` 100 seeds x 200 ops,
`...FakeDB` 20 x 100) and a real-goroutine test over fakedb with latency, lost answers and lost writes under `-race`.

| seeds | failed | slots | writer deaths (holes) | lost slots | unknown-outcome retries | hole-fills |
|---|---|---|---|---|---|---|
| 100,000 (1..100000) | 0 | 3.91 M | 178,531 | 3.94 M | 1.02 M | 26,408 |

Harness sensitivity: a mutant that skips a hole locally instead of writing the no-op was caught at once (seed 11).
Not covered: replica crash and restart, clock skew in `HoleAfter` (the clock is the step counter), a zombie writer
that wakes after many slots (covered only by "its conditional put fails"), more than 3 replicas.

### Apply rate (the Delos 5.1 playback question), one core of an M1

| state / partitions per segment | ns per command | commands/s |
|---|---|---|
| 288 partitions, 50 | 12-15 k | 67-84 k |
| 288, 150 | 29-32 k | 31-35 k |
| 40,000, 50 | 20-22 k | 46-49 k |
| 40,000, 150 | 52-55 k | 18-19 k |
| 40,000, 150, decode included (8.7 us per 2,970 B entry) | 64 k | 15.5 k |
| 40,000, 150, decode, 230,000 keys already committed (1 h at 64/s) | 114 k | 8.8 k |

Answer: **yes, with at least 20x headroom.** 430 segments/s needs 430 commands/s; the worst case measured is 8.8 k/s.
A one-minute backlog read from a memory log replays in 224 ms at 64/s (3,840 entries, 11.4 MB) and in 1.75 s at 430/s
(25,800 entries, 76.6 MB): 268x and 34x real time. That excludes DynamoDB read time: 77 MB is about 77 one-MB Query
pages, so it adds roughly 1-2 s if a page takes 10-30 ms (assumption, X3 measures it). Two warnings in section 5:
cost grows with the key count (veritas keeps keys in a sorted slice, O(n) insert), and memory.

### X5: checkpoint of ends plus key set

| partitions | keys (16 B) | serialised | decode | load (decode + build maps) |
|---|---|---|---|---|
| 288 | 100,000 | 1.60 MB | 0.29 ms | 2.2 ms |
| 40,000 | 100,000 (about 1 h at 28 seg/s) | **1.79 MB** | 1.4 ms | **4.5 ms** |
| 40,000 | 230,000 (1 h at 64 seg/s) | 3.87 MB | 1.3 ms | 8.1 ms |

Median of 15. Key hashes are random, so flate saves nothing (1.75 MB). The ends are 190 KB of the 1.79 MB. A
checkpoint is tiny next to replay: it is the key set, not the ends, that grows.

## 4. Commit throughput and latency: K in flight versus design L (modelled)

Method as `commit-path-redesign.md` section 6: 6 brokers seal 150-partition segments open loop at the offered rate,
one commit in flight per broker (L 5.8), 2 s warm-up then 8 s measured, counted by completion time. `A pref`: all
brokers use one replica (sticky routing). `A spread`: 3 replicas with 2 brokers each, all appending to one log (slot
contention). The log is the real `DynLog` over fakedb. **PutItem 5-8 ms (mild) and 15 ms (pessimistic) are
assumptions until X1 runs.** Queries are 4 ms and 8 ms. L is the earlier model: one transaction in flight, group
commit, 8 + 0.7 x items ms (mild) or 15 + 1.5 x items ms (pessimistic), as a sleep.
Cells are committed seg/s, commit p50 / p99 ms.

| mode | mild, 64/s | mild, 256/s | pessimistic, 64/s | pessimistic, 256/s |
|---|---|---|---|---|
| L (one tx) | 64.0, 12 / 23 | 238, 26 / 26 (queue 411 ms) | 64.2, 26 / 42 | 132, 46 / 46 (queue 2.9 s) |
| A pref K=1 | 64.0, 8 / 14 | 140 (saturated) | 61.9, 97 / 101 (saturated) | 62 (saturated) |
| A pref K=4 | 64.1, 7 / 9 | 256, 7 / 11 | 63.9, 16 / 18 | 246, 24 / 33 |
| A pref K=16 | 64.0, 7 / 10 | 256, 7 / 11 | 64.1, 16 / 18 | 256, 16 / 18 |
| A spread K=1 | 64.1, 14 / 33 | 147, 14 / 495 | 63.2, 31 / 507 | 64, 31 / 2,081 |
| A spread K=4 | 64.0, 13 / 22 | 256, 8 / 99 | 64.0, 30 / 79 | 128, 16 / 1,062 |
| A spread K=16 | 64.0, 8 / 18 | 255, 8 / 100 | 63.9, 31 / 85 | 128, 16 / 494 |

Reading it:
- **At 64 seg/s all modes except K=1 at 15 ms keep up.** With one writer at 15 ms per Put the ceiling is 66/s, so K=1
  is at the edge (p50 97 ms is queueing). **K=4 is enough with 6 brokers**; K=16 adds nothing because each broker
  has one commit in flight, so a replica never has more than the number of brokers sticking to it.
- **A pref beats L on latency** (one Put, no read, because a replica applies its own entry as soon as every earlier
  slot is known: 7 ms versus 12 ms) and keeps up where L's group loop is limited. L at 256/s is limited by one commit
  per broker and the no-wait group convoy; it is **not** L's ceiling (the earlier model, 12 brokers, reached 219-432).
- **Contention is the cost of spreading.** Lost slots per commit are 0.2-1.9 (mild) and 1.0-2.0 (pessimistic), so
  1.2-2.9 PutItems and 4.4 Queries per commit at 64/s. Throughput of `A spread` is about in-flight / (Puts per commit x
  Put latency) = 6 / (3 x 15 ms) = 133/s, which is the 128/s measured at 15 ms. Decision 46's contention returns.
  Sticky routing to one appender is therefore not optional at 15 ms Put latency.
- Cost per commit of A pref: 1 PutItem of about 3 WRU (computed), plus the tail polling (one Query per 10 ms per replica).

## 5. Findings that qualify `delos-logdrive-ideas.md`

1. **The checkpoint cannot be "ends plus a key set".** A replay (a retry after `MaybeApplied`) must return the
   ORIGINAL ranges and `ErrConflict` needs the original payload; a 16-byte key hash has neither. Replays of keys older
   than the checkpoint have to be answered by the log, so trim must stay at least `2 x CommitDeadline` (240 s) behind
   the newest checkpoint: 22.8 MB at 64/s, 153 MB at 430/s (computed from 2,970 B per entry), or the checkpoint must
   carry ranges. The one-hour key set is not needed for replays at all (a broker retries only for 120 s); X5 shows it
   is cheap anyway.
2. **veritas is a spec, not a production replica state.** It keeps every run and every segment forever: measured
   about **257 bytes per applied batch** (`TestRetainedMemory`), so 6,450 batches/s is 6 GB/hour, against the 10 B
   per batch assumed for idea B (5.6 GB/day). Its key insert is O(n) (apply drops from 52 to 114 us at 230 k keys).
   A replica that holds the index needs a compact applier, differential-tested against veritas (decision 21 stays).
   A checkpoint-restored replica also cannot rebuild old runs for `Lookup`: it is "tail plus index", not veritas.
3. **Lost slots are not always "one cheap Put"** (ideas doc, risks): 1-3 Puts per commit when appenders are spread.
4. **A fresh Lookup can wait for a hole.** If a hole sits below the tail, any fresh read waits up to `HoleAfter` and
   then writes the no-op; the ideas doc did not name this tail-latency cost.
5. Transient holes also come from one writer pipelining K Puts (n+1 lands before n), so `HoleAfter` must exceed the
   Put p99.9, or the filler wastes a Put and the real writer retries (correct, costs latency; X2 exercises both).
6. Smaller: the entry is 2.97 KB at 150 partitions (ideas doc said 3.4 KB); fakedb lacked `PutItem` (added);
   `cp/dynamo.DB` still has no `PutItem` (`logsm` declares its own narrow interface).

## 6. Open questions

- **Windowed idempotency after trim.** Product decision: guarantee per key for `2 x CommitDeadline` plus checkpoint
  interval, duplicate records beyond it (decision 41 already allows duplicates on retry). A broker whose key is older
  than the window must get a defined answer (not a silent re-commit): `ErrConflict`-like or accepted duplicate.
- **Hole-filling timeout.** 250 ms is a guess. Needs Put p99.9 from X1; a smaller value raises wasted fills, a larger
  one raises Lookup tail latency after a writer death.
- **Memory for long retention** (finding 2): index in memory is hours, not days; the S3 index (idea B) is needed
  before long retention, and then the replica's state is not veritas.
- **Apply-rate headroom:** 20x at 430 seg/s on one core with the oracle itself; recheck with a compact applier and
  with decode of real 3 KB items from DynamoDB reads, and with GC pressure of 1.7 MB/s of new state.
- **What the sim must cover:** a log engine (Put/Query with latency, lost answers, unknown outcome) next to the S3
  engine; replicas as actors with crash and restart from a checkpoint; `HoleAfter` on the virtual clock with skew;
  invariants "all live replicas' `StateHash` equal at a quiesced log", "an answer is derived from the applied slot",
  "no empty slot is passed without a no-op"; veritas stays the oracle over the log's first-slot order; every trace
  hash changes (as for L). X2 here is a single-node harness, not that engine.

Decision candidate (not added to `decisions.md`): | A? | **The control plane applies a log of commands.** `LOG/<slot>` holds
the CommitSegment request, never bases; every replica applies it through the veritas state machine and answers only after
applying the slot; idempotency per key is first slot wins within a window; holes are filled with no-ops after
`HoleAfter`; appends are sticky to one preferred replica with K=4. Pending: X1 (PutItem latency, throttling), the
window decision, the compact applier. |

## 7. What was NOT done

- No AWS call, no real DynamoDB or ExtendDB run: `DynLog` is tested only on fakedb (including `ALL_OLD`, which the real
  service supports; the fallback Query path is covered by a unit test). No X1, X3, X4.
- No `-store` kind, no wiring into `cmd/manicp`, no sim engine, no change to any existing trace hash or `decisions.md`.
- Checkpoint is encode, decode and load only: no trim of the log, no restore into a replica, no checkpoint to S3.
- No indexer, no lease or routing for "sticky"; no group commit of several commands into one entry; no striping.
- Replica restart, more than 3 replicas, clock skew and real time (the model uses real sleeps on one machine,
  about 20% optimistic as in the earlier model) are not covered.

## 8. Production plan (decisions 48-52)

The prototype above becomes the production control plane in five stages. Each stage ends with its exit criteria met
and `gofmt`, `go vet`, `ops/scripts/ci-determinism.sh`, `go test -p 1 ./...` and `make build` (CRAP gate: no new offender
over 30) green. No stage touches an existing sim trace hash except stage 3, which re-baselines them by hand.

| stage | work | exit criteria |
|---|---|---|
| 1. KV applier and lockstep | `cp/applier`: the state machine of decision 49 on an embedded KV store chosen by a bake-off (`docs/applier-bakeoff.md`); commands CommitSegment and CreateTopic, queries Lookup, DescribeTopics, End; runs packed per (partition, segment); per-key digest plus bases for the window; checkpoint, restore and trim by slot. | X2 harness with the applier as every replica's state machine and veritas as the oracle: 100,000 seeds, 0 failures (equal answers, offsets and query digests); `cptest` conformance green against the applier as a `cp.Store`; apply rate well above 430 segments/s at 150 partitions per segment with 40k partitions of state; the store dependency recorded per decisions I and J. |
| 2. Log-backed CP service (DONE, `docs/log-cp-service.md`) | The `cp/` service on the DynamoDB log through the `cp/logsm` seam: committer with K=4, hole filler, hot standby tailing the log (decision 51, the slot is the fence), sticky routing of commits to one node, checkpoint and trim (trim stays behind the key window), indexer writing `R#` rows every 10 s on a pre-warmed table, fresh Lookup through `CheckTail` with shared (batched) tail reads. | Conformance C3 against the real service on fakedb; a standby takes over after the appender is killed with no lost or duplicated offset; restart from checkpoint plus tail gives the same state digest; Lookup after an ack sees it; unit and race tests for each loop. |
| 3. Cap 4096 and sim (DONE, `docs/sim-log-engine.md`) | Raise `MaxSegmentPartitions` from 96 to about 4096 (decision 52); add a sim CP engine for the log (Put/Query with latency, lost answers, unknown outcome; replicas as actors with crash and restart from a checkpoint; `HoleAfter` on the virtual clock) next to the S3 engine; veritas stays the oracle over the log's first-slot order. | Every trace hash re-baselined and reviewed by hand (a diff per hash with the cause); 300-seed campaign green; invariants: all live replicas' query digests equal at a quiesced log, an answer derives from the applied slot, no empty slot is passed without a no-op. |
| 4. Wiring and operations (DONE, below) | `manicp -store` kind for the log, OpenAPI and a CP URL list (or NLB) for brokers with failover to the next URL, `manicp_` observability for apply lag, slot loss, hole fills, checkpoint age, indexer lag, window size. | `test/it/` kill-the-appender test fails over within the budget; metrics documented in `docs/observability.md`; runbook updated. |
| 5. AWS rerun | 500k, 750k and 1M random clients, no partition affinity, with the production CP (X1/X3/X4 numbers re-measured at that load). | Report in `docs/` with commit rate, commit latency p50/p99, tail-query rate, DynamoDB WRU, failover penalty and cost per GiB; the stage-5 results decide whether the S3 index (TODO) is scheduled. |

### Stage 2 status

Built in `cp/logcp` (service, DynamoDB log, committer with group commit and K=4, hole filler, hot standby with
advisory roles, checkpoint and tombstone trim, indexer, `manicp_log_*` metrics), `cp/cpclient` (CP URL list and
failover) and `cp/logcp/logcptest`. Exit criteria: cptest conformance against the service (store, HTTP, fakedb) green;
seeded X2 on the real service, 20,000 seeds, 0 failures (takeover, failback, split brain, crash and restore from a
checkpoint, trim); `-race` test; restart from checkpoint plus tail gives equal state; Lookup after an ack sees it on
any node. Two findings changed the design text (decision 53): the idempotency window must be a function of the
slot number, and trimming must leave tombstones. Not wired into `cmd/manicp` (stage 4).

### Stage 4 status

Done. `manicp -store dynamolog` (`cmd/manicp/logstore.go`, flags in `docs/log-cp-service.md`) wires `cp/logcp` to the real DynamoDB client,
`internal/sys/s3store` checkpoints and Pebble; `-store mem|dynamo` are unchanged (rollback). `manibroker -cp-url a,b` takes the URL list
(cpclient: sticky to the last good URL, next URL on a connect error or 503, each once per call; tests for dead URL, standby and
failback). Tests: `cmd/manicp` runs three nodes in-process over fakedb and an in-memory object store, kills the appender while three
clients commit through a URL list, and checks every acked range is stored once with contiguous offsets on both survivors;
restart from a final checkpoint; startup errors. `test/it/TestKillTheAppenderMidProduce`: three logcp nodes over real HTTP, two brokers,
franz-go produce with idempotence off (decisions 41-44), the active node is crashed mid-stream, acks resume about 2 s later
(takeover 1.5 s), the standby is active, every acked record is stored once at its acked offset, the survivors' state hashes are equal
(race-clean, repeated). Terraform: log store default, `cp_standby_count` (1), `cp_takeover_after`, `cp_store` (`dynamo` brings back
one host and `-max-segment-partitions 96`), TTL on `ttl`, checkpoint IAM on `cp/*`, Alloy per node; dashboards and
`docs/observability.md`, runbook A11, OpenAPI (503, Retry-After, X-CP-Role). Dependency audit: `docs/dep-audits/pebble-indirect-modules.md`.
`terraform fmt` and `init -backend=false` + `validate` (hashicorp/terraform:1.9 in Docker, 2026-10-21): clean. Not verified: `terraform plan`, anything on AWS. The stage 5 run is planned in `docs/stage5-run-plan.md`.
Failover budget and the broker backoff interplay: runbook A11 (takeover 5 s + about 2 s backoff, deadline 120 s).

### Stage 3 status

Done (decision 52 note). `broker.Config.MaxSegmentPartitions` and `manibroker -max-segment-partitions` default to 4096;
the old transaction store needs `-max-segment-partitions 96` (documented in `internal/broker/config.go`, `cp/doc.go`, the flag help,
the runbook; `ops/deploy/aws/terraform` defaults `broker_extra_args` to it because it still runs `manicp -store dynamo`), and
refuses a wider segment with `ErrTooLarge` and no write (tested at 4096 partitions in `cp/dynamo`). The broker does not
learn the limit from the CP: that needs a new endpoint and a contract field, and the CP URL list of stage 4 is the better
place. `internal/verify/sim/engines/logengine` is the sim log CP (`docs/sim-log-engine.md`); `manisim -scenario kafka-log` runs the whole
system on it; four mutants are caught; C2 conformance against veritas passes. The sim's SegmentCaps invariant now uses the
store's item limit (none for the log store) instead of the constant 100.

**Trace hashes: none changed, none pinned.** No golden or pinned hash exists in the repo (the determinism tests compare two
runs of one seed; `sim.rand_test` pins the RNG stream, which did not change). The simulated workload moves 6 partitions, so
a cap of 96 or 4096 never binds: the 300 and 2000 seed campaigns before and after the cap change are identical seed by
seed (same step count, rand calls and trace hash, 0 failing in both):

| campaign (preset `kafka`) | before (cap 96) | after (cap 4096) |
|---|---|---|
| 300 seeds | 0 failing, 5,828,573 steps | identical hash for every seed |
| 2000 seeds | 0 failing, 40,495,135 steps | identical hash for every seed |

The cap change is exercised by wide workloads instead (`internal/verify/sim/kafka` tests): 128 producers over 256 partitions commit a
100-partition segment on the log store (the old store allows at most 96 and would refuse 101 items), and
`TestCapChangeIsExplainedBySealReasons` shows the only difference at the broker: with the veritas CP and no item limit, 2
of 265 segments sealed by the 96-partition rule at cap 96 and none at 4096 (they run on to the timer). The new preset
`kafka-log`: 300 seeds 0 failing (7,519,187 steps), 2000 seeds 0 failing (50,734,757 steps, no coverage gap). No
correctness hole in the log design showed up; one property of mine was too strict (a lagging restored replica may meet a
tombstone again when a newer checkpoint trimmed past it, and restarts again: the real invariant is that a checkpoint is
never below the trim floor, now asserted).

Left for stage 4: `manicp -store` kind for the log, OpenAPI and URL list wiring, the `manicp_log_*` dashboards and alerts,
and then `broker_extra_args` can drop the 96 flag. Stage 5: the AWS rerun at 500k to 1M (this is where the cap lets
segments grow past 96 partitions, which the sim workload cannot show).
