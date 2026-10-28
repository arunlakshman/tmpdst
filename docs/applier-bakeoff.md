# Applier store bake-off: bbolt vs Pebble (stage 1, decision 49)

Status: measured 2026-10-20 on an Apple M1 (8 cores, SSD), Go 1.24. Not AWS. The applier (`cp/applier`) keeps the
control-plane state in an embedded ordered KV behind the small `applier.KV` seam (`View`, `Update`, `Get`, `Floor`,
`Ascend`, `Set`, `Delete`); the same applier ran on both stores. Sync is off: state is rebuilt from the log plus a
checkpoint, never trusted after a crash.

## Workload

- 40,000 partitions (4 topics x 10,000), random segments of 150 distinct partitions with 1 or 2 batches each (225
  batches per segment), segment key `seg/<32 hex>`, batch Len 8-32 KB.
- Preload 10,000 or 20,000 segments (2.25 M or 4.5 M batches, 6 to 12 minutes of traffic at 6,450 batches/s), then
  measure 5,000 more. One goroutine applies, as a replica does. "group" is how many commands share one transaction
  (`Applier.CommitBatch`; a replica applies the slots one tail read delivered together).
- Target from the task: well above 430 segments/s.

## Results (measured)

| apply rate, segments/s | bbolt v1.4.0 | Pebble v1.1.5 |
|---|---|---|
| group 1, 10k-segment state | 303 | 1,330 |
| group 8, 10k-segment state | 431 | 1,335 |
| group 32, 10k-segment state | 497 | 1,217 |
| group 8, 20k-segment state | 362 | 1,280 |
| group 1, 20k-segment state (first run) | 259 | 1,308 |

bbolt falls as the state grows (a copy-on-write B+tree rewrites a leaf page per random partition per transaction:
45-75 k partition-updates/s) and only reaches the 430/s target when 8 or more commands share a transaction, with no
margin. Pebble is 3-5x faster and flat (150-200 k partition-updates/s). The in-repo benchmark
(`go test ./cp/applier -run xxx -bench Apply150 -benchtime 5000x`, default options, group 8) gives **1,099 segments/s**,
2.6x the target. Apply time is 0.8 ms per 150-partition segment.

| other, 20k-segment state (4.5 M batches) | bbolt | Pebble |
|---|---|---|
| process RSS after apply (no snapshot buffer) | 648 MiB | 234 MiB |
| disk | 533 MiB | 247 MiB |
| checkpoint write (stream of the whole store) | 0.8 s, 286 MiB | 1.0 s, 286 MiB |
| restore from that checkpoint into an empty store | 6.5 s | 4.4 s |
| Lookup from offset 0, MaxBytes 1 MiB (~35 ranges) | 101 k/s | 10 k/s |

- **State size:** the checkpoint is 66 B per batch (286 MiB for 4.5 M batches), against 257 B per batch in veritas
  (3.9x smaller): 1.5 GB per hour at 6,450 batches/s (Pebble compresses to about 55 B per batch on disk). Most of the
  size is the 36-byte segment key repeated in every run; packing `seg/<32 hex>` as 16 raw bytes would take about 20 B
  off each run (about 25%). Not done: the target is met without it.
- **Restore** is about 15 s per GB of checkpoint, so one hour of state restores in about 25 s, plus the log tail.
- **Lookup:** Pebble is slower per query (a snapshot and three iterators; From 0 is the worst case, 1 MiB of
  ranges). Fresh Lookups of the newest offsets, the common case, read one or two runs. If the CP needs more, a
  cache of recent runs in front of the applier is the fix, not the store.
- **Memory:** neither store keeps the index on the Go heap (1 MiB heap in both). bbolt's RSS is the mapped file.

## Dependencies

| | new modules in go.mod | cgo | notes |
|---|---|---|---|
| bbolt v1.4.0 | 1 (`go.etcd.io/bbolt`; `x/sys` was already there) | no | v1.5 needs Go 1.25, v1.4.0 fits Go 1.24 |
| Pebble v1.1.5 | 17 (pebble plus 16 indirect: `cockroachdb/{errors,fifo,logtags,redact,tokenbucket}`, `getsentry/sentry-go`, `gogo/protobuf`, `golang/snappy`, `kr/{pretty,text}`, `pkg/errors`, `rogpeppe/go-internal`, `stretchr/testify`, `x/exp`, `x/text`, `DataDog/zstd`) | optional (`CGO_ENABLED=0 go build ./...` passes) | also raises `x/tools` 0.31 to 0.41, `x/mod`, `x/sync` |

Pebble v2.1.7 was tried too: same speed (1,268 segs/s), more modules (29 modules in the dependency list against 25
for v1.1.5), so v1.1.5 was kept. Pebble v1.1.5 is the older line (CockroachDB 24.x); a move to v2 later only touches
`pebblekv`.

## Choice: Pebble

The task said to prefer the smaller dependency surface if performance is adequate. bbolt is not adequate: 303-362
segments/s in the realistic cases (one command per transaction, a state that keeps growing), below the 430 target,
and 497 at best with batching, while the replica must also answer reads and survive a backlog replay after a
failover. Pebble leaves 2.5-3x headroom, uses 2.8x less memory and half the disk. The cost is 16 more indirect modules, all
outside the determinism closure. bbolt and its adapter were removed so the repo depends on one store; the adapter
is 90 lines if it is ever needed again.

## Audit status

- `ops/scripts/audit-dep.sh github.com/cockroachdb/pebble@v1.1.5 github.com/cockroachdb/pebble` ran (network was
  available). detcheck fails, as expected: goroutines, channels, mutable globals, map ranges, many off-list imports.
- Record: `docs/dep-audits/github.com/cockroachdb/pebble@v1.1.5.md`. Verdict: **rejected for the deterministic
  packages, accepted at the edge** (`cp/applier/pebblekv` only), like prometheus. No allow-list entry.
- `ops/scripts/ci-determinism.sh` passes: the applier is outside its package list, and so is Pebble.
- Not audited: the new transitive modules one by one (they are outside the closure; CI does not require it). A
  supply-chain review of `cockroachdb/errors` and `sentry-go` (pulled for Pebble's error type) is still open.

## Lockstep against veritas

- `cp/logsm` X2 harness (`lockstep_test.go`): the same seeded interleavings, faults and 3 replicas as X2, with the
  applier as every replica's state machine (`logsm.Config.Machine`) and veritas beside it as the oracle. Every apply
  and read is answered by both and compared (CPDone equal, including `ErrConflict`, `ErrInvalid`, replays, MaxBytes
  stops); after each commit, `End` and Lookups with MaxBytes at, below and above each running total of Len are
  compared on the touched partitions; at the end a digest of the public queries (DescribeTopics, End and all runs
  of all partitions) must be equal, and the three replicas' `applier.StateHash` must agree (it is not veritas's
  `StateHash`).
- **100,000 seeds (1..100000), memory KV: 0 failures, 3.91 M slots** (the same slot count as the X2 run of the
  prototype, since the seeds are the same). 2,000 seeds on real Pebble stores: 0 failures, 77,942 slots.
- Sensitivity: two mutants of the applier (MaxBytes stop `>=` as `>`, a Lookup skip `<=` as `<`) are caught; the first
  was NOT caught until the edge-MaxBytes probes were added.
- `cptest` conformance (C3), `applier.Store` as a `cp.Store` against veritas: `TestConformanceApplierMem` 200 seeds x
  200 ops, `TestConformanceApplierPebble` 20 x 100. Unit tests: checkpoint and restore (a replay of a window key after
  restore returns the ORIGINAL ranges), trim, damaged checkpoints, corrupt records, store failure is fail-stop.
- Run: `LOGSM_SEEDS=100000 LOGSM_PEBBLE_SEEDS=2000 go test ./cp/logsm -run X2Lockstep -v -timeout 40m` (11 minutes).

## Not done

No wiring into `cmd/` or the replica's apply loop (`Replica` still applies slot by slot; `CommitBatch` is for stage 2),
no checkpoint to S3, no cache in front of Lookup, no AWS.
