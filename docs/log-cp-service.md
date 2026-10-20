# Log-backed control-plane service (stage 2)

Status: built and tested in `cp/logcp` (sim engine and cap 4096 in stage 3, `docs/sim-log-engine.md`); wired in stage 4 as `manicp -store dynamolog` (flags below), the Terraform default for the benchmark. Decisions 48-52,
`docs/design-a-prototype.md` section 8, `docs/applier-bakeoff.md`. Constructor: `logcp.New(Config)`,
`Start(ctx)`, `Handler()`; test builders in `cp/logcp/logcptest`.

## Architecture

```
 brokers --(sticky, cp URL list)--> HTTP handler (role header, standby => 503)
                                         |
              commit/create          Service (cp.Store)            Lookup/Describe/End
                  |                      |                               |
        queue (bounded, FIFO)            |                  fresh(): shared tail check
                  |                      |                    (batch >= TailBatch apart)
        K flushers: group <=256 cmds     |                               |
        into ONE slot, rate <= 200/s     |                               |
                  |                      v                               v
                  +--> Log.Append ---> replica: buffer, hole filler ---> wait applied >= tail
                       (PutItem,           |  contiguous slots, batches never cross
                        attr_not_exists)   v  a trim boundary
   DynamoDB  LOG/<slot>  <--read--  applier.CommitBatch (Pebble)  --> answers to waiters (by entry ID)
   CKPT, IDX, R#, TOPICS                   |            |
   S3  ckpt/<slot>   <-- checkpoint loop --+            +--> feed --> indexer: R# rows, 10 s flush
```

Roles are advisory (decision 51). Rank 0 starts active; others are standbys that tail, apply and serve fresh reads
but answer commit and create-topic with `503` + `Retry-After` + `X-CP-Role: standby`. The active node appends a
no-op heartbeat when idle (1 s). A standby that saw no slot of another writer for `TakeoverAfter` (while its own
reads worked) becomes active. An active node steps down when a writer of a lower rank appends after it took over
(failback). Two active nodes cost retries, never correctness: the slot is the fence.

## Slot format and versioning (decision 53)

Item: `pk=LOG`, `sk=%020d`, `d` (B) = entry, plus `v`, `w` (writer), `id`, `ts`, `n` (commands), `ttl`. Entry v1:
version byte, rank, writer, append ID, ts (ms, advisory, never state), count, then each command as
`cp/logsm` codec (key table, batches). Zero commands is a no-op (hole fill, heartbeat). Size guard: entry plus 256 B
of item overhead <= 350 KB; a single command that cannot fit is answered `ErrTooLarge` and never queued; the group
stops growing at the guard.
**Versioning rule (the open question):** the first byte is the format version. A reader that meets a version above
`logcp.Version` or a payload it cannot decode STOPS (`Blocked`, 503, `/healthz` 503, `manicp_log_blocked`); it never
skips, since skipping forks the state. Upgrade in two deploys: first every node reads v+1, then flip the writers
(`DynLogOptions.Version`). Rollback after the flip needs the old build to understand v+1.

## Flows

**Commit.** Validate size, refuse if standby/blocked/queue full (`ErrNotActive`/`ErrBlocked`/`ErrOverloaded`, all
503-class). Queue; a flusher takes everything queued (<=256 commands) after reserving a slot-rate token, appends at
`max(high, applied)` with `attribute_not_exists`. Unknown outcome: retry the SAME slot (it lands, or the loser sees
whose it is). Lost slot (another writer, or a hole filler): the loser gets the winner from the failed condition
(`ALL_OLD`, no extra read) and appends the same entry at a later slot; no offset was computed from a view. The caller
is answered when the replica has applied the slot, from `applier` output, by entry ID. K=4 appends in flight.
**Window (deviation, see below).** A key is forgotten `WindowSlots` (131,072) after its first slot, evaluated at
every `TrimStride` (1,024) boundary by the apply loop: a function of the log, so all nodes forget alike. At the 200
slots/s ceiling that is 655 s >= 2 x CommitDeadline + checkpoint period. Replays inside it get the original ranges.

**Lookup / Describe / End.** One tail `Query` (consistent, ~2 ms) shared by every read that arrives while a batch is
open; a batch starts at least `TailBatch` after the previous. Then wait until applied >= tail (the tail loop is
kicked), then answer from Pebble. Works on any node, so standbys scale reads. A hole below the tail waits up to
`HoleAfter`, then a no-op fills it.

**Failover.** Active dies: the standby promotes after `TakeoverAfter` (the library default is advisory only; `manicp -takeover-after` defaults to 5 s) and starts
appending at the tail; in-flight entries of the dead node either landed (replays answer them) or never will. Brokers
(`cpclient` URL list `a,b`, comma separated) move on at connect error or 503, sticky to the last good URL, each URL
once per call; commit retries carry the same key. Failback is the same in reverse.

**Checkpoint and trim.** Active node, every `CkptEvery` (5 min) and >= `CkptMinSlots` new slots: snapshot of the
applier (window included) tagged with the applied slot read inside the store view, `ckpt/<slot>` to the object store,
then `CKPT` pointer by conditional put (forward only); keep the pointed object and one older. Then trim: slots below
`min(ckpt - KeepSlots, index watermark)` are overwritten with TOMBSTONES (`TrimDelete`), not deleted (finding 2).
Restore on start: pointer, object, sha-256, `applier.Restore`, replay from the checkpoint slot to the tail. A
damaged object falls back to replay from slot 0 if the log still starts there, else `ErrNoCheckpoint`.

**Index.** Decision 50 as written, ON by default (`NoIndex` to stop). Every 10 s the active node writes the `R#`
rows (`cp/dynamo.RunRow`, byte-identical to the old store) of slots [watermark, applied) and `TOPICS` rows, then
moves `IDX.ix_slot`. The plan is a pure function of the log: ranges come from the applier key records, so any node
derives the same rows and a crashed or duplicate flush is harmless. Per partition rows are written in offset order,
one wave at a time (BatchWriteItem 25, `IndexParallel` 8, `UnprocessedItems` and throttles retried with doubling
backoff, `IndexRate` pacing), so the table always holds a prefix of each partition's runs, never a run with a gap.
**What reads it today: nothing in the service.** Lookup is answered from Pebble. The rows serve a rollback to the
old store (quiesce commits, flush, check `index_lag_slots`=0, switch; `G#` keys are not written, so replays of old
keys after a rollback commit again, allowed by decision 41) and the future S3 index or reader scale-out.

## `manicp -store dynamolog` (stage 4)

Flags (all `-store dynamolog` only; defaults follow decisions 48-53): `-table` (default `<table-prefix>cp`), `-ckpt-bucket`
(empty: no checkpoints and no trim, a restart replays the whole log) `-ckpt-prefix cp/` `-s3-endpoint` `-s3-path-style`,
`-node-name` (hostname) `-rank 0`, `-takeover-after 5s` `-heartbeat 1s` `-hole-after 250ms` `-log-k 4` `-slot-rate 200`,
`-ckpt-every 5m` `-ckpt-min-slots 1000` `-final-checkpoint=true`, `-window-slots 131072` (cluster constant), `-index=true`
`-index-every 10s` `-index-rate 0` `-index-parallel 8`, `-log-ttl 168h`, `-trim-delete=true`, `-pebble-dir` (empty: temporary;
a given directory is wiped at start if it holds a Pebble database, refused if it holds anything else), `-start-timeout 10m`,
`-create-tables` (dev). `-takeover-after` must be several times `-heartbeat`, and far below the brokers' `-commit-deadline`.
Start: open Pebble (empty), restore the newest checkpoint, replay to the tail, THEN listen: while that runs the port refuses
connections and brokers use the other URL. `X-CP-Role` is on every response; `/healthz` is 503 until ready or when blocked.
Shutdown: HTTP drains (15 s), the active node writes a final checkpoint (20 s), loops stop, Pebble closes. `-store mem` and
`-store dynamo` are unchanged.

### Upgrade and rollback

Upgrade from the old store: it is a new table layout on the same table name. Quiesce producers, stop the old manicp, start the
log nodes on the same (or a new) table; the log starts empty, so topics must be created again (or the table is fresh per run,
as in the benchmark). R# rows from the old store are not read by the log service; do not point both stores at one live table.
Rolling the log format forward: two deploys (decision 53). Roll back to `-store dynamo`: stop producers (commits quiesced),
wait for `manicp_log_index_lag_slots` = 0 on the active node (the indexer flushes every 10 s), stop the log nodes, start the old
manicp and give brokers `-max-segment-partitions 96`. G# keys are not written by the log store, so replays of old keys commit
again (decision 41). If the index was off (`-index=false`) or stalled past the window there is nothing to roll back to.

## Config (`logcp.Config`)

`Name`, `Rank`; `K`=4, `MaxSlotRate`=200, `MaxCmdsPerSlot`=256, `QueueMax`=4096, `QueueBytesMax`=64 MiB,
`Heartbeat`=1 s, `TakeoverAfter`=0; `HoleAfter`=250 ms, `TailPoll`=25 ms, `ReadMax`=500, `TailBatch`=10 ms;
cluster constants `WindowSlots`, `TrimStride` (not flags: nodes must agree); `CkptEvery`=5 min, `CkptMinSlots`=1000,
`KeepSlots`=WindowSlots, `TrimDelete`; `NoIndex`, `IndexEvery`=10 s, `IndexParallel`=8, `IndexRate`=0, `FeedMax`;
`DynLogOptions.TTL` (set it long: days) and `StubTTL` (30 d).

## Metrics

`manicp_log_*`, documented in `docs/observability.md`: slot attempts/lost/slots, append and queue-wait seconds,
entries per slot, entry bytes, apply lag, standby lag, hole fills, role and role changes, tail checks and fresh reads,
apply seconds, window keys, blocked, rejected, checkpoint slot/age/bytes/seconds/errors, trimmed slots, index
watermark/lag/flush seconds/rows/unprocessed/throttles/lost runs/errors.

## Failure cases

| case | behaviour |
|---|---|
| Put answer lost / Put fails | retried at the same slot; ours iff the ID matches |
| Slot lost | same entry appended later; counted `lost_slots_total` |
| Writer dies holding slot n | hole; after `HoleAfter` any node writes a no-op; a late writer loses and re-appends |
| Two appenders | retries (`lost_slots_total`); states equal (seeded split-brain mode) |
| Standby too far behind (log trimmed) | reads/append/fill hit a tombstone: node stops (`ErrTrimmed`), restart restores |
| Unknown slot format | node stops, 503, health 503 |
| Index stalled longer than the window | rows for keys that left it are lost, counted `index_lost_runs_total`, watermark moves |
| Restore raced by a newer checkpoint + trim | `Start` returns `ErrTrimmed`; supervisor restarts it |
| DynamoDB/S3 down | commits wait (503 on timeout); reads 503 after 4 tail tries; checkpoint/index retry next period |

## Findings that change earlier text

1. **Idempotency window must be deterministic.** Trimming "by slot" from each replica's own clock would make replicas
   answer a replayed key differently (one replays, one re-commits) and fork. The cut is now a function of the slot
   number (above). Decision 48's "window = log retention" becomes "window = WindowSlots; the log is kept at least
   as long" (checkpoints carry the window, so replay does not need old log slots).
2. **Trim by deleting is unsafe.** A node with a stale slot number (lagging standby that gets promoted) can win an
   `attribute_not_exists` put in a trimmed, now empty slot, and replace history. The seeded run found it. Trim
   leaves tombstones (1 WRU each, cheaper than deleting 3 KB items on the hot key), and TTL on slots is only a
   backstop with a long TTL.

## Verification

Seeded X2 on the real service: 3 nodes, lost/late answers, read and tail failures, crashes, restarts from checkpoint,
takeover, failback, split brain, small windows with trim. 20,000 seeds, 0 failures (`LOGCP_SEEDS=20000`). Checks:
nodes agree and equal an independent model and (large window) the veritas oracle; every client answer matches the
model for a slot holding that command; fresh reads after an ack see it on any node; the R# rows read back through
`cp/dynamo.Store` equal the model. `-race` with real goroutines (3 nodes, DynamoDB-like faults), cptest conformance
(store, HTTP, fakedb), unit tests for every loop.

Model (fakedb: Put 3.5 ms +1% 12 ms, Query/Get 2 ms, BatchWrite 6 ms; Pebble; 6 clients, 150 partitions, 200
batches, 12 s, plus 100 lookups/s; one core-shared host):

| offered/s | committed/s | p50 ms | p99 ms | slots/s | cmds/slot | p99 (index off) |
|---|---|---|---|---|---|---|
| 64 | 64.0 | 5.5 | 15.2 | 65 | 0.98 | 14.1 |
| 128 | 128.0 | 6.3 | 23.5 | 118 | 1.09 | 14.5 |
| 256 | 255.6 | 9.2 | 89.4 | 162 | 1.58 | 16.8 |

Index-on p99 at 256/s includes the indexer and fake sharing one host (about 38k rows/s); the log path alone holds
17 ms. Headroom is confirmed; real latency awaits stage 5.

## NOT done

AWS run (stage 5); full reindex from Pebble when the index stalls past the window; advisory active hint
beyond `X-CP-Role`; S3 index; streaming checkpoints (a checkpoint is built in memory).
