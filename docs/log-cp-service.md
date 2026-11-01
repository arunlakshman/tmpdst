# Log-backed control-plane service (stage 2)

Status: production path is `internal/controlplane/statemachine` + `internal/controlplane/runloop` + gRPC (`manicp -store dynamolog`,
decision 55–58). Sim `kafka-log` uses the same machine as CP processes plus `logstore` (CP-DSM-8,
`docs/sim-log-engine.md`). The goroutine `internal/controlplane/logcp` twin is deleted; DynLog/table helpers
live in `internal/controlplane/executor`. Decisions 48-52, `docs/design-a-prototype.md` section 8,
`docs/applier-bakeoff.md`. Tree path is `internal/controlplane/` (decision 56).

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
   CKPT                                    |
   S3  ckpt/<slot>   <-- checkpoint loop --+
```

Roles are advisory (decision 51). Rank 0 starts active; others are standbys that tail, apply and serve fresh reads
but answer commit and create-topic with `503` + `Retry-After` + `X-CP-Role: standby`. The active node appends a
no-op heartbeat when idle (1 s). A standby that saw no slot of another writer for `TakeoverAfter` (while its own
reads worked) becomes active. An active node steps down when a writer of a lower rank appends after it took over
(failback). Two active nodes cost retries, never correctness: the slot is the fence.

## Slot format and versioning (decision 53)

Item: `pk=LOG`, `sk=%020d`, `d` (B) = entry, plus `v`, `w` (writer), `id`, `ts`, `n` (commands), `ttl`. Entry v1:
version byte, rank, writer, append ID, ts (ms, advisory, never state), count, then each command as
`internal/controlplane/stateupdater` codec (key table, batches). Zero commands is a no-op (hole fill, heartbeat). Size guard: entry plus 256 B
of item overhead <= 350 KB; a single command that cannot fit is answered `ErrTooLarge` and never queued; the group
stops growing at the guard.
**Versioning rule (the open question):** the first byte is the format version. A reader that meets a version above
the format version or a payload it cannot decode STOPS (`Blocked`, 503, `/healthz` 503, `manicp_log_blocked`); it never
skips, since skipping forks the state. Upgrade in two deploys: first every node reads v+1, then flip the writers
(`DynLogOptions.Version`). Going back after the flip needs the old build to understand v+1.

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
`ckpt - KeepSlots` are overwritten with TOMBSTONES (`TrimDelete`), not deleted (finding 2).
Restore on start: pointer, object, sha-256, `applier.Restore`, replay from the checkpoint slot to the tail. A
damaged object falls back to replay from slot 0 if the log still starts there, else `ErrNoCheckpoint`.

There is no R# index and no row in the table other than `LOG/<slot>` and the `CKPT` pointer (decision 54). Lookup is answered from Pebble; a future S3 index or reader scale-out would be built from the log.

## `manicp -store dynamolog` (stage 4)

Flags (all `-store dynamolog` only; defaults follow decisions 48-53): `-table` (default `<table-prefix>cp`), `-ckpt-bucket`
(empty: no checkpoints and no trim, a restart replays the whole log) `-ckpt-prefix cp/` `-s3-endpoint` `-s3-path-style`,
`-node-name` (hostname) `-rank 0`, `-takeover-after 5s` `-heartbeat 1s` `-hole-after 250ms` `-log-k 4`,
`-ckpt-every 5m` `-ckpt-min-slots 1000` `-window-slots 131072` (cluster constant), `-log-ttl 168h`, `-pebble-dir` (empty: temporary;
a given directory is wiped at start if it holds a Pebble database, refused if it holds anything else), `-start-timeout 10m`,
`-create-tables` (dev). `-takeover-after` must be several times `-heartbeat`, and far below the brokers' `-commit-deadline`.
Start: open Pebble (empty), restore the newest checkpoint, replay to the tail, THEN listen: while that runs the port refuses
connections and brokers use the other URL. `X-CP-Role` is on every response; `/healthz` is 503 until ready or when blocked.
Shutdown: HTTP drains (15 s), the active node writes a final checkpoint (20 s), loops stop, Pebble closes. `-store mem` is unchanged (the only other value).

### Upgrade

Rolling the log format forward: two deploys (decision 53). The table holds only the log and the checkpoint pointer, so a fresh
table per run (as in the benchmark) or a new table name starts empty and topics are created again. There is no rollback to the
old transaction-based store (removed, decision 54): going back needs an older git revision and a different table.

## Config (`logcp.Config`)

`Name`, `Rank`; `K`=4, `MaxSlotRate`=200, `MaxCmdsPerSlot`=256, `QueueMax`=4096, `QueueBytesMax`=64 MiB,
`Heartbeat`=1 s, `TakeoverAfter`=0; `HoleAfter`=250 ms, `TailPoll`=25 ms, `ReadMax`=500, `TailBatch`=10 ms;
cluster constants `WindowSlots`, `TrimStride` (not flags: nodes must agree); `CkptEvery`=5 min, `CkptMinSlots`=1000,
`KeepSlots`=WindowSlots, `TrimDelete`; `DynLogOptions.TTL` (set it long: days) and `StubTTL` (30 d).

## Metrics

`manicp_log_*`, documented in `docs/observability.md`: slot attempts/lost/slots, append and queue-wait seconds,
entries per slot, entry bytes, apply lag, standby lag, hole fills, role and role changes, tail checks and fresh reads,
apply seconds, window keys, blocked, rejected, checkpoint slot/age/bytes/seconds/errors, trimmed slots.

## Failure cases

| case | behaviour |
|---|---|
| Put answer lost / Put fails | retried at the same slot; ours iff the ID matches |
| Slot lost | same entry appended later; counted `lost_slots_total` |
| Writer dies holding slot n | hole; after `HoleAfter` any node writes a no-op; a late writer loses and re-appends |
| Two appenders | retries (`lost_slots_total`); states equal (seeded split-brain mode) |
| Standby too far behind (log trimmed) | reads/append/fill hit a tombstone: node stops (`ErrTrimmed`), restart restores |
| Unknown slot format | node stops, 503, health 503 |
| Restore raced by a newer checkpoint + trim | `Start` returns `ErrTrimmed`; supervisor restarts it |
| DynamoDB/S3 down | commits wait (503 on timeout); reads 503 after 4 tail tries; checkpoint retries next period |

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
model for a slot holding that command; fresh reads after an ack see it on any node. `-race` with real goroutines (3 nodes, DynamoDB-like faults), cptest conformance
(store, HTTP, fakedb), unit tests for every loop.

Model (fakedb: Put 3.5 ms +1% 12 ms, Query/Get 2 ms, BatchWrite 6 ms; Pebble; 6 clients, 150 partitions, 200
batches, 12 s, plus 100 lookups/s; one core-shared host):

| offered/s | committed/s | p50 ms | p99 ms | slots/s | cmds/slot | p99 (no index) |
|---|---|---|---|---|---|---|
| 64 | 64.0 | 5.5 | 15.2 | 65 | 0.98 | 14.1 |
| 128 | 128.0 | 6.3 | 23.5 | 118 | 1.09 | 14.5 |
| 256 | 255.6 | 9.2 | 89.4 | 162 | 1.58 | 16.8 |

The last column is the log path alone (the index writer no longer exists, decision 54); the other p99 column was measured
with it sharing the host. Headroom is confirmed; real latency awaits stage 5.

## NOT done

AWS run (stage 5); advisory active hint
beyond `X-CP-Role`; S3 index; streaming checkpoints (a checkpoint is built in memory).
