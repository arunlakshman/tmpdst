# Log-backed control-plane service (stage 2)

Note: this is a historical design record. Package names in it are old (`controlplanetest`,
`logcp`, the Pebble `Store.CommitBatch`, the HTTP handler), and some files it names
(`docs/sim-log-engine.md`, `docs/design-a-prototype.md`, `docs/applier-bakeoff.md`) are not in
this repo. The log, slot, epoch and takeover ideas still hold. For the code as it is, read
`internal/controlplane/doc.go`, `docs/glossary.md` and `docs/simulation.md`.

Status: production path is `internal/controlplane/replica` + `dst/live` + gRPC (`mdscontrolplane -store dynamolog`,
decision 55-58). Sim `kafka-log` uses the same state machine as CP processes plus `logstore` (CP-DSM-8,
`docs/sim-log-engine.md`). The goroutine `internal/controlplane/logcp` twin is deleted; DynamoLog/table helpers
live in `internal/controlplane/executor`. Decisions 48-52, `docs/design-a-prototype.md` section 8,
`docs/applier-bakeoff.md`. Tree path is `internal/controlplane/` (decision 56).

## Architecture

```
 brokers --(sticky, cp URL list)--> HTTP handler (role header, standby => 503)
                                         |
              commit/create          Service (Store)            Lookup/Describe/End
                  |                      |                               |
        queue (bounded, FIFO)            |                  fresh(): shared tail check
                  |                      |                    (batch >= TailBatch apart)
        one flusher: group <=256 cmds    |                               |
        into ONE slot, rate <= 200/s     |                               |
                  |                      v                               v
                  +--> Log.Append ---> replica: buffer -----> wait applied >= tail
                       (PutItem,           |  contiguous slots, batches never cross
                        attr_not_exists)   v  a trim boundary
   DynamoDB  LOG/<slot>  <--read--  Store.CommitBatch (Pebble)  --> answers to waiters (by entry ID)
   S3  checkpoint/<slot>  <-- checkpoint loop --+
```

*Current design (decision 61):* one append in flight, no gap filler. The writer retries the same slot until it knows the
result, so the log has no empty slot below its tail. Where this text says K=4 or a gap filler, that is superseded.

Roles use an epoch (decision 63). The writer is the node that holds the current epoch in `META/WRITER`. Rank 0 claims
at once only when the log has no epoch yet. Other nodes are standbys: they tail, apply and serve fresh reads, but answer
commit and create-topic with `503` + `Retry-After` + `X-CP-Role: standby`. The writer appends a no-op heartbeat when idle (1 s).
A standby that saw no slot of another writer for `TakeoverAfter` (while its own reads worked) claims the next epoch with a
conditional update, writes a fence entry at the tail, and accepts commits only after it has caught up. A writer that sees a
newer epoch (in a slot it applies, or in the entry that took its slot) steps down. It answers its waiting clients with
`ErrUnavailable` (unknown outcome), keeps reading, and waits a full silence period before it can claim again. Failback is gone.
Only one epoch owns the log at a time (section 16 of `docs/writer-fencing-design.md`).

## Slot format and versioning (decision 53)

Item: `pk=LOG`, `sk=%020d`, `d` (B) = entry, plus `v`, `w` (writer), `id`, `ts`, `n` (commands), `ttl`. Entry v1:
envelope (first byte 0xC8; it carries the epoch and the writer), rank, append ID, ts (ms, advisory, never state), count, then each command as
`internal/controlplane/metadb` codec (key table, batches). Zero commands is a no-op (heartbeat only; gap fills were removed, decision 61). Size guard: entry plus 256 B
of item overhead <= 350 KB; a single command that cannot fit is answered `ErrTooLarge` and never queued; the group
stops growing at the guard.
**Versioning rule (the open question):** the first byte is the format version. A reader that meets a version above
the format version or a payload it cannot decode STOPS (`Blocked`, 503, `/healthz` 503, `mdscontrolplane_log_blocked`); it never
skips, since skipping forks the state. Upgrade in two deploys: first every node reads v+1, then flip the writers
(`DynamoLogOptions.Version`). Going back after the flip needs the old build to understand v+1.

## Flows

**Commit.** Validate size, refuse if standby/blocked/queue full (`ErrNotActive`/`ErrBlocked`/`ErrOverloaded`, all
503-class). Queue; a flusher takes everything queued (<=256 commands) after reserving a slot-rate token, appends at
`max(high, applied)` with `attribute_not_exists`. Unknown outcome: retry the SAME slot (it lands, or the loser sees
whose it is). Lost slot (another writer): the loser gets the winner from the failed condition
(`ALL_OLD`, no extra read) and appends the same entry at a later slot; no offset was computed from a view. The caller
is answered when the replica has applied the slot, from the `Store` output, by entry ID. Only ONE append is in
flight (K=1; the old K=4 pipelining was removed, decision 61).
**Window (deviation, see below).** A key is forgotten `WindowSlots` (131,072) after its first slot, evaluated at
every `TrimStride` (1,024) boundary by the apply loop: a function of the log, so all nodes forget alike. At the 200
slots/s ceiling that is 655 s >= 2 x CommitDeadline + checkpoint period. Replays inside it get the original ranges.

**Lookup / Describe / End.** One tail `Query` (consistent, ~2 ms) shared by every read that arrives while a batch is
open; a batch starts at least `TailBatch` after the previous. Then wait until applied >= tail (the tail loop is
kicked), then answer from Pebble. Works on any node, so standbys scale reads. A slot below the tail is always
written (no empty slot is left), so a read never waits for a no-op. The gap filler (`GapAfter`, no-op after 250 ms) was removed. Rows at or below the floor are dropped by reads (decision 63).

**Missing next slot (decision 61).** A node reads the log from its next slot. If that slot is missing but a later slot
exists, the node fell behind a trim. Trim deletes rows (decision 62), so the next slot is simply missing. A read that
finds its first slot later than asked gets the same answer. Such a node restores from the S3 checkpoint, the same path as a trimmed slot.

**Failover.** The writer dies or goes silent. A standby claims the next epoch after `TakeoverAfter` (plus `-takeover-rank-step`
per rank), writes a fence entry at the tail, and then appends. In-flight entries of the old writer either landed (replays answer
them) or never will. If the old writer is alive but slow, the claim fences it: its next put sees the newer epoch and it steps down.
A false takeover costs availability for a moment, not data. Brokers (`controlplaneclient` URL list `a,b`, comma separated) move
on at connect error or 503, sticky to the last good URL, each URL once per call; commit retries carry the same key.
There is no failback: a planned handover is just a takeover.

**Checkpoint and trim.** Active node, every `CheckpointEvery` (5 min) and >= `CheckpointMinSlots` new slots: snapshot of the
Store (window included) tagged with the applied slot read inside the store view. It is stored as its own object
`checkpoint/<20-digit slot>` under `CheckpointPrefix` (decision 62). There is no pointer row in the table. After the new
object is stored, the node deletes the previous checkpoint object it knows (fire and forget: a failed delete only leaves one
extra object). Then trim: the node first raises `META/FLOOR` to the trim point (`checkpoint - KeepSlots`, a conditional update that only moves up),
and only then deletes rows at or below it (`TrimDelete`, a `BatchWriteItem` of `DeleteRequest`s, unprocessed items retried).
Reads drop rows at or below the floor (decision 63). The floor is a row that stays; it does not expire. Decision 62 made trim a
plain delete; decision 63 added the floor, because a plain delete alone lost acknowledged data (finding 2 is superseded).

**Why per-slot keys:** a late or repeated upload can only rewrite the same bytes at its own slot, so an old snapshot can
never replace a newer one. (The old single key `checkpoint/latest` let a lagging node overwrite a newer checkpoint: seeds 439 and 1741.)

Restore on start: LIST the prefix, take the last key in sorted order (the newest, since the slot has a fixed width), read it,
check it, `Store.Restore`, replay from the checkpoint slot to the tail. If that object was already replaced (not found),
list again. A list error is retried; restore never starts from slot 0 because of a list error. A damaged object falls back
to replay from slot 0 if the log still starts there, else the node cannot start. (There is no `ErrNoCheckpoint` error in the code yet.)

There is no R# index and no row in the table other than `LOG/<slot>` (decision 54). Lookup is answered from Pebble; a future S3 index or reader scale-out would be built from the log.

## `mdscontrolplane -store dynamolog` (stage 4)

Flags (all `-store dynamolog` only; defaults follow decisions 48-53): `-table` (default `<table-prefix>cp`), `-checkpoint-bucket`
(empty: no checkpoints and no trim, a restart replays the whole log) `-checkpoint-prefix cp/checkpoint/` (was `-checkpoint-key`, decision 62) `-s3-endpoint` `-s3-path-style`,
`-node-name` (hostname) `-rank 0` (a preference only: rank 0 claims at once on an empty log; other claims wait `-takeover-rank-step` per rank on top of `-takeover-after`), `-takeover-after 5s` `-takeover-rank-step 1s` `-heartbeat 1s` (`-gap-after` and `-log-k` were removed, decision 61),
`-checkpoint-every 5m` `-checkpoint-min-slots 1000` `-window-slots 131072` (cluster constant), `-log-ttl 168h`, `-pebble-dir` (empty: temporary;
a given directory is wiped at start if it holds a Pebble database, refused if it holds anything else), `-start-timeout 10m`,
`-create-tables` (dev). `-takeover-after` must be several times `-heartbeat`, and far below the brokers' `-commit-deadline`.
Start: open Pebble (empty), restore the newest checkpoint, replay to the tail, THEN listen: while that runs the port refuses
connections and brokers use the other URL. `X-CP-Role` is on every response; `/healthz` is 503 until ready or when blocked.
Shutdown: HTTP drains (15 s), the active node writes a final checkpoint (20 s), loops stop, Pebble closes. `-store mem` is unchanged. The other log store is `-store postgres` (below).

## `mdscontrolplane -store postgres`

Status: built and tested for PostgreSQL 17.11. Design, schema, tests and porting notes: [docs/sql-log-backend.md](sql-log-backend.md).

Flags (all `-store postgres` only): `-postgres-dsn` (empty: use the environment variable `MDS_POSTGRES_DSN`, which keeps the password off the command line; the DSN is never logged), `-postgres-prefix mds_` (tables `<prefix>log` and `<prefix>meta`), `-create-tables` (dev). Checkpoint, heartbeat, rank and takeover flags are the same as for `-store dynamolog` (`-checkpoint-bucket`, `-checkpoint-prefix`, `-s3-endpoint`, `-s3-path-style`, `-node-name`, `-rank`, `-takeover-after`, `-takeover-rank-step`, `-heartbeat`, `-checkpoint-every`, `-checkpoint-min-slots`, `-window-slots`, `-pebble-dir`, `-start-timeout`). `-table` and `-log-ttl` do not apply.

Reads must go to the primary database. Do not point the control plane at a read replica.

### Upgrade

Rolling the log format forward: two deploys (decision 53). Decision 63 (epoch envelope) changed the format in one step: no production cluster existed (decision D5 in `docs/writer-fencing-design.md`). The table holds only the log and the checkpoint pointer, so a fresh
table per run (as in the benchmark) or a new table name starts empty and topics are created again. There is no rollback to the
old transaction-based store (removed, decision 54): going back needs an older git revision and a different table.

## Config (`replica.Config`)

`Name`, `Rank`; one append in flight (no `K`, decision 61); `MaxSlotRate`=200, `MaxCmdsPerSlot`=256, `QueueMax`=4096, `QueueBytesMax`=64 MiB,
`Heartbeat`=1 s, `TakeoverAfter`=0, `TakeoverRankStep`=1 s; `TailPoll`=25 ms, `ReadMax`=500, `TailBatch`=10 ms;
cluster constants `WindowSlots`, `TrimStride` (not flags: nodes must agree); `CheckpointEvery`=5 min, `CheckpointMinSlots`=1000,
`KeepSlots`=WindowSlots, `TrimDelete` (deletes rows after the floor is raised, decisions 62 and 63), `CheckpointPrefix`=`checkpoint/` (was `CheckpointKey`); `DynamoLogOptions.TTL` (set it long: days) and `StubTTL` (30 d).

## Metrics

`mdscontrolplane_log_*`, documented in `docs/observability.md`: slot attempts/lost/slots, append and queue-wait seconds,
entries per slot, entry bytes, apply lag, standby lag, role and role changes, epoch and step-downs (`mdscontrolplane_log_epoch`, `mdscontrolplane_log_step_downs_total`; `gap_fills_total` was removed), tail checks and fresh reads,
apply seconds, window keys, blocked, rejected, checkpoint slot/age/bytes/seconds/errors, trimmed slots.

## Failure cases

| case | behaviour |
|---|---|
| Put answer lost / Put fails | retried at the same slot; ours iff the ID matches |
| Slot lost | same entry appended later; counted `lost_slots_total` |
| Writer dies during its append | the slot is written or not; the next active writes at the same slot number. No gap. The new writer's fence entry is one no-op at the tail (decision 63). A late writer loses and re-appends |
| Two writers at once | Only one epoch owns the log. The older writer is fenced at its next put: it steps down and answers `ErrUnavailable` with unknown outcome (decision 63) |
| Standby too far behind (log trimmed) | reads/append find the next slot missing while later slots exist, or the first returned slot is later than asked (decisions 61, 62): node stops (`ErrTrimmed`), restart restores from the checkpoint |
| Writer frozen longer than one checkpoint period plus `KeepSlots`, still believing it is active | Its put into a deleted slot is refused by the floor check, so no commit is acknowledged (decision 63 closes the residual risk of decision 62) |
| Lagging node writes an old checkpoint | cannot replace a newer one: each checkpoint has its own key (decision 62) |
| Unknown slot format | node stops, 503, health 503 |
| Restore raced by a newer checkpoint + trim | `Start` returns `ErrTrimmed`; supervisor restarts it |
| DynamoDB/S3 down | commits wait (503 on timeout); reads 503 after 4 tail tries; checkpoint retries next period |
| False takeover (old writer is alive) | The old writer is fenced. Its next put sees the newer epoch and it steps down. No acknowledged data is lost; the cost is availability for a moment |
| Deposed writer puts into a deleted slot | The put lands at or below the floor. The floor check refuses it: its clients are refused and the node restores from the checkpoint |
| Restoring node | It never claims an epoch and never writes. It is not `ready` until the restore is finished, a tail check ran after it, and it has applied `goalTail` |

## Findings that change earlier text

1. **Idempotency window must be deterministic.** Trimming "by slot" from each replica's own clock would make replicas
   answer a replayed key differently (one replays, one re-commits) and fork. The cut is now a function of the slot
   number (above). Decision 48's "window = log retention" becomes "window = WindowSlots; the log is kept at least
   as long" (checkpoints carry the window, so replay does not need old log slots).
2. **Trim by deleting is unsafe.** A node with a stale slot number (lagging standby that gets promoted) can win an
   `attribute_not_exists` put in a trimmed, now empty slot, and replace history. The seeded run found it. Trim
   left a deleted marker (0xFE, 1 WRU each). **Superseded by decision 62:** trim now deletes rows. The stale-writer
   case is a known residual risk (see the failure table); the simulator property stays as a detector. TTL on slots is only a
   backstop with a long TTL.

## Verification

Seeded X2 on the real service: 3 nodes, lost/late answers, read and tail failures, crashes, restarts from checkpoint,
takeover, split brain, small windows with trim (the failback part is superseded by decision 63). 20,000 seeds, 0 failures (`LOGCP_SEEDS=20000`). Checks:
nodes agree and equal an independent model and (large window) the reference model (`spec`; it was called speccheck when this was measured); every client answer matches the
model for a slot holding that command; fresh reads after an ack see it on any node. Real goroutines (3 nodes, DynamoDB-like faults), controlplanetest conformance
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

AWS run (stage 5); active hint
beyond `X-CP-Role`; S3 index; streaming checkpoints (a checkpoint is built in memory).
