# Writer fencing for the control plane log (design for review)

Status: IMPLEMENTED on branch `refactor/single-append-no-gap` (simulator campaign result: see section 16).
Branch: `refactor/single-append-no-gap`. Written 2026-11-03.

## 1. Why we need this (the problem in short)

The control plane log is a DynamoDB table. One row is one slot (`pk=LOG`, `sk=<20-digit slot>`).
A writer appends with a conditional put (`attribute_not_exists(pk)`). After a checkpoint, old slots are trimmed.

Two facts make this unsafe today:

1. **Roles are only advisory.** A standby promotes itself after `TakeoverAfter` (600 ms to 1.5 s in the
   simulator) with no slot from the active node. The old node does not learn it was replaced. Two nodes
   can write at the same time. The slot condition is the only guard between them.
2. **A trimmed slot looks empty.** Trim now deletes rows. A stale writer's put into a deleted slot succeeds.
   The stale writer applies its own entry and acknowledges the client. No other node will ever read that
   slot, because they are already past it. That is silent loss of acknowledged data.

Simulator evidence (`go run ./cmd/mdssimulator -scenario kafka-log -seed N`):

| Seed | What happened | Root cause |
|---|---|---|
| 1333 | `cp-0` was behind (`applied` 131, `goalTail` 137) but still accepted a commit and chose slot 131. `cp-1` had trimmed through 131 just before. The put succeeded, `cp-0` acknowledged `base=107`, and no other node has it. | Two active writers + write into a deleted slot |
| 2318 | `cp-0` was restoring. Its checkpoint list timed out. The role timer promoted it and it sent a heartbeat. That append completion made it "ready" while `goalTail` was 0 and `checkpointRetry` was true. It took a commit with id `cp-0/2`, but the old log already had an entry `cp-0/2` (`seq` restarts at 0). A broker got the reply for a different segment. | A restoring node writes and becomes ready too early; entry ids repeat |
| 1741 | Both nodes looped on restore because an old checkpoint overwrote a newer one. | **Fixed** by per-slot checkpoint keys (already on the branch) |

## 2. What is already done on this branch (not part of this review)

- One `LogAppend` in flight (K=1). The gap filler is removed. A node whose next slot is missing while later
  slots exist restores from the checkpoint (`onReadDone` in `machine.go`).
- Checkpoints are per-slot objects `checkpoint/<20-digit slot>`. Restore lists the prefix and reads the newest.
- Trim is a plain delete. The deleted-marker rows are removed from the code.
- Leftover: `executor.MemoryLog` (`internal/controlplane/executor/log.go`) still has the old deleted marker.
  It will be replaced by the floor logic below.

This design replaces the "plain delete is enough" assumption, and it replaces advisory roles.

## 3. Goals and non-goals

Goals:

- At most one writer at a time. Others are standbys that read, tail, and can take over.
- A deposed writer must never get a commit acknowledged. It becomes **read-only** (it does not exit).
- A write into a deleted slot must never count as committed.
- A restoring node must not write or serve commits until it is really caught up.
- No clocks in the safety argument.

Non-goals: multi-writer throughput, changing the entry format of commands, changing the gRPC API.

## 4. Design overview

Two small items in the same DynamoDB table, in a separate partition so the log `Query` and the trim never see them:

```
pk=META  sk=WRITER   epoch=7      owner="cp-0"       who holds the write role
pk=META  sk=FLOOR    floor=131                       highest slot that may already be deleted
pk=LOG   sk=0000…0140  e=7  w="cp-0"  id="cp-0/e7/41"  d=<entry>    log rows (new: attribute e)
```

Three mechanisms:

| Mechanism | Stops | Source of the idea |
|---|---|---|
| **Epoch** (`META/WRITER`) with a conditional update | Two writers at once; false takeover | SlateDB RFC 0001 (`writer_epoch`) |
| **Fence entry** at the tail | A deposed writer that does not know it was replaced | SlateDB RFC 0001 (fence SST) |
| **Floor** (`META/FLOOR`) advanced before trim; checked after every put | A write into a deleted slot counting as committed | SlateDB RFC 0026 (boundary files) |

## 5. Takeover (a standby becomes the writer)

Trigger: a standby that is `ready`, not `booting`, fully caught up, with reads working, and that has seen no
slot from the writer for `TakeoverAfter`. (Section 8 lists the exact ready conditions.)

1. `GetItem META/WRITER`, consistent read. It returns `epoch=7` (or no item at the very first start).
2. Claim: compare-and-swap.
   ```
   UpdateItem META/WRITER
     SET epoch = :8, owner = :me
     ConditionExpression  epoch = :7            (first start: attribute_not_exists(epoch))
     ReturnValuesOnConditionCheckFailure  ALL_OLD
   ```
   - Success: this node now holds epoch 8.
   - `ConditionalCheckFailed`: another node won. Stay a standby. Nothing was written to the log.
3. `Query` the log tail (consistent, descending, limit 1). Say the tail is 140.
4. Write a **fence entry**: a no-op entry carrying `e=8` at slot 140 with `attribute_not_exists(pk)`.
   If the old writer filled 140 first, the put fails: learn the new tail and retry at the next slot.
5. Catch up through the fence slot. Only then accept commits.
6. Commit ids are `<writer>/e8/<seq>`. The epoch is in the id, so ids never repeat across restarts.

Why a false takeover is now safe: if the old writer is alive, it is simply fenced and goes read-only. The cost
is availability for a moment, not correctness.

## 6. Normal append by the writer

```
PutItem LOG/<slot> {e=8, ...}  ConditionExpression attribute_not_exists(pk)
        ReturnValuesOnConditionCheckFailure ALL_OLD
then, only if the put succeeded:
GetItem META/FLOOR   (consistent)
```

The writer handles three outcomes:

| Outcome | Meaning | Action |
|---|---|---|
| Put succeeded, `floor < slot` | Normal write | Apply, answer the client |
| Put succeeded, `floor >= slot` | The slot had been deleted. Not a commit. | Do **not** apply or answer. Answer `ErrUnavailable`, go read-only, restore. |
| Put failed, existing entry has `e` > mine | A newer writer exists. I am fenced. | Go read-only |
| Put failed, existing entry has `e` <= mine, not my id | I lost a slot to an older-epoch writer or a standby race | Retry at a later slot (existing "lost slot" path) |
| Put failed, existing entry is my own id | My earlier timed-out put had landed | Treat as success (existing path) |

Slot choice stays `max(high, applied)`, but the writer does not create an append while `applied < goalTail`
or while a catch-up read is outstanding.

## 7. Trim and the floor

Trim runs after a checkpoint is stored. It computes `through = checkpoint - KeepSlots - 1`.

1. `UpdateItem META/FLOOR SET floor = :through` with
   `ConditionExpression attribute_not_exists(floor) OR floor < :through`. (Monotonic: it only moves up.
   A failed condition means a higher floor is already there, which is fine.)
2. Only after step 1 succeeds: `BatchWriteItem` delete requests for slots `<= through`
   (retry unprocessed items; already in the code).

Rule A (trimmer): the floor covers a slot before that slot is deleted.
Rule B (writer): a put is a commit only if the floor read **after** the put is below the slot.

### Why this is safe (the argument)

- Suppose a put into slot N succeeds because N was deleted. By rule A the floor was `>= N` before the delete.
  The floor never decreases and the put came after the delete, so the read after the put sees `floor >= N`.
  The writer detects it.
- Suppose the writer sees `floor < N`. Then N was not deleted when it wrote. A later trim can delete N only
  after a checkpoint passed N, and a checkpoint is taken from applied state, so every checkpoint reader already
  has the entry.
- The stray row in a deleted slot is harmless: no one reads below the floor, and the next trim removes it.
- The log has no empty slot below its tail (one append at a time, a slot is retried until its result is known).
  So a stale put can only land at a deleted slot (caught by the floor) or at a live slot, where every node
  will read it, so it is consistent.

Therefore the fold does **not** need to ignore entries by epoch. The epoch gives "one writer at a time", not
the data safety. (This is my reasoning from the code. The simulator tests in section 11 must confirm it.)

### Honest cost comparison with the old deleted markers

| | Markers (old) | Floor (this design) |
|---|---|---|
| Trim | N marker writes | 1 floor update + N deletes |
| Each append | 0 extra | 1 extra consistent `GetItem` of a tiny item (about 1 read unit, a few ms) |
| Marker rows expire | Yes (`StubTTL`, 30 days), then the slot is empty again | Floor never expires |
| Reads | Skip marker rows | Nothing to skip |

The extra read per append is the price. At most 200 slots a second.

## 8. Restore and readiness (fixes seed 2318)

A node is `ready` only when all of these are true:

1. The checkpoint restore finished: no `checkpointRetry`, no `needsCheckpoint`, no list or get outstanding.
2. A tail check completed **after** the restore finished (`goalTail` is a real value, not the reset 0).
3. `applied >= goalTail`.

`maybeBecomeReady` is the only place that sets `ready`. An append completion can no longer set it by accident.

Takeover and heartbeat run only when `ready && !booting`. A restoring node never writes.

## 9. Read-only state

A fenced or deposed node:

- keeps tailing the log and serves reads (`Lookup`, `Describe`, ...),
- answers commits and topic changes with `ErrUnavailable` (brokers move to the other URL, as today),
- drops all in-flight appends (their callers get `ErrUnavailable` with unknown outcome),
- restores from the newest checkpoint and catches up, then stays a standby that may claim a new epoch later,
- increments a metric and logs the reason (`fenced_by_epoch`, `below_floor`, `lost_slot_to_newer_epoch`).

## 10. Code changes by package

| Package | Change |
|---|---|
| `internal/core` | New action `LogClaimEpoch{Req, Expect}` and event `LogClaimEpochDone{Req, Epoch, Won, Err, Outcome}`. Add to inbox/event lists. Doc update for `LogDeleteOld`: advances the floor, then deletes. |
| `internal/controlplane/executor` | `Log` interface: add `ClaimEpoch`. `DynamoLog`: `ClaimEpoch` (GetItem + conditional UpdateItem), `Append` does the floor read after a successful put and returns the "below floor" shape, `TrimThrough` raises the floor first. `MemoryLog`: same semantics (replace the old marker). Classify errors. |
| `internal/controlplane/fakedynamodb` | Check that `UpdateItem` with `epoch = :x` and `attribute_not_exists(floor) OR floor < :n`, and consistent `GetItem`, are supported. Add if not. (To verify.) |
| `internal/verify/simulator/fakeservices/fakelogstore`, `fakelogservice` | Store holds epoch and floor. Same `Append`, `ClaimEpoch`, `Trim` semantics. Tags and the engine truth model. New mutants (section 11). |
| `internal/controlplane/statemachine` | Epoch state and roles (claim, fence entry, read-only). Envelope carries epoch. Entry ids carry epoch. Ready rules (section 8). Writer rules (section 6). `belowFloor` handling reusing `hitTrimmed`. Remove rank-based failback (decision D3). |
| `cmd/mdscontrolplane` | Flags: none new if possible. Metrics: `epoch`, `fenced_total{reason}`, `floor`. |
| `cmd/mdscpinspect` | Show epoch and floor; check that no log row exists at or below the floor after a trim completed (stray rows allowed, report count). |
| `ops/` Terraform | IAM: the control plane role needs `dynamodb:UpdateItem` and `GetItem` on the table (check what is granted today). |
| `docs/` | `log-cp-service.md`, `decisions.md` (new row), `observability.md`. |

Entry format: the envelope gains the epoch. Rule from decision 53: first deploy readers that understand the new
version, then switch the writers. (See decision D5.)

## 11. Test and verification plan

New deterministic tests (statemachine, executor, fakelogstore):

- Takeover wins and loses (two standbys race); the loser stays standby and writes nothing.
- Fence entry collision: the old writer fills the tail first; the new writer retries.
- Put into a deleted slot: floor check returns "below floor"; no apply, no answer, node goes read-only.
- Put loses a slot to a higher-epoch entry: node goes read-only; lower-epoch entry: retry later.
- A restoring node does not promote, send a heartbeat, or become ready until section 8 holds.
- Entry ids never repeat after a restart.
- Regression tests named for seeds 1333, 1741, 2318.

Simulator:

- Replace the old "trimmed slots deleted" mutant test. That test is vacuous: it asserts `Always(false)` as soon as
  a trim happens, so it proves nothing. New real mutants: (a) skip the floor check after a put; (b) ignore a
  higher-epoch conflict; (c) allow takeover while booting. Each must be caught by a kafka property within a few
  seeds. Seed 1333 should catch (a) at once.
- New invariant in the truth engine: all entries of one epoch come from one writer name.
- Keep the false-takeover and stall faults. Make sure a campaign case has `KeepSlots` small and a stall longer
  than a trim.
- Campaign gate: 3000+ seeds of `kafka-log` and 500 of `kafka` with 0 failures, then refresh golden hashes.

## 12. Decisions needed from you

| # | Question | My recommendation | Status |
|---|---|---|---|
| D1 | How to stop a write into a deleted slot: (a) floor item + one read after each put, (b) `TransactWriteItems` (epoch check + put) on every append, (c) lease + age-gated trim | (a). It needs no clocks and costs one cheap read. (b) is airtight but about 2x write cost. (c) needs bounded clock skew. | approved: recommendation chosen |
| D2 | Signal for "below floor": reuse the old shape (`ErrConflict`, `Taken`, empty `Existing`) or add a new `core.ErrFenced` code | Reuse the shape (small blast radius). Wrap it in one named helper. | approved: recommendation chosen |
| D3 | Rank and failback | Drop failback. Keep rank only as an extra takeover delay (`TakeoverAfter + rank * step`). Planned handover is just a takeover. | approved: recommendation chosen |
| D4 | After a fence, should the node stay read-only until restart, or restore and become eligible again | Restore, catch up, stay standby, eligible again. No process exit. | approved: recommendation chosen |
| D5 | Rollout: do any clusters exist that must keep running during the format change | Docs only show test clusters. If none, change the format without a two-step deploy. | approved: recommendation chosen (no production clusters, format changed without a two-step deploy) |
| D6 | Keep the fence entry (safety does not strictly need it; it makes a deposed writer notice fast) | Keep. It is one no-op per takeover. | approved: recommendation chosen |
| D7 | Keep `META/WRITER` and `META/FLOOR` as two items | Two items, so a trim and a takeover never contend. | approved: recommendation chosen |

## 13. Work plan for sub-agents

Order matters because later steps need the new types.

1. **Types** (lead): `core` action/event, inbox, docs for `LogDeleteOld`. Small, done first.
2. **Executor** (agent A): `Log` interface, `DynamoLog`, `MemoryLog`, `fakedynamodb` support, classify. Tests.
3. **Simulator stores** (agent B): `fakelogstore`, `fakelogservice`, tags, truth engine, mutants, new invariant.
4. **State machine** (lead): roles, epoch, fence entry, writer rules, ready rules, ids, read-only, tests.
   Agents A and B only start after step 1, and they must not touch `statemachine/`.
5. **Tools and ops** (agent C): `mdscontrolplane` metrics, `mdscpinspect`, Terraform IAM.
6. **Docs** (agent D): after code is stable.
7. **Verify** (lead): unit tests, then 3000-seed and 500-seed campaigns, refresh golden hashes, replay seeds
   1333, 1741, 2318.

Each agent reports files changed and tests run. No commits until you approve the result.

## 14. What I have not verified

- The real DynamoDB behavior of every call above (I know the API from the docs, not from a test run here).
- That `fakedynamodb` already supports the needed condition expressions.
- The claim that the fold needs no epoch rule. It follows from "no empty slot below the tail" and the floor
  check, but only the simulator can confirm it.
- SlateDB details beyond RFC 0001, 0014, 0026, 0029, 0030 and the `fence.rs` / `wal/mod.rs` sources.

## 15. References

- SlateDB RFC 0001 (manifest, writer epoch, fence SST): https://slatedb.io/rfcs/0001-manifest/
- SlateDB RFC 0026 (garbage collector boundary files):
  https://raw.githubusercontent.com/slatedb/slatedb/main/rfcs/0026-garbage-collector-boundary.md
- SlateDB RFC 0030 (pluggable WAL, fencing rules):
  https://raw.githubusercontent.com/slatedb/slatedb/main/rfcs/0030-pluggable-wal.md
- Simulator seeds: 1333, 1741, 2318 (`-scenario kafka-log`).


## 16. As built (differences from the plan)

The code is on branch `refactor/single-append-no-gap`. Where this section and sections 4 to 11 disagree, this section is right.

a. **Reserved word.** `owner` is a DynamoDB reserved word. Every expression uses a name alias: `#owner`, `#epoch` and `#floor`.

b. **Stray rows below the floor.** A put that the floor check refuses leaves a row in a slot at or below the floor. So:
   - `Read` drops every row at or below the floor. It runs a `Query` first, then one consistent `GetItem` of the floor, then filters the rows.
   - `Append` also checks the floor when its put fails because the slot is taken. A retry then cannot mistake its own stray row for an entry that landed.

c. **Below the floor.** The state machine gets `LogAppendDone{Taken:true, Err:ErrConflict, Outcome:NotApplied}` with no `Existing`. The state machine refuses its clients and restores from the checkpoint. (Section 6 said `ErrUnavailable`. The built answer is this one.)

d. **Step down.** A node that sees a newer epoch steps down. It can see the newer epoch in a slot it applies, or in the entry that took its slot. When it steps down, it:
   - answers its waiting clients with `ErrUnavailable` and unknown outcome,
   - drops its write role,
   - keeps reading,
   - waits a full silence period (`TakeoverAfter`) before it can claim again.

e. **Claims and rank.** Rank 0 claims at once only when the log has no epoch yet. Every other claim waits `TakeoverAfter + rank * TakeoverRankStep`. The new flag `-takeover-rank-step` sets the step (default 1 s). Failback by rank is gone. Rank only sets a delay now.

f. **Entry ids and envelope.** An entry id is `<writer>/e<epoch>/<seq>`. The envelope marker is 0xC8. The envelope carries the epoch and the writer.

g. **Ready.** A node is `ready` only when:
   - the checkpoint restore is finished,
   - a tail check ran after the restore,
   - `applied >= goalTail`.
   Takeover and heartbeat run only when the node is `ready` and not `booting`.

h. **Core types.** The new core types are `LogClaimEpoch` (action) and `LogClaimEpochDone{Epoch, Owner, Won}` (event). If a claim times out, it is accepted as won when the stored owner is this node and the stored epoch is `Expect+1`. (`Expect` is the epoch the claim started from.)

i. **Metrics.** New metrics: `mdscontrolplane_log_epoch` (gauge: the epoch this node knows) and `mdscontrolplane_log_step_downs_total` (counter: how many times this node stepped down).

j. **IAM.** The control plane role needs `dynamodb:GetItem` and `dynamodb:UpdateItem` on the table.

k. **Simulator.** New property: `logcp-sim: one epoch has one writer`. New mutant: `InjectedBugNoFloorCheck` (value 4). Seeds 1333, 1741 and 2318 pass.

l. **Process freeze.** The simulator kernel can freeze a process (`NodeKills.FreezeProb`, `FreezeDur`, `Process.Freezable`). A frozen node takes no step for 1 to 4 seconds, its inbox keeps filling, and its earlier puts still land. This is a real long pause (VM pause or garbage collection), and it exercises the floor check without any mutant. Scenario `-scenario kafka-log-freeze` (`make sim-campaign` runs it). Over 200 seeds, 37 puts from frozen writers were refused at the floor; the mutant `InjectedBugNoFloorCheck` loses acknowledged data in this scenario within a few seeds.

Campaign result: kafka-log seeds 1 to 10000: 0 failing (399.6M steps). kafka seeds 1 to 1000: 0 failing. kafka-log-freeze: see the pull request description for the latest run.
