# Follow-up: CP attrition + StallProb / CrashProb (stacked faults)

**Status:** deferred after CP-DSM stack land (`toys3` tip `bf460bd`, PR #9 / `cfca8b6`).  
**Owner:** unassigned — pick up independently using this doc.  
**Related:** `docs/sim-log-engine.md`, `.audit/cp-dsm-stack.tsv`, commit `cfca8b6`.

This is **not** “add new knobs.” The knobs and machine paths already exist. The work is to **turn them on under the post–CP-DSM-8 topology**, keep the kafka-log campaign green, and restore the reachables that were dropped while they were forced off.

---

## 1. One-sentence goal

Re-enable three survival-fault families on `kafka-log` so a campaign can kill/restart CP **processes** (attrition) and stall/crash CP **replicas in-process** (`StallProb` / `CrashProb`) **while** bridge + logstore Put coins stay live — without wedging drain, orphaning answers, or losing determinism.

---

## 2. Why this was deferred

CP-DSM-8 cut brokers over to real `cp-*` `controlplane/machine` processes + `sim/engines/logstore`, with `logengine` as a thin bridge.

A technical follow-up then restored **IO** faults (`PutFail` / `PutLost` mirrored onto logstore; bridge boundary faults left live). That alone exposed a drain wedge:

> MaybeApplied commit Puts retried a **fresh** slot and never armed hole-fill → drain stuck → brokers only saw bridge timeouts under stacked faults.

Fixed in `cfca8b6` (`retrySameSlot`, commit retarget/fold-peek, mute/tomb paths). Operator scoped that follow-up to IO only: **keep CP `NoAttrition` and force `StallProb`/`CrashProb` off** until a dedicated stacked-fault pass.

Audit trail:

| when (UTC) | note |
|---|---|
| 2026-10-27T09:20:54Z | follow-up: restore Put/bridge; **keep NoAttrition/stall off**; bar = `make build` + kafka-log 300 + log mutants |
| 2026-10-27T10:28:30Z | DCO rewrite; tip `cfca8b6` |
| 2026-10-27T11:46:19Z | stack #2–#9 merged to `toys3`; attrition/stall remain deferred |

---

## 3. The three things (precise meanings)

They live at **two layers**. Do not conflate them.

### 3.1 Kernel attrition (`sim.Attrition` + `Process.NoAttrition`)

**What:** During `PhaseChaos`, after node Steps / engine landings, the kernel rolls `Attrition.CrashProb`. If it fires, it picks a live non-workload process and either:

- hard-crashes it (`end(..., "attrition")` → fact `sim_crashed` cause `attrition`), or  
- graceful-stops it (weight `GracefulWeight` vs `CrashWeight`).

`MaxDead` caps how many processes may be down/stopping (0 ⇒ attrition off). After a crash, restart is scheduled after `RecoveryDelay`. Process memory is discarded; durable effects already on engines remain.

**CP exclusion today:** kafka-log registers CP processes with `NoAttrition: true`:

```go
// sim/kafka/config.go (configLogWith)
cfg.Processes = append(cfg.Processes, sim.Process{
    Name: "cp", Count: 2, NoAttrition: true,
    FactoryFor: func(sc sim.SeedConfig) core.Factory { return cpFactory(sc, opts) },
})
```

Kernel skips those nodes:

```go
// sim/kernel.go attrition()
case n.workload, n.noAttrition:
```

Comment on the flag (`sim/roles.go`):

> NoAttrition excludes this process from kernel attrition. CP replicas use LogParams StallProb/CrashProb instead (kafka-log).

**Brokers** still participate in attrition (unless a seed swarms `Attrition.CrashProb` to 0). `faultCoverage` in `sim/kafka/invariants.go` still expects campaign-wide `Sometimes` hits for `sim_crashed`/`attrition` and `sim_exited` — those can be satisfied by **broker** attrition alone today.

**What “enable CP attrition” means:** set `NoAttrition: false` (or remove the flag) on the `cp` Process so `cp-0` / `cp-1` can be hard-killed or Stop’d like brokers. That is a **full process reboot** (new `machine.Machine` from factory), not the soft in-process crash below.

### 3.2 In-process stall (`StallProb` / `StallDur`)

**What (post–CP-DSM-8):** On each **role timer tick**, if this node is the ready **appender**, `maybeReplicaFault` may set `stallUntil = now + StallDur`. While `now < stallUntil`, `canCommit` is false and `roleTick` is a no-op — no Offer commits, no heartbeat/takeover progress from this node. Reachable: `logcp-sim: replica stalled`. Fact name via env: `logcp_stall`.

Implementation: `controlplane/machine/role.go` (`maybeReplicaFault`, `roleTick`, `canCommit`).

**Wired off in two places today:**

1. `sim/kafka/cp.go` — factory hard-codes zeros (does not even read swarmed LogParams):

```go
StallProb: 0, // TODO: re-enable with coordinated dual-replica faults
CrashProb: 0,
StallDur:  lp.StallDur,
DownDur:  lp.DownDur,
```

2. `sim/kafka/config.go` `swarmLog` — after scaling Put/Read knobs, forces:

```go
l.StallProb, l.CrashProb = 0, 0 // in-process stall/crash follow-up
```

Defaults that *would* swarm if those lines were removed: `logengine.DefaultParams` / `sim.LogParams` — `StallProb: 0.0002`, `StallDur: [800ms, 2500ms]` (see `sim/engines/logengine/engine.go` `DefaultParams`; params still live on `ControlPlane.Log`).

**Design constraint already encoded:** only the **active ready appender** may stall/crash cooperatively so a hot standby can take over (comment in `maybeReplicaFault`; old in-engine logengine never impaired both replicas at once).

### 3.3 In-process crash (`CrashProb` / `DownDur`)

**What:** Same gate as stall. `softCrash()`:

- marks `crashed`, demotes active, clears ready/booting toward restore  
- wipes queue, flights, waiters, answers, buf, pending, proj, hole-fill state  
- schedules `downUntil`; after that, `afterCrashRestart` (fold reset + checkpoint get or `afterRestore`)

Reachable: `logcp-sim: replica crashed` (and later restore / takeover paths).

This is **not** kernel attrition: the `cp-*` node stays alive; only the machine’s soft state is wiped. Durable logstore slots remain.

Default if re-enabled: `CrashProb: 0.0001`, `DownDur: [300ms, 1500ms]`.

---

## 4. Topology context (must understand before changing knobs)

```
broker CPCall --> logengine bridge (boundary faults: 503/timeout/lost/outage)
                     |
                  CPOffer to cp-0 / cp-1 (controlplane/machine)
                     |
                  Offer / fold / LogAppend|LogRead|LogTrim|Obj*
                     |
                  logstore engine (CPTruth) + objstore + netsim
                     |
                  CPReply --> bridge --> CPDone to broker
```

- Replica apply, roles, hole-fill, checkpoints: **only** in `controlplane/machine`.  
- `sim/engines/logengine` on kafka-log is a **bridge**, not the old monolithic replica engine.  
- Veritas remains the independent Kafka oracle (decision 21).  
- Docs: `docs/sim-log-engine.md` (post–CP-DSM-8 rewrite on `toys3`).

### Already-live stacked IO (do not regress)

After `cfca8b6`:

- `swarmLog` scales `PutFail` / `PutLost` / `ReadFail` and mirrors Put coins onto `LogStore.Unavailable503` / `LostResponse`.  
- Bridge CP boundary knobs are **not** zeroed.  
- `MaxAttempts = 16` for kafka-log.  
- Bridge `CallerTimeout = 1500ms`; logstore `CallerTimeout = 500ms` (must stay above ApplyLatency SlowMax ~200ms so a slow Put is not a false MaybeApplied, but below the bridge so PutFail can retry inside one CommitSegment).  
- Settling 25s / Settle 20s (extended for stacked retries).  
- K floored to `{2,4,8}` (K=1 + PutLost could stall every commit behind one lost Put for the whole bridge timeout).  
- Takeover floored to `{600,900,1500}ms` (above heartbeat so a healthy primary is not displaced).  
- Trim-heavy profile keeps more slots (`KeepSlots` 16/32) for PutFail lag.

### Critical machine fix already landed (do not undo)

In `controlplane/machine/commit.go` (`cfca8b6`):

- **NotApplied and MaybeApplied retry the same slot** (`retrySameSlot`), not `alloc()` a later slot. Abandoning the slot left a permanent hole (progress not armed) and wedged drain.  
- Commit Offer: fold hit answers immediately; in-flight/queued same `SegmentKey` **retargets** waiter Req/From so a bridge timeout cannot orphan the apply answer onto a dead broker ReqID (flight ReqIDs sorted for determinism).  
- Tomb / trimmed-slot path: `hitTrimmed` + restore.  
- Reachable: `logcp-sim: Put not answered, same slot repeated`.

If attrition/stall cause new drain wedges, **start by asking whether a slot was abandoned or a waiter was left pointed at a dead Req** — same failure class.

---

## 5. Current forced-off sites (checklist to flip)

| # | File | What to change |
|---|---|---|
| 1 | `sim/kafka/config.go` `configLogWith` | `NoAttrition: true` → `false` (or omit) when enabling CP process attrition |
| 2 | `sim/kafka/config.go` `swarmLog` | Remove `l.StallProb, l.CrashProb = 0, 0`; restore `scale(&l.StallProb)` / `scale(&l.CrashProb)` (or a safer menu) |
| 3 | `sim/kafka/cp.go` | Pass `StallProb: lp.StallProb`, `CrashProb: lp.CrashProb` instead of hardcoded 0; delete the TODO |
| 4 | `docs/sim-log-engine.md` | Update the “NoAttrition / StallProb forced off” paragraph when done |
| 5 | `sim/kafka/config.go` `logReachables` | Re-add stall/crash/takeover/restore props if the campaign must cover them again (see §7) |

Optional / product judgment:

- Whether brokers + CP may both be down under `MaxDead: 1` (today MaxDead=1 ⇒ at most one attrition victim at a time among eligible processes). Raising MaxDead or excluding brokers when CP attrition is on may be needed for liveness.  
- Whether kernel attrition on CP and in-process softCrash should both be on in the same campaign, or staged (soft first, then process kill).

---

## 6. Suggested execution order

Do **not** flip all three on day one. Prior art: Put coins alone found a wedge.

### Phase A — soft stall/crash only (keep `NoAttrition: true`)

1. Wire `cp.go` from `lp.StallProb` / `lp.CrashProb`.  
2. In `swarmLog`, stop zeroing; optionally start with a **reduced** menu (e.g. scale but cap lower than PutFail) before full `{0,0.5,1,2,4}` × defaults.  
3. Unit: extend `controlplane/machine` role tests for stall → takeover → failback and softCrash → restore (today `role.go` implements this; dedicated campaign-level coverage was thin after the cutover). Old in-engine references: `sim/engines/logengine` takeover/trim/crash tests (pre-bridge) and `TestDeterministic` with Stall/Crash probs.  
4. Bar: `make build`, `go test ./controlplane/machine/...`, kafka-log mutants, then `go run ./cmd/manisim -scenario kafka-log -seeds 300` (prefer `-j 1` once if a seed looks nondeterministic — parallel `-j` flakes were seen during the Put follow-up; confirm with `-j 1` then re-check `-j 8`).  
5. Expect reachables for `replica stalled` / `replica crashed` / takeover / restore to fire `Sometimes` across the campaign; add them back to `logReachables` / `ExpectReachable` as appropriate.

### Phase B — CP process attrition (`NoAttrition: false`)

1. Flip the Process flag.  
2. Reason about interaction with softCrash: process kill discards the machine entirely; in-flight LogAppends may still land on logstore (same class as old “Puts on the wire still land”). Bridge + broker retry must retarget or replay keys (fold window).  
3. Watch Settling liveness (`sim/kafka` liveness invariant / `MaxQuietLatency`): longer Settling/Settle may be needed again.  
4. Re-run the same bar; consider a longer campaign (e.g. 2000 seeds) if 300 is flaky only under attrition.  
5. Confirm `faultCoverage` still makes sense (broker and/or CP attrition).

### Phase C — docs + audit

- Update `docs/sim-log-engine.md`.  
- Append a row to `.audit/cp-dsm-stack.tsv` (or a new audit file) with tip SHA and bar results.  
- Close this follow-up.

---

## 7. Reachables / coverage gaps to restore

`configLogWith` currently appends a **short** `logReachables` list (hole, same-slot Put, window replay, trim). Older / fuller sets also expected:

- `logcp-sim: standby took over`  
- `logcp-sim: failback, the standby stepped down`  
- `logcp-sim: replica stalled`  
- `logcp-sim: replica crashed`  
- `logcp-sim: replica restored from a checkpoint`  
- `logcp-sim: idempotent replay after takeover`  
- `logcp-sim: replay of a key whose slot was trimmed`  
- …

`docs/sim-log-engine.md` still says coverage under attrition is part of CP-DSM-8 live lanes — that is **aspirational** until this follow-up lands. Align ExpectReachable with what the campaign can actually hit once knobs are on.

`machine.Config` comment: `Fault()` suppresses stall/crash after `EnterRecovery` — Settling should not keep injecting new cooperative faults; verify that still holds when probs are non-zero.

---

## 8. Known failure modes to watch

| Symptom | Likely cause | First places to look |
|---|---|---|
| Drain stuck; brokers only see bridge timeouts | Abandoned slot / hole never filled; waiter on dead ReqID | `commit.go` `onCommitAppendDone`, `retrySameSlot`, `retargetCommit`; hole-fill arming |
| Commit never applies after stall | Appender stalled; standby never promotes | `TakeoverAfter` vs heartbeat; `lastOtherAt` / `lastPullOK`; bridge failover hop |
| Double appender / split brain answers | Roles advisory only; both think active | decision 51; slot put as fence; failback on lower-rank writer |
| Nondeterministic trace hash | Unsorted map walk on retarget/refuse; Fault draw order | Sort ReqIDs (already done in retarget/refuse); swarm draw order in `swarm` then `swarmLog` |
| Settling liveness failures | Chaos faults still landing; CP down too long | Phases Settling/Settle lengths; MaxDead; RecoveryDelay / DownDur |
| False MaybeApplied under load | Logstore CallerTimeout ≤ slow ApplyLatency | Keep logstore timeout &lt; bridge and &gt; SlowMax (~200ms) |
| Campaign “green” but stall never fires | Still hardcoded 0 in `cp.go` or zeroed in `swarmLog` | §5 checklist — both sites must be live |

---

## 9. Acceptance bar (definition of done)

All of the following on a tip based on current `toys3`:

1. `make build` green.  
2. `go test ./controlplane/machine/...` green (add/extend stall & softCrash tests if missing).  
3. Log mutants: `go test ./sim/kafka/ -run LogMutant` (or project’s usual mutant target) green.  
4. `go run ./cmd/manisim -scenario kafka-log -seeds 300` — 0 failing (document `-j` used).  
5. Swarm can produce non-zero `StallProb`/`CrashProb` for some seeds; with Phase B, CP nodes can appear in `sim_crashed` / graceful stop facts.  
6. ExpectReachable / Sometimes coverage includes stall, crash, takeover, restore (as designed in §7).  
7. Docs (`sim-log-engine.md` + this file status) updated; no remaining `StallProb: 0 // TODO` in `cp.go`.  
8. Put/bridge coins remain live (do not re-zero `PutFail`/`PutLost` to hide failures).

Out of scope unless they block the bar: third replica, log-wide total outage, Dynamo throttling realism, exhausting the idempotency window in the default preset (see Limits in `sim-log-engine.md`).

---

## 10. Pointers (read these files)

| Path | Why |
|---|---|
| `sim/kafka/config.go` | `configLogWith`, `swarmLog`, timeouts, NoAttrition, reachables |
| `sim/kafka/cp.go` | Factory wiring; hardcoded Stall/Crash zeros |
| `controlplane/machine/role.go` | `maybeReplicaFault`, `softCrash`, takeover/failback |
| `controlplane/machine/commit.go` | same-slot retry, retarget, tomb |
| `controlplane/machine/config.go` | StallProb/CrashProb field docs |
| `sim/roles.go` | `Process.NoAttrition` |
| `sim/kernel.go` | `attrition()`, `end()`, restart scheduling |
| `sim/run.go` | `Attrition` struct |
| `sim/engine.go` | `LogParams` Stall/Crash/Put field docs |
| `sim/kafka/invariants.go` | `faultCoverage` |
| `docs/sim-log-engine.md` | Topology + deferred note |
| `.audit/cp-dsm-stack.tsv` | Program history |
| Commit `cfca8b6` | Put/bridge restore + explicit “Stall/Crash and NoAttrition stay deferred” |

---

## 11. Historical note (pre–CP-DSM-8)

Before the machine cutover, stall/crash lived inside monolithic `sim/engines/logengine` (`faults()` on tick, `stallUntil` / `crash` / `restart` on replicas). Unit tests such as `TestTakeoverReplayAndFailback` and `TestTrimWindowAndRestore` exercised that model. After CP-DSM-8 those semantics moved to `controlplane/machine`; the bridge `logengine` no longer owns replica stall/crash for kafka-log. Prefer machine-level tests and whole-system kafka-log campaigns over resurrecting the old engine tests as the source of truth.
