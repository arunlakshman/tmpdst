# One machine contract: dst sim.Process

Note (after the greenfield refactor): this file is history. The migration it
plans is done. Since then `controlplaneclient`, `controlplanetest`, `runloop` and
`system/run.go` are deleted, and the packages `kv`, `metadata`, `groups` and
`topics` became `metadb/engine`, `metadb`, `domain/rebalance` and `domain/topic`
and `domain/group`. See `internal/controlplane/doc.go` for the tree as it is.

Status (updated after the build): the table below is what was built, with these
differences from the first proposal.

- `Gen` stayed in `TimerFired`: the replica hash folds the generations, so removing
  them would have moved every golden hash. dst replaces a timer with the same ID,
  and the replica still ignores a stale generation.
- `Event` is gone. `Replica` is a `sim.Process`; `Step(cx, inbox)` reads the time
  from `cx.Now()`.
- Pause and resume stayed in the simulator (one user).
- `live` is in `internal/system/live`, not in dst.
- A request from gRPC is an inbox value: `ControlPlaneDeliver` for a command, and
  the new `LocalRead` for a read that answers at once from local state (it does not
  wait for a fresh tail, so production reads keep today's behaviour). The answer is
  an outbound `ControlPlaneReply`. `Offer` and `TakeAnswers` are no longer on the
  server path. Brokers, group role and status still use the inject path.
- `wireboundary` is gone: the handlers convert the generated messages themselves.
  Making the domain use the proto types directly is a later phase.

Original proposal follows.

Goal: the replica becomes a `dst/sim.Process`. It does no real IO. It gets one
inbox value per `Step` and returns outbound values. A runner does the work: in
tests and the simulator it is dst's `sim.Runner`; in production it is `live`.
This replaces `internal/runtime` (machine part), `internal/controlplane/runloop`
and `internal/system/run.go`.

## What dst gives today (read from dst/sim and dst/eng)

- `Process.Step(cx, inbox) []any`. `cx` has Now (mono ns), Rand, Faults, Assert.
- Control values a process may return: `Send`, `SetTimer`, `CancelTimer`,
  `Crash` (itself), `Restart{Who, Boot}`.
- Any other outbound value goes to an `eng.Engine` (`Handle`). An engine can
  `Deliver` a value to a process after a delay. This is where the fake log and
  the fake object store will live.
- Same-time events: the scheduler is a heap with a sequence number, so events
  at the same time run in the order they were queued (first in, first out).
- `SetTimer` with a used ID replaces the old timer (a counter per ID).
- `Crash` only stops Step; the Process object and its timers stay. `Spawn` with
  the same ID replaces the object and clears the crash.

## The table

| Today (runtime) | In dst | Decision |
|---|---|---|
| `Event{At, Wall, Body}` | `Step(cx, inbox)` | Only Body is the inbox value. At = `cx.Now()`. Wall = new `cx.Wall()` (dst change A). |
| `NodeStarted{Self}` | first inbox value | Keep the product type. The runner sends it as `Restart.Boot` or at spawn. |
| `StopRequest{Deadline}` | inbox value | Keep the product type. `live` makes it when shutdown starts (config gives the maker). |
| `TimerFired{ID, Gen}` | `SetTimer{ID, After, Body}` | `Body` is what comes back. `Gen` is not needed: dst replaces a timer with the same ID. Step 2 keeps `TimerFired` as Body. Step 4 removes `Gen`. |
| `SetTimer`, `CancelTimer` (runtime) | `sim.SetTimer`, `sim.CancelTimer` | Direct. `After` is `Duration` ns, same unit. |
| `IOCall` (Log*, Object*) | outbound domain value | Keep the types. The sim engine and the production executor handle them. |
| `IOResult` (`*Done`) | inbox value | Keep the types. The engine delivers them with `Deliver`. |
| `ErrCode`, `Outcome` in results | stay in the result values | They are product words (Applied / MaybeApplied). dst does not need them. |
| `RequestID` | plain `uint64` in the value | Keep. The replica still numbers its calls. |
| `ControlPlaneDeliver{From, Req, Op}` | inbox value | Keep. Production hands it in through `live.Send` (any goroutine). |
| `ControlPlaneReply{To, Done}` | outbound value | Keep. Handled by the engine (sim) or by a reply sink (live). |
| `Machine.StateHash()` | `Process.StateHash()` | Same method. dst hashes it after each step. |
| `Environment.Rand` | `cx.Rand()` | `NewID` = 16 bytes from `cx.Rand()`. Production: `live` seeds Rand from the OS once at start. |
| `Environment.FaultInjector` | `cx.Faults()` | Same three calls. Production: injector is Quiet. |
| `Environment.Assertions` | `cx.Assert()` | Names map to `assert.Property` and `assert.Pair`. Production: the log writes to our logger and metrics. |
| `Clock.Mono / Wall` | `cx.Now() / cx.Wall()` | Only `live` and the adapters read real time. |
| `Clock.AfterFunc` | `SetTimer` | The machine never calls it. Only `runloop` did. |
| `MachineFactory(env, self)` | `func(self) sim.Process` | Used by the runner to build a new incarnation. |

## What does not map, and what we do

1. **Facts / RecordFact.** Nobody emits a fact. Nobody reads one (the
   simulator ignores them; production passes no handler). DELETE in the cheap
   cuts, before step 2.
2. **Control-first inbox picker.** Every event is classified Control
   (`InboxKindOf`) and nothing sends Data, so the picker does nothing today.
   DROP. `live` keeps one FIFO inbox plus one urgent lane for the stop request,
   so a full inbox can not block shutdown (the runloop tests cover this).
3. **StopForRestart{Cause}.** dst `Crash` has no cause. Add `Cause string`
   (dst change B, backward compatible). Simulator: crash, then restart after a
   random delay. `live`: end the loop with `ErrStoppedForRestart`.
4. **ShutdownDone.** Add `sim.Exit{}` (dst change C): the process ended on
   purpose. Simulator: crash and no restart. `live`: return nil.
5. **Incarnation.** A reply or a timer for an old boot must not reach a new
   boot. Today `simNode` checks `Inc`. dst keeps timers across `Spawn`, and
   `Deliver` has no boot number. Needed (dst change D): `Restart` carries a new
   Process object, clears the old timers, and drops deliveries made for an
   older incarnation.
6. **Crash of another node.** The nemesis must crash a node. `Crash` only hits
   the issuer. Add `Crash.Who` (empty = self). (Part of change B.)
7. **Pause / resume (a frozen node).** The simulator holds events while a node
   is paused. dst has no pause. Add `Pause{Who}` and `Resume{Who}` (dst change
   E) or keep it in the engine layer. To decide with you.
8. **Per-step observer.** The simulator checks the epoch holder and the
   per-slot hash after every step, and keeps a ring of the last 48 steps.
   Add `Config.OnStep(who, inbox, out)` (dst change F).
9. **Inject / AfterStep / TakeAnswers (gRPC on the loop goroutine).** The
   server calls `Offer` and `TakeAnswers` on the machine from the loop. In the
   Process model a request is an inbox value (`ControlPlaneDeliver`) and the
   answer is an outbound `ControlPlaneReply`. `TakeAnswers` then goes away.
   This is the biggest change in step 4 (replica commit path).
10. **Golden hashes.** dst hashes (time, id, StateHash, len(out)) after each
    step; the simulator hashes in `observe`. The hash WILL move in step 2.

## Where does `live` go?

In this repo: `internal/system/live` (no control-plane types in its API). Why:
dst stays small and fully deterministic (rule from the `fakes` decision: no API
with one user). Only `live` and the production adapters read real time, so
`live` is NOT on the determinism allow-list; the gate fails if a gated package
imports it. When the broker adopts dst, `live` moves to `dst/live` unchanged.

`live` owns: the goroutine, the inbox, real timers, the urgent stop lane,
StopGrace and the kill, `ErrStoppedForRestart`, `ErrStopped`, `ErrKilled`.

## Cheap cuts: status

- `Facts` / `RecordFact` / `Field` (about 70 lines with the loops): dead. Cut.
- `controlplaneclient` (1235 lines) and `wireboundary` (797 lines) are NOT
  dead. `controlplaneclient` is used by `cmd/mdscontrolplane` tests and
  `controlplanetest`; `wireboundary` is used by `server` and the client.
  Not cut without your decision.
- `system.Run` + `system/inbox.go` duplicate `runloop`. Delete in step 5.

## dst changes (small commits, new dst branch)

A `Wall()` in Context. B `Crash{Who, Cause}`. C `Exit{}`. D `Restart` with a
new Process and incarnation drop. E `Pause` / `Resume` (open). F `OnStep` hook.
