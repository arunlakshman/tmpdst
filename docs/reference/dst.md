# dst

dst is the deterministic simulation library used by MDS (and usable on its own).
A product writes **machines**: state machines that take one input and return
outputs, and do no IO. The **kernel** runs many machines in one goroutine, with
one seeded RNG, virtual time, fault injection, assertions, and a trace hash per
run. The same machine can run in production via the `live` package.

Module: `github.com/manifesterio/mds/dst` (folder `dst/` in this repo).

For package tables, allow-list rules, and how to write a machine, start at
`dst/README.md`. Working examples live under `dst/examples/` (`kv`, `kvchaos`,
`kvsim`). Contributor policy (why, CI gates, campaigns):
[docs/contribute/determinism.md](../contribute/determinism.md).

## The idea

Nothing real runs in a simulation. There is one random stream, one fake clock,
and one queue of events. Each run uses one **seed**. The same seed always gives
the same run, so a bug a seed finds can be replayed.

Two kinds of parts:

- **machines** — hold state; `Step(now, in) []Out`; never do IO, never read the
  real clock, never start goroutines.
- **the world (kernel)** — runs machines, delivers messages, fires timers,
  injects faults.

## Core pieces (names)

| Part | Package | Main names |
|------|---------|------------|
| machine | `machine` | `Machine`, `Env`, `Now`, `Duration` |
| world and routing | `kernel` | `New`, `Spawn`, `AddService`, `Send`, `Call`, `Router` |
| lanes and phases | `kernel` | `NewLanes`, `SendOn`, `SchedulePhases` |
| production loop | `live` | `Run` (not in the determinism gate) |
| spec checks | `check/spectest` | `Spec`, `Lockstep`, `Bug`, `RequireCaught`, `RequireClean` |
| faulty service | `check/faulty` | `New`, `Faults`, `Shape` |
| campaign | `campaign` | `Registry`, `Scenario`, `Config`, `Main` |
| assertions and faults | `assert`, `fault` | `NewAlways`, `NewSometimes`, `NewReachable`, `NewSite` |

A **route** function turns each machine output into a kernel call (typed
`Send` / `Call` / timers). A **service** is an outside system (object store,
shared log); `Handle` draws randomness at issue time and answers later with
`Deliver`. A run has phases: faults on, heal, drain.

## Specs, bugs, campaigns

- **Lockstep** (`check/spectest`): random ops on a fault-free **spec** and on
  the **subject**; compare answers; shrink on mismatch.
- **Planted bugs**: every bug must be caught within a seed budget
  (`RequireCaught`); clean runs must stay clean (`RequireClean`).
- **Campaign**: many seeds of named scenarios; report failed seeds with replay
  commands, never-reached assertions, never-fired fault sites.

Example campaign driver: `dst/examples/kvsim`.

```bash
go run ./examples/kvsim -seeds 200 -report
go run ./examples/kvsim -scenario kvchaos -seed 7
MDS_CAMPAIGN=30s go run ./examples/kvsim
```

(Run from `dst/`.)

## Longer guide

Cut-down walkthroughs of machines, routing, services, lanes, phases, and
Lockstep are in the historical page `docs/dst/v2.md`. Prefer the compiling
examples under `dst/examples/` over those snippets.
