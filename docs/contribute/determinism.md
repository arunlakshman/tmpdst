# Determinism and dst

MDS keeps a sans-I/O core so the same seed always replays the same run. Product
logic under test is typed `machine.Machine`: one input in, outputs out, no real
network, disk, wall clock, or goroutines. The `dst` kernel owns the seeded RNG,
virtual clock, faults, assertions, and trace hash.

Module: `github.com/manifesterio/mds/dst` (folder `dst/`). Package tables and
how to write a machine: `dst/README.md`. Examples: `dst/examples/` (`kv`,
`kvchaos`, `kvsim`).

## Why / rules of thumb

- **One RNG** per run; no `math/rand` or wall-time seeds inside the subject.
- **Virtual time** only via the scheduler heap; no `time.Now` in core machines.
- **Issue-time faults** latched per site so drops/crashes replay stably.
- **Phases**: fault window, heal, quiet/drain, then check.
- **Record and continue**: assertions log failures and keep going; fail at the end.
- **No map-range control flow** in deterministic packages (Go randomizes map order).

## dst pieces (names)

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

A **route** turns each machine output into a kernel call. A **service** is an
outside system; `Handle` draws randomness at issue time and answers later with
`Deliver`. **Lockstep** compares a fault-free spec to the subject. **Campaigns**
run many seeds and report replay commands for failures.

```bash
# from dst/
go run ./examples/kvsim -seeds 200 -report
go run ./examples/kvsim -scenario kvchaos -seed 7
MDS_CAMPAIGN=30s go run ./examples/kvsim
```

## Allow-list and audit records

Deterministic packages may import only what their module allow-list permits.
Third-party modules those packages really use need a reviewed audit file. CI
fails when a record is missing, so a version bump forces a re-audit.

| Module | Allow-list | Audit records | CI script |
|--------|------------|---------------|-----------|
| broker | `ops/broker/determinism-allow.txt` | `docs/broker/dep-audits/` | `ops/broker/scripts/ci-determinism.sh` |
| controlplane | `ops/controlplane/determinism-allow.txt` | `docs/controlplane/dep-audits/` | `ops/controlplane/scripts/ci-determinism.sh` |
| dst | `ops/dst/determinism-allow.txt` | (allow-list only) | `ops/dst/scripts/ci-determinism.sh` |

Tool: `dst/cmd/determinismcheck`. New broker dependency: `ops/broker/scripts/audit-dep.sh`
and `docs/broker/dep-audits/README.md` (same idea for controlplane).

Root `make determinism` runs the gates (also part of module `make build` where wired).

## Simulation campaigns

- Control plane: `make -C controlplane sim-campaign` (or `go run ./cmd/cpsim ...`).
- Broker: module `make sim` / `cmd/mdssimulator` (see `broker/README.md`).
- Library examples: `dst/examples/kvsim`.

A failed seed is a command you can re-run. Golden / trace hashes that move are
intentional behaviour changes and must be re-pinned on purpose.
