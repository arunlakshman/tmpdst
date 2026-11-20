# Determinism

MDS keeps a sans-I/O core so the same seed always replays the same run. That is
how bugs found in simulation stay fixed bugs.

## Why

Product logic under test is typed `machine.Machine` values: one input in, outputs
out, no real network, disk, wall clock, or goroutines. The `dst` kernel owns the
seeded RNG, virtual clock, faults, assertions, and trace hash. Same seed, same
schedule and fingerprint.

Essentials (from DST practice):

- **One RNG** per run; no `math/rand` or wall-time seeds inside the subject.
- **Virtual time** only via the scheduler heap; no `time.Now` in core machines.
- **Issue-time faults** latched per site so drops/crashes replay stably.
- **Phases**: fault window, heal, quiet/drain, then check.
- **Record and continue**: assertions log failures and keep going; fail at the end.
- **No map-range control flow** in deterministic packages (Go randomizes map order).

## Allow-list and audit records

Deterministic packages may import only what their module allow-list permits.
Third-party modules that those packages really use need a reviewed audit file.
CI fails when a record is missing, so a version bump forces a re-audit.

Paths are fixed for CI (do not rename without updating the scripts):

| Module | Allow-list | Audit records | CI script |
|--------|------------|---------------|-----------|
| broker | `ops/broker/determinism-allow.txt` | `docs/broker/dep-audits/` | `ops/broker/scripts/ci-determinism.sh` |
| controlplane | `ops/controlplane/determinism-allow.txt` | `docs/controlplane/dep-audits/` | `ops/controlplane/scripts/ci-determinism.sh` |
| dst | `ops/dst/determinism-allow.txt` | (dst allow-list only) | `ops/dst/scripts/ci-determinism.sh` |

Tool: `dst/cmd/determinismcheck`. New broker dependency: follow
`ops/broker/scripts/audit-dep.sh` and `docs/broker/dep-audits/README.md` (same
idea for controlplane).

Root `make determinism` runs the gates. They are also part of module `make build`
where wired.

## Simulation campaigns

- Control plane: `make -C controlplane sim-campaign` (or `go run ./cmd/cpsim ...`).
  Scenarios, seeds, reports, and planted bugs: still documented at
  [docs/controlplane/simulation.md](../controlplane/simulation.md).
- Broker: module `make sim` / `cmd/mdssimulator` (see `broker/README.md`).
- Library examples: `dst/examples/kvsim` via the `dst` campaign tool.

A failed seed is a command you can re-run until the bug is understood. Golden /
trace hashes that move are intentional behaviour changes and must be re-pinned
on purpose.

## Further reading

- Public DST guide: [docs/reference/dst.md](../reference/dst.md)
- Library README: `dst/README.md`
- Longer lessons (historical): `docs/dst/lessons.md`, `docs/broker/lessons.md`
