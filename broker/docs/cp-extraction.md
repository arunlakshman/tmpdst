# Control plane extraction

The in-tree `internal/controlplane` package has been deleted from this repo.
Production control plane now lives in the sibling repo **mds-controlplane**.

## What stays here

- **Client:** `controlplane/client` (module controlplane) — the gRPC client; the broker plugs it in as `system.ControlPlane`.
- **Protos:** `api/controlplane` — vendored from mds-controlplane with a
  broker-owned `go_package` (`github.com/manifesterio/mds/api/controlplane/...`).
  Regenerate with `make proto`. Sync from the CP repo when the wire API changes.
- **Sim CP:** `fakecontrolplane` for the non-log `kafka` scenario (speccheck-backed).
- **E2E:** `ops/scripts/e2e-kafka-cli.sh` builds `mdscontrolplane` from
  `../mds-controlplane` (or `MDS_CONTROLPLANE_DIR`).

## Wire API renames (vs old in-tree protos)

| Old RPC | New RPC |
|---------|---------|
| `Lookup` | `FindSegments` |
| `GroupHeartbeat` | `Heartbeat` |
| `GroupOffsetCommit` | `OffsetCommit` |
| `GroupOffsetFetch` | `OffsetFetch` |

Shared messages live in `controlplane.shared.v1`.

## Phase plan

### 1. Cleanup (this change) — done

Delete in-tree CP server, keep a client generated from the new protos, keep
`make build` compiling.

### 2. Reconnect broker ↔ live CP (next)

- Run hermetic / fullstack IT against a real `mdscontrolplane` process (or
  sibling binary), not in-process replica.
- Restore mid-produce kill / failover coverage that used `controlplanetest`.
- Decide whether kafka-log sim stays in mds-controlplane only, or this repo
  dials an external CP process under the sim (unlikely; prefer CP-owned sim).
- Keep proto copies in sync (script or buf dep once CP publishes a module).

### 3. Integrate `manifesterio/dst` (after reconnect)

CP already pins public `github.com/manifesterio/dst`. This broker repo still
has its own rng/sched/campaign/mutant copies under `internal/verify/simulator`.

Suggested order (same as the Nov 2026 executive report #1–#3 for the broker):

1. Replace duplicated mechanisms with `dst` imports.
2. Wire shared `campaign` / `mutant` helpers.
3. Drop local copies once hashes are re-baselined if needed.

Typed `dst/machine` and virtual Clock stay later platform bets.

### 4. Product TODOs (later; not blocked on dst)

From `docs/TODO.md`: Fetch MinBytes/MaxWait, Lookup/S3 caches, consume-stage
benchmarks, S3 index for long retention.

## Deferred until reconnect

- **kafka-log / kafka-log-freeze** simulator scenarios (replica + fakelogstore).
- In-process IT that spun up CP via `controlplanetest` / replica / dynamolog.
- `make it-postgres` CP cases (they live in mds-controlplane now).

Until reconnect, those tests skip with a clear message, and
`mdssimulator -scenario kafka-log` exits explaining the extract.
