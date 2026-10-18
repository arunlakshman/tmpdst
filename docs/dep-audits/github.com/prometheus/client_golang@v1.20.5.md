# github.com/prometheus/client_golang@v1.20.5

- Date / auditor: 2026-10-18, Claude (Sonnet 5.5). `scripts/audit-dep.sh` could not finish: its throwaway module
  has no go.sum entries for the transitive modules, so it stopped at "missing go.sum entry". The imports it listed
  before stopping were read by hand.
- Requires Go: 1.22.0
- Verdict: **REJECTED for the deterministic packages**; **ACCEPTED at the edge** (`sys/prod` only).
- Import paths allowed (tools/detcheck/allow.txt): **none**. Deliberately not added.

## Findings
| Finding | Matters? | Why |
|---|---|---|
| `prometheus` imports `time`, `sync`, `sync/atomic`, `os`, `runtime`, `runtime/debug`, `runtime/metrics`, `expvar`, `encoding/json` | Yes | All off the allow-list. Collectors are global-mutable, lock-based and read the clock and the process. They cannot live in `core/`, `broker/` or `sim/`. |
| `promhttp` serves HTTP (`net/http`) | Yes | Network I/O, edge only. |
| Transitive modules: `beorn7/perks`, `cespare/xxhash/v2`, `munnerz/goautoneg`, `prometheus/client_model`, `prometheus/common`, `prometheus/procfs`, `google.golang.org/protobuf` | No | Pulled in for exposition (protobuf model, procfs for the process collector). Outside the determinism closure; `scripts/ci-determinism.sh` does not list them. |

## Rules for our code
1. Import it only from `sys/prod` and `cmd/` (the non-deterministic edge). The broker core emits facts
   (decision 36); `prod.Metrics` turns them into collectors.
2. No allow-list entry. If a deterministic package ever needs metrics, it emits a fact instead.

## Evidence
- `scripts/ci-determinism.sh` passes with the module in go.mod: the module is not in the closure of the
  deterministic packages, so no record was required; this one documents why it is still excluded.
