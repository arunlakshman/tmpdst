# github.com/cockroachdb/pebble@v1.1.5

- Date / auditor: 2026-10-20, Claude (Sonnet 5.5). `ops/scripts/audit-dep.sh github.com/cockroachdb/pebble@v1.1.5
  github.com/cockroachdb/pebble` ran to the end (network available); detcheck exited 1, as expected for a storage engine.
- Requires Go: 1.22.0
- Verdict: **REJECTED for the deterministic packages** (`internal/core/`, `internal/verify/sim/`, `internal/verify/veritas/`, ...); **ACCEPTED at the edge**
  (`cp/applier/pebblekv` only, decision 49).
- Import paths allowed (ops/tools/determinismcheck/allow.txt): **none**. Deliberately not added.

## Findings
| Finding (file:line, from the script's detcheck report) | Matters? | Why |
|---|---|---|
| `go` statements and channels in `table_cache.go:375`, `table_stats.go:46`, `compaction.go`, `db.go` (background flush, compaction, table stats) | Yes | Goroutine scheduling is nondeterministic. A deterministic package must never import it; the applier runs it only in the I/O shell. |
| Imports of `os`, `sync`, `sync/atomic`, `time`, `runtime`, `runtime/pprof`, `unsafe`; package-level mutable vars (`table_cache.go:29-51`); range over maps (`version_set.go:302,600,619`) | Yes | Same reason. None of it can influence the answers: a given sequence of `Set`/`Delete` in one batch and a read of an iterator return the same bytes in key order whatever the compaction timing. |
| Transitive modules new to go.mod: `cockroachdb/{errors,fifo,logtags,redact,tokenbucket}`, `getsentry/sentry-go`, `gogo/protobuf`, `golang/snappy`, `kr/{pretty,text}`, `pkg/errors`, `rogpeppe/go-internal`, `stretchr/testify`, `golang.org/x/{exp,text}`, `DataDog/zstd` | No | Outside the determinism closure. `DataDog/zstd` is cgo but `CGO_ENABLED=0 go build ./...` still passes (checked). |
| Side effect on go.mod: `golang.org/x/tools` 0.31.0 to 0.41.0, `x/mod` 0.24 to 0.32, `x/sync` 0.12 to 0.19 (pulled by pebble's requirements) | Watch | `ops/tools/determinismcheck` and `cmd/riskscore` use `x/tools`. `ops/scripts/ci-determinism.sh` and the CRAP gate pass after the bump. |

## Rules for our code
1. Import it only from `cp/applier/pebblekv`. The applier talks to the `applier.KV` seam, never to Pebble.
2. Commits use `pebble.NoSync`; the store is never trusted after a crash. A restart restores a checkpoint into an
   EMPTY directory and replays the log (decision 49). Do not reuse a directory across incarnations.
3. A Pebble iterator's key/value bytes are valid only until the next call: the seam copies what it returns
   (`Get`, `Floor`) and the applier copies strings out of `Ascend` callbacks before returning.
4. Pebble's logger is silenced (`quiet`); store errors surface through `View`/`Update` and the applier fails stop.

## Evidence
- `ops/scripts/ci-determinism.sh` passes with the module in go.mod: it is not in the closure of the deterministic
  packages, so no record was required; this one documents why it stays outside.
- Bake-off (docs/applier-bakeoff.md): same workload, same applier, bbolt vs Pebble, 3.3x the apply rate.
