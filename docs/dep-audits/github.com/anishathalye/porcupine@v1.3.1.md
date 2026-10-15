# github.com/anishathalye/porcupine@v1.3.1

- Date: 2026-10-15. Audited by a Claude subagent with `scripts/audit-dep.sh`.
  Source read in full (`checker.go`, `model.go`, `porcupine.go`,
  `visualization.go`, `bitset.go`); the module cache copy is identical to
  tag v1.3.1 of the upstream repository. MIT licence, no dependencies.
- Requires Go: 1.22.0 (the repo is on 1.24; no toolchain change).
- Verdict: **ACCEPTED WITH RULES, TEST-ONLY**. It is a linearizability
  checker that runs partitions on goroutines with a wall-clock timeout, so it
  must never be imported by non-test code of a deterministic package. Its
  verdict (Ok/Illegal) is deterministic; only Unknown (timeout) and the
  visualization's ordering are not.
- Import paths allowed (tools/detcheck/allow.txt): **none**. It is imported
  only from `toys3/sim/porcupine_test.go` (conformance check C5). detcheck
  does not check `_test.go` files and `scripts/ci-determinism.sh` lists
  non-test dependencies only, so no allow-list entry is needed, and adding
  one would let non-test code import it.

## Findings

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `checker.go:370-416` `checkParallel`: one goroutine per partition, results over a channel, `select` against the timeout | Not for the verdict | Each partition is checked by `checkSingle`, a sequential WGL search with no shared state; the result is the AND of the partitions, independent of finishing order. On Illegal (without `Verbose`) the other goroutines are cancelled; that changes only work done. |
| `checker.go:387-390` `time.After(timeout)` | **Yes** | Wall-clock: whether a check returns Unknown depends on machine speed and load. Never use it where a result must replay. |
| `checker.go:376`, `model.go:4` `context` | No | Cancellation only. `checkSingle` polls `ctx.Err()` once per search step; a slow `Step` delays cancellation (our model polls it too). On timeout `checkParallel` returns without waiting for its goroutines; they stop at their next poll. |
| `checker.go:64` range over map (`PartialLinearizationsOperations`) | No | Builds a map keyed by op id; order cannot escape. |
| `checker.go:430` range over map (set of partial linearizations) | Visualization only | The order of the partial linearizations in `LinearizationInfo` varies run to run; `visualization.go:179` then sorts them by length with the unstable `sort.Slice`. The HTML can differ between runs; the verdict cannot. |
| `checker.go:260` `cache map[uint64][]cacheEntry` | No | Lookups only (memo of `(linearized set, state)`), keyed by `Model.Hash` and confirmed with `Model.Equal`. |
| `model.go:189-208, 255` `NondeterministicModel.ToModel` merges states with pairwise `Equal` | Performance | Quadratic in the number of possible states; it ignores `Hash`. C5 builds its own power set with hash deduplication instead (see `pcModel`). |
| `visualization.go:124` range over map | No | Collects timestamps, then sorts them. |
| `visualization.go:259` `VisualizePath`: `os.Create` | No | Writes the HTML file it is given. Test-only, on a disagreement. |
| `visualization.go:268-269` `//go:embed` package-level `visualizationFS` | No | Read-only embedded assets, never written. |
| No `unsafe`, `reflect`, `sync`, randomness or `net` | — | `grep` and detcheck agree. |
| Hidden defaults | No | Nothing is filled in: no timestamps, ids or random choices. Ties in `Call`/`Return` order calls before returns (closed intervals, `checker.go:117`); C5's timestamps are the oracle's unique `InvSeq`/`RetSeq`, so only the returns of ops abandoned by one crash share a value. |

## Rules for our code

1. **Test-only.** Import it only from `_test.go` files. It is not on the
   detcheck allow-list and must not be added.
2. **Unknown is not a verdict.** Report timeouts separately; never treat
   Unknown as Ok in a check whose result must be reproducible, and never fold
   it into a golden file.
3. **The model must be pure** (porcupine's own contract): steps return new
   states, never mutate. C5's states are immutable maps.
4. **Poll the context in slow steps** (`Model.StepContext`), or a timeout
   leaves a goroutine spinning until the step returns.

## Evidence

- `scripts/audit-dep.sh github.com/anishathalye/porcupine@v1.3.1`: imports
  `context embed encoding/json fmt io os sort strings time`; no third-party
  packages; detcheck findings are exactly the rows above (imports, the three
  map ranges, the goroutine/channel/select block, the embed variable).
- Verdicts repeat: 396 DST histories (seeds 1..200, correct server and
  mutant g, 3s timeout, the 4 Unknowns excluded) each checked twice gave the
  same Ok/Illegal verdict both times.
- TestGolden, which pins traces and stats for 550 seeds, is unchanged by
  adding C5: nothing in a run calls porcupine.
