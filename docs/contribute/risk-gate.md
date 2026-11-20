# Risk gate

`make build` in the broker module ends with a change-risk score gate. The tool is
`ops/tools/riskscore` (standard library only). It fails the build when a function
is too hard to change safely.

## Formula

Score combines cyclomatic complexity and test coverage (Savoia/Evans / CRAP;
the tool cubes `(1 - cov)`):

```
risk(m) = comp(m)^2 * (1 - cov(m))^3 + comp(m)        cov in 0..1
```

At 100% coverage the score equals complexity. At 0% it is `comp^2 + comp`.

## Threshold

Default threshold is **20** (stricter than the common literature limit of 30).
Override:

```sh
make build RISK_THRESHOLD=15
go run ../ops/tools/riskscore -profile build/cover.out -threshold 10
```

With full coverage, score equals complexity, so a very low threshold forces
tiny functions for the number's sake. 20 still allows large type-switches when
coverage is good.

## What it checks

- **Complexity**: `1 + if + for + range + case` (not default) `+ select` comm
  clause (not default) `+ && + ||`, including nested function literals.
- **Coverage**: from `go test -coverprofile`. Blocks that start inside a
  function's line range count. No statements in a profiled file => 100%; file
  absent from the profile => 0%.
- **Scope**: generated files, `_test.go`, `vendor`, `testdata`, hidden dirs, and
  nested modules (their own `go.mod`) are skipped. No functions are excluded by
  default.

The gate measures what plain `go test ./...` covers (Docker-backed tests that
skip without their env are not required). Backend packages keep Docker-free
fakes so the score is stable on any machine. To fold IT coverage:

```sh
make risk RISK_MERGE=build/it-cover.out
```

## Running

```sh
make risk           # gate on merged unit+sim+it coverprofiles
make risk-strict    # ignore baseline; print every function; fail on all offenders
make risk-baseline  # rewrite ops/broker/risk-baseline.json (review the diff)
```

Output columns: `RISK`, `CC`, `COV%`, `FUNCTION` (`dir.Receiver.Name`), location.
On failure: "over threshold" (new) or "got worse" (baselined score grew past
tolerance, default 0.5).

## Baseline ratchet

`ops/broker/risk-baseline.json` lists known offenders. The gate fails when:

1. a function not in the baseline exceeds the threshold, or
2. a baselined function's score exceeds its baseline by more than the tolerance.

Improvements never fail. Renaming a function changes its key (counts as new).
Commit baseline diffs deliberately; adding or raising entries needs a PR reason.

The determinism gate is separate; see [determinism.md](determinism.md).
