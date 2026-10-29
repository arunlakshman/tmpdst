# CRAP gate

`make build` ends with a gate that fails when a function is too "crappy". The
tool is `cmd/riskscore` (standard library only, per the dependency-audit policy,
decision I). The decision record is decision 40 in `decisions.md`.

## What CRAP is

CRAP (Change Risk Anti-Patterns) scores a function by how hard it is to change
safely, combining cyclomatic complexity and test coverage:

    CRAP(m) = comp(m)^2 * (1 - cov(m))^3 + comp(m)        cov in 0..1

- At 100% coverage CRAP equals the complexity. At 0% coverage it is
  `comp^2 + comp`.
- The commonly cited limit is CRAP > 30 = "crappy". Above complexity ~30 no
  amount of coverage gets under 30.
- Note the exponent: the published formula cubes `(1 - cov)`. Some summaries
  write it squared; the tool uses the original cubed form.

Sources: the formula and threshold are documented by the cargo-crap crate
(<https://docs.rs/crate/cargo-crap/0.4.2/source/src/score.rs>,
<https://docs.rs/crate/cargo-crap/0.4.2/source/README.md>), which cites Savoia &
Evans (2007), the original Crap4j / NDepend metric. These were checked by web
search on the day this was written; the primary Crap4j article was not fetched.

## Complexity rule

`1 + if + for + range + case clause (not default) + select comm clause (not
default) + && + ||`, counted over the whole declaration including nested
function literals (they belong to the enclosing function). This is the gocyclo
rule as we remember it, except that default clauses are not counted; numbers
may differ from gocyclo by the number of `default:` clauses (unverified against
the gocyclo source).

## Coverage rule

Input is a `go test -coverprofile` file (mode set, count or atomic). A profile
block belongs to a function when the block starts inside the function's line
range. Coverage = covered statements / total statements. A function with no
statements in a profiled file counts as 100%; a function in a file absent from
the profile (a package with no tests) counts as 0%.

Skipped tests: `go test ./...` includes Docker-backed tests that skip without
their environment. The gate measures "what plain `go test ./...` covers", which
is stable on any machine, so every backend-facing package has Docker-free tests:
cp/logcp runs against an in-memory fake DynamoDB (cp/fakedb, checked
against the log state machine by the cptest conformance suite), sys/s3store against a fake
S3, cp/cpclient and cp against httptest, the cmd mains through a testable `run`. To count integration
coverage, pass an extra profile and merge it (counts are summed per block):

    make crap CRAP_MERGE=build/it-cover.out

Generate the baseline under the same conditions the gate runs in. A baseline made
without Docker is the conservative choice: more coverage later only lowers
scores, and the gate never fails for improvements.

## Why 20 (not 30, and not 5 or 6)

With full coverage CRAP equals complexity, so a limit of 5-6 means every
function must have complexity <= 5-6 (the usual "simple" band is 1-10) and be
nearly fully covered. That is very strict and invites splitting functions purely
to satisfy the number. The published CRAP literature uses 30; this repo gates at
**20** as a tighter ratchet that still tolerates large type-switches with good
coverage. The threshold is a flag, so you can choose:

    make build CRAP_THRESHOLD=15
    go run ./cmd/riskscore -profile build/cover.out -threshold 10

Default threshold is **20** (tightened from the published 30). Counts on a
recent tree (strict, plain `go test ./...`): run `make crap-strict` for the
full list; expect 0 over 20 when the gate is green.

## Running

    make build          fmt, ascii, vet, lint, compile, unit test, sim, hermetic it, crap
    make test           unit packages only (excludes sim/ and it/)
    make sim            go test ./sim/...
    make it-hermetic    go test ./it/ (Docker fullstack Skips without endpoints)
    make crap           CRAP gate on merged unit+sim+it coverprofiles
    make crap-strict    ignore the baseline, print every function, fail on all offenders
    make crap-baseline  rewrite ops/crap-baseline.json (deliberate, reviewable)

Flags of `go run ./cmd/riskscore`: `-profile`, `-merge-profile a,b`,
`-threshold` (env `CRAP_THRESHOLD`, default 20), `-baseline`, `-update-baseline`,
`-strict`, `-json`, `-top N` (0 = all), `-tolerance` (default 0.5), `-exclude
REGEXP` (on the function key), `-root`. Exit codes: 0 ok, 1 gate failed, 2
usage or input error. Generated files, `_test.go`, `vendor`, `testdata`, hidden
directories and nested modules (toys3/minikv) are skipped. No functions are
excluded by default. The baseline is empty: the gate fails on any function over
the threshold.

The determinism gate stays separate. To run it in `make build`, add
`./scripts/ci-determinism.sh` as a step before `crap` in the `build` recipe.

## Reading the output

    CRAP    CC   COV%  FUNCTION                       LOCATION

`CC` is complexity, `FUNCTION` is `dir.Receiver.Name` (the baseline key, no line
number), sorted worst first. The summary line gives the counts over 30, 15, 10
and 6. On failure the offenders are listed as "over threshold" (new) or "got
worse" (baselined, CRAP grew beyond the tolerance).

## The ratchet

`ops/crap-baseline.json` (currently empty, 0 offenders) lists the functions over
the threshold with their CRAP when it was generated. The gate fails when:

1. a function not in the baseline exceeds the threshold, or
2. a baselined function's CRAP exceeds its baseline by more than the tolerance
   (0.5), for example it gained a branch, or lost coverage.

Improvements never fail; the output says how many baselined functions improved so
you can tighten the file. Renaming or moving a function changes its key, so it
counts as new. If the threshold of the run differs from the one stored in the
baseline, the output warns; regenerate it.

Regenerate deliberately, and commit the diff for review:

    make crap-baseline
    git diff ops/crap-baseline.json

A diff that only lowers numbers or removes entries is good; a diff that adds
entries or raises numbers is accepting new debt and needs a reason in the PR.
