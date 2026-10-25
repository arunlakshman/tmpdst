# Lint baseline (golangci-lint v2.15.0, .golangci.yml)

`make lint` runs on ALL code and is part of `make build`. There is no baseline
exclusion, so `make build` fails until the counts below reach 0. Counts are at
the time the linter was added (uncapped run).

| Linter | Count | Meaning | Refactor task |
|---|---|---|---|
| errcheck | 160 | unchecked error returns | #18 |
| revive | 156 | exported docs/naming, var-naming, use-any (`interface{}`), error-strings, etc. | #8 exported, #6 var-naming, #4 any |
| usestdlibvars | 30 | literals that should be net/http constants (e.g. "GET") | none (new) |
| errorlint | 18 | `==`/type-assert on errors, `%v` instead of `%w` | #16 |
| staticcheck | 12 | correctness/deprecation findings | none (new) |
| gocritic | 9 | style/perf checks | none (new) |
| nakedret | 5 | naked returns in long funcs | none (new) |
| copyloopvar | 1 | redundant loop-var copy (Go 1.22+) | none (new) |
| govet, nolintlint | 0 | | |

gofmt (simplify): 0 diffs. `time.Now` in core/ sim/ is not linted here;
tools/detcheck owns determinism. Re-count: `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0 ./...`.
`make test-race` exists separately: `-race` over all tests exceeded 10 min, too slow for `build`.
