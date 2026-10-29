# Indirect modules pulled in by Pebble (stage 4 audit)

Date: 2026-10-21. Auditor: stage-4 agent. Scope: the 16 modules that appeared in `go.mod` as `// indirect` when
`github.com/cockroachdb/pebble v1.1.5` (decision 49) was added. They are EDGE dependencies: only
`cp/applier/pebblekv` (and so `cmd/manicp`) imports Pebble. `internal/core/`, `internal/verify/simulator/` and every other deterministic package do not
reach them (`ops/scripts/ci-determinism.sh` is green), so none needs a per-module determinism record under
`docs/dep-audits/<module>@<version>.md`; this page is the summary the open item asked for. The Pebble record itself is
`github.com/cockroachdb/pebble@v1.1.5.md`.

## The 16 modules

"Linked" is from `go version -m` of the release build (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath ./cmd/manicp`,
the build `ops/deploy/aws/terraform/scripts/build-and-upload.sh` makes). None is linked into `manibroker`.

| module | version | license (module cache LICENSE) | size in cache / non-test go files | linked in release manicp |
|---|---|---|---|---|
| github.com/DataDog/zstd | v1.4.5 | BSD-2-Clause | 2.5 MB, bundles C zstd, cgo | NO (cgo only; a `CGO_ENABLED=1` dev build links it) |
| github.com/cockroachdb/errors | v1.11.3 | Apache-2.0 | 4.1 MB / 85 | yes |
| github.com/cockroachdb/fifo | 0.0.0-20240606204812 | Apache-2.0 | 68 KB / 4 | yes |
| github.com/cockroachdb/logtags | 0.0.0-20230118201751 | Apache-2.0 | 48 KB / 3 | yes |
| github.com/cockroachdb/redact | v1.1.5 | Apache-2.0 | 308 KB / 19 | yes |
| github.com/cockroachdb/tokenbucket | 0.0.0-20230807174530 | Apache-2.0 | 36 KB / 1 | yes |
| github.com/getsentry/sentry-go | v0.27.0 | MIT | 1.0 MB / 64 | yes |
| github.com/gogo/protobuf | v1.3.2 | BSD-3-Clause | 19 MB / 299 (large, generated code) | yes (5 packages, via cockroachdb/errors) |
| github.com/golang/snappy | v0.0.4 | BSD-3-Clause | 264 KB / 8 | yes |
| github.com/kr/pretty | v0.3.1 | MIT | 72 KB / 4 | yes |
| github.com/kr/text | v0.2.0 | MIT | 68 KB / 9 | yes |
| github.com/pkg/errors | v0.9.1 | BSD-2-Clause | 96 KB / 3 | yes |
| github.com/rogpeppe/go-internal | v1.10.0 | BSD-3-Clause | 1.2 MB / 75 | yes (a small part) |
| github.com/stretchr/testify | v1.10.0 | MIT | 796 KB / 28 | NO (test-only graph entry) |
| golang.org/x/exp | 0.0.0-20230626212559 | BSD-3-Clause | 2.1 MB / 84 | yes (2 packages) |
| golang.org/x/text | v0.34.0 | BSD-3-Clause | 30 MB / 280 | yes (cases, norm and friends, via sentry-go) |

All licenses are permissive (Apache-2.0, MIT, BSD). No module carries cgo except DataDog/zstd. gogo/protobuf and x/text are
large mostly because of generated tables; the linked code is a small part (the release binary is 28 MB in total).
`golang.org/x/mod` and `x/sync` only had version bumps.

## cockroachdb/errors and getsentry/sentry-go: really linked?

Yes, both, in the release `manicp`: `go list -deps ./cmd/manicp` shows `cockroachdb/errors` (19 packages) and
`getsentry/sentry-go` (6 packages). The chain is `cp/applier/pebblekv -> pebble -> cockroachdb/errors ->
withstack/report -> getsentry/sentry-go -> x/text`. Pebble uses `errors` for error wrapping and stack capture;
`errors` imports sentry-go only to format reports (`errors/report`, `withstack`). Nothing here sends anything: sentry-go
is a client library with no DSN configured, so it makes no network call and starts no goroutine unless something calls
`sentry.Init`, and our code never does. They are not in `manibroker`.

Recommendation: keep both. They cannot be dropped without forking Pebble (the import is inside Pebble), and the cost is
about 1 MB of binary and some supply-chain surface. Do not use them directly from our code. If the surface matters later,
the options are (a) a `replace` of `sentry-go` with a stub module (unsupported by cockroachdb and fragile) or (b) the
bbolt alternative recorded in `docs/applier-bakeoff.md`; neither is worth it now.

## Known advisories

`govulncheck@latest` (it needs Go 1.26, so the Go tool fetched go1.26.10 to run it; nothing was added to this repo's
`go.mod`) was run in binary mode on the release `manicp` built with the LOCAL toolchain, go1.24.0:

- 43 findings are reached, from the standard library of go1.24.0 (net/http, crypto/x509, crypto/tls, ...) plus one module.
  All stdlib ones are fixed in later 1.24.x/1.25 releases: building the release binary with a current patch release
  removes them. ACTION FOR STAGE 5: pin the build toolchain to the newest patch release (the benchmark host runs the
  build of the operator's machine) and rerun `govulncheck`.
- One module finding is reached: `golang.org/x/text v0.34.0`, GO-2026-5970 (infinite loop on invalid input in
  `norm.Form.Properties`), fixed in x/text v0.39.0. It is reached only through sentry-go's rate-limit header parsing,
  which manicp never runs on untrusted input, so exploitability is nil in practice. Bumping `x/text` to v0.39.0 is a
  go.mod change outside this stage's rule; it is left for the product owner (it would also lift the `go` directive
  requirements if x/text needs a newer Go; not checked).
- 9 more vulnerabilities in imported packages and 15 in required modules are not called by manicp (govulncheck's
  own classification).

## Follow-up 2026-10-21: x/text v0.39.0 attempted, NOT applied; release toolchain

- `go get golang.org/x/text@v0.39.0` was tried and reverted. x/text v0.39.0 declares `go 1.25.0`, and it forces
  x/mod v0.37.0, x/sync v0.21.0, x/sys v0.46.0, x/tools v0.47.0 (all `go 1.25.0`), so go.mod's `go 1.24.0` would become
  `go 1.25.0`. That is a minimum-version change, left to the product owner. No x/text release newer than v0.34.0 that
  fixes GO-2026-5970 is below go 1.25 (v0.35 to v0.38 not checked individually; the fix is first in v0.39.0 per the advisory).
- Release toolchain: the newest 1.24 patch, go1.24.13, still shows 25 stdlib findings (1.24 is end of life; fixes are
  only in 1.25.13 and 1.26.9). Building the same code with `GOTOOLCHAIN=go1.26.10` (go.mod unchanged, still `go 1.24.0`)
  and `govulncheck -mode=binary` on linux/amd64 and linux/arm64 gives: manibroker, no vulnerabilities; manicp, one
  finding, GO-2026-5970 (x/text v0.34.0, see above). `ops/deploy/aws/terraform/scripts/build-and-upload.sh` now defaults
  `GOTOOLCHAIN` to go1.26.10 (override with the environment).

## Recommendation

Accept all 16 as edge dependencies, record no per-module determinism audits (they are outside the closure), and keep
`detcheck`'s allow-list unchanged. Do not import any of them from `internal/core/` or `internal/verify/simulator/`. Re-run this page's commands when
Pebble is bumped.

## Evidence

    go list -deps ./cmd/manicp | grep -E 'cockroachdb|sentry|gogo|snappy|x/text'
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/manicp-linux ./cmd/manicp && go version -m /tmp/manicp-linux
    go run golang.org/x/vuln/cmd/govulncheck@latest -mode=binary /tmp/manicp-linux
    go mod why golang.org/x/text/unicode/norm
