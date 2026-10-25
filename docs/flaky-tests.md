# Flaky tests and unstable test environments

Found on 2026-10-25 by looping the suite (`go test -count=1 -race ./it ./cp/logcp/... ./cp/logsm ./cp/cptest ./cmd/manicp`, about 10 rounds
over 25 minutes) against the Docker backends of `scripts/it-up.sh`. A single plain `go test` run did not show either problem. A
loop under `-race` is the way to find these; run it before trusting a green result for anything that touches `cp/logcp` or `it/`.

## Flaky test: `TestMetricsAreExported` (`cp/logcp/service_test.go`)

- **Symptom:** `manicp_log_tail_reads_total missing`, in about 1 of several hundred runs under `-race` (`go test -count=400 -race -run TestMetricsAreExported ./cp/logcp`).
- **Cause:** a timing race in the test, not a product bug. The test commits once, does one Lookup and gathers the registry at once. `tail_reads_total` is a counter vector (label `result`) and has no series until the background tailing loop completes its first read, so on a fast run the metric is not registered yet.
- **Not new:** it fails the same way on commit `353ae9b`, before the removal of the transaction store (decision 54).
- **Fix when scheduled:** poll for the series for a bounded time (like the `b.Blocked()` wait in `TestTrimmedNodeStops`) before asserting, or pre-create the `ok` and `error` series in `NewMetrics`.
- **Status:** open, not fixed.

## Unstable environment: ExtendDB in Docker (`it/` tests)

- **Symptom:** `TestFullStackRoundTrip`, `TestFullStackKillBroker` and `TestFullStackCompression` fail in setup with `CreateTable ... exceeded maximum number of attempts, 3 ... StatusCode: 500, InternalServerError` (sometimes "Internal error during authentication"). No test code of ours has run yet when this happens.
- **Cause:** the emulator, not the repo. After roughly 3 to 15 minutes of repeated table create/delete cycles the `deploy-extenddb-1` container logs `no such table: tables`, `gsi_pending` and `metrics` (its own SQLite catalog) and every `CreateTable` returns 500. `docker restart` does not clear it; `scripts/it-down.sh` (which removes the volumes) followed by `scripts/it-up.sh` does.
- **Impact:** only the `it/` full-stack tests; the log tests over `fakedb` (`cp/logcp`, `cp/logsm`, `cp/cptest`, `cmd/manicp`) never failed in the loop.
- **Workaround:** run the `it/` suite once per fresh `it-up.sh`, and for long loops recreate the containers between rounds (`it-down.sh && it-up.sh`).
- **Open question:** whether AWS's own DynamoDB Local behaves the same; not tried.

## What was not flaky

Under the same loop, `cp/logsm`, `cp/cptest`, `cmd/manicp` and `it/` (with a healthy emulator) passed every round, with no data races and no panics.
