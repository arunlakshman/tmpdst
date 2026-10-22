# Chapter 12. Newcomer playbook. Your first week, first PR, and where to change what

Run the tasks in order. Read background in [Chapter 3](03-the-deterministic-core.md), [Chapter 6](06-control-plane.md), [Chapter 8](08-deterministic-simulation.md), and [Chapter 9](09-verification-and-quality-gates.md). Follow the source when a command in this chapter disagrees with the source. Fix this chapter.

## Check the prerequisites

Check each need before you start work.

| Need | Why | Check |
|---|---|---|
| Go 1.24 | `go.mod` says `go 1.24.0` (decision J says franz-go `kmsg` v1.14.0 requires it) | `go version` |
| Docker (with Compose) | Only for the Docker-backed integration tests in `it/` | `docker compose version` |
| AWS credentials | Only for the AWS benchmark under `deploy/aws/` and real DynamoDB and S3 stores | Read `docs/aws-benchmark-runbook.md` |

1. Run `go version`.
2. Check the output names Go 1.24.
3. If you want the Docker-backed integration tests, run `docker compose version`.
4. Check the command prints a version.
5. If you want the AWS benchmark, read `docs/aws-benchmark-runbook.md`.

Run the normal loop with Go alone. Skip Docker and AWS for the normal loop.

## Plan the first week

Complete one task on each day.

1. On day 1, run the build gate, the broker, and a simulator campaign.
2. On day 2, read `loop.md`, `core/event.go`, `core/action.go`, `core/machine.go`, and `broker/broker.go`.
3. Trace one produce from `BytesReceived` to the `produce_acked` fact.
4. On day 3, replay a failing seed.
5. Pick a `broker.Bug*` mutant in `broker/config.go`.
6. Find the test that catches the mutant.
7. Check `TestConformanceBrokerCatchesMutants` in `broker/conformance_test.go` and the mutants in `sim/kafka/`.
8. On day 4, pick a first issue.
9. Merge the issue with the smallest test that proves the change.
10. On day 5, ship the first PR.
11. If the change touched a contract, write or update one `docs/decisions.md` entry.

## Run the build gate

Run the full gate before you ship a change.

```sh
make build          # fmt-check + vet + compile + test + crap (the full gate)
make fmt-check      # fail if gofmt -l . lists anything
make vet            # go vet ./...
make compile        # go build ./...
make test           # go test -coverprofile=build/cover.out ./...
make crap           # CRAP gate alone (runs make test first if no profile)
make crap-strict    # full offender list, ignoring crap-baseline.json
./scripts/ci-determinism.sh   # detcheck + dep audits + determinism canary
```

1. Run `make build`.
2. Check the command exits with code 0.
3. Run `./scripts/ci-determinism.sh` after `make build`.
4. Check the script prints `determinism gate passed`.

Read [Chapter 9](09-verification-and-quality-gates.md) for the gate design. The determinism gate runs apart from `make build`. The comment at the top of the `Makefile` states that split. `make build` ends with `crap` in `cmd/crapscore`.

Choose an option from the list below.

- If you want a threshold of 15, run `make build CRAP_THRESHOLD=15`.
- If you want extra coverage in the CRAP input, run `make crap CRAP_MERGE=build/it-cover.out`.
- Read `docs/crap-gate.md` for both settings.

Warning. Do not run `make crap-baseline` to turn a red gate green. The note in `crap-baseline.json` calls a rewrite of `crap-baseline.json` "deliberate, reviewable".

1. Format each file before you commit the file.
2. Run `gofmt -w <files you touched>`.
3. Run `make fmt-check`.
4. Check `make fmt-check` exits with code 0.

## Run the broker locally

Run the control plane, one broker, and one benchmark client. Read [Chapter 6](06-control-plane.md) for the control plane design.

Terminal 1 runs the control plane with the in-memory store. The `-store` default is `mem`. `cmd/manicp/main.go` defines the flags.

```sh
go run ./cmd/manicp                 # listens on :8080, metrics on :9101
curl http://127.0.0.1:8080/healthz  # -> ok
```

1. Open terminal 1.
2. Run `go run ./cmd/manicp`.
3. Run `curl http://127.0.0.1:8080/healthz`.
4. Check the output is `ok`.

Terminal 2 runs one broker with the in-process object store. The `-objects` default is `s3`. The `mem` value selects a single-broker store. That store loses data on exit. `cmd/manibroker/main.go` defines the option.

```sh
go run ./cmd/manibroker -objects mem   # Kafka on 127.0.0.1:9092, metrics :9100
```

1. Open terminal 2.
2. Run `go run ./cmd/manibroker -objects mem`.
3. Check the broker listens on `127.0.0.1:9092`.

Terminal 3 creates a topic through the HTTP API of the control plane. `cp/handler.go` serves `POST /v1/create-topic`. `CreateTopicReq` in `cp/wire.go` defines the JSON fields. The benchmark client then loads the topic. The header of `cmd/manibench/main.go` gives its usage.

```sh
curl -X POST http://127.0.0.1:8080/v1/create-topic \
  -d '{"topic":"orders","partitions":3}'
go run ./cmd/manibench benchmark-producer -topic orders \
  -brokers 127.0.0.1:9092 -duration 10s
```

1. Open terminal 3.
2. Run the `curl` command.
3. Check the command exits with code 0.
4. Run the `manibench` command.
5. Check the client produces to `orders` for 10s.
6. Stop both processes with Ctrl-C or SIGTERM.
7. Check the broker finishes work in flight within `-stop-grace`.
8. Note the `-stop-grace` default is `10s`.

Choose a store from the list below.

- If you want the log-backed control plane, set `manicp -store` to `dynamolog`.
- If you want the transaction store, set `manicp -store` to `dynamo`.
- Keep `mem` for the local run.

`manicp -store` also accepts `dynamo` and `dynamolog`. The `dynamolog` value selects the log-backed control plane from decisions 48-53. `cmd/manicp/logstore.go` defines its extra flags. Read [Chapter 6](06-control-plane.md) for the store design.

Choose an object store from the list below.

- If you want a single local broker, keep `-objects mem`.
- If you want S3, set `-objects s3` and set `-s3-endpoint/-s3-bucket/-s3-region/-s3-path-style/-s3-create-bucket`.

1. Run `go run ./cmd/manicp -h`.
2. Check the `flag` package prints the flags for `manicp`.
3. Run `go run ./cmd/manibroker -h`.
4. Check the `flag` package prints the flags for `manibroker`.

## Run a simulator campaign

Run the default campaign first. Read [Chapter 8](08-deterministic-simulation.md) for the simulator design.

```sh
go run ./cmd/manisim                       # 100 kafka seeds from seed 1 (-seeds, -start)
go run ./cmd/manisim -v                    # one line per seed: steps/rand/hash/crashes/exits/failures
go run ./cmd/manisim -seed 42              # replay one seed, full step log printed
go run ./cmd/manisim -scenario echo -seeds 20   # smoke scenario; also kafka-log
go run ./cmd/manisim -j 4 -seeds 1000      # shard seeds over 4 jobs (-j defaults to NumCPU)
```

1. Run `go run ./cmd/manisim`.
2. Check the command prints the seed summary line.
3. Check the command exits with code 0.
4. If you want one line per seed, run `go run ./cmd/manisim -v`.
5. If you want the smoke scenario, run `go run ./cmd/manisim -scenario echo -seeds 20`.
6. If you want four jobs, run `go run ./cmd/manisim -j 4 -seeds 1000`.

`cmd/manisim/main.go` defines the `kafka`, `kafka-log`, and `echo` scenarios. Exit code 1 means one of these conditions:

- A seed failed.
- The run violated an Always property.
- The run found nondeterminism.
- A campaign ran at least 100 seeds (`coverageMin`) and never satisfied a Sometimes property.

## Run the integration tests

Run the hermetic test first. Run the full-stack test after the hermetic test passes.

```sh
go test ./it/ -run TestProduceConsumeRoundTrip -count=1 -v   # hermetic, no Docker
./scripts/it-up.sh        # docker compose up (deploy/docker-compose.yml); PRINTS export lines
# copy/paste the two printed lines into your shell:
export MANIFESTER_DYNAMO_ENDPOINT=http://127.0.0.1:18080
export MANIFESTER_S3_ENDPOINT=http://127.0.0.1:8333
go test ./it/ -run TestFullStackRoundTrip -count=1 -v        # now runs instead of skipping
./scripts/it-down.sh      # docker compose down -v
```

1. Run `go test ./it/ -run TestProduceConsumeRoundTrip -count=1 -v`.
2. Check the test passes.
3. Note the hermetic test needs no Docker.
4. Note the hermetic test uses in-process brokers over `cp/memstore`.
5. Note the hermetic test uses an in-memory object store.

If you want the full-stack test, start the Docker backends.

Warning. `scripts/it-up.sh` prints the `export` lines. The script does not set the lines in your shell.

1. Run `./scripts/it-up.sh`.
2. Copy the two printed lines into your shell.
3. Run the two printed lines in your shell.
4. Run `go test ./it/ -run TestFullStackRoundTrip -count=1 -v`.
5. Check the test runs and passes.
6. Check the test does not skip.
7. Run `./scripts/it-down.sh`.
8. Check the command exits with code 0.

Ports bind to loopback only. ExtendDB and DynamoDB use port 18080. SeaweedFS S3 uses port 8333. The full-stack tests in `it/fullstack_test.go` skip when their endpoint variables are unset. The hermetic tests in `it/hermetic_test.go` use in-process brokers over `cp/memstore`. Read `docs/crap-gate.md` for the CRAP gate and Docker-free coverage. Read [Chapter 9](09-verification-and-quality-gates.md) for the test design.

## Replay a failing seed

Copy the repro line from the failure output. Read [Chapter 8](08-deterministic-simulation.md) for replay.

1. Find the repro line in the failure output.
2. Note `manisim` prints `repro: go run ./cmd/manisim -scenario kafka -seed <n>` under each failure.
3. Note `manisim` prints the same line for nondeterminism.
4. Run the repro line.
5. Check replay prints the per-step log with `Step`, `At`, and `Text`.
6. Check replay prints the seed summary.
7. Note the seed summary holds `steps/rand/hash/crashes/trips/exits`.
8. Check replay prints each `FAIL <kind> (<prop>) step=… at=…: <msg>` line after the summary.
9. Note the kinds are `violation`, `inconclusive`, `protocol`, `panic`, `max_steps`, and `nondeterminism` in `cmd/manisim/main.go`.
10. Find the failing step number in the output.
11. Read the fact timeline at that step.
12. Find the `Step(Event)` that went wrong.
13. Run the neighbouring seeds with `-start <n-2> -seeds 5 -v`.
14. Check whether the bug affects one seed or several seeds.

If two runs of one seed diverge, treat the divergence as nondeterminism.

1. Run `TestDeterminismCanary` in `sim/kernel_test.go`.
2. Run `TestDeterminismCanary` in `sim/kafka/kafka_test.go`.
3. Run `TestDeterminismCanary` in `sim/echo/echo_test.go`.
4. Look for a banned construct from the detcheck task below.
5. Look for an uncounted source of randomness.
6. Read lesson 3 in `docs/lessons.md`.
7. Note lesson 3 requires one counted RNG stream with `Calls()` and `Derive(label)`.

## Read a trace

Compare hashes before you compare logs. Read [Chapter 8](08-deterministic-simulation.md) for the timeline.

1. Run `go run ./cmd/manisim -v`.
2. Find `hash=%016x` on one seed line.
3. Note that value is the trace hash.
4. Check equal seeds produce equal hashes.

Use the timeline from the list below.

- Call `Snapshot(name)` to return all facts with one name.
- Call `Since(name, &cursor)` to stream facts with one name.

The sim records facts on the append-only timeline in `sim/timeline.go`. Invariants read the timeline after each step. The sim emits an ack fact in the same `Step` as the ack. It emits the fact before the `Send` that acks.

Warning. Do not rename a fact without care.

`core/fact.go` defines a fact as `core.Fact{Name, Fields}`. Fact names use frozen past-tense constants such as `produce_acked` and `segment_committed`. The name and the field encoding feed the trace hash. A rename re-baselines every golden hash. Decision 12 covers that rule. Read [Chapter 3](03-the-deterministic-core.md) for facts.

## Fix a detcheck failure

Run `./scripts/ci-determinism.sh` first. Read step 1 of the script output. Read [Chapter 9](09-verification-and-quality-gates.md) for the gate.

The gate runs `go run ./tools/detcheck` over the deterministic packages. The gate fails on these constructs:

- An import that does not appear on the exact-path allow-list in `tools/detcheck/allow.txt`.
- A `go` statement.
- Channels or `select`.
- `range` over a map.
- Reflect map iteration.
- A package-level variable.

1. Read the diagnostic as written.
2. Note the diagnostic names the construct and the file.
3. Match the fix to the construct.
4. Iterate sorted keys in place of ranging over a map.
5. Return an `Action` in place of spawning a goroutine.
6. Move wall-clock input and random input behind `core.Env`.
7. Note each event carries its own `At` and `Wall`.
8. Note the core has no `Now()`.
9. Read decision 2 for that rule.

Warning. Do not append a path to `allow.txt` to make CI pass.

If you prove a finding harmless, suppress the finding on one line.

1. Add `//detcheck:ok <reason>` on that line.
2. Write the reason.
3. Check detcheck accepts the line.
4. Note detcheck rejects an empty reason.

If the fix needs a new import, treat the import as a dependency change.

1. Follow the dependency recipe below.
2. Read the audit skill.
3. Note the audit skill says "Never weaken tools/detcheck…".
4. Run `./scripts/ci-determinism.sh`.
5. Check the script prints `determinism gate passed`.

## Fix a CRAP failure

Run `make build` first. Find the CRAP failure at the last step. Read [Chapter 9](09-verification-and-quality-gates.md) for the gate.

1. Run `make test` to refresh `build/cover.out`.
2. Run `make crap` after `make test`.
3. Check `make crap` names the offender.
4. Run `make crap-strict` to show every offender without the baseline.
5. Read the score with the formula in `docs/crap-gate.md`.
6. Note the formula is `CRAP = comp² × (1 − cov)³ + comp`.
7. Note the gate fails above 30 by default.
8. Lower one factor.
9. Split a function to lower complexity.
10. Add a test for the missing case to raise coverage.
11. Write the test for the behaviour.
12. Check `make crap` passes.

Warning. Do not split a function for the number alone. `docs/crap-gate.md` warns against a split that serves the number alone.

Warning. Run `make crap-baseline` after a reviewer agrees to the change. The command exists for a reviewed re-baselining.

1. After a reviewer agrees, run `make crap-baseline`.
2. Check `make build` passes.

## Fix a conformance failure

Find the layer that failed first. Read [Chapter 9](09-verification-and-quality-gates.md) for the checks.

The conformance checks compare code against the `veritas/` executable spec in lockstep. Decision 21 covers that design. C1 checks the real broker against the spec. C2 checks the sim engines against the spec. C3 checks the `sys` adapters against the spec. C4 checks the oracle against the spec.

Choose the command for the layer that failed.

- If the broker failed, run `go test ./broker/ -run TestConformanceBroker -count=1`.
- If the spec failed, run `go test ./veritas/`.
- If the sim world failed, run `go test ./sim/kafka/ -run TestDeterminismCanary`.
- If the sim world failed, run the `c4_*` tests in `sim/kafka/`.

1. Run the command for the failed layer.
2. Check the command names the failing case.
3. Read the shrunk case first.
4. Note the lockstep runner in `veritas/veritastest` shrinks a failing op sequence.
5. Note the original campaign gives less signal than the shrunk case.
6. Decide which side has the bug.
7. Note the spec in `veritas/` is the reference for the checks.
8. Note the spec contains executable code and can contain a bug.
9. If the spec is wrong, fix the spec.
10. Say the spec fix in the PR.
11. Log a decision if the fix moved a contract.

Check the checker after you fix the bug.

1. Run `TestControlPlaneCheckShrinksPlantedBug`.
2. Run `TestConformanceBrokerCatchesMutants`.
3. Check each test passes.
4. Note a change that lets a mutant survive points at the bug in your change.

## Follow a change recipe

Pick the row for your change. Touch the files in that row. Add the test in that row.

| Change X | Touch Y | Add test Z |
|---|---|---|
| Add a Kafka API | Add the API to `kwire/supported.go`. `Supported()` supplies the data for ApiVersions. Add the dispatch case in `broker/ops.go` (`dispatch`). Add the handler in `broker/produce.go`, `broker/fetch.go`, or `broker/metadata.go`. Add the codec in `kwire/codec.go`. | Add a round-trip case in `kwire/codec_test.go`. Add a C1 case in `broker/conformance_test.go`. Add a hermetic case in `it/hermetic_test.go`. Add sim support in `sim/kafka/` if the workload must speak the API. |
| Add a config knob | Add the field and its doc comment in `broker/config.go` (`Config`, `DefaultConfig()`, `withDefaults()`). Add the flag in `cmd/manibroker/main.go` (`parseFlags`). Put tuning in `cmd/manibroker/tuning.go`. Add the value to `manifestTuning` if the runbook reads it. | Add coverage in `cmd/manibroker/tuning_test.go` or `cmd/manibroker/units_test.go`. Add a `broker` test. That test must prove the knob changes behaviour. A parse test alone does not prove it. |
| Add an event or action | Add the type to the closed sets in `core/event.go` and `core/action.go` (`isEvent()` and `isAction()`). Keep the `ReqID()` pairing between `IOAction` and `Completion`. Add handling in the `Step` method in `core/machine.go`. Add execution in `sys/` for production. Add execution in the `sim/` engine that runs the type. | Add a protocol test in `sim/kernel_test.go`. An unknown-ReqID completion must change nothing. Re-baseline golden trace hashes as the point of the change. Decision 11 makes hashed enums append-only. |
| Add a CP op | Add the contract to `core/controlplane.go` (`CPOp` and `CPResp`). Add the wire types in `cp/wire.go`. Add the route in `cp/handler.go`. Add the method to the `cp.Store` interface in `cp/store.go`. Add that method to `cp/memstore`, `cp/dynamo`, and `cp/logcp`. Follow the OpenAPI steps in the next row. | Add the lockstep case in `cp/store_test.go` across the stores. Add the spec tests in `api/openapi/spec_test.go`. Run them with `go test ./api/...`. |
| Add a CP op spec | Edit `api/openapi/controlplane.json`. That file is the source of truth. Add the wire type to `wireTypes` in `api/openapi/spec_test.go`. Regenerate with `go run ./api/openapi/cmd/specgen`. Check the result with `…/specgen -check`. | Add an `x-replay` example for the op. The spec test replays each `x-replay` example against `cp.NewHandler` over `cp/memstore`. An op without an example has no spec-test coverage. |
| Add a dependency | Follow `.claude/skills/audit-go-dependency/SKILL.md`. Run `scripts/audit-dep.sh <module>@<version>`. Write `docs/dep-audits/<module>@<version>.md` with the template in `docs/dep-audits/README.md`. Add the exact path to `tools/detcheck/allow.txt`. Run `go get` after those steps. | Run `./scripts/ci-determinism.sh`. It must pass. The script fails on a module in the dep tree of a deterministic package when that module lacks an audit record for the exact `go.mod` version. |

Apply two rules to every recipe.

- Never let a broker propose offsets.
- Keep commits idempotent per `SegmentKey` under decisions 42-44.

If a recipe seems to need an exception, add a decision row first.

Note the system rejects idempotent producers under decision 41. Note `manibench` disables idempotence for the same reason. Read [Chapter 3](03-the-deterministic-core.md) for events and actions. Read [Chapter 6](06-control-plane.md) for CP ops.

After you add a dependency, run the check below.

1. Run `./scripts/ci-determinism.sh`.
2. Check the script prints `determinism gate passed`.

After you add a CP op spec, run the check below.

1. Run `go run ./api/openapi/cmd/specgen`.
2. Check the generator exits with code 0.
3. Run the generator check with `…/specgen -check`.
4. Check the check exits with code 0.

## Check a deterministic package

Check the package set before you change code in a deterministic package. Read [Chapter 3](03-the-deterministic-core.md) and [Chapter 9](09-verification-and-quality-gates.md).

`scripts/ci-determinism.sh` gates a fixed package set. The script header gives the exact list. The set covers `core/`, `sim/`, `format/`, `kwire`, `broker/`, and `veritas` without its `vsys` adapters. The set also covers `veritas/veritastest` and the derived `toys3` set from the script. These rules apply in those packages:

- Decision I sets an allow-list in place of a ban-list. `tools/detcheck/allow.txt` names the exact imports. The stdlib set covers `bytes`, `encoding/binary`, `errors`, `fmt`, `hash/crc32`, `math`, `math/bits`, `slices`, `sort`, `strconv`, `strings`, and `unicode/utf8`. The list also names `manifester/…` paths and the audited `github.com/twmb/franz-go/pkg/kmsg` path.
- An absent import fails the gate in practice. The absent names are `time`, `math/rand`, `crypto/rand`, `os`, `net`, `sync`, `runtime`, `unsafe`, `reflect`, `maps`, `hash/maphash`, `context`, `io/fs`, and `syscall`. The constructs in the detcheck task fail the gate too. `kgo` appears in `it/` and `cmd/manibench`. It does not appear in a deterministic package.
- Events and actions hold plain data. Byte slices stay immutable after a send under `core/doc.go`. Randomness and time enter through `Env` and the stamps on each event.

1. Run `./scripts/ci-determinism.sh` after you change a deterministic package.
2. Check the script prints `determinism gate passed`.

## Format the code

Format the code before you commit the code.

1. Run `gofmt -w <files you touched>`.
2. Run `make fmt-check`.
3. Check `make fmt-check` exits with code 0.

`gofmt` sets the format. `make fmt-check` runs as step 1 of the build, and a failure stops the build. CRAP gates complexity under the CRAP task. Taste does not gate complexity. Comments in this repo explain why a choice exists. They cite decisions. Reviewers reject a comment that restates the code.

## Add a decision row

Add a numbered row in `docs/decisions.md` after one of these changes:

- You change a contract in `core/` or the CP wire format.
- You rename or add a fact.
- You change a store layout on disk or on the wire.
- You move a limit or a default that clients depend on.
- You accept a dependency.
- You supersede an earlier decision. State the supersession in the row. Decision 41 supersedes part of decision 28.

1. Write one row.
2. Give the reason for the change.
3. Give the evidence for the change.
4. Check the row names the reason, the evidence, and the change.

If you ask "is this big enough for a decision?", the answer is yes.

## Pick a first issue

Pick small and safe work for a first PR.

- Add a missing hermetic `it/` case for an existing API.
- Add a `veritas/` spec test for an edge case that the lockstep runner generates.
- Add coverage that lowers a baselined CRAP score.
- Add an example in `api/openapi`.
- Add a cross-reference in `docs/decisions.md`.
- Add a `kwire` codec round-trip case.

1. Pick one item from the list.
2. Open the recipe for that item.
3. Check the recipe names the files and the test.

## Pair with a reviewer before you touch a dangerous area

Pair with a reviewer before you touch a dangerous area. Add a decision row before that work.

- Offset assignment and commit idempotency in `cp/` and `core/controlplane.go`.
- The scheduler and inbox priority policy in `sim/` and `core/policy.go`. Metastability risk sits in that policy. `docs/flow-control.md` covers that risk.
- The `format/` segment layout. That layout sets the bytes on disk.
- Flush caps and segment caps in `broker/config.go`. Those caps take their sizes from CP limits. Read the long comment in that file before you change a cap.
- An edit to `tools/detcheck/allow.txt`, `crap-baseline.json`, or a golden trace hash. Make that edit the point of the PR. Do not make that edit as a side effect of other work.

1. Name the dangerous area in the PR.
2. Name the reviewer in the PR.
3. Add the decision row.
4. Check the PR holds the area, the reviewer, and the row.

## Ship the first PR

Ship one idea in one PR. Read [Chapter 9](09-verification-and-quality-gates.md) for the gates.

1. Create a branch.
2. Keep the diff to one idea.
3. Note this repo expects small PRs.
4. Note the commit history shows single-concern docs commits and code commits.
5. Add or update the test in the same package as the change.
6. Find the test in the recipe for your change.
7. Format the files you touched with `gofmt -w`.
8. Run `make build`.
9. Check `make build` exits with code 0.
10. Run `./scripts/ci-determinism.sh` if you touched a deterministic package.
11. Run the script if you are unsure.
12. Check the script prints `determinism gate passed`.
13. Note CI uses the same gate.
14. Add a `docs/decisions.md` row if you changed a contract.
15. State what changed in the PR description.
16. State which test proves the change.
17. State which seeds you ran for a change that touches the sim.
18. Note the output line from `go run ./cmd/manisim -seeds 100` gives that seed evidence.
19. Check the PR description holds the change, the test, and the seed evidence.

Prepare one review answer before you open the PR.

Warning. An answer that cites passing tests does not cover the review question.

1. Expect a review question about regression coverage.
2. Note the question is "what catches this if it regresses?"
3. Prepare an answer that names a mutant, a conformance case, or a canary.
4. Check the answer names one of those three items.

## Complete the graduation checklist

Check each item. Review PRs in an area after you check each item in this list.

- [ ] `make build` and `./scripts/ci-determinism.sh` pass on my machine. I can explain what each step gates.
- [ ] I ran `manicp` and `manibroker -objects mem` on my machine. I created a topic through `/v1/create-topic`. I produced into that topic with `manibench`.
- [ ] I replayed a failing seed from its printed repro line. I found the failing `Step` from the step number.
- [ ] I can read a `manisim -v` line with steps, rand, hash, crashes, and exits. I can say what a hash change means.
- [ ] I fixed one detcheck failure, one CRAP failure, and one conformance failure on a branch. Each failure came from the repo or from a planted bug.
- [ ] I shipped one PR from the safe list with its test. I know which recipe my next change follows.
- [ ] I added or updated a `docs/decisions.md` row. I can defend its wording.
- [ ] I know which dangerous areas I should not touch alone yet.

## Use the glossary

- Action. Output of `Step` in `core/action.go`. Each `IOAction` carries a `ReqID` and gets one `Completion`.
- Event. Input to `Step` in `core/event.go`. The sim stamps each event with node-local `At` and wall `Wall` on dequeue.
- Broker and `manibroker`. Kafka state machine in `broker/` as a pure `core.Machine`. Production binary in `cmd/manibroker`. Binary serves Kafka on `:9092`.
- Buggify. Seeded fault injection through `Env`. Production runs leave `Env` inert. `Sometimes` assertions pair with `Env` under lesson 7.
- Canary (determinism). `TestDeterminismCanary` runs each seed twice. Two runs must match on `(TraceHash, RandCalls, Steps)`.
- CommitSegment. CP op that assigns offsets to one uploaded segment. Op works as one atomic unit per `SegmentKey`. Decisions 3 and 42 cover that rule.
- Conformance (C1-C4). Lockstep checks of the broker, the sim, the `sys` adapters, and the oracle against the `veritas/` executable spec. Decision 21 covers that design.
- Control plane (CP) and `manicp`. HTTP service that assigns offsets. Code lives in `cp/` and `cmd/manicp`. Service serves on `:8080`. Stores are `memstore`, `cp/dynamo`, and `cp/logcp` (`dynamolog`).
- CRAP. Score `comp² × (1−cov)³ + comp` for one function. Gate sets the limit at 30 with a baseline. `docs/crap-gate.md` covers the gate.
- detcheck. Tool in `tools/detcheck`. Tool enforces an exact-path import allow-list and bans constructs that break determinism.
- Env. Type in `core/env.go`. Type supplies randomness, buggify, and assertions to the core.
- Fact and Timeline. Fact is an append-only named observation from `core/fact.go`. Sim records facts on the timeline in `sim/timeline.go`. Invariants read the timeline after each step.
- Hermetic test. Test in `it/` that runs in process with memstore and in-memory objects. Full-stack test needs Docker backends through `scripts/it-up.sh`.
- Incarnation. One boot of a node. System drops completions for a dead incarnation. Brokers hold no durable local state. No fencing applies under decisions 43-44.
- Inbox (Control and Data). Two queues for each node. Control has strict priority. System bounds Data. `core/policy.go` and decisions G and H cover that rule.
- Machine and `Step`. `core.StateMachine` defines the machine. `Step(Event) []Action` defines the step. Machine takes `Started` as its first Step.
- manisim. Binary in `cmd/manisim`. Binary runs seed campaigns and replays one seed on request.
- Outcome. `NotApplied`, `MaybeApplied`, and `Applied` name what a completion error proves under lesson 1. An idempotent op can retry a `MaybeApplied` outcome.
- Run. Batch range `[Base, Base+Count)` in a partition log with no gaps. CP derives the next offset from the newest run. Decision 42 covers that rule.
- Seed and TraceHash. Seed drives one sim run. Trace hash covers each step of that run. Equal seeds must produce equal hashes.
- Segment and SegmentKey. Broker buffers batches into a segment. Size, window, and caps seal a segment. `format/` encodes a segment. Broker uploads a segment as one object under `seg/<32 hex>` from `Env.NewID()`. Decision 43 covers that layout.
- Swarm. Per-seed random subset of faults and workload ops. Derived stream draws each subset. Draw lets a run reach extreme cases. Lesson 9 and decision 17 cover that design.
- Trip and Exit. `Trip` stops a node after a failure with untrusted memory and ends the incarnation. `Exit` ends an incarnation after `Stop` without a failure. Decisions 7-8 cover both cases.
- veritas. Package in `veritas/`. Package holds executable specs. Specs double as test fakes. Conformance checks compare code against those specs.

## Key takeaways

- Run `make build` and `./scripts/ci-determinism.sh` as the trusted loop. Other commands run narrower parts of those two gates.
- A local run needs no Docker and no AWS. Run `manicp` with the mem store on `:8080`. Run `manibroker -objects mem` on `:9092`. Docker runs the full-stack tests in `it/`. AWS runs the benchmark.
- A sim failure gives a seed and a step number. Replay prints both values. Fact names and trace hashes stay frozen on purpose. Those values tell you how to read the failure.
- Deterministic packages import from an exact allow-list. A new dependency needs an audit. That audit runs `scripts/audit-dep.sh`. It writes an audit record. It follows the procedure in the audit skill.
- Each change recipe pairs the files to touch with the test that proves the change. A contract change needs a `docs/decisions.md` row.

## Checkpoint

<details><summary>1. `make test` passes but `make build` fails at the last step. What is that step, and what two changes can fix it?</summary>The CRAP gate in `cmd/crapscore` is that step. Lower the cyclomatic complexity of the function. Raise its coverage with a real test. `make crap-baseline` needs agreement from a reviewer. A red build alone does not give that agreement.</details>

<details><summary>2. A colleague says "just add <code>time</code> to <code>allow.txt</code>, the core needs a timestamp." Why not, and what instead?</summary>The allow-list states the determinism contract under decisions I and 2. Deterministic packages get time from the <code>At</code> and <code>Wall</code> stamps on each event. Use those stamps. A new import needs the dependency-audit recipe. The recipe forbids a change to detcheck that exists to pass CI.</details>

<details><summary>3. `manisim` reports <code>FAIL seed 77 … repro: go run ./cmd/manisim -scenario kafka -seed 77</code>. What do you run, and what three numbers do you compare across runs?</summary>Run that repro line. Compare <code>steps</code>, <code>rand</code> for RNG calls, and <code>hash</code> for the trace hash. One seed must reproduce each number. A mismatch points to nondeterminism. It does not point to a logic bug.</details>

<details><summary>4. You need a new CP operation. Name the files in order.</summary>Start with <code>core/controlplane.go</code> for the op and response contract. Move then to <code>cp/wire.go</code>. Move then to the route in <code>cp/handler.go</code>. Move then to <code>cp.Store</code> in <code>cp/store.go</code> and each store. Move then to <code>api/openapi/controlplane.json</code> and <code>wireTypes</code> in <code>spec_test.go</code>. Regenerate with <code>go run ./api/openapi/cmd/specgen</code>. Cover the op with a store lockstep test. Cover the op with an <code>x-replay</code> example.</details>

<details><summary>5. For the hermetic <code>it/</code> test, the full-stack <code>it/</code> test, <code>make build</code>, and the AWS benchmark, which one needs Docker, which one needs AWS, and which ones need neither?</summary>The hermetic test needs neither. <code>make build</code> needs neither. The full-stack test needs Docker through the endpoints from <code>scripts/it-up.sh</code>. The benchmark under <code>deploy/aws/</code> needs AWS. <code>docs/aws-benchmark-runbook.md</code> covers that benchmark.</details>

## Further reading

- Read `loop.md` and `docs/lessons.md`. Those files give the `Step(Event) []Action` model and the 14 lessons behind the interfaces.
- Read `docs/decisions.md`. That file gives the contract history. Read decisions I, J, 21, and 40-45 before your second week.
- Read `docs/crap-gate.md`. That file gives the CRAP formula. It gives the threshold reason and the baseline policy.
- Read `docs/flow-control.md`. That file covers inbox priority and backpressure. It explains why control traffic does not queue behind data.
- Read `api/openapi/README.md` and `docs/log-cp-service.md`. Those files cover the CP spec workflow and the log-backed store from decisions 48-53.
- Read `docs/aws-benchmark-runbook.md`. That file covers the one part of the repo that needs AWS.
- Read `.claude/skills/audit-go-dependency/SKILL.md`. That file gives the dependency-audit procedure in the recipe.

## Try it (a half-day hands-on lab)

Budget about four hours. Each part ends at an artifact you can check.

1. Start cold. Take 30 minutes. Run `make build` from a clean checkout. Run `./scripts/ci-determinism.sh` after that. Note the time each command takes. Name the step that catches a stray `import "time"` in `core/`. Prove that step on a scratch branch. Revert the branch after the proof. Check the branch holds no stray import.
2. Run the system. Take 45 minutes. Start `manicp` and `manibroker -objects mem` in the broker task. Create `orders` with 3 partitions. Run `manibench` for 10 s. Stop the broker in the middle of the run. Restart the broker. Write down what survived. Write down which store held what and why it survived. Check the notes name the surviving data and the store.
3. Break the system in simulation. Take 60 minutes. Enable one `broker.Bug*` mutant on your machine. Run `go test ./broker/ -run TestConformanceBrokerCatchesMutants`. Run `go run ./cmd/manisim -seeds 100 -v`. Record the first seed that catches the mutant. Replay that seed with `-seed`. Write three sentences. Those sentences name the `Step` and the invariant that caught the mutant. Revert the mutant after that. Check the three sentences name the `Step`, the invariant, and the seed.
4. Make one safe change. Take 60 minutes. Pick an item from the safe list. A hermetic `it/` case suits this part. Apply the recipe for that item. Run the loop for the first PR. Open the PR. Put the seed evidence or the test evidence in the PR description. Check the PR holds the evidence.
5. Leave a paper record. Take 15 minutes. Add the `docs/decisions.md` row if your change moved a contract. Write the reason in the PR if your change moved no contract. Record that reason in each PR. Check the row or the PR holds the reason.
