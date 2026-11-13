# dst

Deterministic simulation tools for Go tests. A product writes **machines**: state machines that take one input
and return outputs, and do no IO. The **kernel** runs many machines in one goroutine, with one seeded `rng`,
virtual-time `sched`, a `fault` injector, an `assert` log and a `trace` fingerprint per run. The same machine
also runs in production with the `live` package.

Module: `github.com/manifesterio/mds/dst`

This module is a folder of the MDS monorepo. It imports no other module of the repo.

A short guide with one example for each part is in [docs/v2.md](docs/v2.md). The design is in
[docs/v2-design.md](docs/v2-design.md).

## Build

From the repo root: `make -C dst build`. It runs format, ASCII, vet, lint, modernize, the tests and the
determinism check. The determinism tool is in `cmd/determinismcheck`.

## Packages

| Package | Role |
|---|---|
| `machine` | The machine contract: `Machine[In, Out]` (`Step(now, in) []Out`, `StateHash`), `Factory`, `Env` (random stream, buggify, assertions), time types, node names, boot numbers, `Fact`. Product machine code imports only this package. |
| `kernel` | The simulation world: `Spawn` nodes, `AddService` services, typed `Addr[In]` and `Ref[In]`, a `Router[In]` per Step, `Send` / `SendOn` / `Call`, timers, lanes (`Lanes[K]`), two inboxes per node, the cost model, phases, incarnations, crash / restart / stop / pause, attrition, rules, the fact timeline and the trace hash. |
| `live` | The production loop for the same machines: one goroutine, a two-queue `Inbox[In]` (Control before Data, Data may be bounded), real timers, a `Route` function of the product for the outputs. Real time and goroutines: it is not in the determinism gate. |
| `rng` | SplitMix64 stream (`Uint64`, `Intn`, `Float64`, `Chance`, `Between`, `Calls`, `Bytes`), and `Derive(label)` for an independent stream that draws nothing |
| `sched` | Virtual mono clock and `(at, tie, seq)` event heap |
| `clock` | Wall offset over `sched`, `AfterFunc` / `Advance` on the same heap |
| `syncclock`, `realclock` | Clock adapters for code that needs a real or synchronised clock outside a run |
| `trace` | Running FNV-1a `Hash`, optional `Ring[T]` |
| `assert` | Always / AlwaysOrUnreachable / Sometimes / Reachable / Unreachable with coverage; typed handles and the catalog |
| `fault` | Maybe / MaybeP / Fault with per-site latch and Quiet; declared sites (`NewSite`) and the catalog |
| `campaign` | The one campaign tool: scenario `Registry`, parallel seeds folded in seed order, one `Report` (text and JSON), until-stable mode, the determinism re-run, the standard flags and `<PREFIX>_SEEDS` / `_SEED` / `_CAMPAIGN` variables, and `Main` for a product command |
| `check` | The log oracle of a simulator (`Replay`, `NoHole`, `Holders`, `Prefixes`, `Monotonic`, `Converged`) and the crash checks by hand (`Persistence`, `ForwardProgress`, `DirtyReboot`) |
| `check/spectest` | The spec toolkit for tests, on rapid: `Spec`, `Lockstep` (ops, reboots, crashes, checkpoints), `SimplestFirst`, `SelfCheck`, planted bugs (`Bug`, `RequireCaught`, `RequireClean`), `Rerun` |
| `check/faulty` | Puts timing and faults (slow replies, errors, timeouts, lost answers, outages) around a fault-free spec, so the code under test meets an outside system that fails |
| `golden` | Fixed seed-to-hash asserts plus optional golden file Load/Save |

## How to write a machine

1. Give the machine an input sum type and an output sum type: an interface with an unexported marker
   method and a `//sumtype:decl` comment. Every case has a `Fold(h *trace.Hash)` method that writes a
   fixed tag number, then the fields.
2. Implement `machine.Machine[In, Out]`.
3. `w := kernel.New(kernel.Config{Seed: seed})`, then `kernel.AddService(w, svc)` and
   `addr := kernel.Spawn(w, kernel.NodeSpec[In, Out]{Name, New, Route, ...})`.
4. `Route` is one type switch over every case of `Out`. Each case calls `r.SetTimer`, `r.End`,
   `kernel.Send(r, addr, msg)`, `kernel.Call(r, service, req)`, and so on. The `gochecksumtype` linter
   fails the build when a case is missing.
5. `w.RunUntil(nil)` and `res := w.Finish()`. The same seed gives the same `res.Hash`.

Go infers a type parameter from every argument, and the types must be the same. So when a message is
a concrete case, convert it to the sum type: `kernel.Send(r, server, serverIn(putRequest(x)))`, or
write the type: `kernel.Inject[tin](w, a, 0, note{"x"})`.

`examples/kv` and `examples/kvchaos` are the two examples. `examples/kvsim` runs both through the campaign tool.

Assertions and buggify sites are declared once, in package-level var blocks:
`var oneWriter = assert.NewAlways("sim: one writer per epoch")`,
`var slowAnswer = fault.NewSite("store: slow answer")`. The declarations form a catalog
(`assert.Catalog`, `fault.Sites`), so a campaign can list what was never reached or never fired.

Deterministic packages must not use goroutines, channels, map ranges, package-level state, or imports
outside `ops/determinism-allow.txt`. `ops/scripts/ci-determinism.sh` lists the packages that are checked.

## Docs

- [docs/v2.md](docs/v2.md) - a short guide with one small example for each part
- [docs/v2-design.md](docs/v2-design.md) - the design of v2 and its plan
- [docs/lessons.md](docs/lessons.md) - one RNG, virtual time, issue-time faults, phases, record-and-continue, Go sans-I/O
- [docs/paper.md](docs/paper.md) - SOSP'21 ShardStore ideas mapped onto dst

## How to run campaigns

A product simulator command is a few lines. It registers its scenarios and calls `campaign.Main`.
`examples/kvsim` is a full example (it runs `kv` and `kvchaos`).

```go
reg := &campaign.Registry{}
reg.Add(campaign.Scenario{Name: "kv", Detail: "a store with no faults",
    Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
        r := kv.RunWith(kernel.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst, Tail: 20})
        return campaign.FromKernel(r, "") // "": the replay command is made from Config.Tool
    }})
cfg := campaign.Config{Tool: "kvsim", EnvPrefix: "MDS", Defaults: campaign.Options{CheckEvery: 10}}
os.Exit(campaign.Main(cfg, reg, os.Args[1:], os.Stdout, os.Stderr))
```

```bash
go run ./examples/kvsim -seeds 200 -report         # both scenarios, 200 seeds each, with the report
go run ./examples/kvsim -scenario kvchaos -seed 7  # one seed, with its step log
go run ./examples/kvsim -until-stable              # more seeds until no new assertion appears
go run ./examples/kvsim -json                      # the report as JSON, nothing else
MDS_CAMPAIGN=30s go run ./examples/kvsim           # seeds until 30 seconds have passed
```

Flags: `-seeds`, `-first` (or `-start`), `-seed` (replay one seed), `-scenario` (a name, a list with
commas, or `all`), `-j`, `-report`, `-json`, `-v`, `-until-stable`, `-stable-after`, `-max-seeds`,
`-campaign` (a time limit), `-check-every`. The variables are `<EnvPrefix>_SEEDS`, `<EnvPrefix>_SEED`
and `<EnvPrefix>_CAMPAIGN`. A flag wins over its variable. `ParseOptions` is the one place that reads both.

The report lists: counts, failed runs with the command that replays them, every assertion with its
checks and passes, the paths that were never reached, the Always properties that were never checked,
the buggify sites (asked, on, calls, fired), the sites that never fired, and the coverage curve.
"Never reached", "never checked" and "never fired" come from the catalog (`assert.Catalog`,
`fault.Sites`), so a path that never ran is still named. Every `CheckEvery`-th seed runs twice with the
step log switched, and a different hash, step count or number of random calls fails the seed
("nondeterminism"). A campaign of `MinSeedsForCoverage` seeds or more also fails on a coverage gap.

`campaign.RunSeeds` is the parallel fold under it. It stamps `Result.Seed`, folds hashes in seed
order, and merges coverage by summing `Checks` and `Passes`. A test can call it directly.
Same seed must still replay to the same hash. Cross-seed hash collisions are normal and not signal.

For planted bugs, use `spectest.RequireCaught` so each bug fails at least once in seeds `1..budget`, and `spectest.RequireClean` so the same budget raises no false alarms. With a `spectest.Lockstep`, `RunSeed(seed)` runs one seed and returns the shrunk failure.

A failed `spectest.Lockstep` prints the seed, the shrunk steps as Go code, and the command to run it again. The product sets `Rerun.EnvPrefix` (for example `P`); then `P_SEED=<n> go test -run '^TestX$' -count=1 -v .` runs only seed n. `Check` reads the same three variables as the campaign tool, with the same parser (`campaign.ReadEnv`): `P_SEED` (one seed), `P_CAMPAIGN` (seeds until a time has passed) and `P_SEEDS` (the number of checks). `Lockstep.Checks` and `Lockstep.StepsPerCheck` set the number of checks and the average number of steps in a check.

```bash
go test ./examples/kv -run TestCampaign -count=1
go test ./examples/kvchaos -run TestCampaign -count=1
go test ./examples/kvsim -count=1
go test ./campaign ./check/... ./...
```

## Stay out

These stay out of this module on purpose:

- Closed domain message vocabularies (Kafka, S3, product RPCs)
- Product specs, and product scenario registries
- Product I/O fakes beyond the tiny KV examples
- Full network-partition simulators

Generic spec machinery is in: `check/spectest` and `check/faulty` work on any spec with `Apply` and
`StateHash`. They have no product words. A product brings its own spec, its own fault numbers and its
own message types.

Product wall clocks, product fault policy, and domain IDs belong in the importing project. Use `clock` only for virtual time over `sched`.

## License

Apache-2.0. See [LICENSE](LICENSE).
