# dst

Deterministic simulation tools for single-threaded Go tests. A product implements `sim.Process` and `eng.Engine`; the runner owns one seeded `rng`, virtual-time `sched`, cooperative `fault` injector, record-and-continue `assert` log, and `trace` fingerprint per run.

Module: `github.com/manifesterio/mds/dst`

This module is a folder of the MDS monorepo. It imports no other module of the repo.

## Build

From the repo root: `make -C dst build`. It runs format, ASCII, vet, lint, modernize, the tests and the
determinism check. The determinism tool is in `cmd/determinismcheck`.

## dst v2 (new)

dst v2 is the typed kernel of [docs/v2-design.md](docs/v2-design.md). It lives beside the old `sim` and
`eng` packages until both products move to it (plan tasks T1.11 and T1.12); then the old packages are
deleted.

| Package | Role |
|---|---|
| `machine` | The machine contract: `Machine[In, Out]` (`Step(now, in) []Out`, `StateHash`), `Factory`, `Env` (random stream, buggify, assertions), time types, node names, boot numbers, `Fact`. Product machine code imports only this package. |
| `kernel` | The simulation world: `Spawn` nodes, `AddService` services, typed `Addr[In]` and `Ref[In]`, a `Router[In]` per Step, `Send` / `SendOn` / `Call`, timers, lanes (`Lanes[K]`), two inboxes per node, the cost model, phases, incarnations, crash / restart / stop / pause, attrition, rules, the fact timeline and the trace hash. |
| `live` | The production loop for the same machines: one goroutine, a two-queue `Inbox[In]` (Control before Data, Data may be bounded), real timers, a `Route` function of the product for the outputs. Real time and goroutines: it is not in the determinism gate. |

### How to run a machine on the kernel

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

`examples/v2/kv` and `examples/v2/kvchaos` are the two examples on v2 (new copies of the old
examples, with golden hashes). `examples/v2/kvsim` runs both through the campaign tool.

Assertions and buggify sites are declared once, in package-level var blocks:
`var oneWriter = assert.NewAlways("sim: one writer per epoch")`,
`var slowAnswer = fault.NewSite("store: slow answer")`. The declarations form a catalog
(`assert.Catalog`, `fault.Sites`), so a campaign can list what was never reached or never fired.

## Contract packages (old)

Wire these first.

| Package | Role |
|---|---|
| `eng` | `Engine` + `Context` (`Deliver`, `Self`). Returns `ErrNotMine` for foreign actions. Does not import `sim`. |
| `sim` | `Process`, `Runner`, control values (`Send`, `SetTimer`, `CancelTimer`, `Crash`, `Restart`). Owns one `rng`, `sched`, `fault.Injector`, `assert.Log`, and `trace.Hash` per run. |

### How to wire a Process and Engine

1. Implement `sim.Process` (`ID`, `StateHash`, `Step`).
2. Implement `eng.Engine` (`ID`, `StateHash`, `Handle`, `OnSelf`).
3. `r := sim.New(sim.Config{Seed: seed})`, then `r.Register(engine)`, `r.Spawn(process)`, `r.Inject(...)`, `r.Run()`.

`Process.Step(cx, inbox any) []any` takes one inbox value and returns outbound values. Those are either control structs in package `sim` or domain values an `eng.Engine` understands. There is no `sim.Event` / `sim.Action` alias.

The runner handles control types first, then tries engines in register order until one does not return `eng.ErrNotMine`.

**Crash / Restart.** `Crash` marks the issuing process crashed; deliveries to it are ignored until `Restart` (or `Spawn`) clears the flag. `Restart{Who, Boot}` clears crash for `Who` and, when `Boot` is non-nil, delivers that inbox at delay 0. Pending timers still fire while crashed; `Step` is a no-op until restart.

`examples/kv` is a happy-path store. `examples/kvchaos` adds write drops (`Fault` + MaybeApplied client retries) and Crash+Restart on some seeds.

## Mechanism packages

| Package | Role |
|---|---|
| `rng` | SplitMix64 stream (`Uint64`, `Intn`, `Float64`, `Chance`, `Between`, `Calls`, `Bytes`), and `Derive(label)` for an independent stream that draws nothing |
| `sched` | Virtual mono clock and `(at, tie, seq)` event heap |
| `clock` | Wall offset over `sched`, `AfterFunc` / `Advance` on the same heap |
| `trace` | Running FNV-1a `Hash`, optional `Ring[T]` |
| `assert` | Always / AlwaysOrUnreachable / Sometimes / Reachable / Unreachable with coverage; typed handles and the catalog |
| `fault` | Maybe / MaybeP / Fault with per-site latch and Quiet; declared sites (`NewSite`) and the catalog |
| `campaign` | The one campaign tool: scenario `Registry`, parallel seeds folded in seed order, one `Report` (text and JSON), until-stable mode, the determinism re-run, the standard flags and `<PREFIX>_SEEDS` / `_SEED` / `_CAMPAIGN` variables, and `Main` for a product command |
| `check` | The log oracle of a simulator (`Replay`, `NoHole`, `Holders`, `Prefixes`, `Monotonic`, `Converged`) and the crash checks by hand (`Persistence`, `ForwardProgress`, `DirtyReboot`) |
| `check/spectest` | The spec toolkit for tests, on rapid: `Spec`, `Lockstep` (ops, reboots, crashes, checkpoints), `SimplestFirst`, `SelfCheck`, planted bugs (`Bug`, `RequireCaught`, `RequireClean`), `Rerun` |
| `mutant` | Old name of the planted-bug helpers; now calls `check/spectest`. Deleted in T1.16 |
| `model` | Old lockstep on rapid; `spectest.Lockstep` replaces it. Deleted in T1.14 / T1.16 |
| `golden` | Fixed seed-to-hash asserts plus optional golden file Load/Save |

## Docs

- [docs/lessons.md](docs/lessons.md) - one RNG, virtual time, issue-time faults, phases, record-and-continue, Go sans-I/O
- [docs/paper.md](docs/paper.md) - SOSP'21 ShardStore ideas mapped onto dst

## How to run campaigns

A product simulator command is a few lines. It registers its scenarios and calls `campaign.Main`.
`examples/v2/kvsim` is a full example (it runs `kv` and `kvchaos`).

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
go run ./examples/v2/kvsim -seeds 200 -report         # both scenarios, 200 seeds each, with the report
go run ./examples/v2/kvsim -scenario kvchaos -seed 7  # one seed, with its step log
go run ./examples/v2/kvsim -until-stable              # more seeds until no new assertion appears
go run ./examples/v2/kvsim -json                      # the report as JSON, nothing else
MDS_CAMPAIGN=30s go run ./examples/v2/kvsim           # seeds until 30 seconds have passed
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
order, and merges coverage by summing `Checks` and `Passes`. A test can call it directly. (`Run`, the
old name, stays until the control plane command moves to `Main`.) Same seed must still replay to the
same hash. Cross-seed hash collisions are normal and not signal.

For planted bugs, use `spectest.RequireCaught` so each bug fails at least once in seeds `1..budget`, and `spectest.RequireClean` so the same budget raises no false alarms. With a `spectest.Lockstep`, `RunSeed(seed)` runs one seed and returns the shrunk failure.

A failed `spectest.Lockstep` prints the seed, the shrunk steps as Go code, and the command that runs it again. The product sets `Rerun.EnvPrefix` (for example `P`); then `P_SEED=<n> go test -run '^TestX$' -count=1 -v .` runs only seed n.

```bash
go test ./examples/kv -run TestCampaign -count=1
go test ./examples/kvchaos -run TestCampaign -count=1
go test ./examples/v2/kvsim -count=1
go test ./campaign ./mutant ./...
```

## Stay out

These stay out of this module on purpose:

- Spec / rapid property engines
- Closed domain message vocabularies (Kafka, S3, product RPCs)
- Corpus orchestration and product scenario registries
- Product I/O fakes beyond the tiny KV examples
- Full network-partition simulators

Product wall clocks, product fault policy, and domain IDs belong in the importing project. Use `clock` only for virtual time over `sched`.

## License

Apache-2.0. See [LICENSE](LICENSE).
