# dst

Deterministic simulation tools for single-threaded Go tests. A product implements `sim.Process` and `eng.Engine`; the runner owns one seeded `rng`, virtual-time `sched`, cooperative `fault` injector, record-and-continue `assert` log, and `trace` fingerprint per run.

Module: `github.com/manifesterio/mds/dst`

This module is a folder of the MDS monorepo. It imports no other module of the repo.

## Build

From the repo root: `make -C dst build`. It runs format, ASCII, vet, lint, modernize, the tests and the
determinism check. The determinism tool is in `cmd/determinismcheck`.

## Contract packages

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
| `rng` | SplitMix64 stream (`Uint64`, `Intn`, `Float64`, `Chance`, `Between`, `Calls`, `Bytes`) |
| `sched` | Virtual mono clock and `(at, tie, seq)` event heap |
| `clock` | Wall offset over `sched`, `AfterFunc` / `Advance` on the same heap |
| `trace` | Running FNV-1a `Hash`, optional `Ring[T]` |
| `assert` | Always / Sometimes / Reachable / Unreachable with coverage |
| `fault` | Maybe / MaybeP / Fault with per-site latch and Quiet |
| `campaign` | Parallel seed runner. Folds per-seed hashes in seed order and merges coverage. |
| `mutant` | `Require` / `RequireClean` helpers for planted-bug and false-alarm checks |
| `model` | Lockstep `Model`/`System` on rapid: `SimplestFirst`, `CheckGen`, `DirtyReboot` |
| `golden` | Fixed seedtohash asserts plus optional golden file Load/Save |

## Docs

- [docs/lessons.md](docs/lessons.md) - one RNG, virtual time, issue-time faults, phases, record-and-continue, Go sans-I/O
- [docs/paper.md](docs/paper.md) - SOSP'21 ShardStore ideas mapped onto dst

## How to run campaigns

Use `campaign.Run` for a parallel seed sweep, or keep a plain loop in the test.

```go
sum := campaign.Run(1, 100, 0, func(seed uint64) campaign.Result {
    r, err := myScenario.Run(seed) // product code
    return campaign.Result{Hash: r.Hash, OK: err == nil && len(r.Failures) == 0, Coverage: toCampaign(r.Coverage)}
})
if len(sum.Failed) > 0 {
    t.Fatalf("%d failed; first seed %d: %s", len(sum.Failed), sum.Failed[0].Seed, sum.Failed[0].Reproduce)
}
```

`campaign.Run` stamps `Result.Seed`, folds hashes with `trace` in seed order, and merges coverage by summing `Checks`/`Passes`. Same seed must still replay to the same hash. Cross-seed hash collisions are normal and not signal.

For planted bugs, use `mutant.Require` so each bug fails at least once in seeds `1..budget`, and `mutant.RequireClean` so the same budget raises no false alarms.

```bash
go test ./examples/kv -run TestCampaign -count=1
go test ./examples/kvchaos -run TestCampaign -count=1
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
