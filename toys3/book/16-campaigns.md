# Chapter 16: Running Campaigns

> *"A seed that passed is a fact about one universe. A campaign is a census."*

Everything up to this chapter has been about building the machine: the state machine, the storage model, the workload, the oracle, the invariants, the mutants. This chapter is about *driving* it. How do you run one seed, a thousand seeds, ten minutes of seeds on every core? How do you read what comes back? When something fails, what is the shortest path from "red" to "I understand the bug"? And how does all of this fit into CI so that determinism and correctness are defended on every push?

There are exactly two entry points, and it is worth knowing both intimately:

1. **`go test ./toys3/sim`** and its flags, defined in `toys3/sim/sim_test.go` and `toys3/sim/mutants_test.go`. This is the developer's loop: replay one seed, run N seeds, run for D seconds, run the mutants.
2. **`go run ./toys3/cmd/toys3sim`**, the parallel campaign runner in `toys3/cmd/toys3sim/main.go`. This is the "burn CPU for ten minutes and tell me what you found" tool.

Both are thin. All of the interesting work happens in `sim.Run(cfg)` (`toys3/sim/config.go:198`), which takes a `Config` and returns a `Result`. Every entry point is a different way of choosing configs and summarizing results.

---

## 16.1 The unit of work: `sim.Run(DefaultConfig(seed))`

Before looking at the drivers, fix the atom in your head:

```go
// toys3/sim/config.go:196-198
// Run executes one simulation. It never panics on server misbehavior: a panic
// inside Step is recovered and reported as a Failure.
func Run(cfg Config) Result { return New(cfg).Run() }
```

and

```go
// toys3/sim/config.go:137-145
func DefaultConfig(seed uint64) Config {
	cfg := BaseConfig(seed)
	r := NewRand(seed ^ 0x5a5a_c0ff_ee00_1234)
	cfg.Workload.Keys = []int{1, 2, 3, 3, 4, 8, 16, 32}[r.Intn(8)]
	cfg.Workload.Clients = []int{2, 4, 4, 6}[r.Intn(4)]
	cfg.CrashProb = []float64{0.001, 0.004, 0.004, 0.01}[r.Intn(4)]
	cfg.Disturbances.EIOProb = []float64{0.002, 0.01, 0.01, 0.03}[r.Intn(4)]
	return cfg
}
```

A seed is therefore *two* things: the seed of the run's `Rand` (everything that happens inside the run), and, via a separate stream (`seed ^ 0x5a5a_c0ff_ee00_1234`), the swarm parameters of the run (how many keys, clients, how crashy, how EIO-prone). Chapter 12 explains why those must be separate streams. For campaigns the consequence is simple: **"seed 132" fully names a universe**, including its shape. Two people running seed 132 at the same commit get the same keys, clients, crash rate, EIO rate, event log and trace hash.

The `Result` (`toys3/sim/config.go:47-57`) carries `Failures` (empty means pass), `Stats` (counters), `TraceHash` and `RandCalls` (determinism fingerprints), `Steps`, `Crashes`, `EndTime`, and, only if `cfg.Log` is set, the full `Log`.

## 16.2 The test entry points (`toys3/sim/sim_test.go`)

### The flags

```go
// toys3/sim/sim_test.go:11-15
var (
	flagSeeds    = flag.Int("seeds", 200, "TestSeeds: number of seeds to run (from 1)")
	flagSeed     = flag.Int64("seed", -1, "TestSeeds: replay this one seed with the event log printed on failure")
	flagDuration = flag.Duration("duration", 0, "TestSeeds: run seeds 1, 2, ... until this much wall-clock time has passed (overrides -seeds)")
)
```

and in `mutants_test.go`:

```go
// toys3/sim/mutants_test.go:329
var mutantSeeds = flag.Int("mutant-seeds", 500, "TestCatchesBugs: seeds to try per mutant")
```

These are package-level `flag` variables in `_test.go` files. That is allowed: `detcheck` skips `_test.go` files (`tools/detcheck/detcheck.go:107-109`), and in any case the flags are read once, before any simulation starts, and are only used to choose *which* seeds to run, never inside a run.

Because they are test-binary flags, you pass them after the package path: `go test ./toys3/sim -run TestSeeds -seed=132 -v`. Go's test runner forwards unknown flags to the test binary.

### `TestSeeds`: three modes in one function

```go
// toys3/sim/sim_test.go:44-81
func TestSeeds(t *testing.T) {
	if *flagSeed >= 0 {
		seed := uint64(*flagSeed)
		cfg := DefaultConfig(seed)
		cfg.Log = true
		r := Run(cfg)
		t.Logf("seed %d: steps=%d crashes=%d trace=%016x\n%s", seed, r.Steps, r.Crashes, r.TraceHash, r.Stats)
		if len(r.Failures) > 0 {
			fmt.Fprint(os.Stderr, FormatLog(r.Log))
			for _, f := range r.Failures {
				t.Errorf("FAIL %s at step %d (t=%s): %s", f.Invariant, f.Step, fmtTime(f.At), f.Msg)
			}
		}
		return
	}
	start := time.Now()
	var agg Stats
	n := 0
	for seed := uint64(1); ; seed++ {
		if *flagDuration > 0 {
			if time.Since(start) >= *flagDuration {
				break
			}
		} else if seed > uint64(*flagSeeds) {
			break
		}
		r := Run(DefaultConfig(seed))
		agg.Add(r.Stats)
		n++
		if len(r.Failures) > 0 {
			f := r.Failures[0]
			t.Errorf("seed %d FAILED %s at step %d: %s\nrepro: %s", seed, f.Invariant, f.Step, f.Msg, repro(seed))
		}
	}
	t.Logf("%d seeds in %v\n%s", n, time.Since(start).Round(time.Millisecond), agg)
}
```

The three modes:

| Invocation | Mode | What you get |
|---|---|---|
| `-seed=S` | **Replay** one seed with logging on | Stats line; on failure, the full event log on stderr and one `FAIL` line per failure |
| `-seeds=N` (default 200) | **Sweep** seeds 1..N | One error per failing seed with its repro command; an aggregate stats line |
| `-duration=D` | **Timed sweep** from seed 1 until D elapses | Same as sweep; the count of seeds depends on machine speed |

A few details are worth pausing on.

**Replay always turns `Log` on.** That is safe *only* because logging is proven not to perturb the run. `TestDeterminismCanary` checks precisely that (`toys3/sim/sim_test.go:32-37`): the trace hash with `Log = true` must equal the trace hash without. If someone ever made the logger draw from the RNG (for example, to sample which events to log), the canary would catch it, and replays would silently stop reproducing failures. This is one of those tests whose job is to protect the debugging workflow itself.

**Replay prints the log only on failure.** A passing replay prints only the stats line. That is a deliberate choice (a passing seed's log is thousands of lines of noise) but it surprises people the first time: "I ran `-seed=1 -v` and there was no event log!" Section 16.5 shows how to get a log for a passing seed.

**The sweep starts at seed 1, always.** There is no `-start-seed` for `TestSeeds`. That is fine for a developer loop (you want "the first 200 seeds", stable from run to run) and it is why the campaign runner exists for anything bigger.

**The sweep reports every failing seed, not just the first.** `t.Errorf` does not stop the test. A broken change usually fails many seeds; the list of failing seeds and their invariant names is itself diagnostic ("all failures are `acked-is-durable`" points at a very different bug from "a mix of `linearizable` and `liveness`").

**Only `r.Failures[0]` is reported per seed.** A run stops at the first step with a failure (`toys3/sim/simulator.go:365-375` sets `halted`), but the invariant loop in `afterStep` (`toys3/sim/simulator.go:349-353`) keeps calling the remaining invariants for that same step, so several can be recorded at one step. Mutant e at seed 18 produces two (`no-corrupt-reads` and `linearizable`, both at step 657). The sweep shows only the first; the replay shows all.

**The `repro` helper** is a one-liner:

```go
// toys3/sim/sim_test.go:17-19
func repro(seed uint64) string {
	return fmt.Sprintf("go test ./toys3/sim -run TestSeeds -seed=%d -v", seed)
}
```

Every failure message in the sweep, and every failure in the campaign runner, ends in a command you can paste. This sounds trivial and is one of the most important ergonomic decisions in the whole project: **the distance from a red CI log to a local replay is one copy-paste.** `loop.md` makes it rule 15: "Make failures one command."

Here is what a timed sweep printed while this chapter was written:

```
$ go test ./toys3/sim -run TestSeeds -duration=2s -v
...
    sim_test.go:80: 551 seeds in 2s
```

(followed by the aggregate stats line). Roughly 275 seeds per second on one core on a laptop that was busy with other work.

### `TestDeterminismCanary`

```go
// toys3/sim/sim_test.go:24-42
func TestDeterminismCanary(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		a := Run(DefaultConfig(seed))
		b := Run(DefaultConfig(seed))
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf(...)
		}
		// Log mode only records; it must not change the run.
		cfg := DefaultConfig(seed)
		cfg.Log = true
		if c := Run(cfg); c.TraceHash != a.TraceHash || len(c.Log) == 0 {
			t.Fatalf(...)
		}
		if seed > 1 && a.TraceHash == Run(DefaultConfig(seed-1)).TraceHash {
			t.Fatalf("seeds %d and %d produced the same trace", seed, seed-1)
		}
	}
}
```

Three properties per seed, for twelve seeds:

1. **Same seed, same run**: trace hash, number of random draws and number of steps all match. Three fingerprints rather than one, because each catches a different kind of leak: `RandCalls` diverges if some code path draws a different number of random numbers (often a map iteration deciding how many things to process); `Steps` diverges if the event count changes; `TraceHash` diverges if anything about the state or the events differs (Chapter 8).
2. **Logging does not perturb.** Discussed above.
3. **Different seeds, different runs.** This is the canary for the *opposite* failure: a simulator that ignores its seed (for example, because someone hard-coded `NewRand(1)` while debugging) would happily pass property 1 forever. Comparing seed *k* with seed *k−1* catches it.

This test is what `scripts/ci-determinism.sh` runs (section 16.8). It costs about half a second.

### `TestCoverage`

```go
// toys3/sim/sim_test.go:86-118  (abridged)
func TestCoverage(t *testing.T) {
	var agg Stats
	for seed := uint64(1); seed <= 200; seed++ {
		agg.Add(Run(DefaultConfig(seed)).Stats)
	}
	t.Log(agg)
	checks := []struct{ name string; n int }{
		{"crashes", agg.Crashes},
		{"crashes mid-PUT/COPY", agg.CrashesMidPut},
		...
		{"linearizability windows", agg.Windows},
	}
	for _, c := range checks {
		if c.n == 0 {
			t.Errorf("coverage: no %s in 200 seeds", c.name)
		}
	}
}
```

This is the "a passing run that never crashed mid-PUT proves nothing" test. It fails if, across the first 200 seeds, any one of sixteen interesting things *never happened*: no crash in the middle of a PUT, no torn file, no read corruption, no 400, no Trip, no `CorruptionDetected` telemetry, no temp removed by recovery, and so on. It is a crude, binary form of what the DST literature calls **"sometimes" assertions** (Antithesis's `Sometimes`, the "Reachable" assertions in `docs/lessons.md` §11): properties that must hold *at least once* across a campaign, as evidence that the fault model reaches the code it claims to.

When it ran for this chapter:

```
$ go test ./toys3/sim -run TestCoverage -v
    sim_test.go:91: reqs=47270 200=33446 400=887 404=6842 500=1035 indet=5060 fsops=113656 eio=1061 crashes=1691 trips=673 midput=1473 torn=87 lost=1539 kept=1350 readcorrupt=71 corruptdet=70 tempsrm=1006 windows=6708 maxwin=23 widened=0
--- PASS: TestCoverage (0.87s)
```

Notice that this is the *same* aggregate as `TestSeeds -seeds=200`. Of course it is: the same 200 configs, the same 200 universes.

### `TestCorruptionUnreachableWithoutBitFlips`

```go
// toys3/sim/sim_test.go:124-133
func TestCorruptionUnreachableWithoutBitFlips(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Disturbances.ReadCorruptProb = 0
		r := Run(cfg)
		if len(r.Failures) > 0 || r.Stats.CorruptionsDetected > 0 {
			t.Fatalf(...)
		}
	}
}
```

The inverse of a coverage check: something that must *never* happen under a given configuration. With bit flips off, the correct protocol never leaves a torn file under `obj/`, so the CRC check never fires. This documents a fact about the design (the CRC is a safety net, not a mechanism) and it would fail loudly if a future change to the protocol started producing torn objects that the CRC happened to mask. Chapter 14 and Chapter 15 (mutant e) build on it.

### `TestCatchesBugs` and `-mutant-seeds`

Covered in depth in Chapter 15. The operational facts for campaigns: each mutant runs seeds 1..`-mutant-seeds` (default 500) twice, once with `DefaultInvariants` and once with `BlackBoxInvariants`, stopping at the first failing seed each time. A mutant that survives every seed *fails the test*: the simulator is supposed to have teeth.

The flag is useful in both directions. Lower it to see what "not caught" looks like:

```
$ go test ./toys3/sim -run 'TestCatchesBugs/d2' -mutant-seeds=50 -v
...
--- FAIL: TestCatchesBugs (0.23s)
    --- FAIL: TestCatchesBugs/d2-no-halt-on-dir-fsync-error (0.23s)
FAIL
```

d2 is first caught at seed 132, so 50 seeds is not enough. Raise it when you add a subtle mutant of your own.

### How long does `go test ./toys3/sim` take?

All of the above together (canary, 200-seed sweep, 200-seed coverage, 200-seed CRC check, the storage model, queue and oracle unit tests, all seven mutants twice, and, since the refactor, the golden test of Chapter 8 §8.12 with its 550 seeds and 14 mutant searches):

```
$ time go test ./toys3/sim -count=1
ok  	manifester/toys3/sim	7.669s
go test ./toys3/sim -count=1  7.57s user 0.55s system 98% cpu 8.277 total
```

About eight seconds, of which the golden test is roughly three (the first edition measured about five seconds before it existed). That is the budget to defend. A DST suite that takes five seconds runs on every save; one that takes five minutes runs before lunch; one that takes an hour runs never. Long campaigns belong in the campaign runner.

## 16.3 The campaign runner, line by line (`toys3/cmd/toys3sim/main.go`)

The package doc states the design in one paragraph:

```go
// toys3/cmd/toys3sim/main.go:1-10
// Command toys3sim runs a DST campaign against the toys3 server: many seeds of
// toys3/sim, in parallel, for a fixed wall-clock budget.
//
//	go run ./toys3/cmd/toys3sim -duration 10m -check-determinism
//
// Every seed is an independent sim.Run (its own Rand, queue, storage model
// and server), so seeds run on separate goroutines with no shared state;
// only the seed counter and the result tallies are shared, outside the
// simulation. Any failure prints the one-line repro command and makes the
// exit status non-zero.
```

The crucial sentence is "no shared state". A simulation is single-threaded *inside*. But nothing stops you from running many simulations on many goroutines at once, as long as they share nothing. This is the parallelism model of essentially every DST shop: **parallelism across seeds, never within one.** It is also why `detcheck` bans package-level variables in `toys3/sim` (Chapter 8): a global counter in the simulator would be shared across concurrently running seeds and would make each seed's behaviour depend on what its neighbours were doing.

`toys3sim` itself lives in `cmd/` and is not checked by detcheck. It uses goroutines, atomics, a mutex, wall-clock time and a ticker freely. That is correct: it is the harness, not the simulation.

### Flags

```go
// toys3/cmd/toys3sim/main.go:31-36
	duration := flag.Duration("duration", time.Minute, "wall-clock budget")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "parallel seeds")
	startSeed := flag.Uint64("start-seed", 1, "first seed")
	checkDet := flag.Bool("check-determinism", false, "run every -det-every'th seed twice and compare traces")
	detEvery := flag.Uint64("det-every", 10, "with -check-determinism: replay every K-th seed")
	maxFailures := flag.Int("max-failures", 20, "stop early after this many failing seeds")
```

| Flag | Default | Meaning |
|---|---|---|
| `-duration` | 1m | Wall-clock budget. Workers stop *starting* new seeds after it; a seed in progress finishes. |
| `-workers` | `GOMAXPROCS` | Number of goroutines, each running seeds back to back. |
| `-start-seed` | 1 | First seed. Use it to shard a campaign across machines (`-start-seed 1000000`) or to resume. |
| `-check-determinism` | off | Re-run some seeds and compare fingerprints. |
| `-det-every` | 10 | With the above: re-run seeds whose number is a multiple of K. |
| `-max-failures` | 20 | Stop early after this many failing seeds (a broken build would otherwise print thousands). |

Note that seeds are not partitioned among workers; they are *pulled*. That means which worker runs which seed is nondeterministic, and so is the *order* in which results are tallied. Neither matters: each seed's result depends only on the seed, and the tallies are sums (and one max), which are commutative.

### Shared state, all outside the simulation

```go
// toys3/cmd/toys3sim/main.go:39-49
var (
	next     atomic.Uint64
	done     atomic.Int64
	detRuns  atomic.Int64
	steps    atomic.Int64
	mu       sync.Mutex
	agg      sim.Stats
	failures []failure
	stop     atomic.Bool
)
next.Store(*startSeed)
start := time.Now()
deadline := start.Add(*duration)
```

`next` is the seed dispenser. `done`, `detRuns` and `steps` are progress counters. `agg` and `failures` are protected by `mu`. `stop` is set when `-max-failures` is reached.

### Recording a failure

```go
// toys3/cmd/toys3sim/main.go:53-61
	record := func(f failure) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, f)
		fmt.Printf("FAIL seed %d: %s\n  repro: go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed, f.msg, f.seed)
		if len(failures) >= *maxFailures {
			stop.Store(true)
		}
	}
```

Failures print immediately, with the repro command, so you can start debugging while the campaign keeps running.

### The worker loop

```go
// toys3/cmd/toys3sim/main.go:63-90
for w := 0; w < *workers; w++ {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() && time.Now().Before(deadline) {
			seed := next.Add(1) - 1
			r := sim.Run(sim.DefaultConfig(seed))
			steps.Add(int64(r.Steps))
			if len(r.Failures) > 0 {
				f := r.Failures[0]
				record(failure{seed, fmt.Sprintf("%s at step %d: %.300s", f.Invariant, f.Step, f.Msg)})
			}
			if *checkDet && *detEvery > 0 && seed%*detEvery == 0 {
				r2 := sim.Run(sim.DefaultConfig(seed))
				detRuns.Add(1)
				if r2.TraceHash != r.TraceHash || r2.RandCalls != r.RandCalls || r2.Steps != r.Steps {
					record(failure{seed, fmt.Sprintf("NONDETERMINISM: trace %016x vs %016x, rand %d vs %d, steps %d vs %d",
						r.TraceHash, r2.TraceHash, r.RandCalls, r2.RandCalls, r.Steps, r2.Steps)})
				}
			}
			mu.Lock()
			agg.Add(r.Stats)
			mu.Unlock()
			done.Add(1)
		}
	}()
}
```

Line by line:

- `seed := next.Add(1) - 1`: atomically claim the next seed. `Add` returns the new value, so subtracting one gives the claimed one; the first worker gets `-start-seed`.
- `sim.Run(sim.DefaultConfig(seed))`: exactly the same call as `TestSeeds` makes. A campaign failure therefore always reproduces under `go test ... -seed=S`. This is not automatic; it is a design constraint. If the campaign runner tweaked the config (say, a higher crash rate), its repro line would lie. Keep the two in lockstep.
- Only `r.Failures[0]` is recorded, truncated to 300 characters (`%.300s`). The full message, and the full log, come from the replay.
- The determinism check is **sampled**: only seeds divisible by `-det-every` are run twice, so the overhead is 1/K (10% by default). The comparison is the same triple as the canary (trace hash, rand calls, steps). A mismatch is recorded as a failure whose message starts with `NONDETERMINISM:` and whose repro line, alone, may *not* reproduce it (nondeterminism is, by definition, not reliably reproducible). Chapter 8 explains how to hunt one: run the seed twice under `-count=2`, diff the logs, look for the first diverging line, and check `RandCalls` to see whether the RNG was consumed differently.
- Note one subtlety: in a determinism replay, *both* runs execute in the same process, possibly concurrently with other seeds on other goroutines. That is a stronger check than the canary, which runs sequentially. Shared mutable state between seeds (a package-level cache, say) is much more likely to show up here.

### Progress and summary

```go
// toys3/cmd/toys3sim/main.go:92-103
	progress := func(final bool) {
		mu.Lock()
		defer mu.Unlock()
		el := time.Since(start)
		n := done.Load()
		label := "progress"
		if final {
			label = "SUMMARY"
		}
		fmt.Printf("%s %v: seeds=%d (%.0f/s) steps=%d determinism-replays=%d failures=%d workers=%d\n  %s\n",
			label, el.Round(time.Second), n, float64(n)/el.Seconds(), steps.Load(), detRuns.Load(), len(failures), *workers, agg)
	}
```

A ticker prints `progress` every ten seconds (`toys3/cmd/toys3sim/main.go:104-115`); when all workers finish, a final `SUMMARY` line is printed (`toys3/cmd/toys3sim/main.go:117`). Then:

```go
// toys3/cmd/toys3sim/main.go:118-125
	if len(failures) > 0 {
		fmt.Printf("%d failing seed(s); reproduce with:\n", len(failures))
		for _, f := range failures {
			fmt.Printf("  go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed)
		}
		os.Exit(1)
	}
	fmt.Println("all seeds passed")
```

Exit status 1 on any failure (including nondeterminism), 0 otherwise. That is what makes it usable in a scheduled CI job.

### A real short run

This was run once while updating this chapter for commit `a4018d5` (two workers, 15 seconds; the first edition's run, against `210c1fa`, got through 7,240 seeds and printed the same kind of line under the old key names `halts=` and `corruptobj=`):

```
$ go run ./toys3/cmd/toys3sim -duration 15s -workers 2 -check-determinism
progress 10s: seeds=4961 (496/s) steps=8524197 determinism-replays=496 failures=0 workers=2
  reqs=1160932 200=825531 400=21780 404=166206 500=26142 indet=121273 fsops=2808841 eio=26695 crashes=42881 trips=17143 midput=36667 torn=2168 lost=36463 kept=33893 readcorrupt=1588 corruptdet=1568 tempsrm=25530 windows=174680 maxwin=28 widened=5
SUMMARY 15s: seeds=7428 (495/s) steps=12771088 determinism-replays=742 failures=0 workers=2
  reqs=1739198 200=1237219 400=32452 404=248474 500=39270 indet=181783 fsops=4207767 eio=40101 crashes=64230 trips=25710 midput=54834 torn=3274 lost=54753 kept=50889 readcorrupt=2368 corruptdet=2344 tempsrm=38423 windows=260865 maxwin=28 widened=9
all seeds passed
```

64,230 simulated power losses in fifteen seconds on two cores.

Two things are visible here that the 200-seed runs never showed. First, `maxwin=28` (the 200-seed sweep peaked at 23): bigger campaigns find bigger windows, and the WGL search cost grows with window size, so keep an eye on this number. Second, `widened=9`: over 7,428 seeds, the oracle had to widen keys to "unknown" nine times (Chapter 13). Widening is rare but real, and each one is a moment where the check was weaker. If `widened` ever grows into the thousands, the quiesce interval or the key-group cap needs tuning.

## 16.4 Reading the summary line, field by field

The summary line has two parts: the campaign header (from `toys3sim`) and the aggregated `Stats` (from `Stats.String`, `toys3/sim/config.go:106-113`). We will use the 200-seed aggregate from the preface as the running example, because you can reproduce it exactly with `go test ./toys3/sim -run 'TestSeeds$' -seeds=200 -v`:

```
reqs=47270 200=33446 400=887 404=6842 500=1035 indet=5060 fsops=113656 eio=1061 crashes=1691 trips=673 midput=1473 torn=87 lost=1539 kept=1350 readcorrupt=71 corruptdet=70 tempsrm=1006 windows=6708 maxwin=23 widened=0
```

### The header (campaign runner only)

| Field | Example | Meaning |
|---|---|---|
| label | `progress` / `SUMMARY` | Periodic tick or the final line. |
| elapsed | `15s` | Wall-clock time since start, rounded to the second. |
| `seeds` | `7240 (482/s)` | Seeds completed (not started), and the rate. Determinism replays are *not* counted as seeds. |
| `steps` | `12453061` | Sum of `Result.Steps` over completed seeds: queue entries popped and processed. A useful unit of "simulation work done". About 1,700 steps per seed here. |
| `determinism-replays` | `724` | How many seeds were run a second time. With `-det-every 10`, about a tenth of `seeds`. |
| `failures` | `0` | Failing seeds recorded so far, including nondeterminism. |
| `workers` | `2` | The `-workers` value. |

### The `Stats` fields

The struct is at `toys3/sim/config.go:61-80`; each field is incremented in a specific place, listed below so you can trust what the number means.

**`reqs=47270`**: `Stats.Requests`, incremented in `Simulator.wake` each time a client issues a request (`toys3/sim/simulator.go:250`). It counts *issued* requests, including those that will be abandoned by a crash and those that will get a 400. About 236 per seed.

**`200=33446 400=887 404=6842 500=1035`**: `Stats.ByStatus`, indexed by `statusIndex` (`toys3/sim/config.go:116-126`), incremented in `Simulator.reply` (`toys3/sim/simulator.go:236`) for every reply delivered to a client. Note `statusIndex` maps *any* status that is not 200, 400 or 404 into the "500" bucket. The four buckets sum to 42,210, which is `reqs` minus the requests that never got a reply.
- `400` comes from the workload's `Invalid` mix weight (`OpMix.Invalid`, empty keys; `toys3/sim/workload.go:109-115`). About 2% of requests, matching weight 2 of a total 107.
- `404` comes from GETs, DELETEs and COPYs of absent keys. It is high because keys are often absent: deletes, lost un-synced creates, and the start of every run.
- `500` comes from injected EIOs, from corrupt reads (`CorruptionDetected`), and, importantly, never from anything issued in the Settling phase (the `liveness` invariant enforces that).

**`indet=5060`**: `Stats.Indeterminate` (spelled `Indetermined` before the refactor), incremented in `Simulator.crash` once per request that was outstanding at the crash (`toys3/sim/simulator.go:283-284`). These are the oracle's `Abandoned` ops: the client never learned what happened. Check: `200+400+404+500 + indet = 42210 + 5060 = 47270 = reqs`. Every issued request either got exactly one reply or was abandoned by exactly one crash. That identity is a cheap sanity check on the simulator itself; if it ever fails, requests are leaking or being double-counted.

**`fsops=113656`**: `FSCounters.Landed` (`toys3/sim/storagemodel.go:167`), copied into `Stats.FSOps` at the end of the run (`toys3/sim/simulator.go:115`). It counts filesystem actions that *landed* (took effect in the StorageModel). Actions issued but dropped by a crash before landing are not counted. About 2.4 FS operations per request: a GET is one, a PUT is four, recovery adds a list, some unlinks and an fsync per boot.

**`eio=1061`**: `FSCounters.EIOs` (`toys3/sim/storagemodel.go:197-199`): landed actions that completed with `ErrIO`. Around 0.9% of landed actions, consistent with a swarm mix of EIO rates between 0.2% and 3%. EIOs are only *planned* during Chaos (`StorageModel.Plan`, `toys3/sim/storagemodel.go:149`), but a planned failure can land just after Chaos ends; see Chapter 17.

**`crashes=1691`**: `Result.Crashes`, incremented in `Simulator.crash` (`toys3/sim/simulator.go:299`), copied to `Stats.Crashes`. Includes Trips. About 8.5 per seed.

**`trips=673`**: `Stats.Trips` (printed as `halts=` and named `Stats.Halts` before the refactor), incremented when the server returns a `Trip` action (`toys3/sim/simulator.go:205`). A subset of `crashes`. 40% of all crashes here were the server choosing to fail-stop after an error (Chapter 7); the rest were injected by `maybeCrash`.

**`midput=1473`**: `Stats.CrashesMidPut`, incremented once per crash during which at least one PUT or COPY was outstanding (`toys3/sim/simulator.go:282-295`). This is the single most important coverage number for a storage system: it says how often the adversary cut the power in the middle of a write. 87% of crashes here hit a write in progress, because writes are the long-running operations (four FS steps each).

**`torn=87`**: `CrashReport.Torn` summed (`toys3/sim/simulator.go:278`): files whose un-synced content a crash cut to a strict prefix (`toys3/sim/storagemodel.go:506-508`). Only files with dirty content and a name can tear, and only with probability `TornWriteProb` (0.3). Low in absolute terms because a correct server keeps dirty files short-lived; these are almost all temp files that recovery then deletes.

**`lost=1539`** and **`kept=1350`**: un-synced changes that a crash discarded (`lost`) or happened to make durable anyway (`kept`) (`toys3/sim/storagemodel.go:472-514`). They include both directory-entry changes and file contents. `lost` also counts every fsyncgate-poisoned inode (`toys3/sim/storagemodel.go:503-504`). Roughly a 50/50 split, as expected from `rng.Intn(2)`. Both must be non-zero for the fault model to be adversarial in both directions; `TestCoverage` enforces that.

**`readcorrupt=71`**: `FSCounters.ReadCorruptions` (`toys3/sim/storagemodel.go:399-402`): FSReads that returned one flipped bit.

**`corruptdet=70`**: `Stats.CorruptionsDetected` (printed as `corruptobj=` and named `Stats.CorruptObjects` before the refactor), incremented when the server emits a `CorruptionDetected` telemetry (`toys3/sim/simulator.go:195-196`). Why 70 and not 71? A read corruption is counted when the read *lands*; the server only sees it when the completion is *delivered*. If a crash happens in between, the corrupted completion is dropped and the server never emits the fact. One of the 71 was in that window. In the 15-second campaign the gap was 2,318 versus 2,294. With the correct server, `corruptdet` can never exceed `readcorrupt`; `TestCorruptionUnreachableWithoutBitFlips` checks the extreme case (with zero bit flips, zero corrupt objects).

**`tempsrm=1006`**: sum of `Recovered.TempsRemoved` (`toys3/sim/simulator.go:197-198`): leftover temp files that recovery unlinked successfully. Notice it is *less* than `crashes` (1,691): many crashes leave no temp file behind (a crash during a GET, or after a temp was already renamed, or one whose temp create was itself lost). Notice also that mutant f (skip temp cleanup) produces `tempsrm=0` in every run, which is a handy way to confirm the mutant is actually active (Chapter 15).

**`windows=6708`**: `OracleStats.Windows`: how many linearizability windows the oracle searched (`toys3/sim/oracle.go:369`). A window is checked at each quiescent point and each crash. About 34 per seed.

**`maxwin=23`**: `OracleStats.MaxWindowOps`, the most operations in any single window. **This one is a max, not a sum**: `Stats.Add` uses `max` for it (`toys3/sim/config.go:101`). It is the number to watch for oracle cost, because WGL search is exponential in the worst case.

**`widened=0`**: `OracleStats.Widenings` (`toys3/sim/oracle.go:432,480`): times the oracle gave up precision on some keys because a component would have started from more than 4,096 possible states. Zero in the first 200 seeds, nine in 7,428. Widening never causes a false failure, but it can hide a real one for a moment.

## 16.5 Getting the full event log of any seed

`TestSeeds -seed=S` only dumps the log when the seed fails. When you want to *study* a passing seed (and you should; reading a few healthy logs is how you learn what normal looks like), you need a few lines of your own code. There are two clean ways that do not involve editing the simulator.

### Option 1: a throwaway test in the package

Create, say, `toys3/sim/zz_trace_test.go` (and do not commit it):

```go
package sim

import (
	"fmt"
	"os"
	"testing"
)

func TestDumpSeed(t *testing.T) {
	seed := uint64(*flagSeed)
	cfg := DefaultConfig(seed)
	cfg.Log = true
	r := Run(cfg)
	fmt.Fprintf(os.Stderr, "keys=%d clients=%d crash=%v eio=%v\n%s\n",
		cfg.Workload.Keys, cfg.Workload.Clients, cfg.CrashProb, cfg.Disturbances.EIOProb, r.Stats)
	fmt.Print(FormatLog(r.Log))
}
```

Run it with `go test ./toys3/sim -run TestDumpSeed -seed=1 -v > seed1.log`. It reuses the existing `-seed` flag. Because it is inside the package, it can also reach unexported things like `fmtTime` and the oracle's `History()`.

### Option 2: an external module with a `replace` directive

This is how the traces in this book were produced, without touching the repository at all. In a scratch directory:

```
// go.mod
module tracer

go 1.24.0

require manifester v0.0.0

replace manifester => /path/to/tmpdst
```

```go
// main.go
package main

import (
	"flag"
	"fmt"
	"os"

	"manifester/toys3/sim"
)

func main() {
	seed := flag.Uint64("seed", 1, "")
	flag.Parse()
	cfg := sim.DefaultConfig(*seed)
	cfg.Log = true
	r := sim.Run(cfg)
	fmt.Fprintf(os.Stderr, "seed %d steps=%d crashes=%d trace=%016x\n%s\n",
		*seed, r.Steps, r.Crashes, r.TraceHash, r.Stats)
	for _, f := range r.Failures {
		fmt.Fprintf(os.Stderr, "FAIL %s step %d: %s\n", f.Invariant, f.Step, f.Msg)
	}
	fmt.Print(sim.FormatLog(r.Log))
}
```

Copy `go.sum` from the repository, then `go build -o tracer . && ./tracer -seed 1 > seed1.log`. Seed 1's log is 1,338 lines.

The external module can only use exported API: `sim.Run`, `sim.DefaultConfig`, `sim.BaseConfig`, `Config.Factory`, `Config.Invariants`, `sim.BlackBoxInvariants`, `sim.FormatLog`. That is enough for nearly everything, including plugging in your own broken server through `Config.Factory`, which is exactly what `DESIGN.md:238-239` invites. (The mutants themselves live in a `_test.go` file and cannot be imported; to replay a mutant externally you copy its code into your module. That is how the mutant traces in Chapter 15 were made.)

## 16.6 Reading an event log

`LogEntry.String` (`toys3/sim/log.go:119-138`) renders each entry as

```
#<step> t=<virtual time> inc=<incarnation> <kind> [<event>] [=> <actions>] [<note>]
```

with the time printed by `fmtTime` (`toys3/sim/log.go:151-153`) as milliseconds with six decimal places, i.e. nanosecond precision. A real excerpt from seed 1:

```
#1      t=0.000000ms   inc=1 restart     boot
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#4      t=0.444359ms   inc=1 deliver     Request{c1 GET "k2"}
#6      t=0.523067ms   inc=1 deliver     Request{c2 GET "k0"}
#7      t=0.665229ms   inc=1 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK []}
#9      t=0.745782ms   inc=1 deliver     Request{c3 PUT "" value-of-3} => Reply{c3 400}
#10     t=0.748746ms   inc=1 deliver     FSListDone{io1 OK []} => FSFsyncDir{io2 /data/tmp}
...
#16     t=1.947497ms   inc=1 deliver     FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSRead{io3 /data/obj/k2} FSRead{io4 /data/obj/k0}
```

How to read it:

- **Step numbers skip.** Steps 3, 5 and 8 are missing. Every popped queue entry is a step, but only some kinds log a line: `client-wake` entries never log (`Simulator.wake` writes nothing), so a request shows up only when its `deliver` step runs. Seed 1 has 1,628 steps but its log's last line is step 1,627; the final step was a client wake.
- **`deliver` lines** show the event the server consumed and, after `=>`, every action it returned, in order. `Request{c1 GET "k2"}` with nothing after it means the request was *queued* (here, behind recovery). Steps 4 and 6 are requests arriving during recovery; step 16 shows them starting the moment recovery ends.
- **`fs-land` lines** show an FS action taking effect in the StorageModel and the completion it produced (`land X -> Y`). The completion is delivered later, as a separate `deliver` line. The gap between them is the feedback delay, `ActuationPlan.FeedbackDelay` (Chapter 10).
- **`crash` lines** carry the crash report: `CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries`. The reason in parentheses is either `injected crash` or `trip: <cause>`.
- **`restart` lines** say `boot` and carry the new incarnation number. The next line is always the `Started` delivery for that incarnation.
- **`quiesce`, `settle`, `settling-end`** have `inc=0` because they are not addressed to a server incarnation.
- **Failures** are appended to the log with kind `crash` and a note starting `FAILURE` (`toys3/sim/simulator.go:371-374`), because `fail` reuses `KindCrash` for its log entry. So the last line of a failing log reads `inc=3 crash FAILURE linearizable: ...` even though no crash happened at that step. Chapter 17 lists this as a cosmetic wart.
- **Payloads are named, not dumped.** `obj(value-of-26)` is an object file whose payload is the value written by PUT request 26. `raw(29 bytes)` is something that failed to decode as an object (for example, a bit-flipped read). `garbage(13 bytes)` in a history op means a body no PUT wrote. These come from `describePayload` (`toys3/sim/log.go:30-44`) and `ValueWriter`; Chapter 12 explains why self-describing values make this possible.
- **IOIDs restart per incarnation.** `io1` in inc 2 is unrelated to `io1` in inc 1. Temp file names are the IOID in 16 hex digits, so `/data/tmp/0000000000000031` was written by `io49` (0x31) of some incarnation. Chapter 15's mutant f story turns on exactly this reuse.

### A reading technique that works

When a seed fails, do not start at the top. Start at the bottom:

1. Read the `FAIL` lines. Note the invariant, the step, and any request ids or keys in the message.
2. `grep` the log for that key (`grep -n '"k1"\|/k1' seed.log`). This collapses a 1,500-line log to the 30 lines that matter.
3. Find every `crash` line in that range. Most durability bugs need a crash between two observations.
4. For the decisive steps, read the *full* surrounding window (`sed -n 250,310p seed.log`) to see what else was in flight.
5. Write the story in three sentences. If you cannot, you have not understood the bug yet.

Chapter 7 (d2) and Chapter 15 (f) are worked examples of exactly this procedure.

## 16.7 Bisecting, shrinking, and keeping failing seeds

### Bisecting a regression

A seed is a fixed input, so it bisects beautifully. If seed 4711 passed at commit A and fails at commit B:

```sh
git bisect start B A
git bisect run sh -c 'go test ./toys3/sim -run TestSeeds -seed=4711 >/dev/null'
```

`go test` exits non-zero exactly when the replay records an error. One caveat that is specific to DST: **a change that alters the number of random draws reshuffles every later event in every seed.** A commit that merely adds one more `rng.Intn` call in the workload changes seed 4711's universe completely. So "seed 4711 started failing at commit X" can mean "commit X introduced the bug" or "commit X moved the universe so that a pre-existing bug became reachable at this seed". When bisecting lands on a commit that seems unrelated, re-run a few hundred seeds at its parent: if *some other* seed fails there, the bug is older.

This sensitivity is also why the trace hash is a fingerprint of a commit's *behaviour*: any change in RNG consumption changes the hash of nearly every seed.

### Shrinking by hand

toys3 has no automatic shrinker (property-testing frameworks call it shrinking; FoundationDB and others reduce failing runs by re-running with fewer clients or fewer faults). You can still shrink manually, by holding the seed fixed and turning knobs in a throwaway config:

- `cfg := sim.BaseConfig(seed)` instead of `DefaultConfig` removes the swarm variation.
- `cfg.Workload.Clients = 1` removes concurrency. If the bug survives, it is not a concurrency bug.
- `cfg.CrashProb = 0` and `cfg.Disturbances.EIOProb = 0` remove a fault family. If the bug disappears, that family is necessary.
- `cfg.Invariants = sim.BlackBoxInvariants()` tells you whether a client could ever notice.

Caveat as above: each knob changes RNG consumption, so the *same* seed is now a different universe. What you are looking for is "does the bug still happen *somewhere* in the first few hundred seeds under this restriction", which you can answer with a small loop that calls `sim.Run` for seeds 1..N. That is how the mutant f ablation in Chapter 15 was measured.

### Keeping failing seeds as regression tests

`loop.md` rule 15 says to "keep failing seeds as regression tests". In toys3 there is no explicit list of regression seeds today; the fixed sweep of seeds 1..200 in `TestSeeds` and `TestCoverage` plays that role, and the mutants in `TestCatchesBugs` record the first catching seed in `DESIGN.md`'s table. If you find a bug with a campaign at seed 3,812,077, the cheap way to keep it is a table-driven test:

```go
func TestRegressionSeeds(t *testing.T) {
	for _, seed := range []uint64{3812077 /* describe the bug here */} {
		if r := Run(DefaultConfig(seed)); len(r.Failures) > 0 {
			t.Errorf("seed %d: %s: %s", seed, r.Failures[0].Invariant, r.Failures[0].Msg)
		}
	}
}
```

Know its limit: because of the RNG-consumption sensitivity above, a regression seed protects against the bug only as long as the universe it names stays the same. After a change that alters RNG consumption, the seed still runs, but it no longer exercises the original scenario. The durable form of a regression test for a protocol bug is a **mutant** (Chapter 15): a wrapper that reintroduces the bug, plus the assertion that the simulator catches it within N seeds. Mutants survive refactors that seeds do not.

## 16.8 CI: what runs on every push

The workflow is short enough to quote whole:

```yaml
# .github/workflows/determinism.yml
name: determinism

on:
  push:
  pull_request:

jobs:
  determinism:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: go vet ./...
      - run: go test ./...
      - run: scripts/ci-determinism.sh
```

Three steps:

1. **`go vet ./...`**: ordinary hygiene.
2. **`go test ./...`**: every test in the module, which for toys3 includes the full `go test ./toys3/sim` (canary, 200-seed sweep, coverage, CRC reachability, all mutants), `toys3/control`'s exact-sequence tests, and `toys3/prod`'s end-to-end HTTP tests on a real temp directory. Also `tools/detcheck`'s own analyzer test.
3. **`scripts/ci-determinism.sh`**: the determinism gate, covered in Chapter 8. It runs detcheck over `./core/... ./sim/...` and every toys3 package except `prod` and the commands (derived with `go list`, so today `toys3/control`, `toys3/format` and `toys3/sim`), checks that every third-party module those packages depend on has an audit record, and runs `TestDeterminismCanary` in every simulator package that has one. A run while writing this chapter:

```
$ scripts/ci-determinism.sh
== detcheck
== dependency audit records
== determinism canary
skipped ./sim/...: TestDeterminismCanary not implemented yet
ok  	manifester/toys3/sim	0.540s
determinism gate passed
```

The canary is run twice in CI (once inside `go test ./...`, once in the gate). That redundancy is deliberate: the gate is meant to be runnable alone, locally, before pushing.

What CI does *not* do is run a long campaign. A campaign is open-ended by nature and belongs in a scheduled job (nightly, or on a dedicated machine), with its exit status wired to an alert. A reasonable layout, in increasing cost:

| When | What | Budget |
|---|---|---|
| Every save | `go test ./toys3/sim` | ~5 s |
| Every push | the workflow above | ~1 min |
| Nightly | `toys3sim -duration 30m -check-determinism -start-seed $(date +%s)` | 30 min × cores |
| Before a release | a long campaign from seed 1 plus the mutants at `-mutant-seeds=5000` | hours |

Using a time-derived `-start-seed` for the nightly job means each night explores fresh universes instead of re-running the same ones; the printed repro line still contains the absolute seed, so nothing is lost.

## 16.9 The ten-minute campaign

The orchestrating session for this book ran the campaign recommended by `DESIGN.md:231`:

```sh
go run ./toys3/cmd/toys3sim -duration 10m -check-determinism
```

on the author's machine (all cores, `-det-every 10`). Its final output is pasted below. When you read it, apply section 16.4 field by field, and ask three questions: did any seed fail; did any determinism replay disagree; and did every fault family fire (are `torn`, `readcorrupt`, `trips`, `midput`, `tempsrm` all non-zero)? Then compare `maxwin` and `widened` with the 15-second run above: a larger campaign should push both upward, and that is where you learn how much headroom the oracle has.

<!-- CAMPAIGN-RESULTS -->
### The 10-minute campaign (2026-10-12)

Command: `go run ./toys3/cmd/toys3sim -duration 10m -check-determinism -start-seed 1000000` on 8 workers, against commit `210c1fa`.

*A note on this output.* This campaign was run before the domain-driven-design refactor of Chapter 3a, so the line it actually printed said `halts=950432` and `corruptobj=88992`. The keys below have been rewritten to their current names (`trips=`, `corruptdet=`), and nothing else has been changed. The values are still valid for the current code: the refactor changed no behaviour (Chapter 8, §8.12, including a golden test that pins the stats of 550 seeds), so the same command against `a4018d5` would explore exactly the same seeds with exactly the same results, up to how many seeds fit in ten minutes on a given machine. The 10-minute campaign was not re-run for this edition.

```
SUMMARY 10m0s: seeds=272755 (455/s) steps=471589953 determinism-replays=27276 failures=0 workers=8
  reqs=64239570 200=45638953 400=1198872 404=9282926 500=1454896 indet=6663923 fsops=155434729 eio=1482376 crashes=2358979 trips=950432 midput=2008744 torn=117706 lost=2020518 kept=1881167 readcorrupt=89856 corruptdet=88992 tempsrm=1411446 windows=9634731 maxwin=34 widened=375
all seeds passed
```

Every seed passed, and all 27,276 determinism replays reproduced their trace hash exactly. Read the line with the field-by-field guide earlier in this chapter. Note `widened=375`: in 375 of 9.6M windows the oracle hit its state budget and weakened its check on some keys.


## Key takeaways

- There are two drivers and one atom. `TestSeeds` (replay, sweep, timed sweep) and `toys3sim` (parallel campaign) both call exactly `sim.Run(sim.DefaultConfig(seed))`; that is what makes every campaign failure reproducible with one `go test` command.
- Parallelism is across seeds, never within a seed. Nothing is shared between concurrent seeds, which is why detcheck bans package globals in the simulator.
- Determinism is checked three ways: the canary (12 seeds, sequential, plus "logging does not perturb" and "different seeds differ"), sampled replays in campaigns (every K-th seed, concurrent), and the trace hash itself.
- Read the stats line as a coverage report, not just a pass/fail. `midput`, `torn`, `lost`/`kept`, `readcorrupt`, `trips` and `tempsrm` tell you the adversary reached the places it claims to; `maxwin` and `widened` tell you how hard the oracle is working.
- `reqs = 200 + 400 + 404 + 500 + indet` is an identity worth checking; `corruptdet ≤ readcorrupt` is another.
- Seeds bisect, but RNG-consumption changes reshuffle universes. Keep durable regressions as mutants, not just as seed numbers.

## Exercises

1. **Verify the identity.** Run `go test ./toys3/sim -run 'TestSeeds$' -seeds=500 -v` and check that the four status buckets plus `indet` equal `reqs`. Then explain, from the code, why a request can never be counted in both a status bucket and `indet`. (Look at `Workload.Crash`, `toys3/sim/workload.go:135-144`, and `Oracle.Return`.)
2. **Make the canary fire.** In a scratch copy (or a branch you will throw away), make `Simulator.note` draw one random number when `cfg.Log` is true. Predict which assertion of `TestDeterminismCanary` fires. Run it. Revert.
3. **Make the other canary fire.** Change `New` to use `NewRand(1)` instead of `NewRand(cfg.Seed)`. Predict which assertion fires first and why the first assertion (same seed, same run) still passes. Revert.
4. **Dump a passing seed.** Using option 1 or option 2 of section 16.5, dump the log of seed 2. Find the first `crash` line. Using only the log, list every request that was outstanding at that crash, and check your count against the `dropped N queued entries` note. (They measure different things; explain the difference.)
5. **Profile the oracle.** Run `toys3sim -duration 30s -workers 1` twice: once as is, once after changing `QuiesceEvery` in `BaseConfig` from 8 ms to 32 ms. Compare `seeds/s`, `maxwin` and `widened`. Explain the trade-off you just measured.
6. **Shard a campaign.** Run two `toys3sim` processes at once with `-start-seed 1` and `-start-seed 1000000` and `-workers` set to half your cores each. Why is it safe to add their `seeds` counts but not to compare their `maxwin` values as if they came from the same population?
7. **Bisect a planted bug.** On a throwaway branch, make three commits: an innocent refactor, the mutant-a change applied to the real server (drop the `FSFsyncDir(obj)` step), another innocent change. Use `git bisect run` with `go test ./toys3/sim -run TestSeeds -seed=1` to find the bad commit.

## Further reading

- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014: the operational side of running simulation on many cores continuously.
- Alex Groce et al., *"Swarm Testing"*, ISSTA 2012: why the per-seed configuration in `DefaultConfig` finds more bugs than any fixed configuration.
- The `git bisect run` documentation (`git help bisect`).
- Antithesis documentation on "Sometimes" assertions and property coverage: the generalization of `TestCoverage`.
- TigerBeetle's VOPR documentation: an example of a seed-driven fuzzer with a long-running, scheduled campaign infrastructure.
