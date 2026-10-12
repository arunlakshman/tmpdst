// Command toys3sim runs a DST campaign against the toys3 server: many seeds of
// toys3/sim, in parallel, for a fixed wall-clock budget.
//
//	go run ./toys3/cmd/toys3sim -duration 10m -check-determinism
//
// Every seed is an independent sim.Run (its own Rand, queue, fake filesystem
// and server), so seeds run on separate goroutines with no shared state;
// only the seed counter and the result tallies are shared, outside the
// simulation. Any failure prints the one-line repro command and makes the
// exit status non-zero.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"manifester/toys3/sim"
)

type failure struct {
	seed uint64
	msg  string
}

func main() {
	duration := flag.Duration("duration", time.Minute, "wall-clock budget")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "parallel seeds")
	startSeed := flag.Uint64("start-seed", 1, "first seed")
	checkDet := flag.Bool("check-determinism", false, "run every -det-every'th seed twice and compare traces")
	detEvery := flag.Uint64("det-every", 10, "with -check-determinism: replay every K-th seed")
	maxFailures := flag.Int("max-failures", 20, "stop early after this many failing seeds")
	flag.Parse()

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

	record := func(f failure) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, f)
		fmt.Printf("FAIL seed %d: %s\n  repro: go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed, f.msg, f.seed)
		if len(failures) >= *maxFailures {
			stop.Store(true)
		}
	}

	var wg sync.WaitGroup
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
	ticker := time.NewTicker(10 * time.Second)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
loop:
	for {
		select {
		case <-ticker.C:
			progress(false)
		case <-finished:
			break loop
		}
	}
	ticker.Stop()
	progress(true)
	if len(failures) > 0 {
		fmt.Printf("%d failing seed(s); reproduce with:\n", len(failures))
		for _, f := range failures {
			fmt.Printf("  go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed)
		}
		os.Exit(1)
	}
	fmt.Println("all seeds passed")
}
