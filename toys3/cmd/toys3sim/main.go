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
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
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

// options are the parsed command-line flags.
type options struct {
	duration    time.Duration
	workers     int
	startSeed   uint64
	checkDet    bool
	detEvery    uint64
	maxFailures int
}

func parseFlags(args []string, errOut io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("toys3sim", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.DurationVar(&o.duration, "duration", time.Minute, "wall-clock budget")
	fs.IntVar(&o.workers, "workers", runtime.GOMAXPROCS(0), "parallel seeds")
	fs.Uint64Var(&o.startSeed, "start-seed", 1, "first seed")
	fs.BoolVar(&o.checkDet, "check-determinism", false, "run every -det-every'th seed twice and compare traces")
	fs.Uint64Var(&o.detEvery, "det-every", 10, "with -check-determinism: replay every K-th seed")
	fs.IntVar(&o.maxFailures, "max-failures", 20, "stop early after this many failing seeds")
	err := fs.Parse(args)
	return o, err
}

// campaign is the shared tally of one run; the simulation itself shares
// nothing between seeds.
type campaign struct {
	o        options
	out      io.Writer
	next     atomic.Uint64
	done     atomic.Int64
	detRuns  atomic.Int64
	steps    atomic.Int64
	stop     atomic.Bool
	mu       sync.Mutex
	agg      sim.Stats
	failures []failure
	start    time.Time
	deadline time.Time
	// runSeed is sim.Run; tests replace it.
	runSeed func(seed uint64) sim.Result
}

func (c *campaign) record(f failure) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = append(c.failures, f)
	fmt.Fprintf(c.out, "FAIL seed %d: %s\n  repro: go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed, f.msg, f.seed)
	if len(c.failures) >= c.o.maxFailures {
		c.stop.Store(true)
	}
}

// checkReplay reruns seed and records a failure if the replay differs.
func (c *campaign) checkReplay(seed uint64, r sim.Result) {
	r2 := c.runSeed(seed)
	c.detRuns.Add(1)
	if r2.TraceHash != r.TraceHash || r2.RandCalls != r.RandCalls || r2.Steps != r.Steps {
		c.record(failure{seed, fmt.Sprintf("NONDETERMINISM: trace %016x vs %016x, rand %d vs %d, steps %d vs %d",
			r.TraceHash, r2.TraceHash, r.RandCalls, r2.RandCalls, r.Steps, r2.Steps)})
	}
}

// oneSeed runs a single seed and tallies it.
func (c *campaign) oneSeed(seed uint64) {
	r := c.runSeed(seed)
	c.steps.Add(int64(r.Steps))
	if len(r.Failures) > 0 {
		f := r.Failures[0]
		c.record(failure{seed, fmt.Sprintf("%s at step %d: %.300s", f.Invariant, f.Step, f.Msg)})
	}
	if c.o.checkDet && c.o.detEvery > 0 && seed%c.o.detEvery == 0 {
		c.checkReplay(seed, r)
	}
	c.mu.Lock()
	c.agg.Add(r.Stats)
	c.mu.Unlock()
	c.done.Add(1)
}

func (c *campaign) worker() {
	for !c.stop.Load() && time.Now().Before(c.deadline) {
		c.oneSeed(c.next.Add(1) - 1)
	}
}

func (c *campaign) progress(final bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el := time.Since(c.start)
	n := c.done.Load()
	label := "progress"
	if final {
		label = "SUMMARY"
	}
	fmt.Fprintf(c.out, "%s %v: seeds=%d (%.0f/s) steps=%d determinism-replays=%d failures=%d workers=%d\n  %s\n",
		label, el.Round(time.Second), n, float64(n)/el.Seconds(), c.steps.Load(), c.detRuns.Load(), len(c.failures), c.o.workers, c.agg)
}

// wait blocks until every worker has finished, printing progress every tick.
func (c *campaign) wait(wg *sync.WaitGroup, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	for {
		select {
		case <-ticker.C:
			c.progress(false)
		case <-finished:
			return
		}
	}
}

// exec runs the campaign and returns the process exit status.
func (c *campaign) exec(tick time.Duration) int {
	c.next.Store(c.o.startSeed)
	c.start = time.Now()
	c.deadline = c.start.Add(c.o.duration)
	var wg sync.WaitGroup
	for w := 0; w < c.o.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker()
		}()
	}
	c.wait(&wg, tick)
	c.progress(true)
	if len(c.failures) > 0 {
		fmt.Fprintf(c.out, "%d failing seed(s); reproduce with:\n", len(c.failures))
		for _, f := range c.failures {
			fmt.Fprintf(c.out, "  go test ./toys3/sim -run TestSeeds -seed=%d -v\n", f.seed)
		}
		return 1
	}
	fmt.Fprintln(c.out, "all seeds passed")
	return 0
}

// run parses args, runs the campaign writing to out, and returns the exit
// status.
func run(args []string, out io.Writer) int {
	o, err := parseFlags(args, out)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	c := &campaign{o: o, out: out, runSeed: func(seed uint64) sim.Result { return sim.Run(sim.DefaultConfig(seed)) }}
	return c.exec(10 * time.Second)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}
