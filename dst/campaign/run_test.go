package campaign

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/kernel"
)

// fake makes a scenario from a function. The function gets the seed and
// RunOptions. The result gets the seed as its hash when it has none, and it
// passes unless the function says otherwise.
func fake(name string, f func(seed uint64, o RunOptions) Result) Scenario {
	return Scenario{Name: name, Run: func(seed uint64, o RunOptions) Result {
		r := f(seed, o)
		if r.Hash == 0 {
			r.Hash = seed*7919 + 1
		}
		if r.Steps == 0 {
			r.Steps = seed + 10
		}
		return r
	}}
}

// pass is a scenario that passes every seed and reaches the paths a, b, ...
// with the number of seeds in grow.
func pass(name string) Scenario {
	return fake(name, func(uint64, RunOptions) Result {
		return Result{OK: true, Counters: map[string]uint64{"steps": 5}}
	})
}

func registry(t *testing.T, list ...Scenario) *Registry {
	t.Helper()
	reg := &Registry{}
	for _, s := range list {
		reg.Add(s)
	}
	return reg
}

func TestRegistry(t *testing.T) {
	reg := registry(t, pass("b"), pass("a"))
	if got := reg.Names(); !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("names %v: want the order of Add", got)
	}
	if _, ok := reg.Get("a"); !ok {
		t.Fatal("scenario a is missing")
	}
	if _, ok := reg.Get("zzz"); ok {
		t.Fatal("scenario zzz should not exist")
	}
	all, err := reg.Resolve([]string{"all"})
	if err != nil || !slices.Equal(all, []string{"b", "a"}) {
		t.Fatalf("all: %v %v", all, err)
	}
	if _, err := reg.Resolve([]string{"a", "zzz"}); err == nil || !strings.Contains(err.Error(), `unknown scenario "zzz"`) {
		t.Fatalf("unknown name: %v", err)
	}
}

func TestRegistryAddPanics(t *testing.T) {
	for name, s := range map[string]Scenario{
		"duplicate": pass("a"),
		"no name":   {Run: pass("x").Run},
		"no run":    {Name: "y"},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: Add did not panic", name)
				}
			}()
			registry(t, pass("a")).Add(s)
		}()
	}
}

func TestFromKernelNamesTheCounterAndKeepsFailures(t *testing.T) {
	r := FromKernel(kernelResult(), "")
	if r.OK || r.Counters["steps"] != 40 || r.Steps != 40 || r.RandCalls != 9 || r.Hash != 0xabc {
		t.Fatalf("result %+v", r)
	}
	if len(r.Failures) != 1 || r.Failures[0].Kind != "violation" || r.Failures[0].AtMs != 2500 || r.Failures[0].Repeats != 3 {
		t.Fatalf("failures %+v", r.Failures)
	}
	if c := r.Coverage["x"]; c.Kind != "sometimes" || c.Checks != 4 || c.Passes != 1 {
		t.Fatalf("coverage %+v", c)
	}
	if len(r.Tail) != 1 || !strings.Contains(r.Tail[0], "last step") || len(r.Log) != 0 {
		t.Fatalf("tail %v log %v", r.Tail, r.Log)
	}
}

// kernelResult is a failed kernel run with one failure, one coverage entry
// and one tail line.
func kernelResult() kernel.Result {
	return kernel.Result{
		Seed: 3, Steps: 40, RandCalls: 9, Hash: 0xabc,
		Failures: []kernel.Failure{{Kind: kernel.FailViolation, Property: "p", Detail: "d", Step: 7, At: kernel.Time(2500 * time.Millisecond), Repeats: 3}},
		Coverage: map[string]assert.Coverage{"x": {Kind: assert.KindSometimes, Checks: 4, Passes: 1}},
		Tail:     []kernel.LogEntry{{Step: 40, At: 1, Text: "last step"}},
	}
}

// ---- determinism re-run ----

// tracker counts how often each seed ran and with which Log setting.
type tracker struct {
	mu   sync.Mutex
	runs map[uint64][]bool // the Log value of each run of the seed
}

func (k *tracker) note(seed uint64, o RunOptions) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.runs == nil {
		k.runs = map[uint64][]bool{}
	}
	k.runs[seed] = append(k.runs[seed], o.Log)
	return len(k.runs[seed])
}

func TestDeterminismCheckRunsEveryNthSeedTwice(t *testing.T) {
	var k tracker
	reg := registry(t, fake("s", func(seed uint64, o RunOptions) Result {
		k.note(seed, o)
		return Result{OK: true}
	}))
	rep, err := RunCampaign(reg, Options{Scenarios: []string{"s"}, First: 1, Seeds: 10, CheckEvery: 3, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if rep.HasFailed() || rep.Failed != 0 {
		t.Fatalf("report %+v", rep)
	}
	for seed := uint64(1); seed <= 10; seed++ {
		due := (seed-1)%3 == 0 // seeds 1, 4, 7, 10
		got := k.runs[seed]
		if due && !slices.Equal(got, []bool{false, true}) {
			t.Errorf("seed %d ran with Log %v, want [false true]", seed, got)
		}
		if !due && len(got) != 1 {
			t.Errorf("seed %d ran %d times, want once", seed, len(got))
		}
	}
}

func TestDeterminismCheckOffByDefault(t *testing.T) {
	var k tracker
	reg := registry(t, fake("s", func(seed uint64, o RunOptions) Result {
		k.note(seed, o)
		return Result{OK: true}
	}))
	if _, err := RunCampaign(reg, Options{First: 1, Seeds: 6}); err != nil {
		t.Fatal(err)
	}
	for seed, runs := range k.runs {
		if len(runs) != 1 {
			t.Errorf("seed %d ran %d times", seed, len(runs))
		}
	}
}

// TestDeterminismCheckFindsADifferentSecondRun: the scenario gives another
// hash the second time it runs seed 4. The campaign fails that seed with the
// kind "nondeterminism" and lists it, and it still counts the first run.
func TestDeterminismCheckFindsADifferentSecondRun(t *testing.T) {
	var k tracker
	reg := registry(t, fake("s", func(seed uint64, o RunOptions) Result {
		n := k.note(seed, o)
		r := Result{OK: true}
		if seed == 4 && n == 2 {
			r.Hash = 12345
		}
		return r
	}))
	var out bytes.Buffer
	rep, err := RunCampaign(reg, Options{First: 1, Seeds: 10, CheckEvery: 3, Tool: "tool", Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.HasFailed() || rep.Failed != 1 || rep.Passed != 9 {
		t.Fatalf("report %+v", rep)
	}
	if len(rep.Nondeterminism) != 1 || rep.Nondeterminism[0].Seed != 4 || rep.Nondeterminism[0].Scenario != "s" {
		t.Fatalf("nondeterminism %+v", rep.Nondeterminism)
	}
	f := rep.FailedRuns[0]
	if f.Seed != 4 || f.Replay != "go run ./cmd/tool -scenario s -seed 4" || len(f.Failures) != 1 || f.Failures[0].Kind != KindNotRepeatable {
		t.Fatalf("failed run %+v", f)
	}
	if !strings.Contains(out.String(), "FAIL s seed 4: nondeterminism (determinism)") {
		t.Fatalf("progress text:\n%s", out.String())
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	if !strings.Contains(text.String(), "nondeterminism (two runs of one seed gave different results):\n  s seed 4:") {
		t.Fatalf("report text:\n%s", text.String())
	}
}

func TestDeterminismCheckComparesStepsAndRandCalls(t *testing.T) {
	for name, change := range map[string]func(*Result){
		"steps":      func(r *Result) { r.Steps++ },
		"rand calls": func(r *Result) { r.RandCalls++ },
	} {
		var k tracker
		reg := registry(t, fake("s", func(seed uint64, o RunOptions) Result {
			r := Result{OK: true, RandCalls: 5}
			if k.note(seed, o) == 2 {
				change(&r)
			}
			return r
		}))
		rep, err := RunCampaign(reg, Options{First: 1, Seeds: 1, CheckEvery: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Nondeterminism) != 1 {
			t.Errorf("%s: change not found: %+v", name, rep)
		}
	}
}

// ---- until stable ----

// growing is a scenario that reaches one more assertion at each seed in the
// list, and nothing new at the other seeds.
func growing(newAt ...uint64) Scenario {
	return fake("s", func(seed uint64, _ RunOptions) Result {
		cov := map[string]Coverage{"base": {Kind: "always", Checks: 1, Passes: 1}}
		for _, at := range newAt {
			if seed >= at {
				cov[fmt.Sprintf("path %d", at)] = Coverage{Kind: "reachable", Checks: 1, Passes: 1}
			}
		}
		return Result{OK: true, Coverage: cov}
	})
}

func TestUntilStableStopsWhenQuiet(t *testing.T) {
	reg := registry(t, growing(2, 5))
	rep, err := RunCampaign(reg, Options{First: 1, UntilStable: true, StableAfter: 20, MaxSeeds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	// The first batch has 50 seeds. The last new assertion is at seed 5, so
	// 45 seeds in a row were quiet, which is 20 or more.
	if rep.Seeds != 50 || rep.LastGrowth != 5 || rep.UntilStable == nil || rep.UntilStable.HitLimit {
		t.Fatalf("report %+v %+v", rep, rep.UntilStable)
	}
}

func TestUntilStableGoesOnInBatchesUntilQuiet(t *testing.T) {
	reg := registry(t, growing(70))
	rep, err := RunCampaign(reg, Options{First: 1, UntilStable: true, StableAfter: 60, MaxSeeds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	// After 50 seeds: 49 quiet. After 100: 30 quiet. After 150: 80 quiet.
	if rep.Seeds != 150 || rep.LastGrowth != 70 || rep.UntilStable.HitLimit {
		t.Fatalf("seeds %d last growth %d", rep.Seeds, rep.LastGrowth)
	}
}

func TestUntilStableWarnsWhenTheLimitCameFirst(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		// A new assertion at every seed: never stable.
		return Result{OK: true, Coverage: map[string]Coverage{fmt.Sprint("p", seed): {Kind: "reachable", Checks: 1, Passes: 1}}}
	}))
	rep, err := RunCampaign(reg, Options{First: 1, UntilStable: true, StableAfter: 10, MaxSeeds: 120})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Seeds != 120 || !rep.UntilStable.HitLimit {
		t.Fatalf("seeds %d until stable %+v", rep.Seeds, rep.UntilStable)
	}
	var b bytes.Buffer
	rep.WriteText(&b)
	if !strings.Contains(b.String(), "WARNING: the limit of 120 seeds came first. No 10 seeds in a row without a new assertion was reached") {
		t.Fatalf("no warning:\n%s", b.String())
	}
}

func TestUntilStableStopsAtAFailure(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		r := Result{OK: seed != 7, Coverage: map[string]Coverage{fmt.Sprint("p", seed): {Kind: "reachable", Checks: 1, Passes: 1}}}
		if !r.OK {
			r.Failures = []Failure{{Kind: "violation", Property: "p", Detail: "d"}}
		}
		return r
	}))
	rep, err := RunCampaign(reg, Options{First: 1, UntilStable: true, StableAfter: 1000, MaxSeeds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Seeds != 50 || rep.Failed != 1 || rep.UntilStable.HitLimit {
		t.Fatalf("report %+v", rep)
	}
}

func TestUntilStableSeesAssertionsOfAnyScenario(t *testing.T) {
	// Scenario b reaches a new path at seed 60; scenario a never does. The
	// campaign is stable only after both are quiet.
	a := fake("a", func(uint64, RunOptions) Result {
		return Result{OK: true, Coverage: map[string]Coverage{"base": {Kind: "always", Checks: 1, Passes: 1}}}
	})
	b := fake("b", func(seed uint64, _ RunOptions) Result {
		cov := map[string]Coverage{}
		if seed >= 60 {
			cov["late"] = Coverage{Kind: "reachable", Checks: 1, Passes: 1}
		}
		return Result{OK: true, Coverage: cov}
	})
	rep, err := RunCampaign(registry(t, a, b), Options{First: 1, UntilStable: true, StableAfter: 60, MaxSeeds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	// After 100 seeds the last new assertion is at seed 60: 40 quiet seeds.
	// After 150 seeds: 90 quiet seeds, which is 60 or more.
	if rep.LastGrowth != 60 || rep.Seeds != 150 || rep.Runs != 300 {
		t.Fatalf("report %+v", rep)
	}
}

// ---- time limit ----

func TestDurationRunsBatchesUntilTheTimeIsUp(t *testing.T) {
	reg := registry(t, pass("s"))
	t0 := time.Unix(1000, 0)
	calls := 0
	now := func() time.Time { // each call is one second later
		calls++
		return t0.Add(time.Duration(calls-1) * time.Second)
	}
	rep, err := runCampaign(reg, Options{First: 1, Seeds: 5, Duration: 3 * time.Second}, catalog{}, now)
	if err != nil {
		t.Fatal(err)
	}
	// The deadline is 3 s after the first call. Batches end at 1 s, 2 s and 3 s.
	if rep.Seeds != 3*batchSize {
		t.Fatalf("seeds %d, want %d (Seeds is replaced by Duration)", rep.Seeds, 3*batchSize)
	}
}

func TestDurationStopsAtAFailure(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		return Result{OK: seed != 3}
	}))
	now := func() time.Time { return time.Unix(0, 0) } // time never passes
	rep, err := runCampaign(reg, Options{First: 1, Duration: time.Hour}, catalog{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Seeds != batchSize || rep.Failed != 1 {
		t.Fatalf("report %+v", rep)
	}
}

// ---- the campaign as a whole ----

func TestRunCampaignFoldsInSeedOrderWhateverTheWorkers(t *testing.T) {
	hashOf := func(workers int) uint64 {
		var sum uint64
		reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
			time.Sleep(time.Duration(8-seed%8) * time.Millisecond)
			return Result{OK: true}
		}))
		var out bytes.Buffer
		if _, err := RunCampaign(reg, Options{First: 1, Seeds: 16, Workers: workers, Out: &out}); err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Sscanf(out.String()[strings.Index(out.String(), "hash ")+5:], "%x", &sum)
		return sum
	}
	if a, b := hashOf(1), hashOf(8); a != b || a == 0 {
		t.Fatalf("hash with 1 worker %x, with 8 workers %x", a, b)
	}
}

func TestRunCampaignUnknownScenario(t *testing.T) {
	_, err := RunCampaign(registry(t, pass("a")), Options{Scenarios: []string{"nope"}, Seeds: 1})
	if err == nil || !strings.Contains(err.Error(), `unknown scenario "nope"`) {
		t.Fatalf("error %v", err)
	}
}

func TestRunCampaignFoldsSitesAndCounts(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		return Result{OK: true, Sites: map[string]fault.SiteCount{"x": {On: seed%2 == 0, Calls: 3, Fired: 1}}}
	}))
	rep, err := RunCampaign(reg, Options{First: 1, Seeds: 4, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Sites) != 1 || rep.Sites[0] != (SiteRow{Name: "x", Reached: 4, On: 2, Calls: 12, Fired: 4}) {
		t.Fatalf("sites %+v", rep.Sites)
	}
}

func TestSummaryLine(t *testing.T) {
	sum := Summary{
		Counters: map[string]uint64{"steps": 480, "writes": 12, "acked": 3},
		Hash:     0x1234abcd5678, Counts: Counts{Seeds: 4, Failed: 1}, Elapsed: 1500 * time.Millisecond,
	}
	got := summaryLine("kv", 4, sum)
	want := "kv                 4 seeds        480 steps        3 acked       12 writes    1.5s  hash abcd5678  FAILED 1 of 4 seeds"
	if got != want {
		t.Fatalf("got:\n%q\nwant:\n%q", got, want)
	}
}

// ---- Main, flags and variables ----

func env(pairs ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return func(k string) string { return m[k] }
}

func TestParseOptionsDefaults(t *testing.T) {
	o, err := ParseOptions(Config{Tool: "t", EnvPrefix: "MDS"}, nil, env(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Seeds != 100 || o.First != 1 || !slices.Equal(o.Scenarios, []string{"all"}) || o.HasSeed ||
		o.StableAfter != 200 || o.MaxSeeds != 5000 || o.Report || o.Workers < 1 || o.Tool != "t" {
		t.Fatalf("options %+v", o)
	}
}

func TestParseOptionsFlags(t *testing.T) {
	args := []string{"-seeds", "7", "-start", "30", "-scenario", "a,b", "-j", "2", "-json", "-v",
		"-check-every", "4", "-stable-after", "9", "-max-seeds", "99", "-campaign", "2s"}
	o, err := ParseOptions(Config{Tool: "t"}, args, env(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Seeds != 7 || o.First != 30 || !slices.Equal(o.Scenarios, []string{"a", "b"}) || o.Workers != 2 ||
		!o.JSON || !o.Report || !o.Verbose || o.CheckEvery != 4 || o.StableAfter != 9 || o.MaxSeeds != 99 ||
		o.Duration != 2*time.Second {
		t.Fatalf("options %+v", o)
	}
	o, err = ParseOptions(Config{Tool: "t"}, []string{"-first", "12", "-until-stable"}, env(), &bytes.Buffer{})
	if err != nil || o.First != 12 || !o.UntilStable || !o.Report {
		t.Fatalf("options %+v err %v", o, err)
	}
}

func TestParseOptionsVariablesAndFlagsWin(t *testing.T) {
	c := Config{Tool: "t", EnvPrefix: "MDS"}
	vars := env("MDS_SEEDS", "40", "MDS_SEED", "17", "MDS_CAMPAIGN", "30s")
	o, err := ParseOptions(c, nil, vars, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Seeds != 40 || !o.HasSeed || o.Seed != 17 || o.Duration != 30*time.Second {
		t.Fatalf("options from variables %+v", o)
	}
	o, err = ParseOptions(c, []string{"-seeds", "5", "-seed", "3", "-campaign", "1m"}, vars, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Seeds != 5 || o.Seed != 3 || o.Duration != time.Minute {
		t.Fatalf("flags should win: %+v", o)
	}
	// A variable with a different prefix is not read, and an empty one is not set.
	o, err = ParseOptions(Config{Tool: "t", EnvPrefix: "OTHER"}, nil, env("MDS_SEEDS", "40", "OTHER_SEEDS", ""), &bytes.Buffer{})
	if err != nil || o.Seeds != 100 {
		t.Fatalf("options %+v err %v", o, err)
	}
}

func TestParseOptionsBadVariable(t *testing.T) {
	c := Config{Tool: "t", EnvPrefix: "MDS"}
	for _, kv := range [][2]string{{"MDS_SEEDS", "0"}, {"MDS_SEEDS", "x"}, {"MDS_SEED", "-1"}, {"MDS_CAMPAIGN", "soon"}, {"MDS_CAMPAIGN", "0s"}} {
		_, err := ParseOptions(c, nil, env(kv[0], kv[1]), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), kv[0]+" has the value") {
			t.Errorf("%s=%s: error %v", kv[0], kv[1], err)
		}
	}
}

func TestParseOptionsConfigDefaultsAndExtraFlags(t *testing.T) {
	var nodes int
	c := Config{
		Tool: "t",
		Defaults: Options{Seeds: 20, CheckEvery: 10, MinSeedsForCoverage: 100,
			Scenarios: []string{"kafka"}},
		ExtraFlags: func(fs *flag.FlagSet) { fs.IntVar(&nodes, "nodes", 3, "replicas") },
	}
	o, err := ParseOptions(c, []string{"-nodes", "5"}, env(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Seeds != 20 || o.CheckEvery != 10 || o.MinSeedsForCoverage != 100 || !slices.Equal(o.Scenarios, []string{"kafka"}) || nodes != 5 {
		t.Fatalf("options %+v nodes %d", o, nodes)
	}
}

// runMain runs Main with the given variables and returns the exit code and the output.
func runMain(t *testing.T, c Config, reg *Registry, vars func(string) string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := mainWith(c, reg, args, vars, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestMainJSONAndUntilStable(t *testing.T) {
	reg := registry(t, growing(2))
	c := Config{Tool: "tool"}
	code, out, errOut := runMain(t, c, reg, env(), "-seeds", "3", "-scenario", "s", "-json")
	if code != 0 {
		t.Fatalf("code %d: %s", code, errOut)
	}
	var rep Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("not json: %v\n%s", err, out)
	}
	if rep.Seeds != 3 || rep.Runs != 3 || rep.Passed != 3 || len(rep.Assertions) == 0 {
		t.Fatalf("report %+v", rep)
	}

	reg = registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		return Result{OK: true, Coverage: map[string]Coverage{fmt.Sprint("p", seed): {Kind: "reachable", Checks: 1, Passes: 1}}}
	}))
	code, out, errOut = runMain(t, c, reg, env(), "-until-stable", "-scenario", "s", "-stable-after", "50", "-max-seeds", "50")
	if code != 0 {
		t.Fatalf("code %d: %s", code, errOut)
	}
	if !strings.Contains(out, "WARNING: the limit of 50 seeds came first") {
		t.Fatalf("no warning:\n%s", out)
	}
	if strings.Contains(out, " seeds  ") && strings.Contains(out, "hash ") {
		t.Fatalf("until-stable should not print a summary line for each batch:\n%s", out)
	}
}

func TestMainTextOfACampaign(t *testing.T) {
	reg := registry(t, pass("a"), pass("b"))
	code, out, _ := runMain(t, Config{Tool: "tool"}, reg, env(), "-seeds", "4", "-j", "1", "-v")
	if code != 0 {
		t.Fatalf("code %d:\n%s", code, out)
	}
	for _, want := range []string{"a seed 1: steps=11 rand=0 hash=", "b seed 4:", "\na ", "\nb ", "coverage: all 0 expected paths were reached"} {
		if !strings.Contains(out, want) {
			t.Errorf("output has no %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "report:") || strings.Contains(out, "FAILED") {
		t.Fatalf("no report was asked for:\n%s", out)
	}
}

func TestMainFailureText(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, _ RunOptions) Result {
		if seed != 2 {
			return Result{OK: true}
		}
		return Result{Failures: []Failure{{Kind: "violation", Property: "p", Detail: "bad", Step: 3, AtMs: 20}}, Tail: []string{"tail line"}}
	}))
	code, out, _ := runMain(t, Config{Tool: "tool"}, reg, env(), "-seeds", "3", "-report")
	if code != 1 {
		t.Fatalf("code %d", code)
	}
	for _, want := range []string{
		"s                  3 seeds", "FAILED 1 of 3 seeds",
		"FAIL s seed 2: violation (p) step=3 at=20ms: bad",
		"  repro: go run ./cmd/tool -scenario s -seed 2",
		"    tail line",
		"report: 3 seeds, 3 runs, 2 passed, 1 failed",
		"  FAILED s seed 2: go run ./cmd/tool -scenario s -seed 2",
		"tool: FAILED",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output has no %q:\n%s", want, out)
		}
	}
}

func TestMainCoverageGapFailsOnlyInABigCampaign(t *testing.T) {
	reg := registry(t, pass("s"))
	cat := catalog{asserts: testCatalog.asserts}
	run := func(seeds int) Report {
		rep, err := runCampaign(reg, Options{First: 1, Seeds: seeds, MinSeedsForCoverage: 10}, cat, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	if rep := run(9); rep.HasFailed() || len(rep.NeverReached) == 0 {
		t.Fatalf("small campaign: failed=%v never reached=%v", rep.HasFailed(), rep.NeverReached)
	}
	if rep := run(10); !rep.HasFailed() {
		t.Fatal("a campaign of 10 seeds should fail on a coverage gap")
	}
}

func TestMainBadInput(t *testing.T) {
	reg := registry(t, pass("a"), pass("b"))
	c := Config{Tool: "tool", EnvPrefix: "MDS"}
	cases := []struct {
		name string
		vars func(string) string
		args []string
		want string
	}{
		{"unknown scenario", env(), []string{"-scenario", "zzz"}, `tool: unknown scenario "zzz"`},
		{"seed needs one scenario", env(), []string{"-seed", "3"}, "tool: -seed needs one -scenario"},
		{"bad variable", env("MDS_SEEDS", "many"), nil, "MDS_SEEDS has the value"},
		{"bad flag", env(), []string{"-nope"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		code, _, errOut := runMain(t, c, reg, tc.vars, tc.args...)
		if code != 2 || !strings.Contains(errOut, tc.want) {
			t.Errorf("%s: code %d stderr %q", tc.name, code, errOut)
		}
	}
	if code, _, _ := runMain(t, c, reg, env(), "-h"); code != 0 {
		t.Errorf("-h: code %d", code)
	}
}

func TestReplay(t *testing.T) {
	reg := registry(t, fake("s", func(seed uint64, o RunOptions) Result {
		r := Result{OK: true, Hash: 0xbeef, Steps: 2}
		if o.Log {
			r.Log = []string{"line 1", "line 2"}
		}
		if seed == 9 {
			r.OK = false
			r.Failures = []Failure{{Kind: "panic", Property: "p", Detail: "boom", Step: 2, AtMs: 1000}}
		}
		if !o.HaltOnFirst {
			t.Error("Replay should set HaltOnFirst")
		}
		return r
	}))
	var b bytes.Buffer
	if code := Replay(reg, "s", 5, &b); code != 0 {
		t.Fatalf("code %d", code)
	}
	if want := "line 1\nline 2\nseed 5: steps=2 rand=0 hash=000000000000beef\n"; b.String() != want {
		t.Fatalf("got %q, want %q", b.String(), want)
	}
	b.Reset()
	if code := Replay(reg, "s", 9, &b); code != 1 || !strings.Contains(b.String(), "FAIL panic (p) step=2 at=1s: boom") {
		t.Fatalf("code %d:\n%s", code, b.String())
	}
	b.Reset()
	if code := Replay(reg, "zzz", 1, &b); code != 2 {
		t.Fatalf("unknown scenario: code %d", code)
	}
}

func TestMainReplaysTheSeedOfTheVariable(t *testing.T) {
	reg := registry(t, fake("only", func(_ uint64, o RunOptions) Result {
		r := Result{OK: true}
		if o.Log {
			r.Log = []string{"a step"}
		}
		return r
	}))
	// One scenario in the registry: "all" means that one.
	code, out, errOut := runMain(t, Config{Tool: "tool", EnvPrefix: "MDS"}, reg, env("MDS_SEED", "4"))
	if code != 0 || !strings.HasPrefix(out, "a step\nseed 4:") {
		t.Fatalf("code %d out %q err %q", code, out, errOut)
	}
}
