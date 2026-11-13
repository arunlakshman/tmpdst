package campaign

import (
	"flag"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Options say what one campaign does.
type Options struct {
	Scenarios []string // names; empty or "all": every registered scenario
	First     uint64   // first seed of the campaign
	Seeds     int      // seeds to run for each scenario
	Workers   int      // goroutines; 0: GOMAXPROCS

	// Until-stable mode: run seeds in batches until StableAfter seed numbers
	// in a row add no new assertion, up to MaxSeeds. It implies Report.
	UntilStable bool
	StableAfter int
	MaxSeeds    int

	// Duration, when set, runs batches of seeds until this much time has
	// passed (or a seed fails). It replaces Seeds.
	Duration time.Duration

	// CheckEvery runs every Nth seed a second time and compares the two runs:
	// the determinism check. 0 switches it off.
	CheckEvery int

	// MinSeedsForCoverage is the campaign size from which a coverage gap
	// (a path never reached, an Always never checked) fails the campaign.
	// Below it, or when it is 0, a gap is only reported.
	MinSeedsForCoverage int

	// SummaryCounters says which counters the summary line of a scenario
	// shows after the steps, in this order, and how. A Format has one number
	// verb and the label, for example "%7d slots". When it is empty, the line
	// shows every counter, sorted by name, as "%7d name".
	SummaryCounters []SummaryCounter

	Report  bool // print the report after the campaign
	JSON    bool // print the report as JSON and nothing else
	Verbose bool // print one line for each seed

	// HasSeed and Seed ask for the replay of one seed (flag -seed, variable
	// SEED). Main replays it instead of running a campaign.
	HasSeed bool
	Seed    uint64

	// Tool is the command name. It makes the replay command of a failed seed
	// that has none.
	Tool string

	// Out, when it is not nil, receives the progress text of RunCampaign: one
	// summary line for each scenario and batch, and the details of each failed
	// seed. Main sets it. It is not read from flags.
	Out io.Writer
}

// SummaryCounter is one column of the summary line of a scenario.
type SummaryCounter struct {
	Key    string // the name of the counter in Result.Counters
	Format string // for example "%7d slots"
}

// Config is what a product gives the standard main.
type Config struct {
	// Tool is the command name, for replay lines ("cpsim").
	Tool string
	// EnvPrefix names the environment variables. With "MDS" they are MDS_SEEDS
	// (the number of seeds), MDS_SEED (one seed to replay) and MDS_CAMPAIGN
	// (a duration, for example 30s). A flag always wins over its variable.
	EnvPrefix string
	// Defaults are the values the flags start with. A zero field gets the
	// built-in default: 100 seeds, first seed 1, all scenarios, stable after
	// 200 seeds, at most 5000 seeds.
	Defaults Options
	// ExtraFlags, when it is set, adds the flags of the product to the flag
	// set. A scenario reads the values when it runs.
	ExtraFlags func(fs *flag.FlagSet)
}

// Built-in defaults of the flags.
const (
	defaultSeeds       = 100
	defaultFirstSeed   = 1
	defaultStableAfter = 200
	defaultMaxSeeds    = 5000
)

// ParseOptions reads the standard flags (-seeds, -first or -start, -seed,
// -scenario, -j, -report, -json, -v, -until-stable, -stable-after,
// -max-seeds, -campaign, -check-every) and the environment variables of
// Config.EnvPrefix. Flags win over variables. This is the one place that
// reads both. A bad variable is an error, because a silent fallback would
// hide a typing mistake.
func ParseOptions(c Config, args []string, getenv func(string) string, stderr io.Writer) (Options, error) {
	d := withBuiltInDefaults(c.Defaults)
	seedDefault := int64(-1)
	if c.EnvPrefix != "" {
		e, err := ReadEnv(c.EnvPrefix, getenv)
		if err != nil {
			return Options{}, err
		}
		if e.Seeds > 0 {
			d.Seeds = e.Seeds
		}
		if e.Duration > 0 {
			d.Duration = e.Duration
		}
		if e.HasSeed {
			seedDefault = int64(e.Seed)
		}
	}

	o := d
	o.Tool = c.Tool
	scenario := strings.Join(d.Scenarios, ",")
	seed := seedDefault
	fs := flag.NewFlagSet(c.Tool, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.IntVar(&o.Seeds, "seeds", d.Seeds, "seeds to run for each scenario")
	fs.Uint64Var(&o.First, "first", d.First, "first seed of a campaign")
	fs.Uint64Var(&o.First, "start", d.First, "same as -first")
	fs.Int64Var(&seed, "seed", seed, "run this one seed with a full step log (-1: run a campaign)")
	fs.StringVar(&scenario, "scenario", scenario, "scenario names, separated by commas, or all")
	fs.IntVar(&o.Workers, "j", d.Workers, "seeds to run in parallel (0: one for each CPU)")
	fs.BoolVar(&o.Report, "report", d.Report, "after the campaign, print a report: failed seeds, assertion counts, coverage curve")
	fs.BoolVar(&o.JSON, "json", d.JSON, "print the report as JSON and nothing else (implies -report)")
	fs.BoolVar(&o.Verbose, "v", d.Verbose, "print one line for each seed")
	fs.BoolVar(&o.UntilStable, "until-stable", d.UntilStable, "run seeds until no new assertion appears for -stable-after seeds, up to -max-seeds (implies -report)")
	fs.IntVar(&o.StableAfter, "stable-after", d.StableAfter, "with -until-stable: seeds in a row without a new assertion")
	fs.IntVar(&o.MaxSeeds, "max-seeds", d.MaxSeeds, "with -until-stable: the most seeds to run for each scenario")
	fs.DurationVar(&o.Duration, "campaign", d.Duration, "run seeds until this much time has passed, for example 30s (0: use -seeds)")
	fs.IntVar(&o.CheckEvery, "check-every", d.CheckEvery, "run every Nth seed twice to check that it repeats (0: off)")
	if c.ExtraFlags != nil {
		c.ExtraFlags(fs)
	}
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	if o.Workers <= 0 {
		o.Workers = runtime.GOMAXPROCS(0)
	}
	o.Scenarios = splitNames(scenario)
	if seed >= 0 {
		o.HasSeed, o.Seed = true, uint64(seed)
	}
	if o.JSON || o.UntilStable {
		o.Report = true
	}
	return o, nil
}

// withBuiltInDefaults fills the zero fields of d.
func withBuiltInDefaults(d Options) Options {
	if d.Seeds == 0 {
		d.Seeds = defaultSeeds
	}
	if d.First == 0 {
		d.First = defaultFirstSeed
	}
	if len(d.Scenarios) == 0 {
		d.Scenarios = []string{AllScenarios}
	}
	if d.StableAfter == 0 {
		d.StableAfter = defaultStableAfter
	}
	if d.MaxSeeds == 0 {
		d.MaxSeeds = defaultMaxSeeds
	}
	return d
}

func splitNames(s string) []string {
	var out []string
	for n := range strings.SplitSeq(s, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// EnvSettings are the values of the three variables <prefix>_SEEDS,
// <prefix>_SEED and <prefix>_CAMPAIGN. A variable that is not set (or is
// empty) leaves its field at the zero value.
type EnvSettings struct {
	// Seeds is the value of <prefix>_SEEDS: the number of seeds (1 or more).
	// 0: not set.
	Seeds int
	// HasSeed is true when <prefix>_SEED is set. Seed is its value (0 or more).
	HasSeed bool
	Seed    uint64
	// Duration is the value of <prefix>_CAMPAIGN, for example 30s. 0: not set.
	Duration time.Duration
}

// ReadEnv reads the variables of prefix with getenv. A bad value is an error,
// because a silent fallback would hide a typing mistake. An empty value counts
// as not set, so that "VAR=" in a shell is not a bad value. This is the one
// place that parses these variables: ParseOptions uses it, and so does the
// spec toolkit (dst/check/spectest) for its tests.
func ReadEnv(prefix string, getenv func(string) string) (EnvSettings, error) {
	e := envReader{prefix: prefix, getenv: getenv}
	var out EnvSettings
	var err error
	if out.Seeds, err = e.seeds(0); err != nil {
		return EnvSettings{}, err
	}
	if out.Duration, err = e.campaign(0); err != nil {
		return EnvSettings{}, err
	}
	seed, err := e.seed(-1)
	if err != nil {
		return EnvSettings{}, err
	}
	if seed >= 0 {
		out.HasSeed, out.Seed = true, uint64(seed)
	}
	return out, nil
}

// envReader reads one variable of a prefix.
type envReader struct {
	prefix string
	getenv func(string) string
}

func (e envReader) value(suffix string) (string, string) {
	name := e.prefix + "_" + suffix
	if e.getenv == nil {
		return name, ""
	}
	return name, e.getenv(name)
}

func (e envReader) seeds(def int) (int, error) {
	name, v := e.value("SEEDS")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, envError{fmt.Sprintf("%s has the value %q: it must be a whole number of 1 or more, for example 200", name, v)}
	}
	return n, nil
}

func (e envReader) seed(def int64) (int64, error) {
	name, v := e.value("SEED")
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, envError{fmt.Sprintf("%s has the value %q: it must be a whole number of 0 or more, for example 17", name, v)}
	}
	return n, nil
}

func (e envReader) campaign(def time.Duration) (time.Duration, error) {
	name, v := e.value("CAMPAIGN")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, envError{fmt.Sprintf("%s has the value %q: it must be a duration longer than zero, for example 30s or 5m", name, v)}
	}
	return d, nil
}

// DescribeSettings returns a short help text for the three variables of
// prefix. Docs can use it, so the text and the code do not drift apart.
func DescribeSettings(prefix string) string {
	return prefix + "_SEED=<n>        run only seed n (n is 0 or more); the strongest setting\n" +
		prefix + "_CAMPAIGN=<time> run seeds until the time ends, for example 30s\n" +
		prefix + "_SEEDS=<n>       run seeds 1 to n (n is 1 or more)\n"
}

// envError is a bad value in an environment variable. The flag package prints
// its own errors; this one is printed by Main.
type envError struct{ msg string }

func (e envError) Error() string { return e.msg }
