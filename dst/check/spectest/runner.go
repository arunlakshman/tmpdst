package spectest

import (
	"cmp"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/campaign"
	"pgregory.net/rapid"
)

// Check runs the lockstep check with rapid.Check. On a failure it prints the
// seed, the shrunk steps as Go code, and the command that runs it again.
//
// When Rerun.EnvPrefix is set, Check reads three environment variables. The
// parser is campaign.ReadEnv, the same one that the simulation tools use. With
// the prefix "P" they are:
//
//   - P_SEED=<n>: run only the rapid seed n (it sets the flags -rapid.seed and
//     -rapid.checks for this test). The strongest setting.
//   - P_CAMPAIGN=<duration>: run the seeds 1, 2, 3 and so on, one check for
//     each seed, until the time has passed, for example 30s.
//   - P_SEEDS=<n>: run n checks (random lists of steps). Without it, Check runs
//     Lockstep.Checks checks, or the rapid default (100) when that is 0.
//
// Do not use Check with t.Parallel: the rapid flags are global.
func (l Lockstep[Op, Res, S]) Check(t *testing.T) {
	t.Helper()
	env, err := l.Rerun.env()
	if err != nil {
		t.Fatalf("%v", err)
		return
	}
	switch {
	case env.HasSeed:
		if env.Seed == 0 {
			t.Fatalf("%s", errSeedZero(l.Rerun.SeedVar()))
			return
		}
		restore, err := setRapidFlags(env.Seed, false)
		if err != nil {
			t.Fatalf("%v", err)
			return
		}
		defer restore()
	case env.Duration > 0:
		l.runFor(t, env.Duration)
		return
	default:
		checks := cmp.Or(env.Seeds, l.Checks)
		if checks > 0 {
			restore, err := setRapidChecks(checks)
			if err != nil {
				t.Fatalf("%v", err)
				return
			}
			defer restore()
		}
	}
	l.check(t)
}

// runFor runs the seeds 1, 2, 3 and so on, one check for each seed, until d has
// passed. It stops at the first failure.
func (l Lockstep[Op, Res, S]) runFor(t *testing.T, d time.Duration) {
	t.Helper()
	end := time.Now().Add(d)
	n := 0
	for seed := uint64(1); time.Now().Before(end); seed++ {
		n++
		if f := l.RunSeed(seed); f != nil {
			t.Fatalf("%s", l.Report(f, t.Name()))
			return
		}
	}
	t.Logf("%d seeds passed in %s", n, d)
}

// errSeedZero is the message for a seed variable with the value 0. Rapid takes
// the seed 0 to mean "choose a random seed", so it cannot replay anything.
func errSeedZero(name string) error {
	return fmt.Errorf("%s has the value \"0\". A rapid seed must be a whole number of 1 or more, for example 17", name)
}

// env reads the three variables of the prefix. With no prefix it returns the
// zero value.
func (r Rerun) env() (campaign.EnvSettings, error) {
	if r.EnvPrefix == "" {
		return campaign.EnvSettings{}, nil
	}
	return campaign.ReadEnv(r.EnvPrefix, os.Getenv)
}

// seedFromEnv reads the seed variable (SeedVar). ok is false when there is no
// prefix or the variable is not set. An empty value counts as not set.
func (r Rerun) seedFromEnv() (uint64, bool, error) {
	env, err := r.env()
	if err != nil || !env.HasSeed {
		return 0, false, err
	}
	if env.Seed == 0 {
		return 0, false, errSeedZero(r.SeedVar())
	}
	return env.Seed, true, nil
}

// check is Check without the environment, so a test can give a fake t.
func (l *Lockstep[Op, Res, S]) check(t rapid.TB) {
	t.Helper()
	f := l.find(t)
	if f != nil {
		t.Fatalf("%s", l.Report(f, t.Name()))
	}
}

// RunSeed runs one rapid check that starts at seed, without printing
// anything, and returns the shrunk failure, or nil when the check passes.
// Seed 0 means a random seed. Use it with RequireCaught and RequireClean.
// Do not use it with t.Parallel: the rapid flags are global.
func (l Lockstep[Op, Res, S]) RunSeed(seed uint64) *Failure[Op] {
	restore, err := setRapidFlags(seed, true)
	if err != nil {
		return &Failure[Op]{Seed: seed, Message: err.Error()}
	}
	defer restore()
	return l.find(&recorder{})
}

// find runs rapid.Check with t wrapped in a recorder, and returns the
// failure, or nil.
func (l *Lockstep[Op, Res, S]) find(t rapid.TB) *Failure[Op] {
	if l.StepsPerCheck > 0 {
		restore, err := setFlags([][2]string{{"rapid.steps", strconv.Itoa(l.StepsPerCheck)}})
		if err != nil {
			return &Failure[Op]{Message: err.Error()}
		}
		defer restore()
	}
	rec := &recorder{out: t}
	var steps []Step[Op]
	var message string
	rapid.Check(rec, func(rt *rapid.T) {
		message = ""
		l.steps(rt, &steps, &message)
	})
	if !rec.failed {
		return nil
	}
	if message == "" {
		message = rec.message
	}
	// The last call of the property is rapid's replay of the shrunk case, so
	// steps holds the shrunk list.
	return &Failure[Op]{Seed: seedOf(rec.message), Steps: steps, Message: message}
}

// seedOf finds the seed in the failure message of rapid. It returns 0 when
// the message has none.
func seedOf(msg string) uint64 {
	_, digits, found := strings.Cut(msg, "-rapid.seed=")
	if !found {
		return 0
	}
	end := strings.IndexFunc(digits, func(c rune) bool { return c < '0' || c > '9' })
	if end >= 0 {
		digits = digits[:end]
	}
	seed, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0
	}
	return seed
}

// setRapidFlags makes rapid run one check that starts at seed. quiet also
// stops rapid from writing fail files. It returns a func that puts the old
// flag values back.
func setRapidFlags(seed uint64, quiet bool) (func(), error) {
	checks := "1"
	if testing.Short() {
		checks = "5" // rapid runs a fifth of the checks with -short
	}
	values := [][2]string{{"rapid.seed", strconv.FormatUint(seed, 10)}, {"rapid.checks", checks}}
	if quiet {
		values = append(values, [2]string{"rapid.nofailfile", "true"})
	}
	return setFlags(values)
}

// setRapidChecks makes rapid run n checks from a random seed. It returns a
// func that puts the old flag value back.
func setRapidChecks(n int) (func(), error) {
	return setFlags([][2]string{{"rapid.checks", strconv.Itoa(n)}})
}

// setFlags sets the named flags and returns a func that puts the old values
// back.
func setFlags(values [][2]string) (func(), error) {
	var old [][2]string
	restore := func() {
		for _, o := range slices.Backward(old) {
			_ = flag.Set(o[0], o[1])
		}
	}
	for _, v := range values {
		f := flag.Lookup(v[0])
		if f == nil {
			restore()
			return nil, fmt.Errorf("the rapid flag %s is missing", v[0])
		}
		old = append(old, [2]string{v[0], f.Value.String()})
		if err := flag.Set(v[0], v[1]); err != nil {
			restore()
			return nil, fmt.Errorf("set the rapid flag %s: %w", v[0], err)
		}
	}
	return restore, nil
}

// recorder is the testing.TB that rapid.Check gets. It sends all output to
// out (nil: nowhere), keeps the failure message of rapid, and does not stop
// the test on FailNow, so that Lockstep can print its own report after rapid.
type recorder struct {
	out     rapid.TB
	failed  bool
	message string
}

// Helper marks the caller as a helper.
func (r *recorder) Helper() {
	if r.out != nil {
		r.out.Helper()
	}
}

// Name returns the test name. Rapid uses it for the fail file name.
func (r *recorder) Name() string {
	if r.out != nil {
		return r.out.Name()
	}
	return "lockstep"
}

// Logf logs a line.
func (r *recorder) Logf(format string, args ...any) {
	if r.out != nil {
		r.out.Helper()
		r.out.Logf(format, args...)
	}
}

// Log logs a line.
func (r *recorder) Log(args ...any) {
	if r.out != nil {
		r.out.Helper()
		r.out.Log(args...)
	}
}

// Skipf skips the test.
func (r *recorder) Skipf(format string, args ...any) {
	r.Logf(format, args...)
	r.SkipNow()
}

// Skip skips the test.
func (r *recorder) Skip(args ...any) {
	r.Log(args...)
	r.SkipNow()
}

// SkipNow skips the test.
func (r *recorder) SkipNow() {
	if r.out != nil {
		r.out.SkipNow()
	}
}

// Errorf keeps the message and marks the run as failed.
func (r *recorder) Errorf(format string, args ...any) {
	r.Error(fmt.Sprintf(format, args...))
}

// Error keeps the message and marks the run as failed.
func (r *recorder) Error(args ...any) {
	msg := fmt.Sprint(args...)
	if r.message == "" {
		r.message = msg
	}
	r.failed = true
	r.Log(msg)
}

// Fatalf is Errorf. Rapid does not call it.
func (r *recorder) Fatalf(format string, args ...any) { r.Errorf(format, args...) }

// Fatal is Error. Rapid does not call it.
func (r *recorder) Fatal(args ...any) { r.Error(args...) }

// FailNow marks the run as failed but does not stop it. Rapid calls it as
// its last step.
func (r *recorder) FailNow() { r.failed = true }

// Fail marks the run as failed.
func (r *recorder) Fail() { r.failed = true }

// Failed reports whether the run failed.
func (r *recorder) Failed() bool { return r.failed }
