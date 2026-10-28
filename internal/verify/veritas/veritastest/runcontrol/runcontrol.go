package runcontrol

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"os"

	"manifester/internal/verify/veritas/veritastest"
)

// Names of the environment variables.
const (
	EnvSeeds    = "MANIFESTER_SEEDS"
	EnvSeed     = "MANIFESTER_SEED"
	EnvCampaign = "MANIFESTER_CAMPAIGN"
)

// Options reads the environment and returns the options for veritastest.Run.
// With no variable set, it runs seeds 1 to defaultSeeds. A bad value stops
// the test with t.Fatalf, because a silent fallback would hide a typing error.
func Options(t testing.TB, defaultSeeds int) veritastest.Options {
	t.Helper()
	o := veritastest.Options{
		Seeds:  defaultSeeds,
		Seed:   -1,
		Replay: replayFunc(t.Name()),
	}

	if v, ok := lookup(EnvSeeds); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("%s has the value %q. It must be a whole number of 1 or more, for example 200.", EnvSeeds, v)
			return o
		}
		o.Seeds = n
	}

	if v, ok := lookup(EnvCampaign); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("%s has the value %q. It must be a duration longer than zero, for example 30s or 5m.", EnvCampaign, v)
			return o
		}
		end := time.Now().Add(d)
		o.Stop = func() bool { return !time.Now().Before(end) }
	}

	if v, ok := lookup(EnvSeed); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			t.Fatalf("%s has the value %q. It must be a whole number of 0 or more, for example 17.", EnvSeed, v)
			return o
		}
		o.Seed = n
	}
	return o
}

// Describe returns a short help text for the three variables. Docs use it so
// the text and the code do not drift apart.
func Describe() string {
	return EnvSeed + "=<n>        run only seed n (n is 0 or more); the strongest setting\n" +
		EnvCampaign + "=<duration> run seeds 1, 2, 3 and so on until the time ends, for example 30s\n" +
		EnvSeeds + "=<n>       run seeds 1 to n (n is 1 or more)\n"
}

// lookup treats an empty value as not set, so that "VAR=" in a shell does not
// count as a bad value.
func lookup(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// replayFunc builds the replay command. The command uses the top-level test
// name, because go test -run matches the top-level name first. The user must
// run it inside the package directory.
func replayFunc(testName string) func(seed uint64) string {
	top, _, _ := strings.Cut(testName, "/")
	return func(seed uint64) string {
		return fmt.Sprintf("MANIFESTER_SEED=%d go test -run '^%s$' -count=1 -v .", seed, top)
	}
}
