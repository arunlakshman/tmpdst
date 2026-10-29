package runcontrol

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"os"

	"manifester/internal/verify/speccheck/checker"
)

// Names of the environment variables.
const (
	EnvSeedListName = "MANIFESTER_SEEDS"
	EnvSeedName     = "MANIFESTER_SEED"
	EnvCampaignName = "MANIFESTER_CAMPAIGN"
)

// Options reads the environment and returns the options for checker.Run.
// With no variable set, it runs seeds 1 to defaultSeeds. A bad value stops
// the test with t.Fatalf, because a silent fallback would hide a typing error.
func Options(t testing.TB, defaultSeeds int) checker.Options {
	t.Helper()
	o := checker.Options{
		Seeds:  defaultSeeds,
		Seed:   -1,
		Replay: rerunFunc(t.Name()),
	}

	if v, ok := readSetting(EnvSeedListName); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("%s has the value %q. It must be a whole number of 1 or more, for example 200.", EnvSeedListName, v)
			return o
		}
		o.Seeds = n
	}

	if v, ok := readSetting(EnvCampaignName); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("%s has the value %q. It must be a duration longer than zero, for example 30s or 5m.", EnvCampaignName, v)
			return o
		}
		end := time.Now().Add(d)
		o.Stop = func() bool { return !time.Now().Before(end) }
	}

	if v, ok := readSetting(EnvSeedName); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			t.Fatalf("%s has the value %q. It must be a whole number of 0 or more, for example 17.", EnvSeedName, v)
			return o
		}
		o.Seed = n
	}
	return o
}

// DescribeSettings returns a short help text for the three variables. Docs use it so
// the text and the code do not drift apart.
func DescribeSettings() string {
	return EnvSeedName + "=<n>        run only seed n (n is 0 or more); the strongest setting\n" +
		EnvCampaignName + "=<duration> run seeds 1, 2, 3 and so on until the time ends, for example 30s\n" +
		EnvSeedListName + "=<n>       run seeds 1 to n (n is 1 or more)\n"
}

// readSetting treats an empty value as not set, so that "VAR=" in a shell does not
// count as a bad value.
func readSetting(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// rerunFunc builds the replay command. The command uses the top-level test
// name, because go test -run matches the top-level name first. The user must
// run it inside the package directory.
func rerunFunc(testName string) func(seed uint64) string {
	top, _, _ := strings.Cut(testName, "/")
	return func(seed uint64) string {
		return fmt.Sprintf("MANIFESTER_SEED=%d go test -run '^%s$' -count=1 -v .", seed, top)
	}
}
