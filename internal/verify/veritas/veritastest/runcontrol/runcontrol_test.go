package runcontrol

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// recordingTB records Fatalf calls so a test can check them without failing.
type recordingTB struct {
	testing.TB
	name  string
	fatal []string
}

func (f *recordingTB) Helper()      {}
func (f *recordingTB) Name() string { return f.name }
func (f *recordingTB) Fatalf(format string, args ...any) {
	f.fatal = append(f.fatal, fmt.Sprintf(format, args...))
}

func clearEnv(t *testing.T) {
	t.Setenv(EnvSeedListName, "")
	t.Setenv(EnvSeedName, "")
	t.Setenv(EnvCampaignName, "")
}

func TestDefault(t *testing.T) {
	clearEnv(t)
	f := &recordingTB{TB: t, name: "TestX"}
	o := Options(f, 25)
	if len(f.fatal) != 0 {
		t.Fatalf("expected no Fatalf call, got %v", f.fatal)
	}
	if o.Seeds != 25 || o.Seed != -1 || o.Stop != nil || o.Replay == nil {
		t.Fatalf("expected Seeds 25, Seed -1, no Stop and a Replay function, got Seeds %d, Seed %d, Stop set %v, Replay set %v",
			o.Seeds, o.Seed, o.Stop != nil, o.Replay != nil)
	}
}

func TestSeeds(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvSeedListName, "7")
	o := Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Seeds != 7 || o.Seed != -1 {
		t.Fatalf("expected Seeds 7 and Seed -1, got Seeds %d and Seed %d", o.Seeds, o.Seed)
	}
}

func TestSeed(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvSeedName, "0")
	o := Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Seed != 0 {
		t.Fatalf("expected Seed 0, got %d", o.Seed)
	}
	t.Setenv(EnvSeedName, "42")
	o = Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Seed != 42 {
		t.Fatalf("expected Seed 42, got %d", o.Seed)
	}
}

func TestCampaign(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvCampaignName, "50ms")
	o := Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Stop == nil {
		t.Fatalf("expected Stop to be set for a campaign of 50ms")
	}
	if o.Stop() {
		t.Fatalf("expected Stop to return false right after the start of a 50ms campaign")
	}
	time.Sleep(80 * time.Millisecond)
	if !o.Stop() {
		t.Fatalf("expected Stop to return true 80ms after the start of a 50ms campaign")
	}
}

func TestPrecedence(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvSeedListName, "7")
	t.Setenv(EnvCampaignName, "1h")
	o := Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Stop == nil || o.Seed != -1 {
		t.Fatalf("expected the campaign to be set and Seed -1, got Stop set %v and Seed %d", o.Stop != nil, o.Seed)
	}
	t.Setenv(EnvSeedName, "3")
	o = Options(&recordingTB{TB: t, name: "TestX"}, 25)
	if o.Seed != 3 {
		t.Fatalf("expected %s to win with Seed 3, got %d", EnvSeedName, o.Seed)
	}
}

func TestBadValues(t *testing.T) {
	cases := []struct{ name, value string }{
		{EnvSeedListName, "abc"}, {EnvSeedListName, "0"}, {EnvSeedListName, "-3"}, {EnvSeedListName, "1.5"},
		{EnvSeedName, "abc"}, {EnvSeedName, "-1"}, {EnvSeedName, "2x"},
		{EnvCampaignName, "abc"}, {EnvCampaignName, "10"}, {EnvCampaignName, "0s"}, {EnvCampaignName, "-5s"},
	}
	for _, c := range cases {
		clearEnv(t)
		t.Setenv(c.name, c.value)
		f := &recordingTB{TB: t, name: "TestX"}
		Options(f, 25)
		if len(f.fatal) != 1 {
			t.Fatalf("%s=%q: expected exactly 1 Fatalf call, got %d", c.name, c.value, len(f.fatal))
		}
		msg := f.fatal[0]
		if !strings.Contains(msg, c.name) || !strings.Contains(msg, fmt.Sprintf("%q", c.value)) {
			t.Fatalf("%s=%q: the message must name the variable and the value, got %q", c.name, c.value, msg)
		}
	}
}

func TestReplayFormat(t *testing.T) {
	clearEnv(t)
	o := Options(&recordingTB{TB: t, name: "TestTop/sub/deeper"}, 5)
	want := "MANIFESTER_SEED=9 go test -run '^TestTop$' -count=1 -v ."
	if got := o.Replay(9); got != want {
		t.Fatalf("expected the replay command %q, got %q", want, got)
	}
}

func TestDescribeNamesAllVariables(t *testing.T) {
	d := DescribeSettings()
	for _, n := range []string{EnvSeedListName, EnvSeedName, EnvCampaignName} {
		if !strings.Contains(d, n) {
			t.Fatalf("expected Describe to mention %s, got %q", n, d)
		}
	}
}
