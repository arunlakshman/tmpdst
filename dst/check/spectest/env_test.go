package spectest

import (
	"flag"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// countingSubjects returns a Lockstep that counts how many subjects it made:
// one for each rapid check.
func countingSubjects(n *int) Lockstep[setOp, setRes, *setSubject] {
	l := setLockstep("")
	l.NewSubject = func(*rapid.T) *setSubject { *n++; return &setSubject{} }
	return l
}

func TestCheckRunsTheChecksOfTheField(t *testing.T) {
	if testing.Short() {
		t.Skip("rapid runs a fifth of the checks with -short")
	}
	n := 0
	l := countingSubjects(&n)
	l.Checks = 7
	l.Check(t)
	if n != 7 {
		t.Fatalf("made %d subjects, want 7", n)
	}
	if v := flag.Lookup("rapid.checks").Value.String(); v == "7" {
		t.Fatalf("rapid.checks was not put back")
	}
}

func TestCheckSeedsVariableWinsOverTheField(t *testing.T) {
	if testing.Short() {
		t.Skip("rapid runs a fifth of the checks with -short")
	}
	t.Setenv("P_SEEDS", "4")
	n := 0
	l := countingSubjects(&n)
	l.Checks = 7
	l.Check(t)
	if n != 4 {
		t.Fatalf("made %d subjects, want 4", n)
	}
}

func TestCheckCampaignRunsSeedsUntilTheTimeEnds(t *testing.T) {
	t.Setenv("P_CAMPAIGN", "100ms")
	n := 0
	start := time.Now()
	countingSubjects(&n).Check(t)
	if n < 2 {
		t.Fatalf("made %d subjects in a 100ms campaign, want 2 or more", n)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("the campaign ended after %s", d)
	}
}

// The strongest setting wins: P_SEED beats P_CAMPAIGN, and P_CAMPAIGN beats
// P_SEEDS. The old broker settings had the same order.
func TestCheckSeedBeatsCampaignAndSeeds(t *testing.T) {
	t.Setenv("P_SEED", "7")
	t.Setenv("P_CAMPAIGN", "300ms") // short: a wrong order would run it, not wait
	t.Setenv("P_SEEDS", "50")
	n := 0
	countingSubjects(&n).Check(t)
	if n != 1 {
		t.Fatalf("made %d subjects with P_SEED set, want 1 (one seed)", n)
	}
}

func TestCheckCampaignBeatsSeeds(t *testing.T) {
	t.Setenv("P_CAMPAIGN", "100ms")
	t.Setenv("P_SEEDS", "1")
	n := 0
	countingSubjects(&n).Check(t)
	if n < 2 {
		t.Fatalf("made %d subjects, want 2 or more: P_CAMPAIGN must win over P_SEEDS", n)
	}
}

func TestStepsSetsTheAverageLengthOfACheck(t *testing.T) {
	if testing.Short() {
		t.Skip("rapid halves the steps with -short")
	}
	average := func(steps int) float64 {
		total := 0
		l := setLockstep("")
		l.StepsPerCheck, l.Checks = steps, 200
		l.Invariant = func(*setSubject) error { total++; return nil }
		l.Check(t)
		return float64(total) / 200
	}
	if got := average(0); got < 20 || got > 40 {
		t.Fatalf("the default average is %.1f steps, want about 30", got)
	}
	if got := average(90); got < 65 || got > 115 {
		t.Fatalf("with StepsPerCheck 90 the average is %.1f steps, want about 90", got)
	}
	if v := flag.Lookup("rapid.steps").Value.String(); v == "90" {
		t.Fatalf("rapid.steps was not put back")
	}
}

func TestRerunEnvReadsAllThreeVariables(t *testing.T) {
	t.Setenv("QQ_SEEDS", "9")
	t.Setenv("QQ_SEED", "5")
	t.Setenv("QQ_CAMPAIGN", "2s")
	got, err := Rerun{EnvPrefix: "QQ"}.env()
	if err != nil || got.Seeds != 9 || !got.HasSeed || got.Seed != 5 || got.Duration != 2*time.Second {
		t.Fatalf("env = %+v, err %v", got, err)
	}
	if got, err := (Rerun{}).env(); err != nil || got.HasSeed || got.Seeds != 0 {
		t.Fatalf("no prefix: env = %+v, err %v", got, err)
	}
	t.Setenv("QQ_SEEDS", "many")
	if _, err := (Rerun{EnvPrefix: "QQ"}).env(); err == nil || !strings.Contains(err.Error(), "QQ_SEEDS has the value") {
		t.Fatalf("a bad value must be an error, got %v", err)
	}
}
