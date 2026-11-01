package main

import (
	"bytes"
	"strings"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/simulator"
)

func TestRunEchoCampaign(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "echo", "-seeds", "3", "-j", "2", "-v"}, &out, &errb); code != 0 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "seed 1: steps=") || !strings.Contains(out.String(), "3 seeds from 1: 0 failing") {
		t.Fatal(out.String())
	}
}

func TestRunReplay(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "echo", "-seed", "2"}, &out, &errb); code != 0 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "seed 2: steps=") {
		t.Fatal(out.String())
	}
}

func TestRunKafkaOneSeed(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"-seeds", "1", "-j", "1"}, &out, &errb)
	if code != 0 && code != 1 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "1 seeds from 1:") {
		t.Fatal(out.String())
	}
}

func TestRunBadArgs(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "nope"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), `unknown scenario "nope"`) {
		t.Fatal(code, errb.String())
	}
	errb.Reset()
	if code := run([]string{"-bogus"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
	if code := run([]string{"-h"}, &out, &errb); code != 0 {
		t.Fatal(code)
	}
}

func TestKind(t *testing.T) {
	for k, want := range failureKindNames {
		if kind(k) != want {
			t.Errorf("kind(%v)", k)
		}
	}
	if kind(simulator.FailKind(99)) != "kind(99)" {
		t.Fatal(kind(simulator.FailKind(99)))
	}
}

func TestPrintAndVerdict(t *testing.T) {
	rep := simulator.Report{
		Seeds: []simulator.SeedResult{
			{Seed: 4, Steps: 10, Failures: []simulator.Failure{{Kind: simulator.FailPanic, Prop: "p", Step: 3, Msg: "boom"}}},
			{Seed: 5, Steps: 7},
		},
		Always:         []simulator.AssertionStat{{Prop: "inv", FirstSeed: 4, FirstStep: 3, Seeds: []uint64{4}}},
		Coverage:       []simulator.AssertionStat{{Prop: "cov", Kind: "sometimes"}},
		Nondeterminism: []uint64{9},
	}
	var out bytes.Buffer
	bad, steps := printSeeds(&out, rep, options{scenario: "echo", verbose: true})
	printCampaign(&out, rep, "echo")
	if bad != 1 || steps != 17 {
		t.Fatal(bad, steps)
	}
	for _, want := range []string{"FAIL seed 4: panic (p)", "repro: go run ./cmd/mdssimulator -scenario echo -seed 4", `VIOLATED "inv"`, `coverage gap: "cov"`, "NONDETERMINISM seed 9"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	if !failed(rep, 1) || failed(simulator.Report{}, 0) {
		t.Fatal("verdict")
	}
	cov := simulator.Report{Coverage: rep.Coverage, Seeds: make([]simulator.SeedResult, minimumCoverage)}
	if !failed(cov, 0) {
		t.Fatal("coverage gap in a big campaign must fail")
	}
	cov.Seeds = cov.Seeds[:1]
	if failed(cov, 0) {
		t.Fatal("coverage gap in a small campaign must not fail")
	}
}

func TestMergeReports(t *testing.T) {
	a := simulator.Report{
		Seeds: []simulator.SeedResult{{Seed: 1}},
		Assertions: []simulator.AssertionStat{
			{Prop: core.PropertyID("inv"), Kind: "always", Checks: 3, Passes: 3},
			{Prop: core.PropertyID("cov"), Kind: "sometimes", Checks: 2, Passes: 0},
		},
	}
	b := simulator.Report{
		Seeds:          []simulator.SeedResult{{Seed: 2}},
		Nondeterminism: []uint64{2},
		Assertions: []simulator.AssertionStat{
			{Prop: core.PropertyID("inv"), Kind: "always", Checks: 2, Passes: 1, Seeds: []uint64{2}, FirstSeed: 2, FirstStep: 8},
			{Prop: core.PropertyID("cov"), Kind: "sometimes", Checks: 1, Passes: 1, Seeds: []uint64{2}, FirstSeed: 2, FirstStep: 1},
			{Prop: core.PropertyID("never"), Kind: "unreachable", Checks: 1, Passes: 1},
		},
	}
	got := combineReports([]simulator.Report{a, b})
	if len(got.Seeds) != 2 || len(got.Nondeterminism) != 1 || len(got.Assertions) != 3 {
		t.Fatalf("%+v", got)
	}
	inv := got.Assertions[0]
	if inv.Checks != 5 || inv.Passes != 4 || inv.FirstSeed != 2 || inv.FirstStep != 8 || len(inv.Seeds) != 1 {
		t.Fatalf("%+v", inv)
	}
	if len(got.Always) != 1 || got.Always[0].Prop != "inv" || len(got.Coverage) != 0 {
		t.Fatalf("always %+v coverage %+v", got.Always, got.Coverage)
	}
	// coverage: sometimes with no seeds anywhere
	c := combineReports([]simulator.Report{{Assertions: []simulator.AssertionStat{{Prop: "x", Kind: "sometimes"}}}})
	if len(c.Coverage) != 1 || len(c.Always) != 0 {
		t.Fatalf("%+v", c)
	}
}

func TestRunSharded(t *testing.T) {
	echoConfig, _ := scenarioConfig("echo")
	rep, err := runInParts(echoConfig, []uint64{1, 2, 3, 4, 5}, 3)
	if err != nil || len(rep.Seeds) != 5 {
		t.Fatal(len(rep.Seeds), err)
	}
	one, err := runInParts(echoConfig, []uint64{1, 2}, 1)
	if err != nil || len(one.Seeds) != 2 {
		t.Fatal(err)
	}
}
