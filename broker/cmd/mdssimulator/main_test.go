package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/campaign"
)

func TestRunEchoCampaign(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "echo", "-seeds", "3", "-j", "2", "-v"}, &out, &errb); code != 0 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "echo seed 1: steps=") || !strings.Contains(out.String(), "3 seeds") {
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

// TestRunKafkaIsTheDefaultScenario: no -scenario runs kafka.
func TestRunKafkaIsTheDefaultScenario(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"-seeds", "1", "-j", "1"}, &out, &errb)
	if code != 0 && code != 1 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.HasPrefix(out.String(), "kafka ") {
		t.Fatal(out.String())
	}
}

func TestRunVariablesAndBadArgs(t *testing.T) {
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
	// The kafka-log scenarios live in the control plane repo and are not here.
	if code := run([]string{"-scenario", "kafka-log"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
}

// TestTheTwoScenariosAreRegistered: the names and their order are part of the
// command line of the tool.
func TestTheTwoScenariosAreRegistered(t *testing.T) {
	names := registry().Names()
	if len(names) != 2 || names[0] != "kafka" || names[1] != "echo" {
		t.Fatalf("scenarios %v", names)
	}
}

// TestEveryDeclaredNameHasAnOwner: a name that no scenario owns would never be
// in a report, so a new name must start with a known prefix.
func TestEveryDeclaredNameHasAnOwner(t *testing.T) {
	for _, d := range assert.Catalog() {
		if !ownsKafka(d.Name) && !ownsEcho(d.Name) {
			t.Errorf("no scenario owns the name %q", d.Name)
		}
		if ownsKafka(d.Name) && ownsEcho(d.Name) && !isSharedFake(d.Name) {
			t.Errorf("the name %q belongs to both scenarios, but it is not a name of a fake service", d.Name)
		}
	}
}

// isSharedFake says whether name is a name of the fake services, which both
// scenarios use.
func isSharedFake(name string) bool {
	for _, p := range []string{"obj ", "cp ", "net: "} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// TestARunRecordsOnlyNamesOfItsScenario: what a seed checks is owned by the
// scenario that ran it.
func TestARunRecordsOnlyNamesOfItsScenario(t *testing.T) {
	reg := registry()
	for _, name := range reg.Names() {
		sc, _ := reg.Get(name)
		for seed := uint64(1); seed <= 3; seed++ {
			r := sc.Run(seed, campaign.RunOptions{})
			if !r.OK {
				t.Fatalf("%s seed %d failed: %+v", name, seed, r.Failures)
			}
			for covered := range r.Coverage {
				if !sc.Owns(covered) {
					t.Errorf("scenario %s seed %d checked %q, which it does not own", name, seed, covered)
				}
			}
		}
	}
}
