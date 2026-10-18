package main

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"manifester/toys3/sim"
)

func TestRunPasses(t *testing.T) {
	var out bytes.Buffer
	code := run([]string{"-duration", "300ms", "-workers", "2", "-check-determinism", "-det-every", "1"}, &out)
	if code != 0 || !strings.Contains(out.String(), "all seeds passed") {
		t.Fatalf("code %d:\n%s", code, out.String())
	}
}

func TestRunBadFlagAndHelp(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"-nope"}, &out); code != 2 {
		t.Errorf("bad flag: code %d", code)
	}
	if code := run([]string{"-h"}, &out); code != 0 {
		t.Errorf("help: code %d", code)
	}
}

func newCampaign(out *bytes.Buffer, o options, f func(uint64) sim.Result) *campaign {
	return &campaign{o: o, out: out, runSeed: f}
}

func TestCampaignReportsFailuresAndStops(t *testing.T) {
	var out bytes.Buffer
	o := options{duration: time.Minute, workers: 1, startSeed: 1, maxFailures: 2}
	c := newCampaign(&out, o, func(seed uint64) sim.Result {
		return sim.Result{Steps: 1, Failures: []sim.Failure{{Invariant: "inv", Step: 3, Msg: "boom"}}}
	})
	if code := c.exec(time.Millisecond); code != 1 {
		t.Fatalf("code %d", code)
	}
	s := out.String()
	for _, want := range []string{"FAIL seed 1: inv at step 3: boom", "2 failing seed(s)", "-seed=2 -v"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
}

func TestCampaignDetectsNondeterminism(t *testing.T) {
	var out bytes.Buffer
	var n atomic.Uint64
	o := options{duration: time.Minute, workers: 1, startSeed: 1, checkDet: true, detEvery: 1, maxFailures: 1}
	c := newCampaign(&out, o, func(seed uint64) sim.Result {
		return sim.Result{TraceHash: n.Add(1)}
	})
	if code := c.exec(time.Millisecond); code != 1 || !strings.Contains(out.String(), "NONDETERMINISM") {
		t.Fatalf("code %d:\n%s", code, out.String())
	}
}

func TestCampaignProgressTicks(t *testing.T) {
	var out bytes.Buffer
	o := options{duration: 50 * time.Millisecond, workers: 1, startSeed: 1, maxFailures: 1}
	c := newCampaign(&out, o, func(seed uint64) sim.Result {
		time.Sleep(10 * time.Millisecond)
		return sim.Result{}
	})
	c.exec(5 * time.Millisecond)
	if !strings.Contains(out.String(), "progress ") {
		t.Errorf("no progress line:\n%s", out.String())
	}
}
