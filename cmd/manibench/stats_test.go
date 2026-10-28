package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFormatIntervalShape(t *testing.T) {
	iv := interval{N: 46798, Bytes: 46798 * 1000, Min: 186476 * time.Microsecond,
		Avg: 300852090 * time.Nanosecond, Max: 557667 * time.Microsecond}
	got := formatInterval(iv, 5*time.Second, 2842)
	want := "46798 records sent (44.63 MiB), 9359.60 records/sec (8.93 MiB/sec), 186.476ms min latency, 300.852ms avg latency, 557.667ms max latency, 2842 buffered records."
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestAggregation(t *testing.T) {
	s := newLatencyStats()
	for i := 1; i <= 100; i++ {
		s.add(1000, time.Duration(i)*time.Millisecond, nil)
	}
	s.add(1000, 0, errors.New("boom"))
	s.add(1000, 0, errors.New("boom"))
	s.add(1000, 0, errors.New("bang"))
	iv := s.takeInterval()
	if iv.N != 100 || iv.Bytes != 100000 || iv.Min != time.Millisecond || iv.Max != 100*time.Millisecond {
		t.Fatalf("interval %+v", iv)
	}
	if iv.Avg != 50500*time.Microsecond {
		t.Fatalf("avg %v", iv.Avg)
	}
	if again := s.takeInterval(); again.N != 0 {
		t.Fatalf("interval not reset: %+v", again)
	}
	sm := s.summarize(10 * time.Second)
	if sm.N != 100 || sm.P50 != 50*time.Millisecond || sm.P90 != 90*time.Millisecond || sm.P99 != 99*time.Millisecond || sm.Max != 100*time.Millisecond {
		t.Fatalf("summary %+v", sm)
	}
	if sm.Errors != 3 || len(sm.FirstErrors) != 2 {
		t.Fatalf("errors %d %v", sm.Errors, sm.FirstErrors)
	}
	out := formatSummary(sm)
	for _, w := range []string{"100 records", "10.00 records/sec", "p99 99ms", "produce errors: 3", "error: boom"} {
		if !strings.Contains(out, w) {
			t.Errorf("summary missing %q:\n%s", w, out)
		}
	}
}
