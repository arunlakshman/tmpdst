package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

const mib = 1048576.0

// stats aggregates produce results across all clients. Safe for concurrent use.
type stats struct {
	mu sync.Mutex

	// current interval
	iN     int64
	iBytes int64
	iMin   time.Duration
	iMax   time.Duration
	iSum   time.Duration

	// whole run
	lat      []time.Duration
	bytes    int64
	errCount int64
	errSeen  map[string]bool
	errFirst []string
}

func newStats() *stats { return &stats{errSeen: map[string]bool{}} }

const maxDistinctErrs = 5

// add records one finished record. err != nil counts an error and no latency.
func (s *stats) add(size int, lat time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errCount++
		msg := err.Error()
		if !s.errSeen[msg] && len(s.errFirst) < maxDistinctErrs {
			s.errSeen[msg] = true
			s.errFirst = append(s.errFirst, msg)
		}
		return
	}
	if s.iN == 0 || lat < s.iMin {
		s.iMin = lat
	}
	if lat > s.iMax {
		s.iMax = lat
	}
	s.iN++
	s.iBytes += int64(size)
	s.iSum += lat
	s.lat = append(s.lat, lat)
	s.bytes += int64(size)
}

// interval is one reporting period's aggregate.
type interval struct {
	N        int64
	Bytes    int64
	Min, Max time.Duration
	Avg      time.Duration
}

// takeInterval returns and resets the interval accumulators.
func (s *stats) takeInterval() interval {
	s.mu.Lock()
	defer s.mu.Unlock()
	iv := interval{N: s.iN, Bytes: s.iBytes, Min: s.iMin, Max: s.iMax}
	if s.iN > 0 {
		iv.Avg = s.iSum / time.Duration(s.iN)
	}
	s.iN, s.iBytes, s.iMin, s.iMax, s.iSum = 0, 0, 0, 0, 0
	return iv
}

func rd(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

// formatInterval renders the per-interval line, e.g.
//
//	46798 records sent (446.00 MiB), 9359.60 records/sec (89.26 MiB/sec), 186.476ms min latency, 300.85209ms avg latency, 557.667ms max latency, 2842 buffered records.
func formatInterval(iv interval, elapsed time.Duration, buffered int64) string {
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	return fmt.Sprintf("%d records sent (%.2f MiB), %.2f records/sec (%.2f MiB/sec), %v min latency, %v avg latency, %v max latency, %d buffered records.",
		iv.N, float64(iv.Bytes)/mib, float64(iv.N)/secs, float64(iv.Bytes)/mib/secs,
		rd(iv.Min), rd(iv.Avg), rd(iv.Max), buffered)
}

// summary is the whole-run result.
type summary struct {
	N             int64
	Bytes         int64
	Elapsed       time.Duration
	Min, Avg, Max time.Duration
	P50, P90, P99 time.Duration
	Errors        int64
	FirstErrors   []string
}

// pct returns the nearest-rank percentile of sorted.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := max(0, int(p/100*float64(len(sorted))+0.999999999)-1)
	return sorted[min(i, len(sorted)-1)]
}

func (s *stats) summarize(elapsed time.Duration) summary {
	s.mu.Lock()
	defer s.mu.Unlock()
	sorted := append([]time.Duration(nil), s.lat...)
	slices.Sort(sorted)
	sm := summary{N: int64(len(sorted)), Bytes: s.bytes, Elapsed: elapsed, Errors: s.errCount,
		FirstErrors: append([]string(nil), s.errFirst...)}
	if len(sorted) > 0 {
		var sum time.Duration
		for _, d := range sorted {
			sum += d
		}
		sm.Min, sm.Max = sorted[0], sorted[len(sorted)-1]
		sm.Avg = sum / time.Duration(len(sorted))
		sm.P50, sm.P90, sm.P99 = pct(sorted, 50), pct(sorted, 90), pct(sorted, 99)
	}
	return sm
}

func formatSummary(sm summary) string {
	secs := sm.Elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	var out strings.Builder
	fmt.Fprintf(&out, "SUMMARY: %d records (%.2f MiB) in %v, %.2f records/sec (%.2f MiB/sec)\n",
		sm.N, float64(sm.Bytes)/mib, sm.Elapsed.Round(time.Millisecond), float64(sm.N)/secs, float64(sm.Bytes)/mib/secs)
	fmt.Fprintf(&out, "latency: min %v avg %v p50 %v p90 %v p99 %v max %v\n",
		rd(sm.Min), rd(sm.Avg), rd(sm.P50), rd(sm.P90), rd(sm.P99), rd(sm.Max))
	fmt.Fprintf(&out, "produce errors: %d\n", sm.Errors)
	for _, e := range sm.FirstErrors {
		out.WriteString("  error: " + e + "\n")
	}
	return out.String()
}
