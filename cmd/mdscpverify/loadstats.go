package main

import (
	"maps"
	"math"
	"slices"
	"sync"
	"time"
)

// loadSummary is the one JSON line that a load run prints on stdout.
type loadSummary struct {
	Acked        int            `json:"acked"`
	Failed       int            `json:"failed"`
	Retries      int            `json:"retries"`
	ErrorsByKind map[string]int `json:"errors_by_kind"`
	LatencyMs    latencySummary `json:"latency_ms"`
	MaxStallMs   float64        `json:"max_stall_ms"`
	DurationS    float64        `json:"duration_s"`
}

// latencySummary holds percentiles of the acked commits, in milliseconds.
type latencySummary struct {
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// loadStats is what the workers add up. One mutex guards all of it.
type loadStats struct {
	mu           sync.Mutex
	acked        int
	failed       int
	retries      int
	errorsByKind map[string]int
	latencies    []time.Duration // acked commits: first attempt to ack, retries included
	stall        stallTracker
	writeErr     error
}

func newLoadStats() *loadStats {
	return &loadStats{errorsByKind: map[string]int{}}
}

func (s *loadStats) recordError(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errorsByKind[kind]++
}

func (s *loadStats) recordRetry() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retries++
}

func (s *loadStats) recordFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
}

// recordAck counts one acked commit. The time is taken under the lock, so the
// stall tracker sees the acks in order.
func (s *loadStats) recordAck(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked++
	s.latencies = append(s.latencies, latency)
	s.stall.add(time.Now())
}

func (s *loadStats) noteWriteError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr == nil {
		s.writeErr = err
	}
}

func (s *loadStats) writeError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeErr
}

// counts returns the acked, failed and retry counts for the progress line.
func (s *loadStats) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked, s.failed, s.retries
}

func (s *loadStats) summary(elapsed time.Duration) loadSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return loadSummary{
		Acked:        s.acked,
		Failed:       s.failed,
		Retries:      s.retries,
		ErrorsByKind: maps.Clone(s.errorsByKind),
		LatencyMs:    summarizeLatencies(s.latencies),
		MaxStallMs:   millis(s.stall.longest),
		DurationS:    round3(elapsed.Seconds()),
	}
}

// stallTracker keeps the longest gap between two consecutive acks.
type stallTracker struct {
	last    time.Time
	longest time.Duration
}

func (t *stallTracker) add(at time.Time) {
	if !t.last.IsZero() {
		t.longest = max(t.longest, at.Sub(t.last))
	}
	t.last = at
}

// summarizeLatencies gives the p50, p99 and max of the latencies, in milliseconds.
func summarizeLatencies(latencies []time.Duration) latencySummary {
	sorted := slices.Clone(latencies)
	slices.Sort(sorted)
	return latencySummary{
		P50: nearestRankMs(sorted, 0.50),
		P99: nearestRankMs(sorted, 0.99),
		Max: nearestRankMs(sorted, 1),
	}
}

// nearestRankMs returns the value at rank ceil(p * n) of sorted, in
// milliseconds. It returns 0 when there are no values.
func nearestRankMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	rank = min(max(rank, 0), len(sorted)-1)
	return millis(sorted[rank])
}

// millis converts a duration to milliseconds, rounded to 3 decimal places.
func millis(d time.Duration) float64 {
	return round3(float64(d) / float64(time.Millisecond))
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}
