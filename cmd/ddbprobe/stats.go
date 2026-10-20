package main

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go"
)

// result is the outcome of one logical operation (one slot, one batch call, one lookup).
type result struct {
	capacity float64 // consumed capacity units across all calls of the op
	calls    int     // API calls made on behalf of the op (0 for a coalesced lookup)
	ccf      int     // ConditionalCheckFailed answers seen (retried inside the op)
	items    int     // items written (batch mode)
	unproc   int     // UnprocessedItems seen (batch mode)
	err      error
}

type sample struct {
	at  time.Duration // offset from run start
	dur time.Duration
}

// recorder aggregates results from concurrent operations.
type recorder struct {
	mu      sync.Mutex
	start   time.Time
	samples []sample
	errs    map[string]int
	tot     result
	ok      int
	delayed int
}

func newRecorder(start time.Time) *recorder {
	return &recorder{start: start, errs: map[string]int{}}
}

func (r *recorder) add(t0 time.Time, d time.Duration, res result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tot.capacity += res.capacity
	r.tot.calls += res.calls
	r.tot.ccf += res.ccf
	r.tot.items += res.items
	r.tot.unproc += res.unproc
	if res.err != nil {
		r.errs[classify(res.err)]++
		return
	}
	r.ok++
	r.samples = append(r.samples, sample{at: t0.Sub(r.start), dur: d})
}

func (r *recorder) addDelayed() {
	r.mu.Lock()
	r.delayed++
	r.mu.Unlock()
}

// classify maps an error to a short bucket name.
func classify(err error) string {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return "other"
	}
	code, msg := ae.ErrorCode(), ae.ErrorMessage()
	switch {
	case code == "ConditionalCheckFailedException":
		return "ConditionalCheckFailed"
	case code == "TransactionCanceledException":
		return classifyCancel(msg)
	case isThrottleText(code):
		return "throttle"
	}
	return "other:" + code
}

func classifyCancel(msg string) string {
	if isThrottleText(msg) {
		return "throttle"
	}
	if strings.Contains(msg, "ConditionalCheckFailed") {
		return "ConditionalCheckFailed"
	}
	return "TransactionCanceled"
}

func isThrottleText(s string) bool {
	return strings.Contains(s, "Throttl") || strings.Contains(s, "ProvisionedThroughput") || strings.Contains(s, "RequestLimitExceeded")
}

// quantiles of durations in milliseconds.
type quantiles struct {
	P50, P90, P99, Max float64
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func quant(ds []time.Duration) quantiles {
	if len(ds) == 0 {
		return quantiles{}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	at := func(p float64) float64 { return ms(ds[int(p*float64(len(ds)-1))]) }
	return quantiles{P50: at(0.50), P90: at(0.90), P99: at(0.99), Max: ms(ds[len(ds)-1])}
}

func durs(ss []sample, from, to time.Duration) []time.Duration {
	var out []time.Duration
	for _, s := range ss {
		if s.at >= from && s.at < to {
			out = append(out, s.dur)
		}
	}
	return out
}

// summary is the JSON document printed for a run.
type summary struct {
	Mode         string         `json:"mode"`
	Params       map[string]any `json:"params"`
	ElapsedSec   float64        `json:"elapsed_sec"`
	OK           int            `json:"ok"`
	Errors       map[string]int `json:"errors"`
	AchievedPerS float64        `json:"achieved_ops_per_s"`
	DelayedTicks int            `json:"delayed_ticks"`
	Calls        int            `json:"api_calls"`
	CallsPerOp   float64        `json:"calls_per_op"`
	CCF          int            `json:"conditional_check_failed_seen"`
	Items        int            `json:"items"`
	Unprocessed  int            `json:"unprocessed_seen"`
	Capacity     float64        `json:"capacity_total"`
	CapPerOp     float64        `json:"capacity_per_op"`
	CapPerItem   float64        `json:"capacity_per_item,omitempty"`
	Latency      quantiles      `json:"latency_ms"`
	FirstHalf    quantiles      `json:"latency_first_half_ms"`
	SecondHalf   quantiles      `json:"latency_second_half_ms"`
}

func (r *recorder) summarize(mode string, params map[string]any, elapsed time.Duration) summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	half := elapsed / 2
	s := summary{Mode: mode, Params: params, ElapsedSec: elapsed.Seconds(), OK: r.ok, Errors: r.errs,
		DelayedTicks: r.delayed, Calls: r.tot.calls, CCF: r.tot.ccf, Items: r.tot.items,
		Unprocessed: r.tot.unproc, Capacity: r.tot.capacity}
	s.Latency = quant(durs(r.samples, 0, elapsed+time.Hour))
	s.FirstHalf = quant(durs(r.samples, 0, half))
	s.SecondHalf = quant(durs(r.samples, half, elapsed+time.Hour))
	if elapsed > 0 {
		s.AchievedPerS = float64(r.ok) / elapsed.Seconds()
	}
	if r.ok > 0 {
		s.CallsPerOp = float64(r.tot.calls) / float64(r.ok)
		s.CapPerOp = r.tot.capacity / float64(r.ok)
	}
	if r.tot.items > 0 {
		s.CapPerItem = r.tot.capacity / float64(r.tot.items)
	}
	return s
}
