package logcp_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"manifester/core"
	"manifester/cp/applier"
	"manifester/cp/applier/pebblekv"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
)

// The model run of stage 2 (docs/log-cp-service.md "Model numbers"): six
// broker-like clients seal segments open-loop at the offered rate, each commits
// one segment at a time in seal order (decision 48's broker rule), against one
// active node and one standby over fakedb with DynamoDB-like latency (PutItem
// 3.5 ms with 1% 12 ms outliers, Query and GetItem 2 ms, BatchWriteItem 6 ms,
// numbers from docs/x1-x3-x4-results.md), the Pebble applier
// on. 150 partitions per segment, 200 batches. Not part of the default run:
//
//	LOGCP_MODEL=1 [LOGCP_MODEL_SECS=10] go test ./cp/logcp -run Model -v -timeout 30m

func latency(call string) time.Duration {
	switch call {
	case "PutItem":
		if rand.Intn(100) == 0 {
			return 12 * time.Millisecond
		}
		return 3500 * time.Microsecond
	case "BatchWriteItem":
		return 6 * time.Millisecond
	}
	return 2 * time.Millisecond
}

func modelSegment(r *rand.Rand, key string) core.CommitSegment {
	c := core.CommitSegment{SegmentKey: key}
	parts := make([]int32, 150)
	for i := range parts {
		parts[i] = int32(r.Intn(40000))
	}
	for i := 0; i < 200; i++ {
		c.Batches = append(c.Batches, core.PendingBatch{
			TP:      core.TP("load", parts[i%150]),
			Records: 100, Loc: core.SegmentRef{Key: key, Off: int64(i) * 4096, Len: 4096},
		})
	}
	return c
}

type modelRow struct {
	offered, committed, p50, p99, pmax  float64
	slotsPerSec, cmdsPerSlot, lookupP99 float64
	lost                                float64
}

func runModel(t *testing.T, rate int, dur time.Duration) modelRow {
	db := newDB(t)
	db.Latency = latency
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	kv, err := pebblekv.Open(filepath.Join(t.TempDir(), "state"), pebblekv.Options{CacheBytes: 64 << 20, MemTableBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	reg := prometheus.NewRegistry()
	mod := func(c *logcp.Config) {
		c.TailBatch, c.TailPoll, c.HoleAfter, c.MaxSlotRate = 20*time.Millisecond, 25*time.Millisecond, 250*time.Millisecond, 200
		c.CkptEvery = -1
	}
	cfg := logcptest.FastConfig("n0", 0, log)
	cfg.KV, cfg.DB, cfg.Table, cfg.Objects, cfg.Metrics = kv, db, "cp", objs, logcp.NewMetrics(reg)
	mod(&cfg)
	active := logcptest.Start(t, cfg)
	scfg := logcptest.FastConfig("n1", 1, log)
	scfg.KV, scfg.DB, scfg.Table, scfg.Objects = applier.NewMemKV(), db, "cp", objs
	mod(&scfg)
	logcptest.Start(t, scfg)

	const clients = 6
	var mu sync.Mutex
	var lats, lookups []float64
	var wg sync.WaitGroup
	start := time.Now()
	stop := start.Add(dur)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(c) + 1))
			every := time.Duration(float64(time.Second) * clients / float64(rate))
			next := start.Add(time.Duration(r.Float64() * float64(every))) // random phase
			for n := 0; next.Before(stop); n++ {
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				sealed := next
				next = next.Add(every)
				seg := modelSegment(r, "m"+strconv.Itoa(c)+"-"+strconv.Itoa(n))
				for {
					if _, code, err := active.CommitSegment(bg, seg); err == nil && code == core.OK {
						break
					}
					time.Sleep(time.Millisecond)
				}
				mu.Lock()
				lats = append(lats, float64(time.Since(sealed))/1e6)
				mu.Unlock()
			}
		}()
	}
	wg.Add(1)
	go func() { // a consumer-ish read load: 100 fresh Lookups a second
		defer wg.Done()
		r := rand.New(rand.NewSource(99))
		for tick := time.NewTicker(10 * time.Millisecond); time.Now().Before(stop); <-tick.C {
			t0 := time.Now()
			if _, _, err := active.Lookup(bg, core.Lookup{TP: core.TP("load", int32(r.Intn(40000))), MaxBytes: 1 << 20}); err == nil {
				mu.Lock()
				lookups = append(lookups, float64(time.Since(t0))/1e6)
				mu.Unlock()
			}
		}
	}()
	wg.Wait()
	el := time.Since(start).Seconds()
	slices.Sort(lats)
	slices.Sort(lookups)
	pct := func(v []float64, p float64) float64 {
		if len(v) == 0 {
			return 0
		}
		return v[min(len(v)-1, int(p*float64(len(v))))]
	}
	slots := metricSum(t, reg, "manicp_log_slots_total")
	cmds := metricSum(t, reg, "manicp_log_entries_per_slot")
	return modelRow{offered: float64(rate), committed: float64(len(lats)) / el, p50: pct(lats, .5), p99: pct(lats, .99), pmax: pct(lats, 1),
		slotsPerSec: slots / el, cmdsPerSlot: cmds / max(slots, 1), lookupP99: pct(lookups, .99), lost: metricSum(t, reg, "manicp_log_lost_slots_total")}
}

func TestModelThroughput(t *testing.T) {
	if os.Getenv("LOGCP_MODEL") == "" {
		t.Skip("model run: LOGCP_MODEL=1")
	}
	secs := 10
	if v, err := strconv.Atoi(os.Getenv("LOGCP_MODEL_SECS")); err == nil && v > 0 {
		secs = v
	}
	t.Logf("%8s %10s %8s %8s %8s %10s %10s %10s %6s", "offered", "committed", "p50 ms", "p99 ms", "max ms", "slots/s", "cmds/slot", "lookup p99", "lost")
	for _, rate := range []int{64, 128, 256} {
		r := runModel(t, rate, time.Duration(secs)*time.Second)
		t.Logf("%8.0f %10.1f %8.1f %8.1f %8.1f %10.1f %10.2f %10.1f %6.0f", r.offered, r.committed, r.p50, r.p99, r.pmax, r.slotsPerSec, r.cmdsPerSlot, r.lookupP99, r.lost)
		if r.committed < 0.9*r.offered {
			t.Errorf("offered %v, committed only %.1f", r.offered, r.committed)
		}
	}
	_ = fmt.Sprint
}
