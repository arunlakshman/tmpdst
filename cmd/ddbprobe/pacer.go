package main

import (
	"context"
	"sync"
	"time"
)

// op runs the i-th logical operation.
type op func(ctx context.Context, i int64) result

// runPaced issues op at rate per second for dur with at most k in flight. It does not skip ticks: when k is
// exhausted the dispatcher blocks and catches up later (counted in delayed), so a rate the K cannot sustain
// shows up as a lower achieved rate.
func runPaced(ctx context.Context, rate float64, k int, dur time.Duration, o op, rec *recorder) {
	interval := time.Duration(float64(time.Second) / rate)
	sem := make(chan struct{}, k)
	var wg sync.WaitGroup
	start := rec.start
	for i := int64(0); ; i++ {
		due := start.Add(time.Duration(i) * interval)
		if due.Sub(start) >= dur || sleepUntil(ctx, due) != nil {
			break
		}
		acquire(sem, rec)
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			res := o(ctx, i)
			rec.add(t0, time.Since(t0), res)
		}(i)
	}
	wg.Wait()
}

func acquire(sem chan struct{}, rec *recorder) {
	select {
	case sem <- struct{}{}:
	default:
		rec.addDelayed()
		sem <- struct{}{}
	}
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tm.C:
		return nil
	}
}
