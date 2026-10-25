package logcp

import (
	"context"
	"time"
)

// Env is everything the service takes from the outside world: the clock, goroutine
// creation and blocking. The default is the real one; the seeded harness
// replaces it with a scheduler so the real code runs under a deterministic
// interleaving.
type Env struct {
	// Now is the clock in nanoseconds.
	Now func() int64
	// Go runs f on its own goroutine.
	Go func(f func())
	// Wait blocks until ready delivers a value or is closed, or d has passed
	// (d <= 0: no timeout), or ctx is done. A nil ready only waits for d. It
	// returns ctx's error if ctx is done, otherwise nil: the caller re-checks
	// its condition.
	Wait func(ctx context.Context, ready <-chan struct{}, d time.Duration) error
}

// wallTime is the real wall clock (DynLog TTL when no clock is injected).
func wallTime() time.Time { return time.Now() }

// realNow is the wall clock in nanoseconds.
func realNow() int64 { return wallTime().UnixNano() }

// RealEnv is the wall clock, plain goroutines and timers.
func RealEnv() Env {
	return Env{
		Now: realNow,
		Go:  func(f func()) { go f() },
		Wait: func(ctx context.Context, ready <-chan struct{}, d time.Duration) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var tick <-chan time.Time
			if d > 0 {
				t := time.NewTimer(d)
				defer t.Stop()
				tick = t.C
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
			case <-tick:
			}
			return nil
		},
	}
}

func (e Env) sleep(ctx context.Context, d time.Duration) error { return e.Wait(ctx, nil, d) }
