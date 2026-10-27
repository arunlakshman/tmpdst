package machine

import "manifester/core"

// Config tunes the control-plane machine. Zero value keeps catch-up off (CP-DSM-2
// scaffold behaviour). CatchUpEvery > 0 arms the timer-driven log catch-up
// sketch (CheckTail/Read only; no apply yet).
type Config struct {
	// CatchUpEvery is the pause between catch-up rounds. Zero disables catch-up.
	CatchUpEvery core.Duration
	// ReadLimit caps entries per LogRead during catch-up. Zero means 64.
	ReadLimit int32
}

func (c Config) readLimit() int32 {
	if c.ReadLimit > 0 {
		return c.ReadLimit
	}
	return 64
}
