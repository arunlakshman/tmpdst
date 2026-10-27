package machine

import "manifester/core"

// Config tunes the control-plane machine. Zero value keeps catch-up off
// (CP-DSM-2 scaffold behaviour). CatchUpEvery > 0 arms timer-driven catch-up
// with slot-ordered apply and hole-fill (CP-DSM-4).
type Config struct {
	// CatchUpEvery is the pause between catch-up rounds. Zero disables catch-up.
	CatchUpEvery core.Duration
	// ReadLimit caps entries per LogRead during catch-up. Zero means 64.
	ReadLimit int32
	// HoleAfter is how long the next unapplied slot may stay empty while later
	// slots are buffered before Step emits a no-op LogAppend. Zero fills at
	// once when a hole is visible.
	HoleAfter core.Duration
	// Writer names no-op append IDs (writer/seq). Empty means "cp".
	Writer string
}

func (c Config) readLimit() int32 {
	if c.ReadLimit > 0 {
		return c.ReadLimit
	}
	return 64
}

func (c Config) writer() string {
	if c.Writer != "" {
		return c.Writer
	}
	return "cp"
}
