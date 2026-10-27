package machine

import "manifester/core"

// Config tunes the control-plane machine. Zero value keeps catch-up and the
// commit queue off (CP-DSM-2 scaffold behaviour). CatchUpEvery > 0 arms
// timer-driven catch-up with slot-ordered apply and hole-fill (CP-DSM-4).
// K > 0 arms the commit queue with up to K in-flight LogAppend slots (CP-DSM-5).
type Config struct {
	// CatchUpEvery is the pause between catch-up rounds. Zero disables catch-up.
	CatchUpEvery core.Duration
	// ReadLimit caps entries per LogRead during catch-up. Zero means 64.
	ReadLimit int32
	// HoleAfter is how long the next unapplied slot may stay empty while later
	// slots are buffered before Step emits a no-op LogAppend. Zero fills at
	// once when a hole is visible.
	HoleAfter core.Duration
	// Writer names append IDs (writer/seq). Empty means "cp".
	Writer string

	// K is the max number of commit LogAppends in flight. Zero disables the
	// commit queue (Offer refuses).
	K int
	// MaxCmdsPerSlot caps commands packed into one commit append. Zero means 256.
	MaxCmdsPerSlot int
	// QueueMax caps queued client ops before Offer answers ErrUnavailable.
	// Zero means 4096.
	QueueMax int
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

func (c Config) maxCmds() int {
	if c.MaxCmdsPerSlot > 0 {
		return c.MaxCmdsPerSlot
	}
	return 256
}

func (c Config) queueMax() int {
	if c.QueueMax > 0 {
		return c.QueueMax
	}
	return 4096
}
