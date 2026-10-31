package statemachine

import (
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// Config tunes the control-plane machine. Zero value keeps catch-up and the
// commit queue off (CP-DSM-2 scaffold behaviour). CatchUpEvery > 0 arms
// timer-driven catch-up with slot-ordered apply and gap-fill (CP-DSM-4).
// K > 0 arms the commit queue with up to K in-flight LogAppend slots (CP-DSM-5).
// Heartbeat or TakeoverAfter arms advisory roles (CP-DSM-6). CheckpointEvery or
// CheckpointKey arms checkpoint restore/publish and optional LogTrim.
type Config struct {
	// CatchUpEvery is the pause between catch-up rounds. Zero disables catch-up.
	CatchUpEvery core.Duration
	// ReadLimit caps entries per LogRead during catch-up. Zero means 64.
	ReadLimit int32
	// GapAfter is how long the next unapplied slot may stay empty while later
	// slots are buffered before Step emits a no-op LogAppend. Zero fills at
	// once when a gap is visible.
	GapAfter core.Duration
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
	// MaxEntryBytes caps the size of one log entry (envelope, group header and
	// commands). A group stops growing at this size, and one command that does
	// not fit alone is refused with ErrTooLarge. Zero means 350 KiB, under the
	// 400 KiB item limit of DynamoDB. It is not part of StateHash.
	MaxEntryBytes int

	// Rank is the advisory preference (0 preferred). Used for failback.
	Rank int
	// Heartbeat is how long an idle active node waits before a no-op append.
	// Zero disables heartbeat (and, with TakeoverAfter 0, roles).
	Heartbeat core.Duration
	// TakeoverAfter is how long a standby waits with no other-writer slot
	// before promoting. Zero means only Promote flips the role.
	TakeoverAfter core.Duration

	// CheckpointEvery is the pause between checkpoint attempts on the appender.
	// Zero disables the timer; set CheckpointKey alone to restore on NodeStarted.
	CheckpointEvery core.Duration
	// CheckpointMinSlots skips a checkpoint when fewer slots were applied since the
	// last. Zero means 1.
	CheckpointMinSlots int64
	// CheckpointKey is the object-store key for the latest snapshot. Empty means
	// "ckpt/latest" when checkpointing is enabled.
	CheckpointKey string
	// KeepSlots is how many log slots to keep behind a published checkpoint
	// before LogTrim. Zero skips trim.
	KeepSlots int64

	// OpenStore makes the empty key-value store that the fold keeps its state
	// in. It runs when the machine is built and again after a crash restart or
	// a trimmed-slot restore, once the old store has been closed. Nil means an
	// in-memory store. The machine itself never imports a database.
	OpenStore func() (stateupdater.KeyValueStore, error)

	// InjectedBug selects a sim mutant (fakelogservice.Bug* constants). Zero is none.
	InjectedBug uint8
	// WindowSlots is the idempotency window length. Keys older than this many
	// slots are forgotten at TrimStride boundaries (decision 48). Zero skips.
	WindowSlots int64
	// TrimStride is how often (in slots) the idempotency window is cut.
	// Zero means 16 when WindowSlots > 0.
	TrimStride int64

	// StallProb / CrashProb are per role-tick cooperative faults (sim only).
	// Fault() suppresses them after EnterRecovery. Zero disables.
	StallProb, CrashProb float64
	StallDur, DownDur    [2]core.Duration
}

// InjectedBug constants match sim/engines/fakelogservice (kept numeric for LogParams.Bug).
const (
	InjectedBugNone             uint8 = iota
	InjectedBugWriterOffsets          // offsets chosen by the writer instead of by apply
	InjectedBugTrimDelete             // trimmed slots deleted, not tombstoned (logstore)
	InjectedBugPerReplicaWindow       // each replica forgets keys on its own clock
	InjectedBugGapSkip                // pass an empty slot without a no-op fill
)

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

func (c Config) maxEntryBytes() int {
	if c.MaxEntryBytes > 0 {
		return c.MaxEntryBytes
	}
	return 350 << 10
}

func (c Config) queueMax() int {
	if c.QueueMax > 0 {
		return c.QueueMax
	}
	return 4096
}

// openFold builds a fold over a new empty store: Config.OpenStore, else memory.
func (c Config) openFold() (*stateupdater.StateUpdater, error) {
	if c.OpenStore == nil {
		return stateupdater.NewMem(), nil
	}
	keyValue, err := c.OpenStore()
	if err != nil {
		return nil, err
	}
	return stateupdater.New(keyValue), nil
}
