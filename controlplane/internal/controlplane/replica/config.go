package replica

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Config tunes the control-plane machine. Zero value keeps catch-up and the
// commit queue off (CP-DSM-2 scaffold behaviour). CatchUpEvery > 0 arms
// timer-driven catch-up with slot-ordered apply (CP-DSM-4). The commit queue
// keeps one LogAppend in flight (CP-DSM-5).
// Heartbeat or TakeoverAfter arms advisory roles (CP-DSM-6). CheckpointEvery or
// CheckpointPrefix arms checkpoint restore/publish and optional LogTrim.
type Config struct {
	// CatchUpEvery is the pause between catch-up rounds. Zero disables catch-up.
	CatchUpEvery runtime.Duration
	// ReadLimit caps entries per LogRead during catch-up. Zero means 64.
	ReadLimit int32
	// Writer names append IDs (writer/seq). Empty means "cp".
	Writer string
	// OnCheckpointDecision, when set, is told every time the checkpoint timer
	// fires: whether a checkpoint was started or skipped, and why. It only
	// reports; it must not touch the machine. The simulator leaves it nil.
	OnCheckpointDecision func(CheckpointDecision)

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

	// Rank orders nodes for the first claim: rank 0 claims at once on an empty
	// log, and every other claim waits TakeoverAfter + Rank*TakeoverRankStep.
	Rank int
	// Heartbeat is how long an idle active node waits before a no-op append.
	// Zero disables heartbeat (and, with TakeoverAfter 0, roles).
	Heartbeat runtime.Duration
	// TakeoverAfter is how long a standby waits with no other-writer slot
	// before it claims the write role. Zero means only Promote flips the role.
	TakeoverAfter runtime.Duration
	// TakeoverRankStep is added to TakeoverAfter once per rank, so the standbys
	// of a lower rank claim first. Zero gives every rank the same wait.
	TakeoverRankStep runtime.Duration

	// CheckpointEvery is the pause between checkpoint attempts on the appender.
	// Zero disables the timer; set CheckpointPrefix alone to restore on NodeStarted.
	CheckpointEvery runtime.Duration
	// CheckpointMinSlots skips a checkpoint when fewer slots were applied since the
	// last. Zero means 1.
	CheckpointMinSlots int64
	// CheckpointPrefix is the object-store key prefix of the snapshots. Each
	// snapshot is stored under prefix + the 20-digit slot it was taken at, and
	// restore reads the newest. Empty means "checkpoint/" when checkpointing is
	// enabled.
	CheckpointPrefix string
	// KeepSlots is how many log slots to keep behind a published checkpoint
	// before LogTrim. Zero skips trim.
	KeepSlots int64

	// OpenStore makes the empty key-value store that the fold keeps its state
	// in. It runs when the machine is built and again after a crash restart or
	// a trimmed-slot restore, once the old store has been closed. Nil means an
	// in-memory store. The machine itself never imports a database.
	OpenStore func() (engine.Engine, error)

	// WindowSlots is the idempotency window length. Keys older than this many
	// slots are forgotten at TrimStride boundaries (decision 48). Zero skips.
	WindowSlots int64
	// TrimStride is how often (in slots) the idempotency window is cut.
	// Zero means 16 when WindowSlots > 0.
	TrimStride int64

	// StallProb / CrashProb are per role-tick cooperative faults (sim only).
	// Fault() suppresses them after EnterRecovery. Zero disables.
	StallProb, CrashProb float64
	StallDur, DownDur    [2]runtime.Duration
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
func (c Config) openMetadata() (*metadb.DB, error) {
	if c.OpenStore == nil {
		return metadb.NewMem(), nil
	}
	keyValue, err := c.OpenStore()
	if err != nil {
		return nil, err
	}
	return metadb.New(keyValue), nil
}
