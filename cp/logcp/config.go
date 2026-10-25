package logcp

import (
	"errors"
	"log/slog"
	"time"

	"manifester/cp/applier"
	"manifester/sys"
)

// Config configures a Service. Log, KV and Name are required; every other
// field has the default stated.
//
// WindowSlots and TrimStride are CLUSTER constants: they decide when a segment
// key is forgotten, which changes how a replayed key is answered, so every node
// reading one log must use the same values (they are not flags; leave them at
// the defaults outside tests).
type Config struct {
	Name string // node name, unique per node; stamped on every slot
	Rank int    // preference: 0 is the preferred appender, higher ranks are standbys
	// Incarnation makes entry IDs unique across restarts of one node
	// (default: the start time in ns).
	Incarnation string

	Log Log
	KV  applier.KV // an empty store: state is rebuilt from the checkpoint and the log

	// DB and Table hold the pointer item (CKPT);
	// Objects holds checkpoints. Without Objects (or DB) there is no checkpoint.
	DB      DB
	Table   string
	Objects sys.ObjectStore

	Env     *Env
	Metrics *Metrics
	Logger  *slog.Logger

	// Appending.
	K              int           // appends in flight (default 4)
	MaxSlotRate    float64       // slot appends per second the active node paces itself to (default 200: one hot key)
	MaxCmdsPerSlot int           // commands grouped in one slot (default 256)
	QueueMax       int           // commands queued before new ones are refused (default 4096)
	QueueBytesMax  int           // bytes queued before new ones are refused (default 64 MiB)
	Heartbeat      time.Duration // an idle active node appends a no-op this often (default 1 s)
	TakeoverAfter  time.Duration // a standby that saw no slot of another writer for this long becomes active (0: only Promote does)

	// Tailing and reads.
	HoleAfter time.Duration // the next slot may stay empty this long, while later ones exist, before a no-op fills it (default 250 ms)
	TailPoll  time.Duration // pause of the tailing loop (default 25 ms)
	ReadMax   int           // slots per tail read (default 500)
	TailBatch time.Duration // minimum time between two tail checks: reads arriving in between share one (default 10 ms)

	// Idempotency window and log retention, in slots (see the type comment).
	CommitDeadline time.Duration // brokers' retry horizon; sizes the defaults below (default 120 s)
	WindowSlots    int64         // a key is forgotten this many slots after its first commit (default 131072)
	TrimStride     int64         // the rule is evaluated every this many slots (default 1024)

	// Checkpoint and trim.
	CkptEvery    time.Duration // checkpoint period (default 5 min); <0 disables the loop
	CkptMinSlots int64         // skip a checkpoint when fewer slots were applied since the last (default 1000)
	KeepSlots    int64         // log slots kept behind the checkpoint (default WindowSlots)
	TrimDelete   bool          // delete trimmed slots explicitly (otherwise only the TTL attribute expires them)
}

func (c *Config) defaults() error {
	if c.Name == "" || c.Log == nil || c.KV == nil {
		return errors.New("logcp: Name, Log and KV are required")
	}
	if c.Env == nil {
		e := RealEnv()
		c.Env = &e
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Incarnation == "" {
		c.Incarnation = itoa(c.Env.Now())
	}
	setI(&c.K, 4)
	setI(&c.MaxCmdsPerSlot, 256)
	setI(&c.QueueMax, 4096)
	setI(&c.QueueBytesMax, 64<<20)
	setI(&c.ReadMax, 500)
	setF(&c.MaxSlotRate, 200)
	setD(&c.Heartbeat, time.Second)
	setD(&c.HoleAfter, 250*time.Millisecond)
	setD(&c.TailPoll, 25*time.Millisecond)
	setD(&c.TailBatch, 10*time.Millisecond)
	setD(&c.CommitDeadline, 120*time.Second)
	if c.CkptEvery == 0 {
		c.CkptEvery = 5 * time.Minute
	}
	setI64(&c.WindowSlots, 131072)
	setI64(&c.TrimStride, 1024)
	setI64(&c.CkptMinSlots, 1000)
	setI64(&c.KeepSlots, c.WindowSlots)
	return nil
}

func setI(p *int, d int) {
	if *p <= 0 {
		*p = d
	}
}

func setI64(p *int64, d int64) {
	if *p <= 0 {
		*p = d
	}
}

func setF(p *float64, d float64) {
	if *p <= 0 {
		*p = d
	}
}

func setD(p *time.Duration, d time.Duration) {
	if *p <= 0 {
		*p = d
	}
}

func itoa(n int64) string { return slotKey(n)[8:] }
