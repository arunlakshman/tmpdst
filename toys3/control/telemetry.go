package control

// Telemetry is a closed set of structured signals the controller publishes
// for supervisory monitoring. They describe what the controller believes and
// have no effect on behavior. Any number of subscribers may read them:
// production logs them; the simulator's monitors (invariants) check those
// beliefs against ground truth (the plant model's durable state and the
// client-visible history), and its coverage stats count them. A telemetry
// value may carry fields only a monitor needs (Committed.Sum).
//
// Every type is a domain event, named in the past tense.
type Telemetry interface{ isTelemetry() }

// Committed is emitted immediately before the 200 Reply of a PUT, COPY or
// DELETE, at the moment the controller believes the mutation is durable. The
// simulator checks it against the plant model's durable state on the same
// step ("acked implies durable"), which catches a missing fsync without
// waiting for a crash to expose it.
type Committed struct {
	Client  ClientReq
	Key     string // the key that changed (Dst for COPY)
	Deleted bool
	Sum     uint32 // CRC-32 of the new payload; 0 when Deleted
}

// Recovered is emitted once per boot when recovery finishes and the
// controller starts serving queued requests.
type Recovered struct {
	TempsRemoved int
}

// CorruptionDetected is emitted when an object file fails its header or CRC
// check (fault detection). A correct controller with an honest filesystem
// never emits it; the simulator counts it (coverage) and the replies around
// it must be 500.
type CorruptionDetected struct {
	Key string
}

func (Committed) isTelemetry()          {}
func (Recovered) isTelemetry()          {}
func (CorruptionDetected) isTelemetry() {}
