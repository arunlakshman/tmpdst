package control

// Fact is a closed set of structured observations the server emits for
// invariants and tracing. Facts describe what the server believes; the
// simulator checks those beliefs against ground truth (the fake filesystem's
// durable state and the client-visible history).
type Fact interface{ isFact() }

// Committed is emitted immediately before the 200 Reply of a PUT, COPY or
// DELETE, at the moment the server believes the mutation is durable. The
// simulator checks it against the fake filesystem's durable state on the same
// step ("acked implies durable"), which catches a missing fsync without
// waiting for a crash to expose it.
type Committed struct {
	Client  ClientReq
	Key     string // the key that changed (Dst for COPY)
	Deleted bool
	Sum     uint32 // CRC-32 of the new payload; 0 when Deleted
}

// Recovered is emitted once per boot when recovery finishes and the server
// starts serving queued requests.
type Recovered struct {
	TempsRemoved int
}

// CorruptObject is emitted when an object file fails its header or CRC check.
// A correct server with an honest filesystem never emits it; the simulator
// counts it (coverage) and the replies around it must be 500.
type CorruptObject struct {
	Key string
}

func (Committed) isFact()     {}
func (Recovered) isFact()     {}
func (CorruptObject) isFact() {}
