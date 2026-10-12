package core

// Event is the input to Step. The runtime stamps At when it dequeues the event,
// so At never decreases across successive Steps of one incarnation.
type Event struct {
	At   Time
	Body EventBody
}

// EventBody is a closed set of plain-data structs (no funcs, channels or
// pointers into shared state), so events can be logged, compared and replayed.
type EventBody interface{ isEvent() }

// ---- lifecycle ----

// Started is the first event of every boot, including after a crash. Nothing
// in memory survived; the server begins recovery (see package doc).
type Started struct{}

// ---- client requests ----

// Request is a client request that has been fully received and decoded. The
// server answers it with exactly one Reply carrying the same Client id (unless
// it crashes first).
type Request struct {
	Client ClientReq
	Op     Op
}

// Op is a closed set of object operations. Keys are opaque, non-empty strings
// (the HTTP layer flattens "bucket/key" into one key); see ValidKey.
type Op interface{ isOp() }

// Put stores Data under Key, replacing any existing object.
type Put struct {
	Key  string
	Data []byte
}

// Get returns the object stored under Key.
type Get struct{ Key string }

// Delete removes Key. Deleting an absent key replies 404.
type Delete struct{ Key string }

// Copy stores the current contents of Src under Dst. Src == Dst is allowed
// (it rewrites the object). An absent Src replies 404 and leaves Dst alone.
type Copy struct{ Src, Dst string }

func (Put) isOp()    {}
func (Get) isOp()    {}
func (Delete) isOp() {}
func (Copy) isOp()   {}

func (Started) isEvent() {}
func (Request) isEvent() {}
