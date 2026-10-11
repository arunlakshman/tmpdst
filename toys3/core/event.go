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

// ---- filesystem completions (one per FS action, matched by IO) ----

// FSWriteDone completes FSWrite. On ErrIO the file may hold any prefix of Data.
type FSWriteDone struct {
	IO  IOID
	Err ErrCode
}

// FSFsyncDone completes FSFsync. On ErrIO nothing is known to be durable.
type FSFsyncDone struct {
	IO  IOID
	Err ErrCode
}

// FSRenameDone completes FSRename. On ErrIO the rename may or may not have
// happened.
type FSRenameDone struct {
	IO  IOID
	Err ErrCode
}

// FSFsyncDirDone completes FSFsyncDir.
type FSFsyncDirDone struct {
	IO  IOID
	Err ErrCode
}

// FSReadDone completes FSRead. Data is the raw file (header included) when
// Err == OK, nil otherwise.
type FSReadDone struct {
	IO   IOID
	Data []byte
	Err  ErrCode
}

// FSUnlinkDone completes FSUnlink. ErrNotFound means the path did not exist.
type FSUnlinkDone struct {
	IO  IOID
	Err ErrCode
}

// FSListDone completes FSList. Names are base names, sorted ascending.
type FSListDone struct {
	IO    IOID
	Names []string
	Err   ErrCode
}

func (Started) isEvent()        {}
func (Request) isEvent()        {}
func (FSWriteDone) isEvent()    {}
func (FSFsyncDone) isEvent()    {}
func (FSRenameDone) isEvent()   {}
func (FSFsyncDirDone) isEvent() {}
func (FSReadDone) isEvent()     {}
func (FSUnlinkDone) isEvent()   {}
func (FSListDone) isEvent()     {}
