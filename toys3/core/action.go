package core

// Action is an output of Step: a request for the runtime to do something.
// Every filesystem action carries an IOID; its result returns as the matching
// ...Done event. Actions in one returned slice are started in order, but the
// filesystem actions among them may complete in any order.
type Action interface{ isAction() }

// ---- client replies ----

// Reply answers the Request with the same Client id. Body is the object
// payload for a 200 GET and nil otherwise.
type Reply struct {
	Client ClientReq
	Status Status
	Body   []byte
}

// ---- filesystem ----
//
// Paths are slash-separated and absolute under Config.Dir (see ObjPath,
// TmpPath). Changes are volatile until made durable: file contents by FSFsync
// of the file, directory entries (create, rename, unlink) by FSFsyncDir of the
// directory that holds them.

// FSWrite creates Path, or truncates it if it exists, and writes Data as the
// whole file content. Creating the entry is a directory change (needs
// FSFsyncDir to be durable); the content needs FSFsync.
type FSWrite struct {
	IO   IOID
	Path string
	Data []byte
}

// FSFsync makes the content of the file at Path durable.
type FSFsync struct {
	IO   IOID
	Path string
}

// FSRename atomically replaces To with From (POSIX rename). Both paths must be
// on the same filesystem. Durable only after FSFsyncDir of To's directory (and
// of From's, for the old name to be durably gone).
type FSRename struct {
	IO       IOID
	From, To string
}

// FSFsyncDir makes the directory entries of Dir durable.
type FSFsyncDir struct {
	IO  IOID
	Dir string
}

// FSRead reads the whole file at Path.
type FSRead struct {
	IO   IOID
	Path string
}

// FSUnlink removes the directory entry Path.
type FSUnlink struct {
	IO   IOID
	Path string
}

// FSList lists the entry names in Dir (used by recovery).
type FSList struct {
	IO  IOID
	Dir string
}

// ---- process control ----

// Halt asks the runtime to kill this incarnation and restart it (fail-stop),
// after executing the actions that precede it in the same slice. Use it when
// in-memory state can no longer be trusted, e.g. after an fsync error. In
// simulation it is an ordinary crash; in production the process exits.
type Halt struct{ Reason string }

// ---- observability ----

// Emit records a Fact. The simulator's invariants read facts; production logs
// them. Emit has no effect on behavior.
type Emit struct{ Fact Fact }

func (Reply) isAction()      {}
func (FSWrite) isAction()    {}
func (FSFsync) isAction()    {}
func (FSRename) isAction()   {}
func (FSFsyncDir) isAction() {}
func (FSRead) isAction()     {}
func (FSUnlink) isAction()   {}
func (FSList) isAction()     {}
func (Halt) isAction()       {}
func (Emit) isAction()       {}
