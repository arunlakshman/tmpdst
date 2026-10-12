package control

// The plant interface: the commands the server sends to the filesystem
// (actuations) and the completions that come back (feedback). The server owns
// this vocabulary; the runtimes implement it (prod.OSExecutor on real disks,
// the simulator's fake filesystem in sim).

// Actuation is a filesystem Action: a command to the plant. Its result
// returns as the Feedback event with the same ID.
type Actuation interface {
	Action
	ID() IOID
}

// Feedback is a filesystem completion event: the plant's answer to the
// Actuation with the same ID.
type Feedback interface {
	EventBody
	ID() IOID
}

// IOID correlates a filesystem Action with its ...Done completion. The server
// assigns IOIDs from a counter; they are unique within one incarnation, which
// is enough because completions addressed to a dead incarnation are dropped.
type IOID uint64

// ErrCode is a filesystem error as plain data, so events stay comparable and
// hashable.
type ErrCode uint8

const (
	OK          ErrCode = iota
	ErrNotFound         // ENOENT: the path (or rename source) does not exist
	ErrIO               // EIO or anything else; the operation may or may not have taken effect
)

func (e ErrCode) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrNotFound:
		return "ENOENT"
	case ErrIO:
		return "EIO"
	}
	return "ErrCode(?)"
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

func (FSWrite) isAction()    {}
func (FSFsync) isAction()    {}
func (FSRename) isAction()   {}
func (FSFsyncDir) isAction() {}
func (FSRead) isAction()     {}
func (FSUnlink) isAction()   {}
func (FSList) isAction()     {}

func (a FSWrite) ID() IOID    { return a.IO }
func (a FSFsync) ID() IOID    { return a.IO }
func (a FSRename) ID() IOID   { return a.IO }
func (a FSFsyncDir) ID() IOID { return a.IO }
func (a FSRead) ID() IOID     { return a.IO }
func (a FSUnlink) ID() IOID   { return a.IO }
func (a FSList) ID() IOID     { return a.IO }

func (FSWriteDone) isEvent()    {}
func (FSFsyncDone) isEvent()    {}
func (FSRenameDone) isEvent()   {}
func (FSFsyncDirDone) isEvent() {}
func (FSReadDone) isEvent()     {}
func (FSUnlinkDone) isEvent()   {}
func (FSListDone) isEvent()     {}

func (d FSWriteDone) ID() IOID    { return d.IO }
func (d FSFsyncDone) ID() IOID    { return d.IO }
func (d FSRenameDone) ID() IOID   { return d.IO }
func (d FSFsyncDirDone) ID() IOID { return d.IO }
func (d FSReadDone) ID() IOID     { return d.IO }
func (d FSUnlinkDone) ID() IOID   { return d.IO }
func (d FSListDone) ID() IOID     { return d.IO }

var (
	_ Actuation = FSWrite{}
	_ Feedback  = FSWriteDone{}
)
