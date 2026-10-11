package core

// Time is a monotonic instant in nanoseconds, stamped on each Event by the
// runtime. In simulation it is the virtual clock.
type Time int64

// Duration is nanoseconds.
type Duration int64

const (
	Nanosecond  Duration = 1
	Microsecond          = 1000 * Nanosecond
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// ClientReq identifies one client request. The runtime assigns it when the
// request arrives (prod: per HTTP request; sim: per workload operation) and the
// server echoes it on the Reply. Unique within one runtime lifetime.
type ClientReq uint64

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

// Status is the HTTP status of a Reply.
type Status uint16

const (
	StatusOK          Status = 200
	StatusBadRequest  Status = 400 // invalid key or object too large; nothing was touched
	StatusNotFound    Status = 404
	StatusServerError Status = 500 // filesystem error or corrupt object; outcome of a mutation is unknown
)
