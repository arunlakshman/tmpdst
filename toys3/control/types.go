package control

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

// Status is the HTTP status of a Reply.
type Status uint16

const (
	StatusOK          Status = 200
	StatusBadRequest  Status = 400 // invalid key or object too large; nothing was touched
	StatusNotFound    Status = 404
	StatusServerError Status = 500 // filesystem error or corrupt object; outcome of a mutation is unknown
)
