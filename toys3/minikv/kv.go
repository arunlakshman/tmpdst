package minikv

import (
	"bytes"
	"fmt"
)

// Time is a virtual instant in nanoseconds. The server never reads a clock;
// the simulator stamps At on each event it delivers.
type Time int64

const (
	Microsecond Time = 1000
	Millisecond      = 1000 * Microsecond
)

// IOID names one filesystem operation. The completion that comes back carries
// the same id, which is how the server tells its in-flight requests apart.
type IOID uint64

// Event is one input to Step.
type Event struct {
	At   Time
	Body EventBody
}

// EventBody is a closed set: the marker method is unexported, so no other
// package can add a case. Events are plain data, which is what makes them
// loggable, hashable, and replayable.
type EventBody interface{ isEvent() }

// Request is a client call that has been fully received.
type Request struct {
	Client uint64
	Op     Op
}

// Op is the closed set of client operations. Part 1 has PUT and GET.
type Op interface{ isOp() }

// Put stores Data under Key, replacing whatever was there.
type Put struct {
	Key  string
	Data []byte
}

// Get returns whatever is stored under Key.
type Get struct{ Key string }

func (Put) isOp() {}
func (Get) isOp() {}

// WriteDone, and the other *Done types, are the disk reporting that an
// operation finished. On this post's perfect disk, finished means succeeded.
type WriteDone struct{ IO IOID }
type FsyncDone struct{ IO IOID }
type RenameDone struct{ IO IOID }
type FsyncDirDone struct{ IO IOID }

// ReadDone is a finished read. Found is false when the path does not exist.
// Data is nil in that case, and a private copy otherwise.
type ReadDone struct {
	IO    IOID
	Found bool
	Data  []byte
}

func (Request) isEvent()      {}
func (WriteDone) isEvent()    {}
func (FsyncDone) isEvent()    {}
func (RenameDone) isEvent()   {}
func (FsyncDirDone) isEvent() {}
func (ReadDone) isEvent()     {}

// Action is one thing Step asks the runtime to do. The server does not do it.
type Action interface{ isAction() }

// Write, Fsync, Rename, FsyncDir, and Read are requests to the disk.
// Each carries the IOID the completion must echo.
type Write struct {
	IO   IOID
	Path string
	Data []byte
}
type Fsync struct {
	IO   IOID
	Path string
}
type Rename struct {
	IO       IOID
	From, To string
}
type FsyncDir struct {
	IO  IOID
	Dir string
}
type Read struct {
	IO   IOID
	Path string
}

// Reply answers the Request with the same Client id.
// Status 200 carries Body for a GET; 404 means the key is absent.
type Reply struct {
	Client uint64
	Status int
	Body   []byte
}

func (Write) isAction()    {}
func (Fsync) isAction()    {}
func (Rename) isAction()   {}
func (FsyncDir) isAction() {}
func (Read) isAction()     {}
func (Reply) isAction()    {}

// phase is where an in-flight request is in its protocol.
type phase int

const (
	phPutWrite phase = iota
	phPutFsync
	phPutRename
	phPutFsyncDir
	phGetRead
)

// inflight is one request that has started and not yet replied.
// Everything a straight-line handler would keep in local variables has to
// live here, because Step returns between I/O operations.
type inflight struct {
	client uint64
	phase  phase
	io     IOID
	key    string
	data   []byte
	tmp    string
}

// Server is minikv. Step is the only place state changes. It never blocks
// and never touches a disk, a clock, or another goroutine.
type Server struct {
	nextIO IOID
	cur    *inflight
	wait   []Request // accepted, not yet started, in arrival order
}

// Step consumes one event and returns the actions it causes.
func (s *Server) Step(ev Event) []Action {
	switch b := ev.Body.(type) {
	case Request:
		return s.onRequest(b)
	case WriteDone:
		return s.onWriteDone(b)
	case FsyncDone:
		return s.onFsyncDone(b)
	case RenameDone:
		return s.onRenameDone(b)
	case FsyncDirDone:
		return s.onFsyncDirDone(b)
	case ReadDone:
		return s.onReadDone(b)
	default:
		panic(fmt.Sprintf("minikv: unknown event %T", ev.Body))
	}
}

func (s *Server) onRequest(r Request) []Action {
	if s.cur != nil {
		s.wait = append(s.wait, r)
		return nil
	}
	return s.start(r)
}

func (s *Server) start(r Request) []Action {
	switch op := r.Op.(type) {
	case Put:
		io := s.alloc()
		s.cur = &inflight{
			client: r.Client,
			phase:  phPutWrite,
			io:     io,
			key:    op.Key,
			data:   bytes.Clone(op.Data),
			tmp:    fmt.Sprintf("tmp/%d", io),
		}
		return []Action{Write{IO: io, Path: s.cur.tmp, Data: bytes.Clone(s.cur.data)}}
	case Get:
		io := s.alloc()
		s.cur = &inflight{client: r.Client, phase: phGetRead, io: io, key: op.Key}
		return []Action{Read{IO: io, Path: "obj/" + op.Key}}
	default:
		return []Action{Reply{Client: r.Client, Status: 400}}
	}
}

func (s *Server) onWriteDone(d WriteDone) []Action {
	s.expect(d.IO, phPutWrite)
	io := s.alloc()
	s.cur.io = io
	s.cur.phase = phPutFsync
	return []Action{Fsync{IO: io, Path: s.cur.tmp}}
}

func (s *Server) onFsyncDone(d FsyncDone) []Action {
	s.expect(d.IO, phPutFsync)
	io := s.alloc()
	s.cur.io = io
	s.cur.phase = phPutRename
	return []Action{Rename{IO: io, From: s.cur.tmp, To: "obj/" + s.cur.key}}
}

func (s *Server) onRenameDone(d RenameDone) []Action {
	s.expect(d.IO, phPutRename)
	io := s.alloc()
	s.cur.io = io
	s.cur.phase = phPutFsyncDir
	return []Action{FsyncDir{IO: io, Dir: "obj"}}
}

func (s *Server) onFsyncDirDone(d FsyncDirDone) []Action {
	s.expect(d.IO, phPutFsyncDir)
	return s.finish(200, nil)
}

func (s *Server) onReadDone(d ReadDone) []Action {
	s.expect(d.IO, phGetRead)
	if !d.Found {
		return s.finish(404, nil)
	}
	return s.finish(200, bytes.Clone(d.Data))
}

// finish replies to the current request and, if one is waiting, starts it
// in the same step. One Step may therefore emit a Reply and the next
// request's first disk action together.
func (s *Server) finish(status int, body []byte) []Action {
	client := s.cur.client
	s.cur = nil
	out := []Action{Reply{Client: client, Status: status, Body: body}}
	if len(s.wait) == 0 {
		return out
	}
	next := s.wait[0]
	s.wait = s.wait[1:]
	return append(out, s.start(next)...)
}

func (s *Server) alloc() IOID {
	s.nextIO++
	return s.nextIO
}

func (s *Server) expect(io IOID, ph phase) {
	if s.cur == nil || s.cur.io != io || s.cur.phase != ph {
		got := "idle"
		if s.cur != nil {
			got = fmt.Sprintf("io=%d phase=%d", s.cur.io, s.cur.phase)
		}
		panic(fmt.Sprintf("minikv: completion io=%d phase=%d, server is %s", io, ph, got))
	}
}
