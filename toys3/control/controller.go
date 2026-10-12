package control

import "manifester/toys3/format"

// Config is the controller's static configuration.
type Config struct {
	// Dir is the data root; obj/ and tmp/ live under it and must already exist.
	Dir string
	// MaxObjectSize bounds PUT payloads (400 above it). 0 means no limit.
	MaxObjectSize int
}

// mode is the controller's operating mode within one incarnation: startup,
// recovering, automatic (closed-loop serving), or tripped. The numeric values
// are folded into StateHash: append new modes, never reorder them.
type mode uint8

const (
	modeStartup    mode = iota // before Started; requests are queued
	modeRecovering             // listing and unlinking temp files; requests are queued
	modeAuto                   // normal operation
	modeTripped                // Trip was returned; every later event is ignored
)

// opKind is the kind of a client request. Hashed by StateHash: append only.
type opKind uint8

const (
	kindPut opKind = iota
	kindGet
	kindDelete
	kindCopy
)

// reqStep is the protocol step a request is on. Each step except
// stepWaiting has exactly one filesystem action outstanding (request.io), and
// names the completion it waits for. Hashed by StateHash: append only.
type reqStep uint8

const (
	stepWaiting    reqStep = iota // start permissive false: an earlier request holds one of its keys (or recovery runs)
	stepReading                   // GET: FSRead(obj/key); COPY: FSRead(obj/src)
	stepWritingTmp                // PUT/COPY: FSWrite(tmp)
	stepFsyncTmp                  // PUT/COPY: FSFsync(tmp)
	stepRenaming                  // PUT/COPY: FSRename(tmp → obj/key)
	stepFsyncObj                  // PUT/COPY/DELETE: FSFsyncDir(obj)
	stepUnlinking                 // DELETE: FSUnlink(obj/key)
)

// request is one accepted client request and its progress.
type request struct {
	seq    uint64 // arrival order; defines every per-key FIFO queue
	client ClientReq
	kind   opKind
	key    string // the key read or written (Dst for COPY)
	src    string // COPY only
	data   []byte // PUT payload; for COPY, the src payload once read
	step   reqStep
	io     IOID   // the outstanding FS action; 0 while stepWaiting
	tmp    string // temp file path, once FSWrite was issued
}

// touches reports whether r reads or writes key.
func (r *request) touches(key string) bool {
	return r.key == key || (r.kind == kindCopy && r.src == key)
}

// interlocked reports whether a and b share a key, so they must not run at
// the same time (the key interlock).
func interlocked(a, b *request) bool {
	return a.touches(b.key) || (b.kind == kindCopy && a.touches(b.src))
}

// Controller is the toys3 state machine: the control law of the object
// server. It turns client requests and plant feedback (filesystem
// completions) into actuations (filesystem commands), replies and telemetry.
// All of its state is plain data, rebuilt from nothing on every boot. It is
// the single aggregate of the domain: one Step is its consistency boundary.
//
// Key interlock: per-key FIFO queues are implicit. reqs holds every accepted,
// unfinished request in arrival order, and the queue of key k is the
// subsequence of reqs that touches k. A request's start permissive holds when
// no earlier request in reqs is interlocked with it (shares a key), i.e. it is
// at the head of every queue it is in. This is O(in-flight²) per step, which
// is fine for a toy and keeps the state small and obviously deterministic (no
// maps at all).
type Controller struct {
	env  Env
	cfg  Config
	mode mode

	nextIO  IOID   // last IOID issued
	nextSeq uint64 // last arrival sequence issued

	// Recovery bookkeeping (modeRecovering only).
	rec        recStep
	recIO      []IOID // outstanding recovery IOs (one, or one per leftover temp)
	recRemoved int    // temps unlinked successfully

	reqs []*request // accepted, unfinished requests, ascending seq
}

// New returns a server for one boot. It does nothing until Started.
func New(env Env, cfg Config) *Controller {
	return &Controller{env: env, cfg: cfg, mode: modeStartup}
}

// Step consumes one event and returns the actions to execute.
func (s *Controller) Step(ev Event) []Action {
	if s.mode == modeTripped {
		return nil
	}
	switch b := ev.Body.(type) {
	case Started:
		return s.onStarted()
	case Request:
		return s.onRequest(b)
	}
	fb, ok := ev.Body.(Feedback)
	if !ok {
		return nil
	}
	io := fb.ID()
	if s.mode == modeRecovering && indexIO(s.recIO, io) >= 0 {
		return s.onRecoveryDone(io, ev.Body)
	}
	for _, r := range s.reqs {
		if r.step != stepWaiting && r.io == io {
			return s.onRequestIO(r, ev.Body)
		}
	}
	return nil // unknown or stale IOID: ignore
}

func (s *Controller) newIO() IOID {
	s.nextIO++
	return s.nextIO
}

// trip fail-stops: nothing after it in this incarnation is trusted. The
// runtime executes the actions before it (here: the 500, if any) and then
// kills the incarnation.
func (s *Controller) trip(cause string, before ...Action) []Action {
	s.mode = modeTripped
	return append(before, Trip{Cause: cause})
}

// ---- requests ----

func (s *Controller) onRequest(req Request) []Action {
	r := &request{client: req.Client}
	switch op := req.Op.(type) {
	case Put:
		if !format.ValidKey(op.Key) || (s.cfg.MaxObjectSize > 0 && len(op.Data) > s.cfg.MaxObjectSize) {
			return badRequest(req.Client)
		}
		r.kind, r.key, r.data = kindPut, op.Key, op.Data
	case Get:
		if !format.ValidKey(op.Key) {
			return badRequest(req.Client)
		}
		r.kind, r.key = kindGet, op.Key
	case Delete:
		if !format.ValidKey(op.Key) {
			return badRequest(req.Client)
		}
		r.kind, r.key = kindDelete, op.Key
	case Copy:
		if !format.ValidKey(op.Src) || !format.ValidKey(op.Dst) {
			return badRequest(req.Client)
		}
		r.kind, r.key, r.src = kindCopy, op.Dst, op.Src
	default:
		return badRequest(req.Client)
	}
	s.nextSeq++
	r.seq = s.nextSeq
	s.reqs = append(s.reqs, r)
	if s.mode == modeAuto && s.permitted(r) {
		return s.start(r)
	}
	return nil // queued behind recovery or an earlier request on the same key
}

func badRequest(c ClientReq) []Action {
	return []Action{Reply{Client: c, Status: StatusBadRequest}}
}

// permitted reports whether r's start permissive holds: r is at the head of
// every key queue it is in (no earlier request is interlocked with it).
func (s *Controller) permitted(r *request) bool {
	for _, o := range s.reqs {
		if o.seq >= r.seq {
			return true
		}
		if interlocked(o, r) {
			return false
		}
	}
	return true
}

// startPermitted starts every waiting request whose start permissive now
// holds, in arrival order.
func (s *Controller) startPermitted() []Action {
	var acts []Action
	for _, r := range s.reqs {
		if r.step == stepWaiting && s.permitted(r) {
			acts = append(acts, s.start(r)...)
		}
	}
	return acts
}

// start issues the first FS action of r.
func (s *Controller) start(r *request) []Action {
	switch r.kind {
	case kindGet:
		return s.issue(r, stepReading, FSRead{IO: s.newIO(), Path: format.ObjPath(s.cfg.Dir, r.key)})
	case kindCopy:
		return s.issue(r, stepReading, FSRead{IO: s.newIO(), Path: format.ObjPath(s.cfg.Dir, r.src)})
	case kindDelete:
		return s.issue(r, stepUnlinking, FSUnlink{IO: s.newIO(), Path: format.ObjPath(s.cfg.Dir, r.key)})
	default: // kindPut
		return s.writeTmp(r)
	}
}

func (s *Controller) writeTmp(r *request) []Action {
	io := s.newIO()
	r.tmp = format.TmpPath(s.cfg.Dir, uint64(io))
	return s.issue(r, stepWritingTmp, FSWrite{IO: io, Path: r.tmp, Data: format.EncodeObject(r.data)})
}

// issue records that r now waits on a's completion.
func (s *Controller) issue(r *request, step reqStep, a Actuation) []Action {
	r.step = step
	r.io = a.ID()
	return []Action{a}
}

// finish removes r, sends its final actions (ending with the Reply), and
// starts whatever r was blocking.
func (s *Controller) finish(r *request, status Status, body []byte, before ...Action) []Action {
	for i, o := range s.reqs {
		if o == r {
			s.reqs = append(s.reqs[:i], s.reqs[i+1:]...)
			break
		}
	}
	acts := append(before, Reply{Client: r.client, Status: status, Body: body})
	return append(acts, s.startPermitted()...)
}

// commit emits Committed and replies 200 to a mutation.
func (s *Controller) commit(r *request) []Action {
	c := Committed{Client: r.client, Key: r.key, Deleted: r.kind == kindDelete}
	if !c.Deleted {
		c.Sum = format.Checksum(r.data)
	}
	return s.finish(r, StatusOK, nil, Emit{Fact: c})
}

// failTrip replies 500 to r and trips. Used for every error after which
// obj/ may hold a change that is visible but not durable (see package doc).
func (s *Controller) failTrip(r *request, what string, err ErrCode) []Action {
	return s.trip(what+": "+err.String(), Reply{Client: r.client, Status: StatusServerError})
}

// onRequestIO advances r by one protocol step. A completion of the wrong type
// for r's step is ignored like a stale one.
func (s *Controller) onRequestIO(r *request, body EventBody) []Action {
	switch r.step {
	case stepReading:
		d, ok := body.(FSReadDone)
		if !ok {
			return nil
		}
		readKey := r.key
		if r.kind == kindCopy {
			readKey = r.src
		}
		switch {
		case d.Err == ErrNotFound:
			return s.finish(r, StatusNotFound, nil)
		case d.Err != OK:
			return s.finish(r, StatusServerError, nil) // a failed read changed nothing
		}
		payload, valid := format.DecodeObject(d.Data)
		if !valid {
			return s.finish(r, StatusServerError, nil, Emit{Fact: CorruptObject{Key: readKey}})
		}
		if r.kind == kindGet {
			return s.finish(r, StatusOK, payload)
		}
		r.data = payload // COPY: continue down the PUT path for dst
		return s.writeTmp(r)

	case stepWritingTmp:
		d, ok := body.(FSWriteDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			// Only a temp file was touched; obj/ is unchanged. The garbage
			// temp file is removed by the next recovery.
			return s.finish(r, StatusServerError, nil)
		}
		return s.issue(r, stepFsyncTmp, FSFsync{IO: s.newIO(), Path: r.tmp})

	case stepFsyncTmp:
		d, ok := body.(FSFsyncDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failTrip(r, "fsync temp", d.Err) // fsyncgate
		}
		return s.issue(r, stepRenaming, FSRename{IO: s.newIO(), From: r.tmp, To: format.ObjPath(s.cfg.Dir, r.key)})

	case stepRenaming:
		d, ok := body.(FSRenameDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failTrip(r, "rename", d.Err) // may have applied, visibly but not durably
		}
		return s.issue(r, stepFsyncObj, FSFsyncDir{IO: s.newIO(), Dir: format.ObjDir(s.cfg.Dir)})

	case stepUnlinking:
		d, ok := body.(FSUnlinkDone)
		if !ok {
			return nil
		}
		switch d.Err {
		case OK:
			return s.issue(r, stepFsyncObj, FSFsyncDir{IO: s.newIO(), Dir: format.ObjDir(s.cfg.Dir)})
		case ErrNotFound:
			return s.finish(r, StatusNotFound, nil)
		default:
			return s.failTrip(r, "unlink", d.Err) // may have applied, visibly but not durably
		}

	case stepFsyncObj:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failTrip(r, "fsync obj dir", d.Err) // fsyncgate
		}
		return s.commit(r)
	}
	return nil
}
