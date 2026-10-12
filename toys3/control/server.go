package control

import "manifester/toys3/format"

// Config is the server's static configuration.
type Config struct {
	// Dir is the data root; obj/ and tmp/ live under it and must already exist.
	Dir string
	// MaxObjectSize bounds PUT payloads (400 above it). 0 means no limit.
	MaxObjectSize int
}

// phase is the server's lifecycle stage within one incarnation.
type phase uint8

const (
	phaseBooting    phase = iota // before Started; requests are queued
	phaseRecovering              // listing and unlinking temp files; requests are queued
	phaseServing                 // normal operation
	phaseHalted                  // Halt was returned; every later event is ignored
)

// recStep is where recovery is (only meaningful while phaseRecovering).
type recStep uint8

const (
	recListing     recStep = iota // waiting for FSListDone(tmp)
	recUnlinking                  // waiting for the FSUnlinkDone of every leftover
	recFsyncingTmp                // waiting for FSFsyncDirDone(tmp)
)

// opKind is the kind of a client request.
type opKind uint8

const (
	kindPut opKind = iota
	kindGet
	kindDelete
	kindCopy
)

// reqStep is the protocol step a request is on. Each step except
// stepWaiting has exactly one filesystem action outstanding (request.io), and
// names the completion it waits for.
type reqStep uint8

const (
	stepWaiting    reqStep = iota // queued behind an earlier request on one of its keys (or recovery)
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

// conflicts reports whether a and b share a key.
func conflicts(a, b *request) bool {
	return a.touches(b.key) || (b.kind == kindCopy && a.touches(b.src))
}

// Server is the toys3 state machine. All of its state is plain data, rebuilt
// from nothing on every boot.
//
// Per-key FIFO queues are implicit: reqs holds every accepted, unfinished
// request in arrival order, and the queue of key k is the subsequence of reqs
// that touches k. A request may run when no earlier request in reqs touches
// any of its keys, i.e. it is at the head of every queue it is in. This is
// O(in-flight²) per step, which is fine for a toy and keeps the state small
// and obviously deterministic (no maps at all).
type Server struct {
	env   Env
	cfg   Config
	phase phase

	nextIO  IOID   // last IOID issued
	nextSeq uint64 // last arrival sequence issued

	// Recovery bookkeeping (phaseRecovering only).
	rec        recStep
	recIO      []IOID // outstanding recovery IOs (one, or one per leftover temp)
	recRemoved int    // temps unlinked successfully

	reqs []*request // accepted, unfinished requests, ascending seq
}

// New returns a server for one boot. It does nothing until Started.
func New(env Env, cfg Config) *Server {
	return &Server{env: env, cfg: cfg, phase: phaseBooting}
}

// Step consumes one event and returns the actions to execute.
func (s *Server) Step(ev Event) []Action {
	if s.phase == phaseHalted {
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
	if s.phase == phaseRecovering && indexIO(s.recIO, io) >= 0 {
		return s.onRecoveryDone(io, ev.Body)
	}
	for _, r := range s.reqs {
		if r.step != stepWaiting && r.io == io {
			return s.onRequestIO(r, ev.Body)
		}
	}
	return nil // unknown or stale IOID: ignore
}

func (s *Server) newIO() IOID {
	s.nextIO++
	return s.nextIO
}

// halt fail-stops: nothing after it in this incarnation is trusted. The
// runtime executes the actions before it (here: the 500, if any) and then
// kills the incarnation.
func (s *Server) halt(reason string, before ...Action) []Action {
	s.phase = phaseHalted
	return append(before, Halt{Reason: reason})
}

// ---- recovery ----

func (s *Server) onStarted() []Action {
	if s.phase != phaseBooting {
		return nil // a second Started in one incarnation: ignore
	}
	s.phase = phaseRecovering
	s.rec = recListing
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSList{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}

// onRecoveryDone advances recovery. Any error halts: recovery must finish
// before serving, and restarting retries it from scratch.
func (s *Server) onRecoveryDone(io IOID, body EventBody) []Action {
	switch s.rec {
	case recListing:
		d, ok := body.(FSListDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.halt("recovery: list tmp: " + d.Err.String())
		}
		s.recIO = nil
		var acts []Action
		for _, name := range d.Names {
			uio := s.newIO()
			s.recIO = append(s.recIO, uio)
			acts = append(acts, FSUnlink{IO: uio, Path: format.TmpDir(s.cfg.Dir) + "/" + name})
		}
		if len(acts) == 0 {
			return s.fsyncTmpDir()
		}
		s.rec = recUnlinking
		return acts

	case recUnlinking:
		d, ok := body.(FSUnlinkDone)
		if !ok {
			return nil
		}
		switch d.Err {
		case OK:
			s.recRemoved++
		case ErrNotFound: // already gone: fine
		default:
			return s.halt("recovery: unlink temp: " + d.Err.String())
		}
		i := indexIO(s.recIO, io)
		s.recIO = append(s.recIO[:i], s.recIO[i+1:]...)
		if len(s.recIO) > 0 {
			return nil
		}
		return s.fsyncTmpDir()

	case recFsyncingTmp:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.halt("recovery: fsync tmp dir: " + d.Err.String())
		}
		s.phase = phaseServing
		s.recIO = nil
		acts := []Action{Emit{Fact: Recovered{TempsRemoved: s.recRemoved}}}
		return append(acts, s.startRunnable()...)
	}
	return nil
}

func (s *Server) fsyncTmpDir() []Action {
	s.rec = recFsyncingTmp
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSFsyncDir{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}

func indexIO(ios []IOID, io IOID) int {
	for i, x := range ios {
		if x == io {
			return i
		}
	}
	return -1
}

// ---- requests ----

func (s *Server) onRequest(req Request) []Action {
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
	if s.phase == phaseServing && s.canRun(r) {
		return s.start(r)
	}
	return nil // queued behind recovery or an earlier request on the same key
}

func badRequest(c ClientReq) []Action {
	return []Action{Reply{Client: c, Status: StatusBadRequest}}
}

// canRun reports whether r is at the head of every key queue it is in.
func (s *Server) canRun(r *request) bool {
	for _, o := range s.reqs {
		if o.seq >= r.seq {
			return true
		}
		if conflicts(o, r) {
			return false
		}
	}
	return true
}

// startRunnable starts every waiting request that is now at the head of all
// its queues, in arrival order.
func (s *Server) startRunnable() []Action {
	var acts []Action
	for _, r := range s.reqs {
		if r.step == stepWaiting && s.canRun(r) {
			acts = append(acts, s.start(r)...)
		}
	}
	return acts
}

// start issues the first FS action of r.
func (s *Server) start(r *request) []Action {
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

func (s *Server) writeTmp(r *request) []Action {
	io := s.newIO()
	r.tmp = format.TmpPath(s.cfg.Dir, uint64(io))
	return s.issue(r, stepWritingTmp, FSWrite{IO: io, Path: r.tmp, Data: format.EncodeObject(r.data)})
}

// issue records that r now waits on a's completion.
func (s *Server) issue(r *request, step reqStep, a Actuation) []Action {
	r.step = step
	r.io = a.ID()
	return []Action{a}
}

// finish removes r, sends its final actions (ending with the Reply), and
// starts whatever r was blocking.
func (s *Server) finish(r *request, status Status, body []byte, before ...Action) []Action {
	for i, o := range s.reqs {
		if o == r {
			s.reqs = append(s.reqs[:i], s.reqs[i+1:]...)
			break
		}
	}
	acts := append(before, Reply{Client: r.client, Status: status, Body: body})
	return append(acts, s.startRunnable()...)
}

// commit emits Committed and replies 200 to a mutation.
func (s *Server) commit(r *request) []Action {
	c := Committed{Client: r.client, Key: r.key, Deleted: r.kind == kindDelete}
	if !c.Deleted {
		c.Sum = format.Checksum(r.data)
	}
	return s.finish(r, StatusOK, nil, Emit{Fact: c})
}

// failHalt replies 500 to r and halts. Used for every error after which
// obj/ may hold a change that is visible but not durable (see package doc).
func (s *Server) failHalt(r *request, what string, err ErrCode) []Action {
	return s.halt(what+": "+err.String(), Reply{Client: r.client, Status: StatusServerError})
}

// onRequestIO advances r by one protocol step. A completion of the wrong type
// for r's step is ignored like a stale one.
func (s *Server) onRequestIO(r *request, body EventBody) []Action {
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
			return s.failHalt(r, "fsync temp", d.Err) // fsyncgate
		}
		return s.issue(r, stepRenaming, FSRename{IO: s.newIO(), From: r.tmp, To: format.ObjPath(s.cfg.Dir, r.key)})

	case stepRenaming:
		d, ok := body.(FSRenameDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failHalt(r, "rename", d.Err) // may have applied, visibly but not durably
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
			return s.failHalt(r, "unlink", d.Err) // may have applied, visibly but not durably
		}

	case stepFsyncObj:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failHalt(r, "fsync obj dir", d.Err) // fsyncgate
		}
		return s.commit(r)
	}
	return nil
}

// ---- hashing ----

// StateHash summarizes the server's entire state (FNV-1a over a fixed
// field order; reqs and recIO are slices, so the order is deterministic).
func (s *Server) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(s.phase))
	h.u64(uint64(s.nextIO))
	h.u64(s.nextSeq)
	h.u64(uint64(s.rec))
	h.u64(uint64(s.recRemoved))
	h.u64(uint64(len(s.recIO)))
	for _, io := range s.recIO {
		h.u64(uint64(io))
	}
	h.u64(uint64(len(s.reqs)))
	for _, r := range s.reqs {
		h.u64(r.seq)
		h.u64(uint64(r.client))
		h.u64(uint64(r.kind))
		h.str(r.key)
		h.str(r.src)
		h.u64(uint64(len(r.data)))
		h.u64(uint64(format.Checksum(r.data)))
		h.u64(uint64(r.step))
		h.u64(uint64(r.io))
		h.str(r.tmp)
	}
	return h.sum
}

// hasher is 64-bit FNV-1a, written out by hand (hash/fnv is not on the
// determinism allow-list, and it is four lines).
type hasher struct{ sum uint64 }

func newHasher() hasher { return hasher{sum: 14695981039346656037} }

func (h *hasher) byte(b byte) {
	h.sum ^= uint64(b)
	h.sum *= 1099511628211
}

func (h *hasher) u64(x uint64) {
	for i := 0; i < 8; i++ {
		h.byte(byte(x >> (8 * i)))
	}
}

func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}
