# Chapter 6: The Controller, Part 2: The Key Interlock and the Four Protocols

> *"Concurrency is not about threads. It is about which things are allowed to happen in which order."*

Chapter 5 followed the server from `Started` through recovery to the moment it emits `Recovered` and begins serving. This chapter covers everything after that: how a request is accepted or rejected, how requests on the same key are kept in order while requests on different keys proceed in parallel, and how each of the four operations (PUT, GET, DELETE, COPY) walks through its filesystem protocol one completion at a time.

By the end of the chapter you will have read every line of `toys3/control/controller.go` from 148 to the end (356), and you will be able to predict, for any sequence of requests and completions, exactly which actions the server returns. That is not an exaggeration: the server's unit tests (`toys3/control/controller_test.go`) assert exact action sequences as strings, and we will use them as worked examples.

---

## 6.1 The shape of the problem

A storage server receives requests concurrently. Some of them conflict (two PUTs to the same key), some do not (a PUT to `a` and a GET of `b`). We want three properties:

1. **Per-key order.** If request A on key `k` arrived before request B on key `k`, then A's effect on `k` happens before B's. In particular, a GET that arrives after a PUT has replied sees that PUT (or something later).
2. **Cross-key parallelism.** Slow I/O on key `a` must not block key `b`. A filesystem operation takes between 20 µs and 1.5 ms in the simulator (`toys3/sim/config.go:167`), and a PUT needs four of them, each followed by a feedback delay of up to 0.5 ms; serializing every request through one global lock would be correct and pointlessly slow.
3. **No deadlock.** COPY touches two keys. Anything that holds two locks can deadlock if two holders acquire them in opposite orders.

In a conventional threaded server you would solve this with a lock per key (`map[string]*sync.Mutex`), take both locks for COPY in sorted order to avoid deadlock, and run each request on its own goroutine. That is a perfectly good design for production, and it is completely unusable inside `toys3/control`, for reasons Chapter 2 laid out: no goroutines, no `sync`, no blocking, no map iteration order deciding behaviour. The controller is a pure function from (state, event) to (state, actions). Whatever we use for per-key ordering has to be *data*, inspected and updated inside `Step`.

toys3's answer is unusually simple: **there are no lock objects at all.** There is one slice of unfinished requests in arrival order, and a request may run when nothing before it in that slice touches any of its keys.

## 6.2 The request record

Every accepted request becomes a `request`:

```go
// toys3/control/controller.go:50-61
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
```

Walk the fields:

- `seq` is a per-incarnation arrival counter, assigned in `onRequest` (`controller.go:176-177`). It is the only thing that defines order. Note that it is *not* the client's `ClientReq`: the runtime assigns `ClientReq` (prod: per HTTP request, `toys3/prod/loop.go:125-127`; sim: per workload operation, `toys3/sim/workload.go:93-94`), and the server must not assume anything about its ordering. In the simulator, `ClientReq` happens to increase with issue time, but a request's arrival at `Step` can be reordered relative to its issue by same-instant tie-breaking in the queue (Chapter 10). `seq` is what the server actually saw.
- `client` is echoed on the `Reply`, so the runtime can route the answer.
- `kind` is one of `kindPut`, `kindGet`, `kindDelete`, `kindCopy` (`controller.go:26-33`).
- `key` is "the key this request is about". For COPY it is the **destination**. This choice makes `commit` (which reports the changed key) and the PUT half of COPY share code paths: after reading the source, COPY *is* a PUT of `r.data` to `r.key`.
- `src` is set only for COPY.
- `data` starts as the PUT payload. For COPY it starts nil and is filled with the *decoded* source payload after the read (`controller.go:296`).
- `step` is the protocol position (below).
- `io` is the single outstanding filesystem action. The invariant "each step except `stepWaiting` has exactly one filesystem action outstanding" is stated at `controller.go:35-37` and is what makes completion routing trivial.
- `tmp` is the temp path, set when the write is issued, used by the fsync and the rename.

The protocol positions:

```go
// toys3/control/controller.go:35-48
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
```

Read that list as a state diagram per operation:

```
PUT     waiting ─► writingTmp ─► fsyncTmp ─► renaming ─► fsyncObj ─► (200, done)
COPY    waiting ─► reading ─► writingTmp ─► fsyncTmp ─► renaming ─► fsyncObj ─► (200, done)
                      └─► (404 / 500, done)
GET     waiting ─► reading ─► (200 / 404 / 500, done)
DELETE  waiting ─► unlinking ─► fsyncObj ─► (200, done)
                      └─► (404, done)
```

Any arrow can also end in "500" or "500 + Trip"; those exits are the subject of Chapter 7. A request in a terminal state is not represented at all: it is removed from `reqs` by `finish`.

## 6.3 Which requests conflict

Two helper functions decide whether two requests may run concurrently:

```go
// toys3/control/controller.go:63-72
// touches reports whether r reads or writes key.
func (r *request) touches(key string) bool {
	return r.key == key || (r.kind == kindCopy && r.src == key)
}

// interlocked reports whether a and b share a key, so they must not run at
// the same time (the key interlock).
func interlocked(a, b *request) bool {
	return a.touches(b.key) || (b.kind == kindCopy && a.touches(b.src))
}
```

`touches` is the set of keys a request reads or writes: `{key}` for PUT, GET, DELETE; `{src, dst}` for COPY. `interlocked(a, b)` is "the key sets of `a` and `b` intersect", written out without allocating sets: does `a` touch `b.key`, or (if `b` is a COPY) does `a` touch `b.src`?

The important thing to notice is what is **not** here: there is no notion of read versus write. **Two GETs on the same key conflict.** A reader–writer lock would let them run in parallel, but toys3 serializes them. This is a deliberate simplification. It costs some read throughput on hot keys (each GET waits for the previous one's `FSRead` to complete) and buys a very simple argument for linearizability: on any one key, the server executes one request at a time, in arrival order, so each request's effect happens strictly within its own `[arrival, reply]` interval and strictly after its predecessor's. You will see this cost in the trace in §6.10, where `c7 GET k2` waits behind `c6 PUT k2`, and in TestSameKeySerialized, where a GET waits behind two PUTs.

The second thing to notice is that COPY touches its source even though it only reads it. That matters: if COPY `a → b` did not hold `a`, a PUT to `a` arriving after the COPY could complete before the COPY's read, and the COPY would copy a value that did not exist when it arrived. Holding `src` is what makes COPY behave "as if it read src and wrote dst atomically" (the oracle's model, `toys3/sim/oracle.go:241-259`, treats COPY as exactly that).

## 6.4 The implicit queues

The `Controller` struct holds all requests in one slice:

```go
// toys3/control/controller.go:74-101
// Controller is the toys3 state machine: the control law of the object
// server. It turns client requests and storage feedback (filesystem
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
```

The comment is the design. Picture `reqs` with six requests:

```
reqs (ascending seq):
  seq 1  PUT  a          ◄ head of queue(a)                     running
  seq 2  GET  b          ◄ head of queue(b)                     running
  seq 3  COPY a → c      in queue(a) behind 1; head of queue(c) waiting
  seq 4  GET  c          in queue(c) behind 3                   waiting
  seq 5  PUT  d          ◄ head of queue(d)                     running
  seq 6  GET  a          in queue(a) behind 1, 3                waiting

implied queues:
  queue(a) = [1, 3, 6]
  queue(b) = [2]
  queue(c) = [3, 4]
  queue(d) = [5]
```

Request 3 (COPY) is at the head of `queue(c)` but not of `queue(a)`, so it waits. Request 4 is behind 3 in `queue(c)` and waits even though nothing is *running* on `c`. That is FIFO: a later request never overtakes an earlier one on a shared key, even if the earlier one is itself waiting. This is what guarantees that request 4 (GET c) observes the effect of request 3 (COPY into c): it cannot start until 3 has finished.

Why no map? Because a `map[string][]*request` would need to be iterated somewhere (to find whose start permissive became true when a request finished), and map iteration order in Go is randomized. You can iterate sorted keys, but then you have to keep the keys sorted, and `detcheck` would rightly flag any slip. A slice in arrival order has a deterministic iteration order by construction, and the "queue of key k" is just a filter over it.

## 6.5 permitted, and why deadlock is impossible

```go
// toys3/control/controller.go:189-201
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
```

Scan `reqs` from the oldest. If we reach `r` itself (or anything not older than it; `o.seq >= r.seq`) without meeting a conflict, `r` is at the head of every queue it is in. If we meet an older request that shares a key, `r` must wait. The final `return true` handles the case where `r` is not (or no longer) in `reqs`, which cannot happen at the call sites but makes the function total.

Note that `permitted` does not check whether the older conflicting request is *running* or *waiting*. A waiting older request blocks just as hard. That is exactly the FIFO rule from §6.4.

**The no-deadlock argument.** A deadlock would be a cycle of requests each waiting for another. But in this scheme a request only ever waits for requests with a *smaller* `seq`. "Waits for" therefore follows a strictly decreasing sequence of integers, which cannot cycle. More concretely: the request with the smallest `seq` in `reqs` has nothing older than it, so `permitted` is true for it, so it is either already running or will be started the next time `startPermitted` runs. It will finish (every running request has exactly one outstanding filesystem action, and every filesystem action completes or the incarnation dies). When it finishes, the next-smallest becomes the head, and so on.

### The vocabulary: interlock and start permissive

Before the refactor this function was `canRun`, `interlocked` was `conflicts`, and `startPermitted` (§6.9) was `startRunnable`. The mechanism has not changed by a single statement. The names now come from industrial control, where they have had precise meanings for decades:

- An **interlock** is a mechanism that prevents two operations from running together because together they would be unsafe: a press cannot cycle while its guard is open; a vessel's inlet and drain valves cannot both open. In toys3, two requests that share a key are *interlocked*: `interlocked(a, b)` is true exactly when running them concurrently could violate per-key order. The whole per-key FIFO mechanism is the **key interlock**.
- A **permissive** is a condition that must hold for an operation to be *allowed to start*. A motor's start permissive is the AND of all of them (lubrication pressure OK, guard closed, no trip active). `permitted(r)` is request `r`'s start permissive: no earlier unfinished request is interlocked with it.
- Permissives are *evaluated*, not latched. That is what `startPermitted` does every time something finishes: it re-evaluates every waiting request's permissive and starts the ones that have become true.

The control vocabulary also makes the boundary with the error policy crisp. An interlock *prevents* a conflict before it happens; a trip (Chapter 7) *reacts* to a condition after it has been detected; an invariant in the simulator (Chapter 14) *detects* a violation and reports it. Three words, three different roles, and the code now uses each for exactly one.

Why not make each key's queue an object of its own, an aggregate in DDD terms? Chapter 3a (§3a.6) answers in detail. In short: COPY spans two keys, the queues are derived rather than stored, recovery and trips are controller-wide, and the order in which newly permitted requests start is observable. The per-key queue is therefore not an aggregate but a named invariant of the one aggregate, the `Controller`, enforced in exactly one function: `permitted`.

Compare with the lock-based design: there, deadlock freedom comes from a *convention* (always lock in sorted key order), which every code path must follow. Here it comes from a *structure*: there is only one order, the arrival order, and every per-key queue is a subsequence of it. All queues agree on the relative order of any two requests they share, so no two queues can disagree in a way that produces a cycle. `toys3/control/doc.go:91-96` states the same argument in one sentence: "Because every queue is ordered by the same arrival sequence, no cycle (deadlock) is possible."

**Liveness caveat.** "Every running request has exactly one outstanding filesystem action, and every filesystem action completes" is an assumption about the runtime. In production, `OSExecutor` calls `done` exactly once per action (`toys3/prod/executor.go:14-20`). In simulation, every FS action becomes an `fs-land` entry followed by a `deliver` entry, unless the incarnation dies, in which case the requests die with it. If either runtime ever silently dropped a completion for a live incarnation, the request would wait forever and so would everything behind it on its keys. The simulator's `Liveness` invariant (Chapter 14) would then fail at the end of the run with "no reply by the end of Settling". This is one reason the simulator never drops completions for the *current* incarnation, even though dropping them would be a tempting "fault" to inject: the server has no timers (`toys3/DESIGN.md:86-87`: "No timers are needed: every wait is on an FS completion"), so a lost completion is a liveness bug in the *environment*, not one the server is designed to tolerate. Chapter 17 discusses what adding timeouts would involve.

## 6.6 Accepting a request: onRequest

```go
// toys3/control/controller.go:150-187
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
```

Three things to notice.

**Validation happens first, and invalid requests never enter `reqs`.** `ValidKey` (`toys3/format/layout.go:58-62`) rejects the empty key and any key whose escaped name exceeds 255 bytes (Chapter 4 covers the escaping). A PUT larger than `MaxObjectSize` is rejected. A nil or unknown `Op` is rejected (the `default` branch; the closed `Op` interface makes this unreachable from well-typed code, except for a nil `Op`, which TestBadRequest exercises). A 400 is returned *immediately, in the same Step*, even during recovery: it touches no state, so there is nothing to wait for. `control/doc.go:100-103` states this: "Invalid requests (bad key, oversize PUT) get 400 at once and never enter it."

This matters for the oracle. A 400 is a *Definite* outcome that "touched nothing", and the oracle skips it entirely (`toys3/sim/oracle.go:278-280`). If the server ever put a 400 request in a queue and later did something with it, that skip would be wrong.

**`seq` is assigned only to accepted requests.** `nextSeq` counts accepted requests, not all requests. A consequence worth noticing is that a 400 changes no server state at all: no field is written before any `return badRequest` (the fresh `r` is simply dropped), so `StateHash` is unchanged by invalid traffic. TestBadRequest does not check this; Exercise 3 asks you to.

**Requests are queued during startup and recovery.** `s.mode == modeAuto && s.permitted(r)` is false in `modeStartup` and `modeRecovering`, so the request is appended and left `stepWaiting` (the zero value). When recovery completes, `onRecoveryDone` calls `startPermitted` (`recovery.go:80-83`), which starts every queued request that is runnable, in arrival order. Chapter 5 explains why requests can arrive even before `Started` in the simulator (a client wake scheduled at the same virtual instant as the restart can be ordered before the `Started` delivery by the seeded tie-break).

What about `modeTripped`? `Step` returns `nil` for every event once tripped (`controller.go:110-112`), so `onRequest` is never reached. A request delivered to a tripped server gets no reply in that incarnation. In the simulator this cannot happen because `Trip` crashes the incarnation in the same step (Chapter 10); in production the loop returns and `Submit` reports `ErrStopped` (Chapter 9).

## 6.7 Starting and advancing: start, writeTmp, issue

When a request's start permissive holds, `start` issues its first filesystem action:

```go
// toys3/control/controller.go:215-233
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
```

- GET reads `obj/<escaped key>`.
- COPY reads `obj/<escaped src>`. The destination is not touched until the source has been read and verified.
- DELETE goes straight to `FSUnlink`. There is no "check existence first": the unlink's `ErrNotFound` *is* the existence check, and it is atomic, whereas a read-then-unlink would need its own reasoning about what could happen in between (nothing could, given per-key FIFO, but the single operation is simpler).
- PUT goes to `writeTmp`, which allocates a fresh IOID and uses it to name the temp file. `format.TmpPath` formats the IOID (converted to a plain `uint64`, because the format package does not know about IOIDs) as 16 zero-padded hex digits (`toys3/format/layout.go:73-79`). So IOID 7 writes `tmp/0000000000000007`, and IOID 17 writes `tmp/0000000000000011` (17 in hex). You will see exactly these names in the trace in §6.10. The temp name is unique within an incarnation because IOIDs are; across incarnations, IOIDs restart at 1, and uniqueness relies on recovery having emptied `tmp/` before anything new is written (Chapter 5, and Chapter 15's mutant **f** for what happens when it does not).
- `EncodeObject` wraps the payload in the 16-byte header (magic, CRC-32, length) described in Chapter 4. The CRC is computed here, once, from `r.data`.

Every path funnels through `issue`:

```go
// toys3/control/controller.go:235-240
// issue records that r now waits on a's completion.
func (s *Controller) issue(r *request, step reqStep, a Actuation) []Action {
	r.step = step
	r.io = a.ID()
	return []Action{a}
}
```

`issue` is the one place where the "one outstanding action per step" invariant is enforced: it sets the step and the IOID together, and returns a one-element action slice.

Its parameter type tells a small story. In the first edition of this code, `issue` took a plain `Action` and recovered the IOID with a seven-case type switch, because Go has no common field access across struct types. This book's first edition noted the alternative ("add an `IOOf() IOID` method to every action") and that the authors had kept Actions as dumb as possible. The refactor took the alternative, in its smallest form: every filesystem action got a one-line `ID()` method, and the interface `Actuation` (Chapter 3, §3.5) names the set of actions that have one. The actions are still dumb plain structs; the method only reads a field. What changed is that `issue` now *says* in its signature that it only ever records a command to storage, and the compiler rejects an attempt to `issue` a `Reply`.

`newIO` is a counter:

```go
// toys3/control/controller.go:135-138
func (s *Controller) newIO() IOID {
	s.nextIO++
	return s.nextIO
}
```

IOIDs start at 1 in each incarnation. Recovery consumes the first few (the `FSList` is always IO 1, `recovery.go:23`), so the first client action of an incarnation usually has an IOID of 3 or more.

## 6.8 Routing completions: Step

Before we follow the per-operation protocols, recall how a completion finds its request:

```go
// toys3/control/controller.go:119-132
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
```

A linear scan of `reqs` for the running request whose outstanding IOID matches. The `r.step != stepWaiting` guard is belt and braces: a waiting request has never been started, so its `io` is still the zero value 0, and IOIDs start at 1, so `r.io == io` would be false anyway. The guard documents intent: only running requests consume completions. Unknown IOIDs, completions for finished requests, and completions of the wrong *type* (checked inside `onRequestIO`) are all ignored without changing state. TestStaleIOIgnored (`controller_test.go:410-432`) checks exactly this, including that `StateHash` is unchanged.

## 6.9 The four protocols, step by step

Here is the whole of `onRequestIO`. We will take it one case at a time.

```go
// toys3/control/controller.go:270-356 (abridged to the success and 404 paths; error
// paths are the subject of Chapter 7)
func (s *Controller) onRequestIO(r *request, body EventBody) []Action {
	switch r.step {
	case stepReading:       ...
	case stepWritingTmp:    ...
	case stepFsyncTmp:      ...
	case stepRenaming:      ...
	case stepUnlinking:     ...
	case stepFsyncObj:      ...
	}
	return nil
}
```

Each case starts the same way: type-assert the completion to the kind expected for this step, and if it is not that kind, return `nil` ("A completion of the wrong type for r's step is ignored like a stale one", `controller.go:270-271`).

### 6.9.1 GET

```go
// toys3/control/controller.go:274-295
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
			return s.finish(r, StatusServerError, nil, Emit{Telemetry: CorruptionDetected{Key: readKey}})
		}
		if r.kind == kindGet {
			return s.finish(r, StatusOK, payload)
		}
```

GET has a one-step protocol:

```
Request{c, Get k} ─► [FSRead{io, obj/k}]
FSReadDone{io, ENOENT}            ─► [Reply{c, 404}, ...started successors]
FSReadDone{io, EIO}               ─► [Reply{c, 500}, ...]
FSReadDone{io, OK, bad bytes}     ─► [Emit{CorruptionDetected{k}}, Reply{c, 500}, ...]
FSReadDone{io, OK, good bytes}    ─► [Reply{c, 200, payload}, ...]
```

`DecodeObject` (`toys3/format/object.go:30-44`) checks the magic, the length field against the actual length, and the CRC. Only if all three pass does the payload reach the client. `payload` *aliases* `d.Data` (it is `file[HeaderSize:]`), which is fine because events are immutable once sent: the StorageModel clones bytes on read (`toys3/sim/storagemodel.go:395`), and in production `os.ReadFile` returns a fresh slice.

A corrupt object produces a plain 500 and a `CorruptionDetected` telemetry, **not** a Trip. The reasoning is in `toys3/control/doc.go:82-84`: the file is already durable, so restarting would not fix it. Chapter 7 contrasts this with the cases that do trip.

### 6.9.2 PUT

```go
// toys3/control/controller.go:299-329 (success paths)
	case stepWritingTmp:
		d, ok := body.(FSWriteDone)
		...
		return s.issue(r, stepFsyncTmp, FSFsync{IO: s.newIO(), Path: r.tmp})

	case stepFsyncTmp:
		d, ok := body.(FSFsyncDone)
		...
		return s.issue(r, stepRenaming, FSRename{IO: s.newIO(), From: r.tmp, To: format.ObjPath(s.cfg.Dir, r.key)})

	case stepRenaming:
		d, ok := body.(FSRenameDone)
		...
		return s.issue(r, stepFsyncObj, FSFsyncDir{IO: s.newIO(), Dir: format.ObjDir(s.cfg.Dir)})
```

and the shared final step:

```go
// toys3/control/controller.go:345-353
	case stepFsyncObj:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.failTrip(r, "fsync obj dir", d.Err) // fsyncgate
		}
		return s.commit(r)
```

So a PUT is five steps, exactly as `DESIGN.md:91-97` draws it:

```
Request{c7, Put k}    ──► Step ──► [FSWrite{io1, tmp/01, hdr+data}]
FSWriteDone{io1}      ──► Step ──► [FSFsync{io2, tmp/01}]
FSFsyncDone{io2}      ──► Step ──► [FSRename{io3, tmp/01 → obj/k}]
FSRenameDone{io3}     ──► Step ──► [FSFsyncDir{io4, obj}]
FSFsyncDirDone{io4}   ──► Step ──► [Emit{Committed}, Reply{c7, 200}]
```

Chapter 4 justified each step: the file fsync makes the bytes durable, the rename makes the switch atomic, the directory fsync makes the rename durable. Here what matters is the *shape*: one action out, one completion in, one step forward. The state machine never has two actions outstanding for one request, so it never has to reason about their relative order.

Notice what PUT does *not* do: it does not fsync `tmp/` after the write (the temp file's *name* is never made durable by the server), and it does not fsync `tmp/` after the rename (the *removal* of the temp name is never made durable). Neither is needed for correctness of the committed object. The first means a crash before the rename may or may not leave a temp file behind (recovery deletes it either way). The second means a crash *after* the rename may resurrect the temp name as a second hard link to the committed object's inode, if some background writeback had made the temp name durable in the meantime. Recovery then unlinks the temp name, which removes one link and leaves the object alone. This is the "aliasing" story from `DESIGN.md:168-173`; you will see it happen in a real trace in Chapter 11, and see it become a real bug when recovery is broken in Chapter 15.

### 6.9.3 commit and finish

```go
// toys3/control/controller.go:242-262
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
	return s.finish(r, StatusOK, nil, Emit{Telemetry: c})
}
```

`finish` is the only way a request leaves `reqs` while the server is serving. It does three things in a fixed order:

1. Remove `r` from `reqs` (preserving the order of the rest; `append(s[:i], s[i+1:]...)` shifts the tail down).
2. Emit `before...` (for `commit`, the `Committed` telemetry; for a corrupt read, the `CorruptionDetected` telemetry), then the `Reply`.
3. Call `startPermitted` and append whatever it starts.

The order inside the returned slice is a contract with the runtime and the simulator:

- **Telemetry before Reply.** `Committed` is emitted immediately before the 200 (`toys3/control/doc.go:86-87`). The simulator's `AckedIsDurable` invariant reads the telemetry after the step and checks the durable state of the StorageModel *at that step* (Chapter 14). The fact describes what the server believes at the moment it acks.
- **Reply before successors.** The next request on the key is started in the *same* step that replies to its predecessor, but after the reply in the action list. TestSameKeySerialized pins this: `"committed c1 k del=false; reply c1 200; write tmp/*"`. There is no window in which the key is idle while a request is waiting for it, and no successor action appears before the reply it depends on.

`commit` computes `Sum` from `r.data`, the *payload*, not the encoded file. For COPY, `r.data` is the source payload after decoding, so a COPY's `Committed.Sum` equals the source object's checksum. You can see this in the seed-1 trace: `Committed{c12 "k1" crc=1ce854a5}` at step 88 has the same CRC as `Committed{c10 "k0" crc=1ce854a5}` at step 54, because c12 was `COPY "k0" -> "k1"` and k0 held c10's value.

```go
// toys3/control/controller.go:203-213
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
```

`startPermitted` scans every waiting request in arrival order and starts the ones whose permissive now holds. Starting a request changes its `step` but not its position in `reqs`, and a started request still blocks later conflicting ones (permitted does not care whether the blocker is running), so starting request *i* can never make request *j > i* newly runnable or newly blocked within this loop. The loop is therefore order-independent in its *result*, and its *action order* is arrival order. Determinism of the action list falls out of iterating a slice.

This scan is called after every `finish` and once at the end of recovery. It is quadratic: `startPermitted` is O(n) iterations, each calling `permitted`, which is O(n). With `n` in-flight requests, that is O(n²) per finished request. The struct comment owns up to this (`controller.go:84-86`). In the simulator, `n` is at most the number of clients (each client has at most one request outstanding, `toys3/sim/workload.go:18-20`), which `DefaultConfig` sets to 2, 4 or 6. In production, `n` is bounded only by HTTP concurrency, and at a few thousand in-flight requests the quadratic term would start to matter. Exercise 6 asks you to design the O(n log n) version and use the simulator to prove it equivalent.

### 6.9.4 DELETE

```go
// toys3/control/controller.go:331-343
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
```

```
Request{c, Delete k}   ─► [FSUnlink{io1, obj/k}]
FSUnlinkDone{io1, OK}  ─► [FSFsyncDir{io2, obj}]
FSFsyncDirDone{io2}    ─► [Emit{Committed{c, k, Deleted}}, Reply{c, 200}]

FSUnlinkDone{io1, ENOENT} ─► [Reply{c, 404}]
```

DELETE shares `stepFsyncObj` with PUT, and therefore shares `commit`. With `Deleted: true`, `commit` leaves `Sum` at 0. Without the directory fsync, a crash could bring the object back: the unlink is a directory-entry change, and it is volatile until `obj/` is fsynced (`DESIGN.md:104`). The 404 path does not fsync anything, because nothing changed. (One subtle question: could the key have been deleted *visibly but not durably* by an earlier failed DELETE, so that this 404 is reporting a state a crash can undo? Only if the earlier DELETE's error did not trip. It does, and that is exactly the subject of Chapter 7.)

### 6.9.5 COPY

COPY's first step is GET's, and its continuation is PUT's:

```go
// toys3/control/controller.go:296-297
		r.data = payload // COPY: continue down the PUT path for dst
		return s.writeTmp(r)
```

```
Request{c, Copy a→b}           ─► [FSRead{io1, obj/a}]
FSReadDone{io1, OK, good}      ─► [FSWrite{io2, tmp/…02, hdr+payload}]
FSWriteDone{io2}               ─► [FSFsync{io3, tmp/…02}]
FSFsyncDone{io3}               ─► [FSRename{io4, tmp/…02 → obj/b}]
FSRenameDone{io4}              ─► [FSFsyncDir{io5, obj}]
FSFsyncDirDone{io5}            ─► [Emit{Committed{c, b, Sum(payload)}}, Reply{c, 200}]

FSReadDone{io1, ENOENT}        ─► [Reply{c, 404}]                    (dst untouched)
FSReadDone{io1, OK, corrupt}   ─► [Emit{CorruptionDetected{a}}, Reply{c, 500}]
```

Three design points:

- **COPY re-encodes.** It does not copy the file bytes; it decodes the source (verifying its CRC) and re-encodes the payload. "Never copy garbage" (`DESIGN.md:105`). A corrupt source becomes a 500 and a `CorruptionDetected{Key: src}` fact (note `readKey`, `controller.go:279-282`: the fact names the key whose file was bad, not the destination).
- **COPY gets exactly PUT's durability.** Because after the read it literally *is* the PUT path, every argument about PUT's durability applies to COPY with no extra reasoning. This is a nice property of the "one step per completion" state machine: reuse is just "jump to a step".
- **COPY with `Src == Dst` is allowed** (`toys3/control/event.go:46-48`: "Src == Dst is allowed (it rewrites the object)"). `touches` returns true for one key, `interlocked` works as usual, and the protocol reads `obj/a`, writes a temp file, and renames it over `obj/a`. The result is a byte-identical object with a new inode. The oracle models it as `set(a, get(a))`, a no-op on the value (`oracle.go:241-259`; `opKeys` collapses the key set to `{src}` when `Src == Dst`, `oracle.go:330-333`).

## 6.10 A real trace: queuing in seed 1

Here are the first 56 steps of seed 1 against the correct server, produced with the log enabled (the rendering is `LogEntry.String`, `toys3/sim/log.go:119-138`; steps that are not logged, mostly client wake-ups, leave gaps in the numbering). Seed 1 has 3 keys and 4 clients.

```
#1      t=0.000000ms   inc=1 restart     boot
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#4      t=0.444359ms   inc=1 deliver     Request{c1 GET "k2"}
#6      t=0.523067ms   inc=1 deliver     Request{c2 GET "k0"}
#7      t=0.665229ms   inc=1 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK []}
#9      t=0.745782ms   inc=1 deliver     Request{c3 PUT "" value-of-3} => Reply{c3 400}
#10     t=0.748746ms   inc=1 deliver     FSListDone{io1 OK []} => FSFsyncDir{io2 /data/tmp}
#12     t=0.762895ms   inc=1 deliver     Request{c4 COPY "k2" -> "k0"}
#14     t=1.142951ms   inc=1 deliver     Request{c5 DELETE "k0"}
#15     t=1.573379ms   inc=1 fs-land     land FSFsyncDir{io2 /data/tmp} -> FSFsyncDirDone{io2 OK}
#16     t=1.947497ms   inc=1 deliver     FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSRead{io3 /data/obj/k2} FSRead{io4 /data/obj/k0}
#17     t=2.197603ms   inc=1 fs-land     land FSRead{io4 /data/obj/k0} -> FSReadDone{io4 ENOENT nil}
#18     t=2.676770ms   inc=1 deliver     FSReadDone{io4 ENOENT nil} => Reply{c2 404}
#19     t=3.358955ms   inc=1 fs-land     land FSRead{io3 /data/obj/k2} -> FSReadDone{io3 ENOENT nil}
#21     t=3.397177ms   inc=1 deliver     Request{c6 PUT "k2" value-of-6}
#22     t=3.555786ms   inc=1 deliver     FSReadDone{io3 ENOENT nil} => Reply{c1 404} FSRead{io5 /data/obj/k2}
#24     t=4.026375ms   inc=1 deliver     Request{c7 GET "k2"}
#25     t=4.768296ms   inc=1 fs-land     land FSRead{io5 /data/obj/k2} -> FSReadDone{io5 ENOENT nil}
#26     t=4.822820ms   inc=1 deliver     FSReadDone{io5 ENOENT nil} => Reply{c4 404} FSUnlink{io6 /data/obj/k0} FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)}
#27     t=4.986087ms   inc=1 fs-land     land FSUnlink{io6 /data/obj/k0} -> FSUnlinkDone{io6 ENOENT}
#28     t=5.462533ms   inc=1 deliver     FSUnlinkDone{io6 ENOENT} => Reply{c5 404}
#30     t=5.560909ms   inc=1 deliver     Request{c8 GET "k0"} => FSRead{io8 /data/obj/k0}
#32     t=5.945995ms   inc=1 deliver     Request{c9 PUT "k2" value-of-9}
#33     t=6.109640ms   inc=1 fs-land     land FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)} -> FSWriteDone{io7 OK}
#34     t=6.152419ms   inc=1 fs-land     land FSRead{io8 /data/obj/k0} -> FSReadDone{io8 ENOENT nil}
#35     t=6.227029ms   inc=1 deliver     FSReadDone{io8 ENOENT nil} => Reply{c8 404}
#37     t=6.423874ms   inc=1 deliver     Request{c10 PUT "k0" value-of-10} => FSWrite{io9 /data/tmp/0000000000000009 obj(value-of-10)}
#38     t=6.531543ms   inc=1 deliver     FSWriteDone{io7 OK} => FSFsync{io10 /data/tmp/0000000000000007}
#39     t=6.559548ms   inc=1 fs-land     land FSWrite{io9 /data/tmp/0000000000000009 obj(value-of-10)} -> FSWriteDone{io9 OK}
#40     t=6.676498ms   inc=1 fs-land     land FSFsync{io10 /data/tmp/0000000000000007} -> FSFsyncDone{io10 OK}
#41     t=6.789500ms   inc=1 deliver     FSWriteDone{io9 OK} => FSFsync{io11 /data/tmp/0000000000000009}
#42     t=6.871157ms   inc=1 deliver     FSFsyncDone{io10 OK} => FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2}
#43     t=7.182477ms   inc=1 fs-land     land FSFsync{io11 /data/tmp/0000000000000009} -> FSFsyncDone{io11 OK}
#44     t=7.233120ms   inc=1 fs-land     land FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} -> FSRenameDone{io12 OK}
#45     t=7.305169ms   inc=1 deliver     FSFsyncDone{io11 OK} => FSRename{io13 /data/tmp/0000000000000009 -> /data/obj/k0}
#46     t=7.377477ms   inc=1 deliver     FSRenameDone{io12 OK} => FSFsyncDir{io14 /data/obj}
#47     t=7.847833ms   inc=1 fs-land     land FSRename{io13 /data/tmp/0000000000000009 -> /data/obj/k0} -> FSRenameDone{io13 OK}
#48     t=8.000000ms   inc=0 quiesce     quiesce
#49     t=8.030661ms   inc=1 fs-land     land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#50     t=8.242306ms   inc=1 deliver     FSFsyncDirDone{io14 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io15 /data/obj/k2}
#51     t=8.276831ms   inc=1 deliver     FSRenameDone{io13 OK} => FSFsyncDir{io16 /data/obj}
#53     t=8.729009ms   inc=1 fs-land     land FSFsyncDir{io16 /data/obj} -> FSFsyncDirDone{io16 OK}
#54     t=8.762883ms   inc=1 deliver     FSFsyncDirDone{io16 OK} => Emit{Committed{c10 "k0" crc=1ce854a5}} Reply{c10 200}
#55     t=8.898034ms   inc=1 fs-land     land FSRead{io15 /data/obj/k2} -> FSReadDone{io15 OK obj(value-of-6)}
#56     t=9.238431ms   inc=1 deliver     FSReadDone{io15 OK obj(value-of-6)} => Reply{c7 200 value-of-6}
```

Let us read it as the server sees it.

**Steps 2–16: requests queue behind recovery.** `Started` produces `FSList(tmp)` (IO 1). While recovery is in progress, four requests arrive: c1 `GET k2`, c2 `GET k0`, c3 `PUT ""` and c4 `COPY k2 → k0`, then c5 `DELETE k0`. c3 has an empty key, so it is answered `400` immediately at step 9, in the middle of recovery, without entering `reqs`. The others produce no actions: they are appended with `stepWaiting`. Recovery finds `tmp/` empty, fsyncs it (IO 2), and at step 16 emits `Recovered{temps=0}` followed by the output of `startPermitted`.

At that moment `reqs` is:

```
seq 1  c1 GET  k2          touches {k2}
seq 2  c2 GET  k0          touches {k0}
seq 3  c4 COPY k2 → k0     touches {k2, k0}   blocked by seq 1 (k2) and seq 2 (k0)
seq 4  c5 DELETE k0        touches {k0}       blocked by seq 2 and seq 3
```

`startPermitted` walks this in order. seq 1 is permitted (nothing older) → `FSRead{io3 obj/k2}`. seq 2: the only older request is seq 1, which touches `k2`, not `k0` → permitted → `FSRead{io4 obj/k0}`. seq 3 conflicts with seq 1 → wait. seq 4 conflicts with seq 2 → wait. Hence step 16 emits exactly `Emit{Recovered} FSRead{io3} FSRead{io4}`, in that order: the fact first (it is the `acts` prefix in `onRecoveryDone`, `recovery.go:82-83`), then the starts in arrival order.

**Steps 17–22: the COPY needs both of its predecessors gone.** c2's read lands first (k0 absent) and step 18 replies 404 to c2. `finish` removes seq 2 and calls `startPermitted`: seq 3 (COPY) is still blocked by seq 1 on `k2`; seq 4 (DELETE k0) is now blocked by seq 3 on `k0`. Nothing starts. Note the asymmetry: seq 4's only *running* conflict just left, but it is blocked by a *waiting* request. That is FIFO.

Then c6 `PUT k2` arrives at step 21. It goes to the back: blocked by seq 1 and seq 3 on `k2`. At step 22, c1's read completes (404), seq 1 is removed, and `startPermitted` finds the COPY permitted: `Reply{c1 404} FSRead{io5 /data/obj/k2}`. The COPY reads its *source*, `k2`.

**Step 26: one completion, three consequences.** The COPY's read returns ENOENT, so COPY replies 404 and leaves `k0` untouched (`TestNotFound` checks the same thing: "COPY of absent src created dst" must not happen). `finish` removes the COPY and `startPermitted` now finds *two* runnable requests: c5 `DELETE k0` (its blockers, c2 and c4, are both gone) and c6 `PUT k2` (its blockers, c1 and c4, are both gone). Step 26's action list is `Reply{c4 404} FSUnlink{io6 /data/obj/k0} FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)}`: reply first, then starts in arrival order (DELETE was seq 4, PUT was seq 5). c7 `GET k2`, which arrived at step 24, stays queued behind c6.

**The temp file name is the IOID.** c6's write is IO 7, so its temp file is `tmp/0000000000000007`. Later, at step 37, c10's write is IO 9 → `tmp/0000000000000009`. (If you look further into the seed-1 log, IO 17 writes `tmp/0000000000000011`: names are hex.)

**Steps 30–54: two PUTs in flight on different keys.** c8 `GET k0` runs at once (step 30, nothing ahead of it on `k0` now that c5 has finished). c9 `PUT k2` arrives at step 32 and queues behind c6 and c7. c10 `PUT k0` arrives at step 37 and starts immediately. From step 37 to step 54, c6's PUT (on `k2`) and c10's PUT (on `k0`) progress concurrently, interleaving their completions: write, write, fsync, fsync, rename, rename, fsyncdir, fsyncdir. Neither waits for the other. This is cross-key parallelism, and it is the reason the simulator's StorageModel has to model many outstanding operations rather than one at a time.

Also notice the virtual-time quiesce marker at step 48 (`inc=0 quiesce`): the simulator has stopped clients from issuing new requests until all outstanding ones have replied (Chapter 12). From here until the window drains, no new requests appear; only completions.

**Step 50: reply, then start the successor, in one step.** c6's directory fsync completes, so `commit` emits `Committed{c6 "k2"}`, `Reply{c6 200}`, and `startPermitted` starts c7 `GET k2` with `FSRead{io15}`. At step 56, c7 reads `value-of-6`: the value of the PUT that replied before it started. c7 had *arrived* (step 24) long before c6 replied (step 50), so a linearizable server could have returned either "absent" or c6's value. toys3 returns c6's value because per-key FIFO forces c7 to run after c6, which is a stronger guarantee than linearizability requires. The oracle (Chapter 13) only checks the weaker property, so the server has slack.

Here is the same story as a per-key timeline:

```
virtual time (ms)  0        1        2        3        4        5        6        7        8        9
                   │ recovery ────────────┤
k2  c1 GET         ·····queued···········[read]──────────┤404
k2  c4 COPY(src)   ··········queued························[read]─┤404
k0  c4 COPY(dst)   ··········queued························(held)─┤
k0  c2 GET         ·····queued···········[read]─┤404
k0  c5 DELETE      ·············queued·······························[unlink]┤404
k2  c6 PUT                                       ····queued·········[write][fsync][rename][fsyncdir]┤200
k2  c7 GET                                              ··queued··························· (waits) [read]┤200 v6
k0  c10 PUT                                                                  [write][fsync][rename][fsyncdir]┤200

legend: ····· waiting in reqs      [x] FS action outstanding      ┤ reply
```

## 6.11 The unit tests as worked examples

The unit tests in `toys3/control/controller_test.go` drive the server by hand. Understanding their harness makes them easy to read. This is the harness as it is today, at commit `3ff1385`:

```go
// toys3/control/controller_test.go:26-36 (at 3ff1385)
// harness drives a Controller by hand. FS actions are answered by veritas.FS
// (no crash semantics: every change is immediately durable). Issued FS
// actions wait in pending until the test completes them.
type harness struct {
	t       *testing.T
	s       *Controller
	fs      *veritas.FS
	pending []Action // issued FS actions, not yet completed
	// fail, if set, decides the error an FS action completes with (OK = run it).
	fail func(Action) ErrCode
}
```

At `10777a2`, the commit the rest of this chapter's line numbers refer to, the harness kept its own `files map[string][]byte` and executed each action against it in a private `exec`. Commit `0f46dfe` replaced that map with `veritas.FS`, the executable spec of the Storage port (Chapter 13a, §13a.5), and moved the file to the external test package `control_test` with a dot import, because `veritas` imports `control`. No test changed. The line numbers below are those of `10777a2`; add about six to find them today.

- `step(b)` delivers one event and appends every FS action it returns to `pending` (`controller_test.go:48-56`).
- `completeNext()` pops the **oldest** pending action, executes it against `veritas.FS` (or fails it, if the test's `fail` hook says so), and delivers its completion (`controller_test.go:60-68`). So the harness completes actions in FIFO issue order, which is one particular (and boring) interleaving. The simulator explores the others.
- `drain()` completes until nothing is pending, returning every action produced along the way.
- `do(c, op)` = `step(Request)` + `drain()`.
- `describe(acts)` renders actions compactly: temp paths become `tmp/*` (because their exact names are IOIDs, which tests should not depend on), and the server's root is `/d` (`controller_test.go:12`).

The server under test is built with `MaxObjectSize: 64` (`controller_test.go:34`).

### TestHappyPath

```go
// toys3/control/controller_test.go:211-225
	expect(t, "PUT", h.do(1, Put{Key: "b/k", Data: []byte("hello")}),
		"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b%2Fk; fsyncdir /d/obj; committed c1 b/k del=false; reply c1 200")
	expect(t, "GET", h.do(2, Get{Key: "b/k"}),
		"read /d/obj/b%2Fk; reply c2 200 hello")
	expect(t, "COPY", h.do(3, Copy{Src: "b/k", Dst: "b/k2"}),
		"read /d/obj/b%2Fk; write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b%2Fk2; fsyncdir /d/obj; committed c3 b/k2 del=false; reply c3 200")
	expect(t, "GET copy", h.do(4, Get{Key: "b/k2"}), "read /d/obj/b%2Fk2; reply c4 200 hello")
	expect(t, "DELETE", h.do(5, Delete{Key: "b/k"}),
		"unlink /d/obj/b%2Fk; fsyncdir /d/obj; committed c5 b/k del=true; reply c5 200")
	expect(t, "GET deleted", h.do(6, Get{Key: "b/k"}), "read /d/obj/b%2Fk; reply c6 404")
```

Each expected string is the concatenation of all actions produced from the request through every completion. Read them against §6.9:

- PUT: the five-step protocol, with the `/` in the key escaped as `%2F` in the object file name (`EscapeKey`, Chapter 4).
- GET: one read, then the payload `hello` in the reply. The harness's filesystem stores the *encoded* file; `DecodeObject` strips the header.
- COPY: GET's read followed by PUT's four steps, committing the **destination** `b/k2`.
- DELETE: unlink, fsyncdir, `committed ... del=true`, 200.
- GET after DELETE: 404.

### TestNotFound

```go
// toys3/control/controller_test.go:241-250
	expect(t, "GET", h.do(1, Get{Key: "x"}), "read /d/obj/x; reply c1 404")
	expect(t, "DELETE", h.do(2, Delete{Key: "x"}), "unlink /d/obj/x; reply c2 404")
	expect(t, "COPY", h.do(3, Copy{Src: "x", Dst: "y"}), "read /d/obj/x; reply c3 404")
	if _, ok := h.files[format.ObjPath(root, "y")]; ok {
		t.Fatal("COPY of absent src created dst")
	}
```

Every 404 path is one action deep and fsyncs nothing. The COPY check verifies that a 404 COPY leaves the destination alone: the oracle relies on this (a Definite 404 COPY has no effect on `dst`, `oracle.go:257-258`).

### TestBadRequest

```go
// toys3/control/controller_test.go:252-260
	expect(t, "empty key", h.step(Request{Client: 1, Op: Get{Key: ""}}), "reply c1 400")
	expect(t, "long key", h.step(Request{Client: 2, Op: Put{Key: strings.Repeat("/", 100)}}), "reply c2 400")
	expect(t, "oversize", h.step(Request{Client: 3, Op: Put{Key: "k", Data: make([]byte, 65)}}), "reply c3 400")
	expect(t, "copy bad dst", h.step(Request{Client: 4, Op: Copy{Src: "a", Dst: ""}}), "reply c4 400")
	expect(t, "nil op", h.step(Request{Client: 5}), "reply c5 400")
```

Each is a single `step`, not a `do`: the 400 comes back in the same step and no FS action is issued. "long key" is instructive: 100 slashes is only 100 bytes as a key, but each `/` escapes to `%2F`, so the escaped name is 300 bytes, over `MaxNameLen` (255). Validity is about the *file name*, not the key length. "oversize" is 65 bytes against the harness's limit of 64. "nil op" exercises the `default` branch of the type switch.

### TestSameKeySerialized

This is the test that pins down per-key FIFO, and every line is worth reading:

```go
// toys3/control/controller_test.go:262-278
	expect(t, "PUT 1", h.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("one")}}), "write tmp/*")
	expect(t, "PUT 2 queued", h.step(Request{Client: 2, Op: Put{Key: "k", Data: []byte("two")}}), "")
	expect(t, "GET queued", h.step(Request{Client: 3, Op: Get{Key: "k"}}), "")
	expect(t, "other key runs", h.step(Request{Client: 4, Op: Get{Key: "other"}}), "read /d/obj/other")

	expect(t, "1 write done", h.completeNext(), "fsync tmp/*")
	expect(t, "other done", h.completeNext(), "reply c4 404")
	expect(t, "1 fsync done", h.completeNext(), "rename tmp/* /d/obj/k")
	expect(t, "1 rename done", h.completeNext(), "fsyncdir /d/obj")
	// PUT 2's write is issued only in the step that replies to PUT 1, after it.
	expect(t, "1 fsyncdir done", h.completeNext(), "committed c1 k del=false; reply c1 200; write tmp/*")
	expect(t, "rest", h.drain(),
		"fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; committed c2 k del=false; reply c2 200; read /d/obj/k; reply c3 200 two")
```

Trace the pending queue (FIFO in the harness) to see why the completions come in this order:

```
after c4:        pending = [write(c1), read(c4 other)]
completeNext →   write(c1) done   → issues fsync(c1)       pending = [read(c4), fsync(c1)]
completeNext →   read(c4) done    → reply c4 404           pending = [fsync(c1)]
completeNext →   fsync(c1) done   → rename(c1)             pending = [rename(c1)]
completeNext →   rename(c1) done  → fsyncdir(c1)           pending = [fsyncdir(c1)]
completeNext →   fsyncdir(c1)     → committed c1; reply c1 200; write(c2)
drain        →   c2's protocol; on its commit, startPermitted starts c3's read; c3 reads "two"
```

Key facts this test asserts: the second PUT and the GET produce *no* actions when they arrive; a request on another key runs at once; PUT 2 starts in the same step that replies to PUT 1 and *after* the reply; and the GET, which arrived before PUT 2 completed, sees PUT 2's value because it is behind PUT 2 in the queue. Also visible: two GETs never appear here, but if c3 and a later GET both targeted `k`, the second would wait for the first's read. Reads are serialized too.

### TestDifferentKeysConcurrent

```go
// toys3/control/controller_test.go:280-285
	expect(t, "a", h.step(Request{Client: 1, Op: Put{Key: "a", Data: []byte("1")}}), "write tmp/*")
	expect(t, "b", h.step(Request{Client: 2, Op: Put{Key: "b", Data: []byte("2")}}), "write tmp/*")
```

The shortest test in the file, and the one that would catch an accidental global lock: the second PUT, on a different key, issues its write immediately while the first is in flight.

### TestCopyHoldsBothKeys

```go
// toys3/control/controller_test.go:287-300
	h.do(1, Put{Key: "a", Data: []byte("v")})
	expect(t, "copy", h.step(Request{Client: 2, Op: Copy{Src: "a", Dst: "b"}}), "read /d/obj/a")
	expect(t, "put src queued", h.step(Request{Client: 3, Op: Put{Key: "a", Data: []byte("w")}}), "")
	expect(t, "get dst queued", h.step(Request{Client: 4, Op: Get{Key: "b"}}), "")
	got := describe(h.drain())
	want := "write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b; fsyncdir /d/obj; committed c2 b del=false; reply c2 200; write tmp/*; read /d/obj/b; " +
		"fsync tmp/*; reply c4 200 v; rename tmp/* /d/obj/a; fsyncdir /d/obj; committed c3 a del=false; reply c3 200"
```

While the COPY `a → b` is in flight, a PUT to `a` (its source) and a GET of `b` (its destination) both queue. When the COPY commits, `startPermitted` starts *both*, in arrival order: `write tmp/*` (c3's PUT to `a`) then `read /d/obj/b` (c4's GET). From then on they interleave: c3's write completes → `fsync tmp/*`; c4's read completes → `reply c4 200 v` (the copied value); c3 continues to rename and commit. Two lessons: the COPY blocks *both* keys for its whole duration, and when it finishes, requests on the two keys become independent again and run concurrently.

### TestCopySameKey

```go
// toys3/control/controller_test.go:302-309
	expect(t, "copy a→a", h.do(2, Copy{Src: "a", Dst: "a"}),
		"read /d/obj/a; write tmp/*; fsync tmp/*; rename tmp/* /d/obj/a; fsyncdir /d/obj; committed c2 a del=false; reply c2 200")
	expect(t, "get", h.do(3, Get{Key: "a"}), "read /d/obj/a; reply c3 200 v")
```

Self-copy is the full protocol, renaming a fresh temp file over the source, and the value is unchanged.

## 6.12 What the per-key design buys the oracle

Chapter 13 builds a linearizability checker that knows nothing about the server's internals. It is worth previewing why per-key FIFO makes the server easy to check, and where it does not help.

On a single key, in a single incarnation, the server runs one request at a time in arrival order, and each request's filesystem effects happen strictly between its arrival (the `Request` event) and its reply. So the client-visible history on one key, absent crashes and errors, is trivially linearizable: linearize each request at, say, the instant its last filesystem action lands. Concurrency between keys is irrelevant to a per-key register model, which is why the oracle can split windows into independent components of keys (`oracle.go:359-362`).

Where it does *not* help: crashes and errors. A PUT abandoned by a crash may or may not have taken effect, and if it did, a later read will see it. A 500 says nothing about whether the rename landed. Those are the hard cases, and they are exactly where the error policy of Chapter 7 does its work.

## Key takeaways

- There are no lock objects. `reqs` holds every accepted, unfinished request in arrival order; the queue of key `k` is the subsequence that touches `k`; a request runs when no earlier request shares a key with it.
- Deadlock is impossible by structure: a request only ever waits for requests with smaller `seq`, so "waits for" cannot cycle.
- Reads are not distinguished from writes: GETs on the same key serialize. COPY holds both `src` and `dst` for its whole duration.
- Each running request has exactly one outstanding FS action, tracked by `request.io`, set only in `issue`. Completions are routed by IOID; stale or wrong-kind completions are ignored without changing state.
- `finish` removes the request, emits its telemetry and reply, then starts its successors in the same step, in arrival order. Telemetry precedes replies; replies precede successors' actions.
- COPY is GET's read followed by PUT's write path on the destination, with the source's payload re-encoded.
- Invalid requests get 400 immediately, even during recovery, and never enter `reqs`.
- `startPermitted` is O(n²) in in-flight requests, which is fine for the simulator (n ≤ 6) and a known cost in production.

## Exercises

1. **Predict a trace.** Using the harness in `toys3/control/controller_test.go`, write a test that sends `Copy{a→b}`, `Copy{b→a}`, `Put{a}` and `Get{b}` (all before any completion), with `a` already holding a value. Before running, write down the full `describe` string you expect from `h.drain()`. Then run it with `go test ./toys3/control -run YourTest -v`. Where did your prediction go wrong, if anywhere?

2. **Allow concurrent reads.** Change `interlocked` so that two GETs on the same key do not conflict (leave everything else alone). Which existing unit test breaks, if any? Now run `go test ./toys3/sim -run 'TestSeeds$' -seeds=200` and `go test ./toys3/sim -run TestCatchesBugs`. Is the server still correct? Argue why or why not before you run it. (Revert afterwards.)

3. **A 400 changes nothing.** Write a test that records `h.s.StateHash()` before and after delivering an invalid request (empty key) to a booted server with one request in flight. Assert that the hash is unchanged. Then explain why `nextSeq` is not incremented for invalid requests by pointing at the exact lines.

4. **Break FIFO.** Modify `permitted` so that it only considers *running* older requests as blockers (skip `o` if `o.step == stepWaiting`). Predict which invariant the simulator will catch this with and on roughly which seed, then run `go test ./toys3/sim -run 'TestSeeds$' -seeds=500`. Read the first failure. (Hint: think about a GET that overtakes a waiting PUT, and what the oracle requires of a read that *starts* after a PUT was invoked but before it replied. You may be surprised: the oracle might accept it. If so, explain why the history is still linearizable, and construct a modification that the oracle *does* catch.)

5. **Trace the queue.** `go test ./toys3/sim -run TestSeeds -seed=1 -v` prints the event log only when the seed fails (`toys3/sim/sim_test.go:53-58`). To see the log of a passing seed, write a tiny program in a scratch module (a `go.mod` with `require manifester v0.0.0` and `replace manifester => /path/to/this/repo`) that sets `cfg := sim.DefaultConfig(1); cfg.Log = true`, calls `sim.Run(cfg)` and prints `sim.FormatLog(r.Log)`. Find the first step where three or more requests are waiting in `reqs` at once and draw the implied per-key queues as in §6.4.

6. **Make it O(n log n).** Design a per-key structure (for example, a sorted slice of `(key, seq)` pairs, or per-key slices of `seq` in a sorted slice of keys) that answers "is `r` at the head of all its queues?" in O(log n) and avoids ranging over maps. Implement it alongside the current scheme and add an assertion in `Step` that both agree. Run 500 seeds. The simulator is your equivalence checker: if the two ever disagree, the run panics and the panic is reported as a failure (`toys3/sim/simulator.go:177-180`).

7. **IOID bookkeeping.** In `issue`, delete the `case FSUnlink:` branch. Which unit test fails first, and with what message? Which simulator invariant would catch it, and why would it be the liveness one rather than a safety one?

## Further reading

- Maurice Herlihy and Jeannette Wing, *"Linearizability: A Correctness Condition for Concurrent Objects"*, ACM TOPLAS 1990. The definition the oracle checks; §6.12 previews why per-key FIFO makes it easy to satisfy.
- Edward G. Coffman, Melanie Elphick and Arie Shoshani, *"System Deadlocks"*, ACM Computing Surveys 1971. The four conditions for deadlock; toys3 breaks "circular wait" by ordering all waits by a single sequence number.
- Cory Benfield, Brett Cannon et al., the "sans-I/O" protocol documentation (https://sans-io.readthedocs.io). The idea that protocol logic should be a pure state machine over bytes and events, which `onRequestIO` embodies.
- Ding Yuan et al., *"Simple Testing Can Prevent Most Critical Failures"*, OSDI 2014. Relevant here because most of `onRequestIO`'s branches are error branches; Chapter 7 is about them.
