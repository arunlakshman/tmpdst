# Chapter 12: Workloads and Swarm Testing

> *"A simulator is only as good as the questions its clients ask."*

The previous chapters built a hostile world: a fake filesystem that loses, tears and lies (Chapter 11), and a simulator that crashes the server at random and drops every completion addressed to a dead incarnation (Chapter 10). None of that hostility matters unless something *uses* the system while the world misbehaves, and records what it saw in a form that an oracle can judge. That something is the **workload**: the simulated clients.

This chapter covers three things:

1. **The client model** in `toys3/sim/workload.go`: how many clients, what they ask for, how often, and how they react to crashes.
2. **Self-describing values**: the small trick that makes every byte a GET returns *name its writer*, which turns linearizability checking from guesswork into exact bookkeeping (Chapter 13 depends on it completely).
3. **Quiescence and swarm testing**: how the workload deliberately pauses to keep the oracle's job tractable, and how `DefaultConfig` varies the shape of the workload and fault model per seed, because no single configuration finds every bug. We will look at an ablation, measured while writing this book, that shows the effect is not subtle.

---

## 12.1 What a workload has to do

Before reading the code, it is worth listing the requirements a DST workload must satisfy, because each one shows up as a specific design decision.

| Requirement | Why | Where in toys3 |
|---|---|---|
| Deterministic | Every choice must come from the run's single `Rand` | `Workload.rng` is the simulator's `rng` (`simulator.go:59`) |
| Concurrent | Same-key races are where the bugs live | `Clients` > 1, each with one request outstanding |
| Contended | Concurrency on *different* keys proves little | small key sets (`Keys`) |
| Exercises every op | A mutant in COPY is invisible to a PUT/GET-only workload | `OpMix` weights all four ops plus invalid ones |
| Identifiable values | The oracle must know which write a read observed | `MakeValue` / `ValueWriter` |
| Crash-aware | Requests outstanding at a crash have unknown outcome | `Workload.Crash`, `Oracle.Crash` |
| Bounded windows | A linearizability search over the whole run is exponential | `QuiesceEvery` |
| Varied across seeds | One shape of workload hides some bugs | `DefaultConfig` swarm |

Notice what is *not* on the list: realism. The workload does not look like real S3 traffic. Real traffic has millions of keys, Zipfian access, multi-megabyte objects and no reason to COPY a key onto itself. The simulated traffic has between 1 and 32 keys, values of 12 to 40 bytes, and frequent self-copies. It is shaped to provoke bugs, not to resemble production.

## 12.2 The configuration

```go
// toys3/sim/workload.go:10-35
// OpMix weights the operations clients issue. Zero weights disable an op.
type OpMix struct {
	Put, Get, Delete, Copy int
	// Invalid issues a request with an empty key (always 400).
	Invalid int
}

// WorkloadConfig shapes client traffic.
type WorkloadConfig struct {
	// Clients is the number of concurrent clients; each has at most one
	// request outstanding, so Clients bounds concurrency.
	Clients int
	// Keys is the size of the key set ("k0".."k{n-1}"). Small sets force
	// same-key contention, which is where the interesting bugs live.
	Keys int
	// MaxValueSize bounds PUT payloads (at least ValueHeader).
	MaxValueSize int
	Mix          OpMix
	// Think is the [min, max] delay between a reply and the client's next request.
	Think [2]core.Duration
	// QuiesceEvery, if > 0, periodically stops clients from issuing until all
	// outstanding requests have replied. Quiescent points split the history
	// into independent windows for the linearizability checker, bounding its
	// search. A crash is also a quiescent point.
	QuiesceEvery core.Duration
}
```

The defaults live in `BaseConfig`:

```go
// toys3/sim/config.go:152-159
		Workload: WorkloadConfig{
			Clients:      4,
			Keys:         3,
			MaxValueSize: 40,
			Mix:          OpMix{Put: 40, Get: 40, Delete: 10, Copy: 15, Invalid: 2},
			Think:        [2]core.Duration{0, 1 * core.Millisecond},
			QuiesceEvery: 8 * core.Millisecond,
		},
```

Some arithmetic worth having in your head:

- The mix weights sum to 107. So each issued request is a PUT with probability 40/107 ≈ 37%, a GET 37%, a DELETE 9%, a COPY 14%, and an invalid request about 1.9%. In seed 1 (225 requests) we observed `400=3`, about 1.3%: the right order of magnitude for a 2/107 coin.
- `MaxValueSize` is 40 and the server's `MaxObjectSize` is 64 (`config.go:171`). So **no valid PUT is ever oversize**. The only 400s in a simulation come from the `Invalid` slot. The oversize branch of `onRequest` (`server.go:263`) is covered by the unit test `TestBadRequest` in `server_test.go`, not by the simulator. (That is an exercise below.)
- Think time is uniform in [0, 1 ms]. Latency of a single FS action is 20 µs to 1.5 ms to land plus 0 to 0.5 ms to deliver (`config.go:165-166`). A PUT takes five FS round trips, so typically several milliseconds. With four clients and sub-millisecond think times, there are usually two to four requests in flight at any instant, and on a 3-key set they frequently collide.

### Defensive normalization

`NewWorkload` fixes up nonsense configurations rather than failing:

```go
// toys3/sim/workload.go:59-77
func NewWorkload(cfg WorkloadConfig, r *Rand) *Workload {
	if cfg.Clients < 1 {
		cfg.Clients = 1
	}
	if cfg.Keys < 1 {
		cfg.Keys = 1
	}
	if cfg.MaxValueSize < ValueHeader {
		cfg.MaxValueSize = ValueHeader
	}
	if cfg.Mix == (OpMix{}) {
		cfg.Mix = OpMix{Put: 1, Get: 1}
	}
	w := &Workload{cfg: cfg, rng: r, clients: make([]clientState, cfg.Clients)}
	for i := 0; i < cfg.Keys; i++ {
		w.keys = append(w.keys, "k"+strconv.Itoa(i))
	}
	return w
}
```

Two details matter. First, the zero `OpMix` becomes PUT/GET only, so a test that builds a `Config` by hand without a mix still produces traffic. Second, the key names are `k0`, `k1`, ... `k31`. They contain only unreserved characters, so `core.EscapeKey` leaves them unchanged and the obj file for `k5` is literally `/data/obj/k5`. That is a deliberate convenience for reading logs; the escaping logic is exercised by the unit tests and the production end-to-end test, not by the simulator.

## 12.3 Clients: one request each

A client is two fields:

```go
// toys3/sim/workload.go:37-41
type clientState struct {
	busy bool           // a request is outstanding
	req  core.ClientReq // the outstanding request, if busy
}
```

and the whole workload is:

```go
// toys3/sim/workload.go:50-56
type Workload struct {
	cfg     WorkloadConfig
	rng     *Rand
	nextReq core.ClientReq
	clients []clientState
	keys    []string
}
```

The **closed-loop** model (each client waits for its reply before issuing the next request) is a deliberate choice. It bounds concurrency to `Clients`, which bounds the size of linearizability windows (Chapter 13), and it means that every client's own history is sequential. An open-loop model (requests arrive on a schedule regardless of replies) would model overload better but would make windows unbounded whenever the server is slow or down.

### Issuing a request

```go
// toys3/sim/workload.go:92-119
func (w *Workload) Issue(client int, now core.Time) core.Request {
	w.nextReq++
	req := w.nextReq
	m := w.cfg.Mix
	total := m.Put + m.Get + m.Delete + m.Copy + m.Invalid
	x := w.rng.Intn(total)
	var op core.Op
	switch {
	case x < m.Put:
		size := int(w.rng.Between(ValueHeader, int64(w.cfg.MaxValueSize)))
		op = core.Put{Key: w.key(), Data: MakeValue(req, size)}
	case x < m.Put+m.Get:
		op = core.Get{Key: w.key()}
	case x < m.Put+m.Get+m.Delete:
		op = core.Delete{Key: w.key()}
	case x < m.Put+m.Get+m.Delete+m.Copy:
		op = core.Copy{Src: w.key(), Dst: w.key()}
	default:
		if w.rng.Intn(2) == 0 {
			op = core.Get{Key: ""}
		} else {
			op = core.Put{Key: "", Data: MakeValue(req, ValueHeader)}
		}
	}
	w.clients[client] = clientState{busy: true, req: req}
	return core.Request{Client: req, Op: op}
}
```

Read it with the RNG in mind. Every request costs at least two draws (`Intn(total)` and one key), a PUT costs three (size and key are separate), and a COPY costs three (two keys). The `now` parameter is unused: the request does not depend on time, only on the RNG state. That is fine, but it is also why you cannot shift a workload in time without changing it.

Three properties of this function produce interesting test cases for free:

1. **COPY draws `Src` and `Dst` independently.** With 3 keys, one COPY in three is a self-copy `COPY "k1" -> "k1"`. With 1 key, *every* COPY is a self-copy. Self-copies exercise the `Src == Dst` path in the server (`TestCopySameKey`, `server_test.go:313-320`) and in the oracle (`opKeys` collapses them to one key, `oracle.go:364-366`). You can see one in seed 1's log at step 118: `Request{c18 COPY "k2" -> "k2"}`, committed at step 142.
2. **The request id doubles as the value's identity.** `req` is a `core.ClientReq` that is unique for the whole run (the counter never resets, even across server incarnations). `MakeValue(req, size)` therefore produces a value that no other request will ever produce.
3. **Invalid requests come in two flavours**: `GET ""` and `PUT ""` with a 12-byte value. Both should get 400 from `onRequest` before touching anything (`server.go:263-270`). They exercise the "never enters the queue" path, and the oracle skips them (`oracle.go:311-313`).

### Completing and crashing

```go
// toys3/sim/workload.go:121-155
func (w *Workload) Complete(rep core.Reply) (client int, ok bool) {
	for i, c := range w.clients {
		if c.busy && c.req == rep.Client {
			w.clients[i] = clientState{}
			return i, true
		}
	}
	return -1, false
}

func (w *Workload) Crash() []core.ClientReq {
	var ids []core.ClientReq
	for i, c := range w.clients {
		if c.busy {
			ids = append(ids, c.req)
			w.clients[i] = clientState{}
		}
	}
	return ids
}

func (w *Workload) Idle() bool {
	for _, c := range w.clients {
		if c.busy {
			return false
		}
	}
	return true
}
```

`Complete` returning `ok=false` is how the simulator detects a reply to an unknown or already-answered request: `Simulator.reply` (`simulator.go:228-237`) turns that into a `protocol` failure. A server that replied twice to the same request, or invented a client id, would be caught here regardless of any other invariant.

`Crash` is the client side of a power loss. Every outstanding request is **abandoned**: the client stops waiting, becomes idle, and will issue a new request after the restart. The simulator's `crash` (`simulator.go:275-306`) uses the returned ids to count `Indetermined` requests, to detect `CrashesMidPut` (a PUT or COPY was outstanding), and to *park* the abandoned clients (`s.parked[op.Client] = true`) so they wake up again after the restart. It then calls `oracle.Crash(now)`, which marks those same requests `Abandoned` (Chapter 13).

### The client life cycle, as the simulator drives it

The workload itself has no clock and no queue. The simulator drives it through three entry kinds and two flags:

```
                    ┌─────────────── KindClientWake(c) ───────────────┐
                    │                                                 │
                    ▼                                                 │
            wake(c) (simulator.go:240-253)                            │
              │ Busy(c)?           → return (stale wake)              │
              │ stopped | quiescing | !alive  → parked[c] = true      │
              │ else:                                                 │
              │   req := workload.Issue(c, now)                       │
              │   oracle.Invoke(now, c, req)                          │
              │   schedule Deliver(req) at now (current incarnation)  │
              ▼                                                       │
        server Step(Request) ... eventually Reply{c, ...}             │
              │                                                       │
              ▼                                                       │
        reply(r) (simulator.go:228-237)                               │
              │ oracle.Return; workload.Complete                      │
              └── schedule ClientWake(c) at now + Think() ────────────┘

   crash:   workload.Crash() → each abandoned client parked
   restart: if !quiescing → wakeParked()      (simulator.go:322-324)
   quiesce end (all idle): if alive → wakeParked()   (simulator.go:339-347)
```

A few subtleties that matter when you read logs:

- **Clients do not issue while the server is down.** `wake` parks a client if `!s.alive`. So there is never a request delivered to a dead server; the only way a request is lost is that it was already queued for delivery (or in progress) when the crash happened. In the log, every `Request{...}` appears with the current incarnation.
- **The oracle's invoke time is the issue time, not the delivery time.** `wake` calls `oracle.Invoke(s.now, ...)` and *then* schedules the delivery at the same virtual instant. Other entries at that instant may run in between (the queue breaks ties randomly, Chapter 10), and a crash can even happen in between, in which case the request is abandoned without ever reaching the server. The `HistOp.Invoke` field's comment says "when the Request was delivered" (`oracle.go:27`); strictly, it is when the request was issued. This only widens the request's window, which makes the oracle more permissive, never wrong.
- **Requests can reach a server that is still booting.** `restart` schedules `Started` at `now` and then wakes parked clients after a think time drawn from [0, 1 ms]. A think time of exactly 0 is possible, so a request can be scheduled at the same instant as `Started` and, depending on the tie-break, delivered *before* it. That is why the server queues requests in `phaseBooting` as well as `phaseRecovering` (`server.go:288-291`). The workload's randomness is what exercises that branch.

You can see the parking and waking in seed 1's log around the first crash:

```
#233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Halt{fsync temp: EIO}
#233    t=53.112270ms  inc=1 crash       CRASH (halt: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#235    t=55.879722ms  inc=2 restart     boot
#236    t=55.879722ms  inc=2 deliver     Started => FSList{io1 /data/tmp}
#238    t=56.016744ms  inc=2 deliver     Request{c31 PUT "k0" value-of-31}
#240    t=56.034093ms  inc=2 deliver     Request{c32 COPY "k1" -> "k0"}
#242    t=56.246986ms  inc=2 deliver     Request{c33 GET "k2"}
#244    t=56.308317ms  inc=2 deliver     Request{c34 PUT "k1" value-of-34}
```

Four clients, four new requests within half a millisecond of the restart, all queued behind recovery (none of them produces an action until `Recovered`). The skipped step numbers (#237, #239, ...) are the `KindClientWake` entries, which the log does not print.

## 12.4 Self-describing values

Here is the problem the next trick solves. A GET returns some bytes. The oracle must decide whether a sequential execution of the history could have produced those bytes. If values were arbitrary (say, random bytes or a counter), the oracle would have to compare the returned bytes against every value ever written to that key and hope none of them collide. Worse, if the server returned a *torn* value (a prefix of one write) or a *mixed* value (the header of one write and the tail of another), the oracle might mistake it for a legitimate value that happens to match, or might be unable to say which write it came from.

toys3's answer is to make every value **name its writer**, and to make the rest of the value a pure function of that name, so that a single byte out of place is detectable.

```go
// toys3/sim/workload.go:157-176
// ValueHeader is the size of the self-describing prefix of every value:
// writer ClientReq (8 bytes LE) + total length (4 bytes LE).
const ValueHeader = 12

// MakeValue returns the unique value written by PUT req: ValueHeader followed
// by filler that is a pure function of (req, size). size >= ValueHeader.
func MakeValue(req core.ClientReq, size int) []byte {
	if size < ValueHeader {
		size = ValueHeader
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint64(b, uint64(req))
	binary.LittleEndian.PutUint32(b[8:], uint32(size))
	x := uint64(req)*0x9e3779b97f4a7c15 + uint64(size)
	for i := ValueHeader; i < size; i++ {
		x = Fold(x, uint64(i))
		b[i] = byte(x)
	}
	return b
}
```

The layout of a value written by PUT request 26 with size 20:

```
 offset  0                   8           12                          20
         ┌───────────────────┬───────────┬───────────────────────────┐
         │ 1a 00 00 00 00 00 │ 14 00 00  │ f0 f1 f2 f3 f4 f5 f6 f7   │
         │ 00 00             │ 00        │ (Fold chain seeded by     │
         │ writer = 26 (LE)  │ len = 20  │  26*φ + 20, one byte per  │
         │                   │  (LE)     │  position)                │
         └───────────────────┴───────────┴───────────────────────────┘
          8 bytes             4 bytes     size-12 bytes of filler
```

(The filler bytes shown as `f0...f7` are placeholders; the real ones are whatever `Fold` produces.) The filler is derived by chaining `Fold` (the same mixing function the trace hash uses, `hash.go:7-11`) from a seed that depends on both `req` and `size`. Two consequences:

- **Two different writes never produce the same bytes.** Different `req` means a different header. Same `req` cannot happen twice (request ids are unique per run).
- **Any modification is detectable.** Truncating the value changes the length; the stored length field no longer matches. Flipping a filler bit makes the filler disagree with the chain. Splicing the header of write A onto the tail of write B makes the filler disagree. Splicing two writes with the same length but different ids makes the header disagree with the filler.

The checker is the inverse:

```go
// toys3/sim/workload.go:178-193
func ValueWriter(b []byte) (writer core.ClientReq, ok bool) {
	if len(b) < ValueHeader || int(binary.LittleEndian.Uint32(b[8:])) != len(b) {
		return 0, false
	}
	writer = core.ClientReq(binary.LittleEndian.Uint64(b))
	want := MakeValue(writer, len(b))
	for i := range b {
		if b[i] != want[i] {
			return 0, false
		}
	}
	return writer, true
}
```

It reconstructs the value the claimed writer *would* have written at this length and compares every byte. `ok` is true only for an exact, complete value. This function is the foundation of three separate checks:

- **`NoCorruptReads`** (`invariant.go:88-111`) calls `ValueWriter` on every 200 GET body. If it fails, the server returned bytes no PUT wrote. It also checks that the named writer exists and was a PUT (not, say, the id of a GET).
- **The oracle** (`oracle.go:318-329`) uses the writer id as the *value* in its model. A GET 200 is reduced to the statement "this read observed the value written by request 26". The model state is a map from key to writer id, not to bytes.
- **Log rendering** (`describe.go:28-43`) uses it to print `obj(value-of-26)` instead of a hex dump, which is why the logs in this book are readable at all.

### Why the server's CRC is not enough

The server has its own integrity check: every object file carries a CRC-32 of the payload (`format.go:23-49`, Chapter 4). Why does the workload need a second one?

Because the two checks answer different questions. The server's CRC answers "is this file what *some* PUT wrote?" The workload's value check answers "is this payload what *the PUT named in it* wrote?" More importantly, the simulator must not trust the server: the server's CRC check is *part of the system under test*. Mutant **e** (`mutSkipCRC`, `mutants_test.go:303-313`) disables it. If the workload relied on the server's CRC, a server that skipped it would go undetected. The value check is outside the server and survives any mutation of it. In seed 18 with mutant e, the FakeFS flipped one bit on a read of `k3`'s 29-byte file (16 header + 13 payload), the mutant re-stamped a valid CRC over the corrupted payload, and the GET returned:

```
FAIL no-corrupt-reads step 657: 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
```

The same seed against the correct server produces, at the same step:

```
#657    t=154.136154ms inc=7 deliver     FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptObject{"k3"}} Reply{c98 500}
```

Same universe, same bit flip, one server catches it and replies 500, the other serves garbage, and the workload's self-describing value is what tells them apart.

### COPY preserves identity

COPY reads the source object and writes the same payload under the destination key. The payload still begins with the *original* writer's id. So after `PUT k0 ← value-of-10` and `COPY k0 → k1`, a GET of `k1` returns bytes that `ValueWriter` attributes to request 10, not to the COPY. The oracle's model mirrors this: COPY sets `dst := writer(src)` (`oracle.go:288-289`). This means the model never needs to know about COPY's bytes, only about which PUT they originally came from. You can see it in seed 1's log at step 88: `Committed{c12 "k1" crc=1ce854a5}`, the same CRC as `Committed{c10 "k0" crc=1ce854a5}` at step 54, because c12 copied c10's value.

There is a subtle consequence that matters for mutant f (Chapter 15): **a value can legitimately appear under a key that its writer never named**, but only via a COPY. When the oracle sees `value-of-231` under `k5` and request 231 was a PUT to `k25`, and no COPY ever moved it, there is no explanation. That is exactly how f is caught.

## 12.5 Quiescence: keeping the oracle's problem small

Linearizability checking is NP-complete in general (Gibbons and Korach, 1997). The WGL search in Chapter 13 explores orderings of concurrent operations, and its cost grows exponentially with the number of operations that overlap. A 360 ms run with four clients issues 200 to 300 requests. Searching all of them at once is out of the question.

The standard trick is to find **quiescent points**: instants at which no operation is pending. At a quiescent point, every earlier operation has returned before every later one is invoked, so in any linearization the earlier operations all come first. The history splits into independent **windows**, and each window can be searched on its own, carrying only the set of possible model states across the boundary.

```
 time ───────────────────────────────────────────────────────────────────────►
 c0  ├──PUT──┤   ├─GET─┤          │ ├──DEL──┤  ├─PUT──┤      │ ├─GET─┤
 c1    ├──GET───┤  ├──COPY───┤    │    ├─GET─┤      ├─GET─┤   │   ├──PUT──┤
 c2       ├─PUT─┤      ├─DEL─┤    │  ├──PUT───┤             │ ├─COPY─┤
                                  ▲                          ▲
                           quiescent point             quiescent point
                        (no op pending: check         (or a crash: all pending
                         window 1, carry states)       ops become Abandoned)
```

Natural quiescent points happen whenever all clients happen to be between requests at once. With four busy clients and sub-millisecond think times, that can take a while. So the workload *forces* them:

```go
// toys3/sim/simulator.go:154-157  (handle, KindQuiesce)
	case KindQuiesce:
		if !s.stopped {
			s.quiescing = true
			s.note(e, "quiesce")
		}
```

While `quiescing` is true, `wake` parks every client that becomes idle instead of letting it issue. Requests already in flight finish. As soon as the last one replies, `afterStep` notices and lifts the pause:

```go
// toys3/sim/simulator.go:338-347
func (s *Simulator) afterStep() {
	if s.quiescing && s.workload.Idle() {
		s.quiescing = false
		if !s.stopped {
			s.schedule(Entry{At: s.now + core.Time(s.cfg.Workload.QuiesceEvery), Kind: KindQuiesce})
		}
		if s.alive {
			s.wakeParked()
		}
	}
	...
```

Note that the *next* quiesce is scheduled `QuiesceEvery` after the *end* of the previous one, not on a fixed grid. In seed 1 the quiesce notes appear at 8.0 ms, 21.09 ms, 35.37 ms, 49.67 ms, 61.11 ms: gaps of 8 ms plus however long the drain took.

Then, in the same `afterStep`, the invariants run, and the `Linearizable` invariant checks the window as soon as the oracle has no pending operation:

```go
// toys3/sim/invariant.go:166-172
func (*Linearizable) AfterStep(v View) []Failure {
	o := v.Oracle()
	if !o.Quiescent() || o.Unchecked() == 0 {
		return nil
	}
	return o.CheckQuiescent()
}
```

### Crashes are quiescent points

A crash abandons every pending request (`Oracle.Crash`, `oracle.go:142-152`). After it, nothing is pending, so the very same `afterStep` checks a window that ends at the crash. This is why the `windows` count in a stats line is roughly "forced quiesces + crashes + natural idle moments". Seed 1 had 10 crashes and a few dozen forced quiesces in 360 ms, and checked 30 windows (some quiesces coincide with natural idleness or crashes and add no new window, because `Unchecked() == 0`).

### Sizing the windows

`QuiesceEvery = 8 ms` is a tuning knob between two failure modes:

- **Too large**: windows grow, the search explodes, and the oracle reports `checker budget exceeded (... memo entries) on N ops: shrink QuiesceEvery` (`oracle.go:568-571`).
- **Too small**: the workload spends much of its time draining, concurrency drops, and fewer same-key races occur.

The `maxwin` statistic tells you where you are. In the 200-seed run from the preface, `maxwin=23`: the largest window searched in 200 seeds had 23 operations. Seed 1's largest had 13. The search handles that comfortably. If you raise `Clients` to 16 or `QuiesceEvery` to 50 ms, watch `maxwin` and the wall time grow (an exercise below).

### The end of the run

Two more scheduled entries shape the workload's life:

```go
// toys3/sim/simulator.go:158-166
	case KindQuiet:
		s.quiet = true
		s.fs.SetQuiet(true)
		s.env.buggify = false
		s.note(e, "chaos over: no more faults or crashes")
	case KindTailEnd:
		s.stopped = true
		s.note(e, "quiet tail over: clients stop")
```

At `Chaos` (300 ms) faults stop but clients keep going; at `Chaos + QuietTail` (360 ms) clients stop issuing (`wake` parks them because `stopped`), no further quiesce is scheduled, and the queue drains. Requests issued during QuietTail are special: the `Liveness` invariant requires that none of them gets a 500 (`invariant.go:176-190`). The workload is what generates those requests; without traffic in the tail, the "a lost object reads back as 500 forever" symptom of mutant b would be invisible to black-box checks (Chapter 14).

## 12.6 One configuration is not enough

Here is the central claim of this chapter's second half: **no single workload and fault configuration finds every bug**, and the fix is not to find the "best" configuration but to *vary the configuration per seed*.

The intuition comes from looking at what different bugs need:

| Bug | What makes it visible | Configuration that helps |
|---|---|---|
| Same-key ordering bugs | Two requests on one key, completions in an unlucky order | **Few keys**, many clients |
| Ack-before-durable (a, c) | A crash after the ack, or just the eager `AckedIsDurable` check | Anything with PUTs |
| No halt after fsync error (d, d2) | An EIO, then a read of the half-applied change, then a crash | **High EIO rate**, few keys (so the read hits the same key), some crashes |
| Resurrected temp file aliasing an object (f) | A temp name made durable, a crash, a later write reusing the name, and a *read of the aliased key long after* | **Many keys** (so the aliased object is not overwritten before it is read), crashes, writeback |
| Lost object reads back as 500 (b, black box) | Traffic in QuietTail on a key whose object was torn | Enough keys that some survive untouched to the tail |

Few keys and many keys pull in opposite directions. A fixed 3-key workload overwrites every key every few milliseconds, which is great for races and terrible for slow-burning damage: the aliased object in mutant f gets overwritten by a legitimate PUT before anyone reads the damage. A fixed 32-key workload leaves objects untouched for long stretches, which is great for slow damage and poor for races.

This is the observation behind **swarm testing** (Groce, Zhang, Eide, Chen and Regehr, *"Swarm Testing"*, ISSTA 2012): instead of running every test with every feature enabled at a "reasonable" rate, run each test with a random *subset* of features, or with randomly chosen rates. The paper's key finding is that features *suppress* each other. A configuration that includes everything at moderate intensity never reaches the extremes where certain bugs live. `docs/lessons.md` §9 calls this **passive suppression**: "everything a little bit on" means fault families crowd each other out.

### DefaultConfig: swarm in four lines

```go
// toys3/sim/config.go:135-143
func DefaultConfig(seed uint64) Config {
	cfg := BaseConfig(seed)
	r := NewRand(seed ^ 0x5a5a_c0ff_ee00_1234)
	cfg.Workload.Keys = []int{1, 2, 3, 3, 4, 8, 16, 32}[r.Intn(8)]
	cfg.Workload.Clients = []int{2, 4, 4, 6}[r.Intn(4)]
	cfg.CrashProb = []float64{0.001, 0.004, 0.004, 0.01}[r.Intn(4)]
	cfg.FS.EIOProb = []float64{0.002, 0.01, 0.01, 0.03}[r.Intn(4)]
	return cfg
}
```

Four knobs, each picked from a small menu. The menus encode weights by repetition: 3 keys and 4 clients and the middle crash and EIO rates appear twice, so they are the most likely. The resulting distribution:

| Knob | Values (probability) |
|---|---|
| Keys | 1 (1/8), 2 (1/8), **3 (1/4)**, 4 (1/8), 8 (1/8), 16 (1/8), 32 (1/8) |
| Clients | 2 (1/4), **4 (1/2)**, 6 (1/4) |
| CrashProb (per step, Chaos only) | 0.001 (1/4), **0.004 (1/2)**, 0.01 (1/4) |
| EIOProb (per FS op, Chaos only) | 0.002 (1/4), **0.01 (1/2)**, 0.03 (1/4) |

That is 7 × 3 × 3 × 3 = 189 distinct shapes, with `BaseConfig` (3 keys, 4 clients, 0.004, 0.01) being the most common single shape at 1/4 × 1/2 × 1/2 × 1/2 = 1/32 of seeds.

### Why a separate RNG stream?

The swarm knobs are drawn from `NewRand(seed ^ 0x5a5a_c0ff_ee00_1234)`, *not* from the run's own `Rand`. Two reasons:

1. **The run's stream must start in the same state regardless of the knobs.** If the knobs were drawn from the run's stream, the first four draws of every run would be consumed by configuration, and changing the menu (say, adding a 64-key option) would shift every subsequent draw of every seed. With a separate stream, `BaseConfig(seed)` and `DefaultConfig(seed)` start their simulations from the same RNG state. The traces still diverge as soon as the knobs change behaviour, but the separation keeps configuration and execution independent.
2. **Configuration is a function of the seed alone.** `DefaultConfig(seed)` is pure. The campaign runner, `TestSeeds`, `TestCatchesBugs` and the repro command all call it with the seed and get the same configuration. A failing seed's repro command (`go test ./toys3/sim -run TestSeeds -seed=N -v`) therefore reproduces the swarm shape too; there is no separate "configuration id" to remember.

XOR with a constant is a cheap way to derive an independent-looking stream: SplitMix64's output function is a strong mixer, so `seed` and `seed ^ C` produce unrelated sequences.

### What the swarm looks like in practice

Here are the configurations and stats lines of the seeds this book uses, all against the correct server (header fields come from the scratch tracer described in Chapter 16; the stats line is identical to what `TestSeeds -seed=N -v` prints):

```
seed 1   keys=3  clients=4 crash=0.001 eio=0.03
  reqs=225 200=154 400=3 404=36 500=7 indet=25 fsops=527 eio=10 crashes=10 halts=9 midput=8 torn=1 lost=9 kept=12 readcorrupt=0 corruptobj=0 tempsrm=7 windows=30 maxwin=13 widened=0
seed 2   keys=16 clients=4 crash=0.01  eio=0.01
  reqs=306 200=195 400=5 404=42 500=5 indet=59 fsops=671 eio=5 crashes=20 halts=2 midput=17 torn=2 lost=14 kept=9 readcorrupt=1 corruptobj=1 tempsrm=7 windows=41 maxwin=17 widened=0
seed 18  keys=4  clients=4 crash=0.004 eio=0.03
  reqs=216 200=145 400=4 404=22 500=15 indet=30 fsops=505 eio=16 crashes=13 halts=9 midput=11 torn=1 lost=11 kept=7 readcorrupt=1 corruptobj=1 tempsrm=5 windows=29 maxwin=14 widened=0
seed 20  keys=32 clients=4 crash=0.004 eio=0.03
  reqs=318 200=182 400=8 404=59 500=15 indet=54 fsops=720 eio=17 crashes=19 halts=9 midput=17 torn=0 lost=27 kept=17 readcorrupt=0 corruptobj=0 tempsrm=12 windows=38 maxwin=15 widened=0
seed 132 keys=2  clients=4 crash=0.001 eio=0.03
  reqs=205 200=146 400=4 404=24 500=13 indet=18 fsops=481 eio=12 crashes=8 halts=7 midput=6 torn=0 lost=6 kept=7 readcorrupt=1 corruptobj=1 tempsrm=3 windows=29 maxwin=10 widened=0
```

Read them comparatively:

- **Seed 2 (crash=0.01, eio=0.01)** crashes 20 times but halts only twice: most of its crashes are injected, not self-inflicted. It has the highest `indet` (59 abandoned requests): frequent injected crashes catch many requests in flight.
- **Seeds 1, 18, 132 (eio=0.03)** halt on most of their crashes (9 of 10, 9 of 13, 7 of 8): with a 3% EIO rate and five FS ops per PUT, roughly one PUT in seven hits an error, and every error after the temp write halts.
- **Seed 20 (keys=32)** has the most 404s (59): with 32 keys, many GETs, DELETEs and COPYs hit keys nobody has written yet. It also has `lost=27`: lots of un-synced changes thrown away by its 19 crashes.
- **Seed 132 (keys=2)** has the lowest `maxwin` (10) and almost no 404 diversity: two keys, constant contention.

These are five quite different universes. A bug that needs "high EIO and few keys" (d2) shows up in seed 132; a bug that needs "many keys and crashes" (f) shows up in seed 20. Neither seed would have found the other's bug quickly.

## 12.7 The ablation: does swarm actually matter?

`DESIGN.md:205-207` makes a strong claim about mutant f (recovery skips temp-file cleanup):

> (f) took seed 429 with neither background writeback nor swarm, seed 36 with writeback alone, and seed 20 with both. Whether a bug gets caught depends on how realistic the fault model is and how varied the workload is.

Claims like this deserve to be re-measured, so while writing this book the experiment was rerun with a small out-of-tree driver (it copies `mutants_test.go`'s mutants and calls `sim.Run` with modified configs; Chapter 16 describes it). For each combination, it runs seeds 1, 2, 3, ... until the first failure. Results:

| Configuration | Writeback (`WritebackProb`) | First failing seed for mutant f |
|---|---|---|
| `BaseConfig` (no swarm) | 0 | **429** |
| `BaseConfig` (no swarm) | 0.05 (default) | **36** |
| `DefaultConfig` (swarm) | 0 | **24** |
| `DefaultConfig` (swarm) | 0.05 (default) | **20** |

The three numbers DESIGN.md reports reproduce exactly. The fourth row ("swarm without writeback: 24") is a new data point not in DESIGN.md, and it is instructive: **swarm alone** gets you almost all the way, from 429 down to 24, an 18× improvement. Writeback alone gets you to 36. Both together, 20.

First-catch seed is a noisy statistic (one sample per configuration). A more rigorous version would measure the *fraction* of seeds that catch the mutant in each configuration over, say, 2,000 seeds. But the order-of-magnitude story is robust, and the mechanism explains it:

- Mutant f's damage is a temp file name that survives a crash as a **second hard link** to a committed object's inode (Chapter 11 explains how the FakeFS produces it; Chapter 15 walks through the run). It only becomes visible if (1) a later incarnation writes a new temp file *with the same name* (temp names are IOIDs, which restart at 1 every incarnation, so name reuse is common), which truncates and overwrites the shared inode, and (2) a client then *reads the original key* before anyone overwrites it legitimately.
- With 3 keys, step (2) almost never happens: some PUT overwrites the aliased key within milliseconds, replacing the shared inode in `obj/` with a fresh one, and the damage is erased before anyone sees it.
- With 32 keys (seed 20's swarm shape), a key can sit untouched for tens of milliseconds, long enough for the alias to be overwritten through the temp name and then read through the object name.
- Writeback makes step (0) (the temp name becoming durable before the rename) happen far more often, so there are more aliases to begin with.

### Ablation 2: read corruption and the CRC

The same driver measured the other ablation DESIGN.md talks about: mutant e (skip the CRC check on read) with `ReadCorruptProb = 0`:

| Configuration | First failing seed for mutant e |
|---|---|
| `DefaultConfig` | 18 |
| `DefaultConfig`, `ReadCorruptProb = 0` | **none in 1,000 seeds** |

Without injected read corruption, a correct protocol never leaves a torn file in `obj/`, so the CRC check never has anything to reject, and deleting it is unobservable. The test `TestCorruptObjectUnreachableWithoutBitFlips` (`sim_test.go:124-133`) pins down the first half of that statement for the correct server: with `ReadCorruptProb = 0`, 200 seeds produce zero `CorruptObject` facts. This is the "dead safety net" problem: a defensive check that no fault ever exercises is indistinguishable from no check at all. The fault model has to be rich enough to *reach* every defensive branch; otherwise mutation testing (Chapter 15) will tell you, correctly, that the branch is untested.

## 12.8 Coverage: did the workload actually exercise the fault model?

A passing run proves nothing if the interesting things never happened. If a refactor accidentally set `CrashProb` to zero, every seed would pass, faster than before, and nobody would notice. `TestCoverage` guards against that:

```go
// toys3/sim/sim_test.go:86-118
func TestCoverage(t *testing.T) {
	var agg Stats
	for seed := uint64(1); seed <= 200; seed++ {
		agg.Add(Run(DefaultConfig(seed)).Stats)
	}
	t.Log(agg)
	checks := []struct {
		name string
		n    int
	}{
		{"crashes", agg.Crashes},
		{"crashes mid-PUT/COPY", agg.CrashesMidPut},
		{"indeterminate (abandoned) requests", agg.Indetermined},
		{"torn files", agg.TornFiles},
		{"lost un-synced changes", agg.LostChanges},
		{"kept un-synced changes", agg.KeptChanges},
		{"EIOs", agg.EIOs},
		{"read corruptions", agg.ReadCorruptions},
		{"200 replies", agg.ByStatus[0]},
		{"400 replies", agg.ByStatus[1]},
		{"404 replies", agg.ByStatus[2]},
		{"500 replies", agg.ByStatus[3]},
		{"Halts", agg.Halts},
		{"CorruptObject facts", agg.CorruptObjects},
		{"temps removed by recovery", agg.TempsRemoved},
		{"linearizability windows", agg.Windows},
	}
	for _, c := range checks {
		if c.n == 0 {
			t.Errorf("coverage: no %s in 200 seeds", c.name)
		}
	}
}
```

This is the toys3 equivalent of FoundationDB's and Antithesis's `Sometimes` assertions (`docs/lessons.md` §11): a property that must hold *at least once* across a campaign. It is a weak check (non-zero, not "enough"), but it catches the catastrophic regressions: a fault family switched off, an op kind never issued, a recovery path never taken.

Two observations from the 200-seed aggregate in the preface are worth decoding:

```
readcorrupt=71 corruptobj=70
```

71 reads were corrupted, but only 70 `CorruptObject` facts were emitted. The missing one is not a bug. A read is corrupted when it *lands* in the FakeFS (`fakefs.go:388-392`); the `CorruptObject` fact is emitted when the completion is *delivered* to the server. A crash between landing and delivery drops the completion (`Queue.DropIncarnation`), so the server never sees the corrupted bytes. That is the "it happened, but nobody was told" gap from Chapter 10, visible in two aggregate counters.

```
tempsrm=1006 halts=673 crashes=1691 midput=1473
```

Recovery removed 1,006 temp files over 1,691 crashes. Most crashes mid-PUT leave a temp file behind (it was created by the `FSWrite` and never renamed), and the coverage check `temps removed by recovery` proves that recovery's unlink loop actually runs.

What `TestCoverage` does *not* check is just as informative. There is no entry for `widened` (in the default campaign the oracle never needs to widen; `widened=0` in every stats line above) and none for `Writebacks` (the FakeFS counts them in `FSCounters.Writebacks`, but `Stats` does not surface it). Adding a coverage line is cheap and is an exercise below.

## 12.9 What the workload does not do

For completeness, the things this workload leaves untested, each of which could be an extension (Chapter 17):

- **Large values and the size limit.** Values are at most 40 bytes; the 64-byte server limit is never hit in simulation.
- **Key escaping.** All keys are `k<n>`; the `%XX` escaping path and the 255-byte name limit are tested only by unit and production tests.
- **Client retries.** A client whose request was abandoned or failed just moves on to a new random request. Real S3 clients retry, and retries interact with idempotency (`docs/lessons.md` §1). A retrying client would also need the oracle to understand that two invocations may be the same logical operation.
- **Reads of values written in a previous run.** Every run starts from an empty disk. Upgrade and format-compatibility testing would need a pre-populated FakeFS.
- **Timeouts.** Simulated clients wait forever for a reply (until a crash). The production `Submit` honours context cancellation (`prod/loop.go:121-154`), which the simulator never exercises.

## Key takeaways

- The workload is a deterministic, closed-loop set of clients, each with at most one request outstanding, drawing every choice from the run's single `Rand`.
- **Self-describing values** (8-byte writer id, 4-byte length, Fold-derived filler) make every returned byte attributable to exactly one PUT and make any torn, truncated or spliced value detectable outside the server under test. COPY preserves the writer id, so the oracle's model can be a map from key to writer.
- **Quiescent points** split the history into small windows for the linearizability search. They are forced every `QuiesceEvery`, and every crash is one too. `maxwin` tells you how big the windows got.
- **Swarm testing** varies keys, clients, crash rate and EIO rate per seed from a separate RNG stream, so that bugs needing opposite configurations both get found. For mutant f, swarm alone took the first catch from seed 429 to seed 24.
- A fault model that never reaches a defensive branch makes that branch untestable (mutant e without read corruption: not caught in 1,000 seeds). `TestCoverage` is the guard that the workload actually exercises the fault model.

## Exercises

1. **Inspect the swarm shape.** You cannot compute `DefaultConfig(7)`'s key count by hand (it needs SplitMix64), so write a five-line test in `toys3/sim` that prints `DefaultConfig(seed).Workload.Keys` for seeds 1 to 20, and check the histogram against the 1/8, 1/4 weights in §12.6. Do 20 samples look uniform? How many would you need?
2. **Kill the swarm.** In a scratch copy of `TestCatchesBugs`, replace `DefaultConfig` with `BaseConfig` in `firstCatch` (`mutants_test.go:347`). Predict, for each of the seven mutants, whether the first-catch seed goes up, down, or stays the same. Run it (`go test ./toys3/sim -run TestCatchesBugs -v`) and compare. Revert afterwards.
3. **Make windows explode.** Set `QuiesceEvery: 0` in `BaseConfig` (in a scratch branch) and run `go test ./toys3/sim -run TestSeeds -seed=1 -v`. Before running, predict: will the oracle still check anything? (Hint: crashes are quiescent points, and so is the end of the run via `CheckFinal`.) Then try `Clients: 16`. Watch `maxwin` and wall time, and look for the `checker budget exceeded` message.
4. **Detect a spliced value.** Add a test to `toys3/sim` that builds `a := MakeValue(5, 20)` and `b := MakeValue(6, 20)`, splices `a[:12]` with `b[12:]`, and asserts `ValueWriter` rejects it. Then construct the nastiest splice you can think of that `ValueWriter` *accepts* but which is not a value any PUT wrote. (You should fail; explain why.)
5. **Cover the oversize branch.** The server's `MaxObjectSize` check is never reached in simulation (§12.2). Change `MaxValueSize` to 80 in `BaseConfig` and predict what happens to the `400=` count and to the oracle. Does the oracle handle a 400 PUT correctly? (Read `oracle.go:311-313`.) Run 50 seeds to confirm.
6. **Add a coverage line.** Surface `FSCounters.Writebacks` in `Stats` (it is already counted in `fakefs.go:220`) and add a `{"writebacks", ...}` entry to `TestCoverage`. Then run the mutant-f ablation with `WritebackProb = 0` and confirm your new coverage check would have flagged that configuration.
7. **Swarm a new knob.** Add `TornWriteProb` to the swarm (menu: `0, 0.3, 0.3, 0.9`). Re-run `TestCatchesBugs`. Does mutant b get caught earlier or later? Explain using what b needs (Chapter 15).

## Further reading

- Alex Groce, Chaoqiang Zhang, Eric Eide, Yang Chen, John Regehr, *"Swarm Testing"*, ISSTA 2012. The source of the idea and of the "feature suppression" argument.
- `docs/lessons.md` §9 (swarm testing and correlated faults) and §11 (`Sometimes` assertions) in this repository.
- Phillip B. Gibbons and Ephraim Korach, *"Testing Shared Memories"*, SIAM Journal on Computing, 1997. The NP-completeness of checking linearizability (sequential consistency of a history), which is why windows matter.
- Kyle Kingsbury, the Jepsen analyses (https://jepsen.io): Jepsen workloads also use unique values per write for the same reason as `MakeValue`.
