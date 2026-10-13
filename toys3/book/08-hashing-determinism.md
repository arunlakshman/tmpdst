# Chapter 8: State Hashing and Determinism

> *"A deterministic simulator that is not checked for determinism is a random simulator with a very good reputation."*

Chapter 1 claimed that a toys3 run is a pure function of its seed. That claim is the foundation of everything: replaying a failing seed, adding logging to a failing run without changing it, bisecting a regression against a fixed seed. This chapter is about how that claim is *made true* and how it is *checked*, continuously, by machines rather than by good intentions.

There are two complementary defences:

1. **Prevention (static).** A custom analyzer, `tools/detcheck`, refuses to compile-check any deterministic package that imports `time`, uses a goroutine, ranges over a map, or declares mutable global state. It works on the source code before anything runs.
2. **Detection (dynamic).** The simulator folds the entire observable state of the run into a running 64-bit **trace hash** after every step. Running the same seed twice must produce the same trace hash, the same number of random draws, and the same number of steps. If not, nondeterminism leaked in somewhere, and the canary test fails.

Neither defence is sufficient alone. Static analysis cannot see that you iterate a slice whose order came from a map three calls away, or that you hashed a pointer. Dynamic detection can only catch nondeterminism that actually manifests within the seeds you run. Together they make leaks rare and, when they happen, loud. Along the way we will look at two real bugs in the *tooling itself* that were found and fixed while toys3 was built; they are among the most instructive things in the repository.

---

## 8.1 Where nondeterminism comes from in Go

Before looking at the defences, enumerate the enemy. Each item below is a way a Go program can behave differently on two runs with identical inputs. The list is the "Go-specific hazards" of `docs/lessons.md` (lesson 12), made concrete.

| Hazard | Why it is nondeterministic | toys3's answer |
|---|---|---|
| `time.Now()`, `time.Since`, timers | Reads a real clock | Time arrives on `Event.At`; `time` is not on the allow-list |
| `math/rand`, `crypto/rand` | Global or OS-seeded state | One `sim.Rand` seeded from the run's seed; injected via `control.Env` |
| `go` statements | The scheduler picks interleavings | Banned by detcheck in `control`/`sim` |
| channels and `select` | `select` picks uniformly at random among ready cases | Banned by detcheck |
| `for k := range m` over a map | The Go runtime *deliberately* randomizes map iteration order | Banned by detcheck unless suppressed with a reason; sort keys first |
| `reflect.Value.MapKeys` / `MapRange` | Same randomized order, via reflection | Banned by detcheck |
| package-level `var` | State leaks between runs in one process (and between parallel runs) | Banned by detcheck, except error sentinels and `_` |
| `os`, `net`, `syscall` | Real I/O | Not on the allow-list |
| `sync`, `sync/atomic` | Only meaningful with concurrency; a symptom | Not on the allow-list |
| pointer values, `%p`, addresses | Allocation addresses vary | Never hash or print pointers; hash content |
| `hash/maphash` | Seeded randomly per process | Not on the allow-list |
| `maps` package iterators | Iterate in map order | Not on the allow-list |

The map-iteration item deserves emphasis because it is the one that bites experienced Go programmers. The Go specification says the iteration order over maps "is not specified and is not guaranteed to be the same from one iteration to the next", and the runtime goes further: it picks a random starting bucket and offset on every `range`, precisely so that programs do not come to depend on an order. In ordinary code this is a kindness. In a simulator it is a landmine: a recovery routine that unlinks temp files in map order would issue its `FSUnlink` actions in a different order on each run, the IOIDs would be assigned differently, the queue's tie-breaks would be drawn in a different order, and the run would diverge within a handful of steps.

toys3's controller avoids maps entirely. The comment on `Controller` says so explicitly:

```go
// toys3/control/controller.go:77-86
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
```

The simulator does use maps (`StorageModel.dirs`, `StorageModel.inodes`, `Oracle.open`, the WGL memo tables), but it only ever *looks things up* in them, or iterates over keys that it sorts first. We will see the two places where that needs a suppression comment in §8.7.

## 8.2 The server's StateHash

The first ingredient of the trace hash is the server's own summary of its state. Every `control.Machine` must implement `Hashable`:

```go
// toys3/control/machine.go:9-20
// Hashable lets the simulator fold state into a running trace hash after every
// step. The same seed must produce the same hash sequence; a divergence means
// nondeterminism leaked in.
type Hashable interface {
	StateHash() uint64
}

// Machine is what a runtime drives: a hashable state machine.
type Machine interface {
	StateMachine
	Hashable
}
```

Making the hash part of the `Machine` contract (rather than something the simulator digs out with reflection) has two benefits. It forces the author of a state machine to think about *what its state is*, which is a useful design discipline. And it lets mutants wrap the real server and delegate (`func (m *skipper) StateHash() uint64 { return m.inner.StateHash() }`, `toys3/sim/mutants_test.go:77`).

Here is the real implementation:

```go
// toys3/control/hash.go:5-32
// StateHash summarizes the server's entire state (FNV-1a over a fixed
// field order; reqs and recIO are slices, so the order is deterministic).
func (s *Controller) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(s.mode))
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
```

Walk through the design decisions line by line.

**A fixed field order.** The fields are hashed in the order they are written, every time. Compare that with `fmt.Sprintf("%v", s)` (which would happily print pointer addresses for `reqs []*request`) or with a reflection-based deep hash (which would need care with maps and pointers). Hand-written is boring, and boring is correct.

**Every field that affects behaviour, and nothing else.** Compare the list with the `Controller` struct at `toys3/control/controller.go:87-101`: `mode`, `nextIO`, `nextSeq`, `rec`, `recIO`, `recRemoved`, `reqs` are all hashed. The two fields that are *not* hashed are `env` and `cfg`. `cfg` is constant for the whole run (and is the same for every incarnation), so it adds no information. `env` is an interface holding a pointer to the simulator's random stream; its state is covered separately by `Result.RandCalls` (§8.5). Hashing it here would mean hashing a pointer, which is exactly the kind of address-dependent value a deterministic hash must avoid.

**Lengths before sequences.** Both slices are prefixed with their length (`h.u64(uint64(len(s.recIO)))`, `h.u64(uint64(len(s.reqs)))`), and every string is length-prefixed by `str`:

```go
// toys3/control/hash.go:51-56
func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}
```

Without length prefixes, the encoding is ambiguous: keys `"ab"` + src `"c"` and keys `"a"` + src `"bc"` would feed the same bytes into the hash. A length-prefixed encoding is *injective*: distinct states produce distinct byte streams, so any collision is a genuine hash collision (probability around 2⁻⁶⁴ per comparison) rather than a structural one.

**Checksum of payload, not the payload.** `r.data` (a PUT payload, or a COPY's source payload once read) is hashed as its length plus its CRC-32, not byte by byte. That is a performance choice: payloads are up to 64 bytes in the default simulation (`MaxObjectSize: 64`, `toys3/sim/config.go:173`) but could be megabytes in production, and the server's hash runs after every single step. The CRC is cheap and almost certainly distinguishes any two payloads that a run would produce. The cost is a theoretical blind spot: two different payloads with the same length and CRC would hash the same. For a nondeterminism detector this is an acceptable trade; nondeterminism almost never manifests as "the same everything except a CRC-colliding payload".

**Enum values, not enum names.** `s.mode`, `s.rec`, `r.kind` and `r.step` are hashed as their numeric values. Renaming a constant (`phaseServing` to `modeAuto`) changes nothing; inserting a new constant in the middle of an `iota` block shifts every value after it and changes every trace hash. That is why each of these enums now carries a comment such as "The numeric values are folded into StateHash: append new modes, never reorder them" (`toys3/control/controller.go:13-15`). It is the same rule as for any serialization format: names are for humans, numbers are the wire format.

**Slices, therefore deterministic order.** `reqs` is ordered by arrival sequence; `recIO` is ordered by issue. Neither depends on anything but the event history. If either were a map, the hash would need to sort keys first or it would be different on every call, even within one run.

**FNV-1a by hand.** Go's standard library has `hash/fnv`. The server does not use it:

```go
// toys3/control/hash.go:34-49
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
```

`14695981039346656037` is the 64-bit FNV offset basis and `1099511628211` the 64-bit FNV prime. FNV-1a XORs the byte in and then multiplies (FNV-1 multiplies first). `u64` feeds the eight bytes little-endian. Why not just allow-list `hash/fnv`? It is almost certainly deterministic. But the allow-list is *fail closed* (§8.6): every entry is a promise that somebody audited the package, and auditing a package to save four lines is a bad trade. This is the allow-list working as intended: it makes you ask "do I really need this import?" every time.

### The test

`TestStateHash` (`toys3/control/controller_test.go:434-470`) checks three properties:

```go
// toys3/control/controller_test.go:451-459
	a, b := run(), run()
	if !slices.Equal(a, b) {
		t.Fatalf("equal histories, different hashes:\n%x\n%x", a, b)
	}
	for i := 1; i < len(a); i++ {
		if a[i] == a[i-1] {
			t.Fatalf("hash did not change at step %d", i)
		}
	}
```

1. **Determinism:** two identical histories (boot, PUT, GET, drain) produce identical hash sequences.
2. **Sensitivity:** every step of that history changes the hash. A hash that does not change when the state changes is useless as a divergence detector; this catches "forgot to hash a field" for the fields this history exercises.
3. **Payload coverage:** two PUTs that differ only in payload (`"v"` vs `"w"`) must hash differently (`toys3/control/controller_test.go:461-469`). This is the test that justifies the `Checksum(r.data)` line.

Note what the test cannot catch: a field that changes only in a path the test history does not exercise. If you add a field to `Controller`, adding it to `StateHash` is a manual obligation. Exercise 2 asks you to break this deliberately.

## 8.3 The simulator's hash toolkit

The simulator has its own small set of hash functions in `toys3/sim/hash.go`:

```go
// toys3/sim/hash.go:3-11
// Fold mixes x into the running trace hash h. The simulator folds the step
// number, the event time and the server's StateHash after every step (and the
// storage model's hash), so two runs of one seed agree only if their entire
// histories agree.
func Fold(h, x uint64) uint64 {
	h ^= x + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h *= 0xff51afd7ed558ccd
	return h ^ (h >> 33)
}
```

`Fold` is a sequential combiner. Its first line is the well-known `hash_combine` shape from Boost (XOR in the value plus the golden-ratio constant `0x9e3779b97f4a7c15` plus shifted copies of the accumulator). Its second and third lines are one round of the MurmurHash3 64-bit finalizer (multiply by `0xff51afd7ed558ccd`, xorshift by 33), which spreads the bits so that small changes in `x` affect all 64 output bits. The important properties are:

- **Order-sensitive.** `Fold(Fold(h, a), b) ≠ Fold(Fold(h, b), a)` in general. That is what we want: the *sequence* of events matters, not just the multiset.
- **Cheap.** A handful of arithmetic instructions. It runs many times per step.
- **Not cryptographic.** It only needs to make accidental collisions improbable, not resist an adversary.

`Fold` is also used outside hashing: `MakeValue` uses it as a pseudorandom byte generator for the filler bytes of self-describing values (`toys3/sim/value.go:26-30`, Chapter 12).

The two other helpers are FNV-1a over strings and over byte slices:

```go
// toys3/sim/hash.go:13-31
func hashString(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// hashBytes is 64-bit FNV-1a of b, length-prefixed.
func hashBytes(b []byte) uint64 {
	h := uint64(14695981039346656037) ^ uint64(len(b))
	...
}
```

`hashBytes` mixes the length into the starting state so that `nil` and `[]byte{}` and short prefixes are distinguished from one another in a way that would otherwise only depend on the content. (`nil` and `[]byte{}` both have length 0 and hash identically; that is fine, because the simulator treats them identically too.)

### Hashing events and actions

`toys3/sim/trace.go:14-83` defines `hashOp`, `hashEvent` and `hashAction`. They have the same shape: a type switch over the closed set of bodies, a distinct small integer tag per type, and then every field:

```go
// toys3/sim/trace.go:29-56 (abridged)
func hashEvent(h uint64, b control.EventBody) uint64 {
	switch e := b.(type) {
	case control.Started:
		return Fold(h, 10)
	case control.Request:
		return hashOp(Fold(Fold(h, 11), uint64(e.Client)), e.Op)
	case control.FSWriteDone:
		return Fold(Fold(Fold(h, 12), uint64(e.IO)), uint64(e.Err))
	...
	case control.FSReadDone:
		return Fold(Fold(Fold(Fold(h, 16), uint64(e.IO)), uint64(e.Err)), hashBytes(e.Data))
	...
	case control.FSListDone:
		h = Fold(Fold(Fold(h, 18), uint64(e.IO)), uint64(e.Err))
		for _, n := range e.Names {
			h = Fold(h, hashString(n))
		}
		return h
	}
	return Fold(h, 19)
}
```

The tags (10..19 for events, 30..40 for actions, 1..5 for ops) make the encoding unambiguous across types: an `FSWriteDone{IO: 3, Err: 0}` and an `FSFsyncDone{IO: 3, Err: 0}` hash differently. This is possible only because Events and Actions are **plain data** (`loop.md` rule 3; Chapter 2): every field is an integer, a string, a byte slice or a slice of strings. If an action carried a callback or a channel, there would be nothing meaningful to hash.

One action is hashed differently: `Emit` folds in a *string*, `hashString(telemetryHashKey(x.Telemetry))` (`toys3/sim/trace.go:79-80`), and that string is a rendering of the telemetry value (`Committed{c6 "k2" crc=9b35c638}`, and so on). `fmt.Sprintf` with `%q` and `%08x` is deterministic, so this is safe for determinism. It is not safe for *renaming*: in the first edition of the code the string was the log rendering itself (`DescribeFact`), so changing how the log printed a telemetry value, or renaming the type it printed, would have changed every trace hash from the first such value onwards. `telemetryHashKey` is a frozen copy of that rendering, deliberately kept apart from the log's `DescribeTelemetry`. Why it exists, and the day it earned its keep, is the story of §8.12.

### Hashing the storage model

The StorageModel hashes its full state, both the durable and the volatile versions:

```go
// toys3/sim/storagemodel.go:617-652
func (fs *StorageModel) Hash() uint64 {
	h := uint64(fs.nextIno)
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		h = Fold(h, hashString(dn))
		for _, name := range d.names() {
			e := d.entries[name]
			h = Fold(h, hashString(name))
			h = Fold(h, uint64(e.cur))
			h = Fold(h, uint64(e.durable))
			h = fs.hashInode(h, e.cur)
			if e.durable != e.cur {
				h = fs.hashInode(h, e.durable)
			}
		}
	}
	return h
}

func (fs *StorageModel) hashInode(h uint64, id InodeID) uint64 {
	in, ok := fs.inodes[id]
	if !ok {
		return Fold(h, 0)
	}
	h = Fold(h, uint64(len(in.durable))<<32|uint64(format.Checksum(in.durable)))
	h = Fold(h, uint64(len(in.volatile))<<32|uint64(format.Checksum(in.volatile)))
	var flags uint64
	if in.dirty {
		flags |= 1
	}
	if in.poisoned {
		flags |= 2
	}
	return Fold(h, flags)
}
```

Notice the two ordered iterations: `fs.dirNames()` returns the directory list that was sorted once at construction (`toys3/sim/storagemodel.go:140-141`, `457`), and `d.names()` returns the entry names sorted (`toys3/sim/storagemodel.go:444-454`). `fs.dirs` and `d.entries` are maps, but they are only *indexed* here, never ranged over. Contents are again summarized as `len<<32 | crc32`, with the same reasoning as the server. The `dirty` and `poisoned` flags are included because they determine future behaviour (whether a crash may lose the content, whether fsync lies; Chapter 11) even when the bytes are identical.

Inodes that have no name are not hashed directly. They matter only if some entry refers to them, and those are reached through the entries.

## 8.4 What the trace hash covers, and when

The trace hash is `Simulator.trace`, initialised to the seed (`trace: cfg.Seed`, `toys3/sim/simulator.go:63`). It is updated at these points, and only these points:

```
toys3/sim/simulator.go
  Run, every step (line 95):  trace = Fold(Fold(Fold(Fold(trace, step), now), kind), inc)
  Run, every step (line 97):  trace = Fold(trace, storage.Hash())     after handle(e)

  deliver (line 173):          trace = hashEvent(trace, body)          the event delivered
  deliver (line 182):          trace = hashAction(trace, a)            each action returned
  deliver (line 184):          trace = Fold(trace, server.StateHash()) server state after Step
  fs-land (line 140):          trace = hashEvent(Fold(trace, 1), body) the completion produced
  wake    (line 252):          trace = hashEvent(Fold(trace, 2), req)  the request issued
  crash   (line 300):          trace = Fold(...Fold(trace, 3), kept), lost), torn)
```

Read as a timeline for a single `KindDeliver` step:

```
 pop entry e ─► trace ⊕= (step, now, kind, inc)
             ─► handle(e) = deliver(body):
                   acts := server.Step(Event{now, body})
                   trace ⊕= hash(body)
                   trace ⊕= hash(a) for each a in acts
                   trace ⊕= server.StateHash()
                   execute acts (FS plans scheduled, replies recorded, maybe crash)
             ─► trace ⊕= fs.Hash()
             ─► invariants
```

The small tags `1`, `2`, `3` in fs-land, wake and crash keep those contributions from being confused with each other or with a delivery. The crash fold includes the `CrashReport` counts (how many un-synced changes were kept, lost and torn), which captures the adversary's resolution choices even before the StorageModel hash at the end of the step captures their *result*.

What is deliberately **not** in the trace hash:

- **The random stream.** Not directly. It is compared separately via `RandCalls` (§8.5). Indirectly, of course, nearly every draw influences something that *is* hashed.
- **Invariant and oracle state.** The oracle and invariants are observers. They do not influence the run, except that a failure stops it, and `Steps` catches that.
- **The log.** `Config.Log` only records; `TestDeterminismCanary` proves it does not change the trace (§8.5).

### Why hash after *every* step?

You could hash only the final state. Two runs that diverge in the middle and reconverge (it happens; for example, two same-instant events processed in a different order that happen to commute) would then compare equal. That might sound harmless, but it means the nondeterminism is still there, waiting for a seed where the events do not commute. Folding every step means the trace hash is a fingerprint of the **whole history**, not of where it ended up. Two runs agree on the trace hash only if they agree on every step, every event, every action and every intermediate state, up to hash collisions.

The cost is real: `fs.Hash()` walks the whole filesystem every step. With at most a few dozen files, that is cheap enough. For a bigger system you would hash incrementally (maintain the hash as the state changes) or hash every *N* steps; the trade-off is how precisely you can localize a divergence.

### Localizing a divergence

The trace hash tells you *that* two runs diverged, not *where*. When the canary fails, the practical workflow is:

1. Run the seed twice with `cfg.Log = true` (which is what `-seed=N` does, `toys3/sim/sim_test.go:47-51`) and diff the two `FormatLog` outputs. The first differing line is the step where the divergence became visible.
2. If the logs are identical but the hashes differ, the divergence is in something hashed but not logged: compare `RandCalls`, then add a temporary per-step dump of `server.StateHash()` and `fs.Hash()`.
3. Look at what code ran on that step. The usual suspects, in order: a map iteration, a goroutine or channel in some helper, a global variable written by an earlier seed in the same process, a pointer or address being hashed or compared.

## 8.5 The determinism canary

The dynamic check is `TestDeterminismCanary`:

```go
// toys3/sim/sim_test.go:21-42
// TestDeterminismCanary runs several seeds twice each: any difference in
// the trace hash, the number of random draws or the number of steps means
// nondeterminism leaked into the simulator or the server.
func TestDeterminismCanary(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		a := Run(DefaultConfig(seed))
		b := Run(DefaultConfig(seed))
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: hash %x/%x rand %d/%d steps %d/%d",
				seed, a.TraceHash, b.TraceHash, a.RandCalls, b.RandCalls, a.Steps, b.Steps)
		}
		// Log mode only records; it must not change the run.
		cfg := DefaultConfig(seed)
		cfg.Log = true
		if c := Run(cfg); c.TraceHash != a.TraceHash || len(c.Log) == 0 {
			t.Fatalf("seed %d: Log mode changed the trace (%x vs %x)", seed, c.TraceHash, a.TraceHash)
		}
		if seed > 1 && a.TraceHash == Run(DefaultConfig(seed-1)).TraceHash {
			t.Fatalf("seeds %d and %d produced the same trace", seed, seed-1)
		}
	}
}
```

Four checks per seed, each guarding against a different failure:

1. **Same trace hash twice.** The core property.
2. **Same `RandCalls` and `Steps`.** Redundant in theory (if the trace matches, these almost certainly do), but they are much more informative when the check fails. "rand 5582/5583" says one extra draw happened somewhere; "steps 1628/1629" says one extra event was scheduled. They are also robust against a bug in the hash functions themselves (for example, a `hashEvent` that forgot a case would return the same tag for distinct events, and the hash could falsely agree).
3. **Log mode does not change the trace.** This is subtle and important. Logging code often calls things: `DescribeEvent`, `describePayload`, `ValueWriter`. If any of them drew a random number, or mutated state, turning on the log (which is exactly what you do when replaying a failing seed!) would change the run, and the failure you are trying to debug would vanish. This check guarantees the observer effect is zero. It also checks `len(c.Log) == 0` is false, so a broken log mode cannot pass vacuously.
4. **Adjacent seeds differ.** This guards against the *opposite* bug: a simulator that ignores its seed. If `NewRand` ignored its argument, or `DefaultConfig` overwrote the seed, every seed would produce the same run and the canary would happily pass "determinism" while the campaign explored exactly one universe. Comparing seed *n* with seed *n-1* is a cheap sanity check that the seed actually matters.

`Result` exposes exactly the three numbers the canary compares (`toys3/sim/config.go:46-57`):

```go
type Result struct {
	Seed      uint64
	Steps     int
	EndTime   control.Time
	Crashes   int
	TraceHash uint64 // identical for two runs of one Config, or nondeterminism leaked
	RandCalls uint64
	Failures  []Failure // empty means the run passed
	Stats     Stats
	Log       []LogEntry // only with Config.Log
}
```

### Running seeds in parallel without breaking determinism

The canary runs in one goroutine. The campaign runner, `toys3/cmd/toys3sim`, runs many seeds in parallel on `-workers` goroutines (default `GOMAXPROCS`). Is that safe? It is, because of the *no package-level variables* rule (§8.6): every seed's `sim.Run` creates its own `Rand`, `Queue`, `StorageModel`, `Workload`, `Oracle` and server. Nothing is shared except what the runner itself shares (a seed counter, atomic step counters, and the aggregated `Stats` under a mutex), and none of that is visible to a run.

With `-check-determinism`, the runner replays every `-det-every`th seed (default 10) and compares the same three numbers:

```go
// toys3/cmd/toys3sim/main.go:76-83
				if *checkDet && *detEvery > 0 && seed%*detEvery == 0 {
					r2 := sim.Run(sim.DefaultConfig(seed))
					detRuns.Add(1)
					if r2.TraceHash != r.TraceHash || r2.RandCalls != r.RandCalls || r2.Steps != r.Steps {
						record(failure{seed, fmt.Sprintf("NONDETERMINISM: trace %016x vs %016x, rand %d vs %d, steps %d vs %d",
							r.TraceHash, r2.TraceHash, r.RandCalls, r2.RandCalls, r.Steps, r2.Steps)})
					}
				}
```

This is a stronger check than the canary in one specific way: the two runs of a seed happen at different times, on possibly different goroutines and OS threads, while other seeds are running concurrently and the garbage collector is busy. If any global state had crept in, concurrent seeds would perturb each other and the replay would disagree. The single-threaded canary would never see that.

## 8.6 The random stream: SplitMix64, Lemire, and Calls()

The one source of randomness is `sim.Rand`:

```go
// toys3/sim/rand.go:10-26
type Rand struct {
	state uint64
	calls uint64
}

// NewRand returns a stream seeded with seed.
func NewRand(seed uint64) *Rand { return &Rand{state: seed} }

// Uint64 returns the next 64 random bits.
func (r *Rand) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
```

This is **SplitMix64** (Steele, Lea and Flood, OOPSLA 2014): a 64-bit counter advanced by the golden-ratio increment, passed through a strong bijective mixer. Its state is one word, it is fast, it has excellent statistical quality for simulation purposes, and, crucially, *its algorithm is in this file*. `math/rand` is banned not because it is low quality but because its algorithm and global state are outside our control: a Go release could change the generator behind `math/rand`'s top-level functions (and `math/rand/v2` did choose different algorithms), silently changing every seed's universe. With SplitMix64 written out, seed 132 means the same thing forever.

Bounded integers use Lemire's multiply-shift method with rejection:

```go
// toys3/sim/rand.go:28-43
// Intn returns a uniform value in [0, n) without modulo bias (Lemire).
// It panics if n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("sim: Rand.Intn: n <= 0")
	}
	bound := uint64(n)
	hi, lo := bits.Mul64(r.Uint64(), bound)
	if lo < bound {
		threshold := -bound % bound
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), bound)
		}
	}
	return int(hi)
}
```

The 128-bit product `x * n` of a uniform 64-bit `x` lands in `[0, n·2⁶⁴)`; its high word is a value in `[0, n)`. That would be very slightly biased, because 2⁶⁴ is not generally a multiple of `n`. The low word tells you whether you are in the biased sliver: if `lo < (2⁶⁴ mod n)` (computed as `-bound % bound` in unsigned arithmetic), reject and draw again. Rejection is rare (probability below `n/2⁶⁴`), so `Intn` almost always costs exactly one draw. Note the consequence for determinism: the number of draws an `Intn` consumes is itself a deterministic function of the stream, so even the rare retry is replayed exactly.

The remaining helpers are thin: `Float64` takes the top 53 bits; `Chance(p)` is `p > 0 && Float64() < p` (note the short-circuit: `Chance(0)` consumes **no** draw, which means configuring a fault probability of exactly zero changes the random stream, and therefore the whole run, compared with a tiny nonzero probability); `Between(lo, hi)` returns `lo` without drawing when `hi <= lo`.

### Calls() as a coordinate

The `calls` counter is the cheapest debugging aid in the simulator:

```go
// toys3/sim/rand.go:59-61
// Calls is the number of Uint64 draws so far: a cheap coordinate for
// debugging ("the runs diverged after draw 1234").
func (r *Rand) Calls() uint64 { return r.calls }
```

Every random decision in the run has a coordinate: its draw number. Seed 1's run makes 5,582 draws (the preface's tracer output reported `rand=5582`). If two runs diverge, the first draw at which they differ points straight at the code that made a different decision. `docs/lessons.md` lesson 3 also notes a future use: "multiverse" exploration (reseed after *N* draws and continue from an interesting state) only works if the generator counts its draws from day one.

### The server sees the same stream

The server's `Env` is backed by the same `Rand`:

```go
// toys3/sim/simulator.go:399-408
// env is the control.Env handed to each server incarnation. It draws from the
// run's single Rand, so the server's random choices replay with the seed.
type env struct {
	rng     *Rand
	buggify bool // false during Settling
}

func (e *env) Uint64() uint64          { return e.rng.Uint64() }
func (e *env) Intn(n int) int          { return e.rng.Intn(n) }
func (e *env) Maybe(control.Site) bool { return e.buggify && e.rng.Chance(0.05) }
```

Today the server never calls its `Env` (the test harness even notes "the server does not draw randomness today", `toys3/control/controller_test.go:14`), so this plumbing is latent. It matters the day someone adds a buggify site or a randomized backoff: those draws will automatically be part of the seed's universe. If instead the server used its own generator, its choices would be a second, independent source of randomness, and the simulator could no longer *steer* or replay them.

## 8.7 detcheck: prevention by static analysis

The dynamic canary only catches nondeterminism that shows up in the seeds you run. A map iteration in a recovery path that only runs when there are two or more leftover temp files might not diverge in the 12 canary seeds. The static defence catches the *construct* regardless of whether it manifests.

`tools/detcheck` is a `go/analysis` analyzer run through `singlechecker`, so it behaves like `go vet`: `go run ./tools/detcheck <packages>`. Its package doc lists what it reports:

```go
// tools/detcheck/detcheck.go:1-17
// Command detcheck enforces the determinism rules of docs/lessons.md (lesson 12)
// on the packages that run inside the simulator (core/, sim/), and is reused by
// scripts/audit-dep.sh to audit third-party packages before they are allowed in.
//
// It reports:
//   - imports not on the exact-path allow-list (allow.txt, embedded);
//   - go statements, channel types, sends, receives and select statements;
//   - range over a map (Go randomizes map iteration order);
//   - reflect map iteration (Value.MapKeys / Value.MapRange);
//   - package-level variables (mutable global state leaks between sim runs),
//     except error sentinels and blank (_) assertions.
//
// A finding is suppressed by a comment on the same or the previous line:
//
//	//detcheck:ok <reason>
//
// The reason is mandatory. _test.go files are not checked.
```

### The allow-list: fail closed

Most determinism linters work with a **deny-list**: "do not import `time`, `math/rand`, `os`". detcheck uses an **allow-list** of exact import paths, embedded from `tools/detcheck/allow.txt`:

```
# tools/detcheck/allow.txt (excerpt)
# Exact import paths the deterministic packages (core/, sim/) may import.
# Matching is EXACT. A trailing "/..." also allows every subpackage.
...
# Deliberately absent: time, math/rand, math/rand/v2, crypto/rand, os, os/exec,
# net, net/..., sync, sync/atomic, runtime, unsafe, reflect, maps (iterators in
# random order), hash/maphash, context, io/fs, plugin, syscall.

# standard library
bytes
encoding/binary
errors
fmt
hash/crc32
math
math/bits
slices
sort
strconv
strings
unicode/utf8

# this module
manifester/...

# audited third-party (see docs/dep-audits/)
github.com/twmb/franz-go/pkg/kmsg
```

Decision I in `docs/decisions.md` records the reason: an allow-list **fails closed**. A deny-list has to anticipate every dangerous package, including ones that do not exist yet; an allow-list makes every new import a conscious decision. When someone adds `import "hash/fnv"` to the server, the build does not quietly accept it; detcheck says no, and the author has to either write four lines of FNV (as toys3 did) or audit `hash/fnv` and add it.

Matching is implemented in `allowList.allows`:

```go
// tools/detcheck/detcheck.go:52-67
type allowList struct {
	exact  map[string]bool
	prefix []string
}

func (a allowList) allows(path string) bool {
	if a.exact[path] {
		return true
	}
	for _, p := range a.prefix {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}
```

Note `strings.HasPrefix(path, p+"/")` rather than `strings.HasPrefix(path, p)`: allowing `manifester/...` must not accidentally allow a module called `manifesterevil`. Exactness is the whole point. `manifester/...` allows every package in this module, which is how `toys3/sim` may import `toys3/control`. But note the consequence: `toys3/prod` is in `manifester/...` too, so the allow-list would *permit* `toys3/sim` to import `toys3/prod`. It does not, and if it did, detcheck would still catch it indirectly only if detcheck also ran on `prod` (where `os` and `go` statements would be flagged). The protection there is really the package layering, enforced by code review.

The allow-list can be overridden with `-allow file` (`tools/detcheck/detcheck.go:46-48`); `scripts/audit-dep.sh` relies on the embedded default when auditing a third-party package.

### The banned constructs

The core of `run` is a single `ast.Inspect` walk:

```go
// tools/detcheck/detcheck.go:154-183
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.GoStmt:
				report(n.Pos(), "go statement: goroutine scheduling is nondeterministic")
			case *ast.SelectStmt:
				report(n.Pos(), "select statement: ready cases are chosen at random")
			case *ast.SendStmt:
				report(n.Pos(), "channel send")
			case *ast.UnaryExpr:
				if n.Op == token.ARROW {
					report(n.Pos(), "channel receive")
				}
			case *ast.ChanType:
				report(n.Pos(), "channel type")
			case *ast.RangeStmt:
				switch pass.TypesInfo.TypeOf(n.X).Underlying().(type) {
				case *types.Map:
					report(n.Pos(), "range over map: iteration order is randomized; iterate sorted keys")
				case *types.Chan:
					report(n.Pos(), "range over channel")
				}
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "MapKeys" || sel.Sel.Name == "MapRange") {
					if t := pass.TypesInfo.TypeOf(sel.X); t != nil && t.String() == "reflect.Value" {
						report(n.Pos(), "reflect map iteration: order is randomized")
					}
				}
			}
			return true
		})
```

Three details worth noticing:

- **The range check uses type information**, not syntax. `for k := range m` looks identical whether `m` is a slice or a map; `pass.TypesInfo.TypeOf(n.X).Underlying()` resolves named types (`type dirMap map[string]dirent` is still a map). This is why detcheck is a `go/analysis` pass with full type checking rather than a regex.
- **Channel *types* are banned, not just operations.** A struct field of type `chan int` in the controller is a design smell even if nothing sends on it, because it signals an intent to communicate across goroutines. `loop.md` rule 3 says Events and Actions must not contain channels.
- **`reflect` is not on the allow-list anyway**, so the `MapKeys`/`MapRange` check is defence in depth for audited third-party packages (which might use reflection internally) rather than for our own code.

### Package-level variables

```go
// tools/detcheck/detcheck.go:135-152
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				for _, id := range spec.(*ast.ValueSpec).Names {
					if id.Name == "_" {
						continue
					}
					obj := pass.TypesInfo.Defs[id]
					if obj != nil && types.Implements(obj.Type(), errorIface) {
						continue
					}
					report(id.Pos(), "package-level variable %s: mutable global state leaks between simulation runs", id.Name)
				}
			}
		}
```

Only top-level `var` declarations are inspected (`f.Decls`, not nested blocks). Two exemptions:

- **`_`**: `var _ control.Env = (*env)(nil)` is a compile-time interface assertion. It has no storage. toys3 uses this pattern in several places (`toys3/sim/simulator.go:397`, `409`; `toys3/control/machine.go:30`).
- **Anything implementing `error`**: `var ErrBad = errors.New("bad")` is the idiomatic sentinel. Technically it is mutable (someone could reassign it), but in practice sentinels are never written, and banning them would fight the language. The check uses `types.Implements` against the universe `error` interface, so custom error types also qualify.

Why are globals so dangerous in a simulator? Because a test binary runs many seeds in one process. A global cache, counter or "last seen" variable written by seed 41 is still there when seed 42 starts, so seed 42's behaviour depends on whether seed 41 ran before it in the same process. That is nondeterminism across invocations (`-seed=42` alone vs. as part of a sweep) and, with the parallel campaign runner, nondeterminism across goroutine scheduling. The canary would catch some of this; detcheck catches all of it statically.

### Suppressions with mandatory reasons

Sometimes a flagged construct is fine. The escape hatch is a comment `//detcheck:ok <reason>` on the same line or the line before:

```go
// tools/detcheck/detcheck.go:111-126
		sup := suppressions(pass, f)
		report := func(pos token.Pos, format string, args ...any) {
			line := pass.Fset.Position(pos).Line
			if reason, ok := sup[line]; ok && reason != "" {
				return
			}
			if reason, ok := sup[line-1]; ok && reason != "" {
				return
			}
			pass.Reportf(pos, format, args...)
		}
		for line, reason := range sup { // order irrelevant: the driver sorts diagnostics
			if reason == "" {
				pass.Reportf(pass.Fset.File(f.Pos()).LineStart(line), "detcheck:ok needs a reason")
			}
		}
```

A suppression *without* a reason does not suppress anything, and is itself reported. This forces the reasoning into the code where a reviewer will see it. (Note, with some amusement, that the detcheck source itself ranges over a map, `for line, reason := range sup`, with a comment explaining why the order is irrelevant. detcheck does not check itself; it is a tool, not a deterministic package.)

toys3 has exactly two suppressions, both in the StorageModel:

```go
// toys3/sim/storagemodel.go:444-454
func (d *fdir) names() []string {
	if d.sorted == nil {
		names := make([]string, 0, len(d.entries))
		for name := range d.entries { //detcheck:ok keys are sorted before use
			names = append(names, name)
		}
		slices.Sort(names)
		d.sorted = names
	}
	return d.sorted
}
```

This is the canonical safe map iteration: collect keys, sort them, use the sorted slice. The randomized order never escapes the function.

```go
// toys3/sim/storagemodel.go:519-529
	// 3. Forget inodes without a name.
	var dead []InodeID
	for id := range fs.inodes { //detcheck:ok only collects ids; deletion order is irrelevant
		if _, found := slices.BinarySearch(live, id); !found {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(fs.inodes, id)
	}
```

Here the order *does* escape the loop (into `dead`), but the only use of `dead` is to delete map entries, and the final state of the map does not depend on the order of deletions. No random draws happen in the loop. The reason comment says exactly that. A reviewer's job with a suppression is to check that the reason is true; if someone later added `fs.rng.Intn(...)` inside this loop, the reason would become false and the run would become nondeterministic, and only the canary would notice.

### What `_test.go` exemption means

```go
// tools/detcheck/detcheck.go:106-110
	for _, f := range pass.Files {
		name := pass.Fset.File(f.Pos()).Name()
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
```

Tests are allowed to use `time`, `flag`, `os` and `testing`. `toys3/sim/sim_test.go` imports `time` for `-duration` and `os` for printing the log to stderr. That is safe because tests orchestrate runs; they are not part of a run. The risk is that a test helper that *is* part of a run (for example, a mutant's `Step` in `mutants_test.go`) escapes the check. The mutants are written carefully to use only allow-listed constructs, but nothing enforces it.

### The analysistest testdata

detcheck is itself tested with the standard `analysistest` harness:

```go
// tools/detcheck/detcheck_test.go:9-11
func TestDetcheck(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), Analyzer, "a")
}
```

`analysistest` loads `testdata/src/a/a.go` and checks that every diagnostic matches a `// want` comment on the same line, and that every `// want` is matched:

```go
// tools/detcheck/testdata/src/a/a.go:3-36
import (
	"errors"
	"sort"
	"time" // want `import "time" is not on the deterministic allow-list`
)

var ErrBad = errors.New("bad")

var _ = sort.Ints

var counter int // want `package-level variable counter`

var cache = map[string]int{} //detcheck:ok read-only lookup table, never written

func Bad(m map[string]int, ch chan int) { // want `channel type`
	go func() {}()     // want `go statement`
	ch <- 1            // want `channel send`
	<-ch               // want `channel receive`
	select {}          // want `select statement`
	for k := range m { // want `range over map`
		_ = k
	}
	_ = time.Now()
}

func Good(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	//detcheck:ok collecting keys to sort; order does not escape
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

Read this file as a specification. It covers: a forbidden import, the error-sentinel exemption (`ErrBad`, no `want`), the blank exemption, a global, a same-line suppression (`cache`), a previous-line suppression (in `Good`), and every banned construct. Notice that `_ = time.Now()` has no `want`: detcheck flags the *import*, not each use, which keeps the diagnostics focused.

### Seeing it work

Running detcheck on all of toys3 (not just the deterministic packages) is a good way to see it in action, because `prod` is supposed to fail:

```
$ go run ./tools/detcheck ./toys3/...
.../toys3/prod/env.go:4:2: import "math/rand/v2" is not on the deterministic allow-list
.../toys3/prod/executor.go:5:2: import "io/fs" is not on the deterministic allow-list
.../toys3/prod/executor.go:6:2: import "log" is not on the deterministic allow-list
.../toys3/prod/executor.go:7:2: import "os" is not on the deterministic allow-list
.../toys3/prod/executor.go:8:2: import "path/filepath" is not on the deterministic allow-list
.../toys3/prod/executor.go:38:2: go statement: goroutine scheduling is nondeterministic
.../toys3/prod/http.go:5:2: import "io" is not on the deterministic allow-list
.../toys3/prod/http.go:6:2: import "net/http" is not on the deterministic allow-list
.../toys3/prod/http.go:7:2: import "net/url" is not on the deterministic allow-list
.../toys3/prod/loop.go:4:2: import "context" is not on the deterministic allow-list
...
```

(output trimmed). Every line is correct: `prod` *should* use `os` and goroutines. That is why the CI script runs detcheck only on `./core/... ./sim/...` plus every toys3 package except `prod` and the commands (today `toys3/control`, `toys3/format` and `toys3/sim`), and why `toys3/prod/doc.go` says the package "is deliberately not checked by detcheck and not run in simulation".

## 8.8 Tooling bug #1: the generated test main

Here is the first of two bugs in the tooling, and it is a great example of a tool interacting with the build system in a way nobody thought about.

`singlechecker` (like `go vet`) analyzes packages *with their tests* by default. When you load a package `p` "with tests", the `go/packages` loader produces up to four packages:

```
p                      the package itself
p [p.test]             p compiled with its in-package _test.go files
p_test [p.test]        the external test package, if any
p.test                 the generated test main: package main, imports "os", "testing",
                       "testing/internal/testdeps", and p [p.test]
```

The last one is a `main` package that the `go` tool *generates* to drive the tests. Its source is synthesized, it does not end in `_test.go`, and it imports `os` and `testing` by construction. So the moment the first deterministic package got a test file, detcheck started reporting violations in code that nobody wrote and nobody could change.

The fix landed in commit `18cbfa8` ("toys3: interfaces for a DST-tested toy S3 server"), whose message says: "Also stop detcheck from analyzing go-generated test mains so that deterministic packages can have tests":

```diff
 func run(pass *analysis.Pass) (any, error) {
+	// The go-tool-generated test main (package path "x.test") imports os and
+	// testing by construction; it is not our code.
+	if strings.HasSuffix(pass.Pkg.Path(), ".test") {
+		return nil, nil
+	}
 	al, err := loadAllow()
```

Three lessons:

1. **Tools that pass on an empty codebase prove nothing.** detcheck was written and tested (with `analysistest`, which loads a single package without a generated main) before the deterministic packages had any tests. The bug was latent until the code it was meant to check started to exist. The first real use of a tool is its real test.
2. **The fix is the narrowest correct one.** An alternative would have been to run detcheck with `-test=false`, skipping test variants entirely. But then in-package `_test.go` files would not be loaded, and more importantly the non-test files of the `[p.test]` variant would not be checked either. Skipping only paths ending in `.test` exempts exactly the generated main, and the per-file `_test.go` skip (above) handles the rest.
3. **Make the exemption explain itself.** The comment says what the package is, why it imports what it imports, and why it is not our code. Without it, a future reader would see a suspicious `return nil, nil` that disables the analyzer for some packages and might "fix" it.

Without that fix, the option you are left with is to suppress or delete the tests of the deterministic packages. A determinism checker that pushes people to not write tests is worse than no checker.

## 8.9 Tooling bug #2: the canary that was silently skipped

The second bug is in `scripts/ci-determinism.sh`, and it is a classic shell trap with a very DST-specific consequence: **the determinism canary was never running in CI, and the CI was green.**

The script's job (`scripts/ci-determinism.sh:1-9`) is to run detcheck, check that every third-party dependency of the deterministic packages has an audit record, and run `TestDeterminismCanary` in every simulator package that has one. Because the top-level `sim/` package has only interfaces and no canary yet, the script first *checks whether the test exists* and skips the package if not. Here is the original version, from commit `011144f`:

```bash
echo "== determinism canary"
if go list ./sim/... 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null | grep -q '^TestDeterminismCanary$'; then
  go test ./sim/... -run '^TestDeterminismCanary$' -count=1
else
  echo "skipped: TestDeterminismCanary not implemented yet"
fi
```

And the script starts with `set -euo pipefail`. Look at the pipeline in the `if` condition:

```
go list ./sim/... | xargs go test -list 'TestDeterminismCanary' | grep -q '^TestDeterminismCanary$'
```

`go test -list` prints the matching test names and then a summary line:

```
$ go test ./toys3/sim -list TestDeterminismCanary
TestDeterminismCanary
ok  	manifester/toys3/sim	0.288s
```

Now follow the processes:

```
  go test -list ────────── pipe ──────────► grep -q '^TestDeterminismCanary$'
     │ writes "TestDeterminismCanary\n"          │ reads it, MATCH
     │                                           │ -q: exit 0 immediately, close pipe
     │ writes "ok  manifester/...\n"             ✗ (gone)
     ▼
  write to a pipe with no reader ──► SIGPIPE ──► go test dies (status 128+13 = 141)
  xargs sees its child killed by a signal ──► xargs exits non-zero
```

Without `pipefail`, a pipeline's exit status is the status of the **last** command, `grep -q`, which is 0: "found". With `pipefail`, the pipeline's status is the status of the rightmost command that failed, which is now the non-zero status from `xargs`. So the pipeline "fails". And because the pipeline is the condition of an `if`, `set -e` does not abort the script; the `if` simply takes the `else` branch, prints `skipped: TestDeterminismCanary not implemented yet`, and the script continues to `echo "determinism gate passed"`.

In other words, **finding the canary caused it to be skipped.** The better the grep did its job (exit early on the first match), the more reliably the canary was disabled.

When this book was written, the old pipeline was reproduced against `toys3/sim`:

```
$ bash -c 'set -o pipefail; go list ./toys3/sim | xargs -r go test -list TestDeterminismCanary 2>/dev/null \
      | grep -q "^TestDeterminismCanary$"; echo "pipeline status=$?"'
pipeline status=1
```

Non-zero, despite the test existing. Note also that the outcome is **racy**: whether `go test` gets SIGPIPE depends on whether it writes its second line before or after `grep` exits and closes the pipe. With a tiny amount of output and a fast grep it nearly always loses, but on a different machine, or with output buffered differently, the same script might sometimes run the canary and sometimes not. A flaky determinism gate. The irony is hard to beat.

The fix, in commit `210c1fa`, captures the output first and greps a variable:

```bash
# scripts/ci-determinism.sh:36-45
echo "== determinism canary"
for simpkg in ./sim/... ./toys3/sim/...; do
  # Capture first: piping into grep -q would SIGPIPE go test under pipefail.
  tests=$(go list "$simpkg" 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null || true)
  if grep -q '^TestDeterminismCanary$' <<<"$tests"; then
    go test "$simpkg" -run '^TestDeterminismCanary$' -count=1
  else
    echo "skipped $simpkg: TestDeterminismCanary not implemented yet"
  fi
done
```

`$(...)` reads the producer's output to completion, so `go test -list` is never cut off. `|| true` makes the capture itself immune to `pipefail` and `set -e` (for packages that fail to list, for instance). Then `grep -q` reads from a here-string, so nobody is writing when it exits early. The loop also extends the gate to `toys3/sim`, which is the package that actually has a canary. The current output:

```
$ scripts/ci-determinism.sh
== detcheck
== dependency audit records
== determinism canary
skipped ./sim/...: TestDeterminismCanary not implemented yet
ok  	manifester/toys3/sim	0.540s
determinism gate passed
```

The general lessons:

- **A check that can be skipped will be skipped.** Any "skip if not applicable" logic in CI is a place where a real check can silently turn into a no-op. The skip message is printed, but nobody reads green CI logs.
- **Test your test infrastructure by breaking the thing it checks.** The fastest way to find this bug would have been to add a deliberate nondeterminism (Exercise 5) and confirm CI goes red. If CI stays green, the gate is broken.
- **`grep -q` + `pipefail` is a known trap.** So is `head -1` + `pipefail`. Capture, then match.

## 8.10 The dependency audit gate

The middle stage of the CI script enforces a process for third-party code:

```bash
# scripts/ci-determinism.sh:22-34
echo "== dependency audit records"
missing=0
while read -r modver; do
  [[ -z $modver ]] && continue
  record="docs/dep-audits/$modver.md"
  if [[ -f $record ]]; then
    echo "ok       $modver"
  else
    echo "MISSING  $record  (run scripts/audit-dep.sh $modver)"
    missing=1
  fi
done < <(go list -deps -f '{{if and .Module (not .Module.Main)}}{{.Module.Path}}@{{.Module.Version}}{{end}}' "${pkgs[@]}" | sort -u)
[[ $missing == 0 ]]
```

For every non-main module in the transitive dependencies of the deterministic packages, a file `docs/dep-audits/<module>@<version>.md` must exist. Including the version in the file name means a version bump forces a new audit. `scripts/audit-dep.sh` does the mechanical part: it downloads the module into a throwaway module, builds detcheck with a compatible toolchain, runs it over the dependency, greps for aliasing/`unsafe`/reflection hints, and prints a reviewer checklist (`scripts/audit-dep.sh:71-81`). The one record present today is for `github.com/twmb/franz-go/pkg/kmsg@v1.14.0`, prepared for the future Kafka engine.

When this book was written the "dependency audit records" section printed nothing at all: `toys3/control`, `toys3/format` and `toys3/sim` depend only on the standard library and this module. That is the ideal state for a deterministic core.

## 8.11 Putting the defences together

```
                    ┌─────────────── static (every build) ───────────────┐
  source code ────► │ detcheck: allow-list imports, no go/chan/select,   │
                    │ no map range, no globals; reasons for suppressions │
                    └────────────────────────────────────────────────────┘
                    ┌─────────────── process (every dep change) ─────────┐
  go.mod ─────────► │ audit record per module@version                    │
                    └────────────────────────────────────────────────────┘
                    ┌─────────────── dynamic (every CI run) ─────────────┐
  seeds 1..12 ────► │ canary: trace, rand calls, steps equal on rerun;   │
                    │ log mode changes nothing; adjacent seeds differ    │
                    └────────────────────────────────────────────────────┘
                    ┌─────────────── dynamic (every campaign) ───────────┐
  every 10th seed ► │ -check-determinism: replay under concurrency       │
                    └────────────────────────────────────────────────────┘
```

Each layer catches what the previous one misses. detcheck cannot see semantic nondeterminism (hashing a pointer, depending on the order of a slice that came from a map in an allow-listed library). The canary cannot see constructs that never execute in its seeds. The campaign replay catches leaks between concurrently running seeds that the single-threaded canary cannot.

## 8.12 The trace hash as a refactoring safety net

Everything so far in this chapter treated the trace hash as a *nondeterminism detector*: run the same seed twice with the same code, compare. This section turns it around. Run the same seed with *different* code, and compare. If the hashes match, the two versions of the code did exactly the same thing: the same events in the same order, the same actions, the same controller state after every step, the same disk contents, the same random draws. That is a far stronger statement than "the tests still pass", and it is what made the domain-driven-design refactor of Chapter 3a possible to do with confidence.

The refactor (commits `9b65410..a4018d5`) touched 51 files, with about 3,800 insertions and 2,000 deletions. It renamed a package, split four files, introduced two interfaces and an anti-corruption layer, and renamed dozens of identifiers, struct fields and printed strings. Its one hard requirement was **no behaviour change**. This section shows how that was proven, commit by commit, and it is a DST lesson in its own right: *the simulator is also your refactoring safety net.*

### What "no behaviour change" means, operationally

A refactor is behaviour-preserving if every observable output is the same. For toys3 the observable outputs of a simulated run are:

- the number of steps, and the virtual time at which the run ended;
- the trace hash (which folds every popped entry, every event, every action, the controller's `StateHash` after every step, and the storage model's hash after every step);
- the number of random draws (`RandCalls`), which pins *where* in the code randomness was consumed;
- the statistics (crashes, trips, torn files, windows checked, ...);
- the failures, and for a mutant, the first seed and step at which it is caught.

If all of those are identical for a large set of seeds, nothing that the simulator can see has changed. Things the simulator cannot see (log text, production log lines, comment wording, the HTTP layer) are checked separately, below.

### Step 0: pin the behaviour before touching anything

The first commit of the refactor, `9b65410`, changed no production code at all. It added a test, `TestGolden`, and its data file, `toys3/sim/testdata/golden-v1.txt`, generated once from the pre-refactor code:

```go
// toys3/sim/golden_test.go:25-39
// statsVector lists every Stats field in a fixed order. When a field is
// renamed, rename it here too, keeping its position.
func statsVector(s Stats) []int {
	return []int{
		s.Requests, s.ByStatus[0], s.ByStatus[1], s.ByStatus[2], s.ByStatus[3],
		s.Indeterminate, s.FSOps, s.EIOs, s.TornFiles, s.LostChanges,
		s.Crashes, s.Trips, s.CrashesMidPut, s.KeptChanges, s.ReadCorruptions,
		s.CorruptionsDetected, s.TempsRemoved, s.Windows, s.MaxWindowOps, s.Widenings,
	}
}

func goldenRow(kind string, seed uint64, r Result) string {
	return fmt.Sprintf("%s seed=%d steps=%d rand=%d trace=%016x crashes=%d end=%d fails=%d stats=%v",
		kind, seed, r.Steps, r.RandCalls, r.TraceHash, r.Crashes, r.EndTime, len(r.Failures), statsVector(r.Stats))
}
```

`goldenText` (`toys3/sim/golden_test.go:41-71`) writes one such row for each of seeds 1..500 under `DefaultConfig`, seeds 1..50 under `BaseConfig`, and then, for each of the seven mutants, the first catching seed, invariant, step and trace hash under both `DefaultInvariants` and `BlackBoxInvariants`, plus the mutant's seed-1 row. That is 571 lines. The first and the last few:

```
default seed=1 steps=1628 rand=5582 trace=1ca97eb77a9044e1 crashes=10 end=371867906 fails=0 stats=[225 154 3 36 7 25 527 10 1 9 10 9 8 12 0 0 7 30 13 0]
...
mutant f-skip-temp-cleanup all caught seed=20 invariant=linearizable step=1750 trace=57679d43aac7df7e
mutant f-skip-temp-cleanup blackbox caught seed=20 invariant=linearizable step=1750 trace=57679d43aac7df7e
mutant-seed1 f-skip-temp-cleanup seed=1 steps=1552 rand=5467 trace=f9a6442332345f03 crashes=10 end=364947495 fails=0 stats=[204 153 3 16 11 21 510 12 0 10 10 9 9 12 0 0 0 30 11 0]
```

`TestGolden` regenerates the text and compares it with the file line by line, failing on the first difference with "behavior changed: ... line N" (`toys3/sim/golden_test.go:73-101`). It runs in about three seconds. The file is regenerated only by passing `-update-golden`, which is reserved for a *deliberate* behaviour change.

Three design choices in that test are worth copying:

1. **Stats as a vector of numbers, not `Stats.String()`.** The refactor was going to rename `Stats.Halts` to `Stats.Trips` and its printed key from `halts=` to `trips=`. If the golden file stored the printed line, that rename would have "changed behaviour" by the test's definition. Storing a fixed-order vector of integers means a field can be renamed in the same commit as its position in `statsVector`, and the golden file does not move. The comment says so: "When a field is renamed, rename it here too, keeping its position."
2. **Mutants are part of the golden.** A mutant's catching seed and step depend on the *interaction* between the controller, the storage model, the oracle and the invariants. If any of them changed behaviour, even in a way that left the correct controller's runs identical, the mutant rows would likely move. The mutants make the golden file sharper.
3. **The hashes the book quotes are in it.** Seed 1's `1ca97eb77a9044e1` and mutant f's `f9a6442332345f03` are rows of the file. If the golden passes, every hash quoted in this book is still true.

### Is the trace hash rename-sensitive?

Before renaming anything, the refactor asked a precise question: *which inputs to the trace hash depend on names?* Go type names never enter the hash; events and actions are folded as numeric tags and field values (§8.3). But "never" deserves checking, input by input:

| Hash input | Where | Sensitive to |
|---|---|---|
| step, time, entry kind, incarnation of each popped entry | `toys3/sim/simulator.go:95` | the **numeric** value of `EntryKind`, not its name |
| event tags 10–19 and their fields | `toys3/sim/trace.go:29-56` | nothing, as long as each type keeps its tag |
| action tags 30–40 and their fields | `toys3/sim/trace.go:58-83` | the same, **except `Emit`** |
| `Emit` → a rendered string | `toys3/sim/trace.go:79-80` | **the text of the rendering**: type names and format |
| `Trip.Cause` | `toys3/sim/trace.go:77-78` | the *text* of the causes (`"fsync temp: EIO"`, ...) |
| paths in actions; directory names in the storage hash | `trace.go`, `storagemodel.go` | the layout (`/obj`, `/tmp`, 16-hex temp names, `%XX` escaping) |
| `StateHash` | `toys3/control/hash.go:7-32` | field order and the **numeric** values of `mode`, `recStep`, `opKind`, `reqStep` |
| storage hash | `toys3/sim/storagemodel.go:617-652` | uses `Checksum`; moving it from `core` to `format` changes nothing |
| `RandCalls` and every later draw | `toys3/sim/rand.go` | **the order of draws**: any "tidy-up" that moves an `rng` call breaks every later decision |

Two rows needed action. The `Trip.Cause` row is easy: the rename from `Halt{Reason}` to `Trip{Cause}` changes a field name, not the strings, and none of the cause strings contains the word "halt". The `Emit` row is the trap.

### Step 1: freeze the telemetry hash key

In the pre-refactor code, `hashAction` folded a telemetry value like this:

```go
	case core.Emit:
		return Fold(Fold(h, 39), hashString(DescribeFact(x.Fact)))
```

`DescribeFact` was the *log renderer*. It produced `CorruptObject{"k3"}` for a corruption. The refactor was going to rename `CorruptObject` to `CorruptionDetected`, and the natural thing to do is to make the log say `CorruptionDetected{"k3"}` too. With the code above, that one cosmetic change would have altered the trace hash of every run from its first corruption onwards: in the 200-seed sweep, every seed with `corruptdet` above zero. The golden test would have failed, correctly, and the refactor would have had to choose between an inconsistent log and a re-baselined golden file (and re-baselined hashes in this book).

The second commit, `db1b837`, removed the trap *before* any rename. It split `describe.go` into two files with different rules: `log.go` (rendering, free to change) and `trace.go` (hashing, frozen), and it gave the hash its own copy of the rendering:

```go
// toys3/sim/trace.go:85-106
// telemetryHashKey is the FROZEN hash encoding of telemetry (trace-hash
// epoch 1). It reproduces, byte for byte, how telemetry (then called facts)
// was rendered when the trace hash was defined, so renaming telemetry types
// or changing DescribeTelemetry never changes a TraceHash. Changing this
// function is a deliberate re-baseline: regenerate testdata/golden-v1.txt and
// update every trace hash quoted in the docs in the same commit.
func telemetryHashKey(t control.Telemetry) string {
	switch x := t.(type) {
	case control.Committed:
		if x.Deleted {
			return fmt.Sprintf("Committed{c%d delete %q}", x.Client, x.Key)
		}
		return fmt.Sprintf("Committed{c%d %q crc=%08x}", x.Client, x.Key, x.Sum)
	case control.Recovered:
		return fmt.Sprintf("Recovered{temps=%d}", x.TempsRemoved)
	case control.CorruptionDetected:
		return fmt.Sprintf("CorruptObject{%q}", x.Key) // legacy spelling, on purpose
	}
	// Unreachable for the current telemetry types (TestTelemetryHashKeyCoversAllTypes).
	// A fixed string, not %T, so no Go type name is ever a hash input.
	return "?"
}
```

(In `db1b837` it was called `factHashKey` and switched on `core.Fact`; commit `91ab309` renamed it along with the types. Its *output* never changed.) Note the comment on the `CorruptionDetected` case: the hash key still spells the old name, on purpose, and the log renderer right next to it in `log.go` spells the new one (`toys3/sim/log.go:112-113`). A test pins the frozen encoding independently of the golden file, so that a well-meaning cleanup of `telemetryHashKey` fails with a message that explains itself:

```go
// toys3/sim/trace_test.go:15-31
// TestTelemetryHashKeyFrozen pins the hash encoding of telemetry. If this fails, a
// change altered every trace hash: see telemetryHashKey.
func TestTelemetryHashKeyFrozen(t *testing.T) {
	for _, c := range []struct {
		f    control.Telemetry
		want string
	}{
		{control.Committed{Client: 7, Key: "k1", Sum: 0xbeef}, `Committed{c7 "k1" crc=0000beef}`},
		{control.Committed{Client: 8, Key: "k2", Deleted: true}, `Committed{c8 delete "k2"}`},
		{control.Recovered{TempsRemoved: 3}, `Recovered{temps=3}`},
		{control.CorruptionDetected{Key: "k3"}, `CorruptObject{"k3"}`},
	} {
		if got := telemetryHashKey(c.f); got != c.want {
			t.Errorf("telemetryHashKey(%#v) = %s, want %s", c.f, got, c.want)
		}
	}
}
```

The lesson generalizes far beyond toys3: **hash inputs must be stable codes, not rendered names.** Every event and action was already hashed as a numeric tag plus fields; telemetry was the one exception, because hashing its description "saved writing a third type switch" (this book's first edition said exactly that, approvingly). The shortcut was harmless for determinism and expensive for evolution. The cleaner fix would be numeric tags for telemetry too (41, 42, 43, ...). The refactor deliberately did *not* do that, because it would change every hash: it is recorded as an optional "epoch 2" for some later commit that announces itself as a re-baseline. Freezing first, renaming second, kept the epoch at 1.

One small residue survived the refactor: the fallback at the end of `telemetryHashKey` was `fmt.Sprintf("%T", t)`, a package-qualified type name (`control.Something`), which *is* rename-sensitive. It was unreachable for the closed set of three telemetry types, so it never contributed to a hash; but it was the one line of the frozen encoding that would not have survived a rename if it were ever reached. Commit `10777a2` replaced it with the fixed string `"?"` (shown above) and added `TestTelemetryHashKeyCoversAllTypes`, which reads every `isTelemetry` receiver from the source of package `control` and fails if one has no explicit case. The golden file did not move, which is the proof that the fallback really was unreachable.

### The commit sequence

Each commit after that was one step of the migration plan in `toys3/REFACTOR-PROPOSAL.md` §7. Each compiled, passed `go vet`, `gofmt`, `go test ./toys3/... ./tools/...` and `scripts/ci-determinism.sh`, and left the golden file byte-for-byte unchanged. Every commit message ends with the same two words: "Golden unchanged." The last two rows are later, smaller commits held to the same rule.

| Commit | Step | What it did | What could have broken the golden, and why it did not |
|---|---|---|---|
| `b234444` | — | The proposal (`toys3/REFACTOR-PROPOSAL.md`). | Documentation only. |
| `9b65410` | 0 | `TestGolden` and `golden-v1.txt`, generated on the old code. | Test only. |
| `db1b837` | 1 | Freeze the telemetry hash key; split `describe.go` into `trace.go` and `log.go`. | The frozen key had to reproduce `DescribeFact` byte for byte; the golden proved it did. |
| `5c5899e` | 2 | The plant port (now the Storage port): FS types move to `plant.go` (now `storage.go`), gain `ID()`, and implement `Actuation`/`Feedback`; five type switches replaced. | The simulator's `deliver` switch changed from seven listed types to `case control.Actuation`. If `Reply`, `Emit` or `Halt` had implemented `Actuation`, an action would have changed branch. None does. |
| `1ef4dcb` | 3 | Extract `toys3/format`; `TmpPath` takes `uint64`; new format tests; CI derives its package list. | A pure move. `TmpPath(root, uint64(io))` formats the same 16 hex digits. |
| `907990e` | 4 | Rename package `core` to `control`. | Mechanical; Go package names never reach the hash. (The `%T` fallbacks would have, had they been reachable.) |
| `401d317` | 5 | Split `server.go` into `controller.go`, `recovery.go`, `hash.go`; `Server` → `Controller`, `phase` → `mode`, `canRun` → `permitted`, `conflicts` → `interlocked`, `startRunnable` → `startPermitted`. | `StateHash` folds the enums numerically: reordering an `iota` would have broken it. None was reordered, and each enum now says "append only". (`phaseHalted` was briefly `modeHalted`, until the next commit.) |
| `020f09c` | 6 | `Halt{Reason}` → `Trip{Cause}`, `halt`/`failHalt` → `trip`/`failTrip`, `modeTripped`, `ErrHalted` → `ErrTripped`, `Stats.Halts` → `Trips`. | The cause strings are hashed; they were kept verbatim. The printed `halts=` became `trips=`, which the golden's numeric vector does not see. |
| `91ab309` | 7 | `Fact` → `Telemetry`, `CorruptObject` → `CorruptionDetected`, `TimedFact` → `Sample`, `View.Facts` → `View.Telemetry`, `DescribeFact` → `DescribeTelemetry`, `CorruptObjects` → `CorruptionsDetected`, `Indetermined` → `Indeterminate`. | **This is the step that proves step 1 was needed.** Without the frozen key, renaming the log output here would have changed the hash of every run with a corruption. |
| `edc610c` | 8 | `FakeFS` → `PlantModel` (now `StorageModel`), `FSFaults` → `Disturbances`, `FSPlan` → `ActuationPlan{Fail, DeadTime, FeedbackDelay}`, `QuietTail` → `Settling`, `KindQuiet`/`KindTailEnd` → `KindSettle`/`KindSettlingEnd`, `modelState` → `refState`, `groups` → `estimate`; `value.go` and `history.go` split out. | `EntryKind` values are folded into the hash at every pop: the new names kept the old numeric positions. Every `rng` call stayed exactly where it was. |
| `2787f65` | 9 | The `GroundTruth` ACL: `View.FS()` → `View.GroundTruth()`; `PlantModel.EachDurable`. | Invariant failures are not hashed, but their *order* is observable in the golden's failure counts and messages; `EachDurable` iterates in the same sorted order as the old `Durable().Files`. |
| `a4018d5` | 10 | Docs and comments: package docs, `DESIGN.md`, the comment-only fixes from Chapter 17. | Zero code. |
| `56b0d2e` | — | After the refactor: rename *plant* to *storage*. `PlantModel` → `StorageModel`, `plantmodel.go` → `storagemodel.go`, `control/plant.go` → `control/storage.go` (the Storage port), `Simulator.plant` → `Simulator.storage`, `TestPlantModel*` → `TestStorageModel*`, and every "plant" in comments and docs. | The only string that changed and is ever printed is a `panic` message. Before renaming, the commit checked that no hash input depends on a Go name; the one candidate, the `%T` fallback of `telemetryHashKey`, is unreachable. Seed 1 still `1ca97eb77a9044e1`; every mutant caught at the same seed and step. |
| `10777a2` | — | Comment drift left by the mechanical renames ("enters the control", "the core only sees the code", "fake filesystem", "latencies"); `telemetryHashKey`'s fallback `%T` → `"?"` plus a coverage test; `prod.Executor.Execute` takes `control.Actuation`; a redundant `uint64` conversion in `format.TmpPath`. | The fallback change is the only one that touches the hash function, and the golden proves it unreachable. The `Execute` signature is production-only (not in the simulator). |

The rows show a pattern. The risky steps were never the big mechanical ones (renaming a package across 32 files is boring and safe); they were the small ones where a *value* that feeds the hash could move without anyone noticing: an enum position, a rendered string, an iteration order, a random draw. The golden file turns each of those into a test failure at the commit that caused it, with a seed number attached.

### What the golden cannot see: logs, through a rename map

The golden pins numbers. It says nothing about the *text* of the event log, which the refactor changed on purpose (`Halt{` became `Trip{`, the `quiet` and `tail-end` entry kinds became `settle` and `settling-end`, and so on). A change that, say, swapped two actions in a log line without changing the run would also be invisible to it.

So the refactor also compared full event logs, through a **rename map**: a short `sed` script that rewrites the old spellings into the new ones. If the old log, rewritten, equals the new log byte for byte, then the only differences are the intended renames. The proposal ran this for nine runs (seeds 1, 18, 20 and 132 against the correct controller; mutants b at seed 1, d at seed 1, d2 at 132, e at 18 and f at 20) and reported them identical.

You can reproduce the comparison for the correct controller with two copies of the tracer program of Chapter 16: one built against a checkout of `0f0aad9` (the last pre-refactor commit), one against `a4018d5`. While updating this book, that was done for seeds 1, 18, 20 and 132. The raw logs differ in 17 to 21 lines each (of 1,220 to 1,836). After this rename map:

```sed
s/Halt{/Trip{/g
s/CRASH (halt: /CRASH (trip: /g
s/CorruptObject{/CorruptionDetected{/g
s/ quiet       chaos over: no more faults or crashes/ settle      chaos over: settling (no new faults or crashes)/
s/ tail-end    quiet tail over: clients stop/ settling-end settling over: clients stop/
```

all four are identical. A typical raw difference, from seed 1:

```
< #233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Halt{fsync temp: EIO}
< #233    t=53.112270ms  inc=1 crash       CRASH (halt: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
---
> #233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Trip{fsync temp: EIO}
> #233    t=53.112270ms  inc=1 crash       CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
```

Note what the map does *not* contain: `Committed{...}` and `Recovered{...}` are rendered identically by both versions, and every path, IOID, client number and timestamp is untouched. A rename map that needed to touch those would be evidence of a behaviour change.

The remaining observable outputs are outside the simulator altogether: production log lines (`toys3: HALT:` became `toys3: TRIP:`, and `toys3: fact core.Committed {...}` became `toys3: telemetry control.Committed {...}`, Chapter 9), the error message of `prod.ErrTripped` ("toys3: server tripped"), and the `Stats.String()` keys (`trips=`, `corruptdet=`). Those were reviewed by eye and covered by the ordinary `prod` tests (`TestTripStopsLoop` was renamed with the rest).

### Why this works, and when it would not

The golden test is a regression oracle of a particular kind: it does not know what the right behaviour is, only what the behaviour *was*. That is exactly what a refactor needs, and exactly what a bug fix must not have. Three conditions make it strong here:

- **Determinism.** Without §8.1 to §8.7, the trace hash of one seed would differ between two runs of the *same* code, and comparing it across two versions would mean nothing.
- **Coverage.** 550 seeds of a swarm-tested configuration crash the controller thousands of times, inject every kind of disturbance, and exercise every protocol path; the mutants add sensitivity to the invariants and the oracle. A refactor that changed a rare path would very likely touch at least one of 550 seeds. (It is not a proof: a path that no seed reaches is not pinned. Chapter 17 lists a recovery path no mutant can show is necessary; the golden pins it no better than the mutants do.)
- **Discipline about what feeds the hash.** The hash had to be insensitive to names and sensitive to everything else. One input (`Emit`) was not, and the first real step of the refactor was to fix that, before any rename made it matter.

The order is the lesson: **pin, then make the pin robust, then change.** Writing the golden test after the refactor would have pinned whatever the refactor produced. Freezing the hash key after the telemetry rename would have been too late.

## Key takeaways

- The trace hash is a fingerprint of the *whole history*: it folds the step number, time, entry kind and incarnation of every step, every event and action, the server's `StateHash` after every delivery, every FS completion, every issued request, every crash resolution, and the StorageModel hash after every step.
- `StateHash` must be injective over behaviour-relevant state: fixed field order, length prefixes, no pointers, no map order. Hashing a payload's length and CRC instead of its bytes is a performance trade.
- The canary compares trace hash, random draw count and step count; checks that logging does not perturb the run; and checks that the seed actually matters.
- SplitMix64 and Lemire's bounded integers are written out in `rand.go` so that a seed's meaning never changes with a Go release. `Calls()` gives every random decision a coordinate.
- detcheck is an allow-list (fail closed) plus bans on goroutines, channels, `select`, map ranges, reflection map iteration and globals, with mandatory reasons for suppressions.
- Both tooling bugs (the generated test main and the SIGPIPE'd canary) were latent until the code they guarded existed. Test the tests by breaking the thing they check.
- The trace hash is also a refactoring safety net. A golden file of steps, draws, trace hashes, stats and mutant catches for 550 seeds turned "no behaviour change" into a test that every commit of the DDD refactor passed. Pin first, make the pin robust (freeze any hash input that depends on names), then change.
- Hash inputs must be stable codes, not rendered names. Enum *values* are hashed, so enums are append-only; telemetry is hashed through a frozen key that still spells `CorruptObject`.

## Exercises

1. **Break determinism with a map.** In `toys3/control/recovery.go`, temporarily change `onRecoveryDone`'s listing case to put the names into a `map[string]bool` and then range over the map to issue the unlinks. Predict: (a) what `go run ./tools/detcheck ./toys3/control` reports, and (b) whether `go test ./toys3/sim -run TestDeterminismCanary -count=1` fails, and on which seed. Run both. If the canary passes, explain why (hint: how many temp files does a typical recovery find?). Revert.
2. **Forget a field.** Remove `h.str(r.tmp)` from `StateHash`. Does `go test ./toys3/control -run TestStateHash` still pass? Why? Does the canary still pass? Now write a two-history test that would fail with the field missing. Revert.
3. **Prove log mode is inert.** Add a call to `s.rng.Uint64()` inside the `if s.cfg.Log` block in `Simulator.deliver` (`toys3/sim/simulator.go:174-176`). Predict which assertion of `TestDeterminismCanary` fails and with what message. Run it. Revert.
4. **Find a divergence by draw number.** Add `fmt.Fprintf(os.Stderr, "%d %d\n", s.step, s.rng.Calls())` temporarily at the end of `Simulator.Run`'s loop body (you will need to exempt it from detcheck mentally; it is a debugging hack). Run seed 1 twice and diff. Then introduce the map bug from Exercise 1 and find the first step where the draw counts differ. Revert everything.
5. **Break the gate, not the code.** Check out the old canary pipeline from commit `011144f` into a scratch copy of `scripts/ci-determinism.sh` (outside the repo), point it at `./toys3/sim/...`, and introduce a deliberate nondeterminism in `toys3/sim` (for example, in `Simulator.New`, XOR the seed with `uint64(time.Now().UnixNano())` before calling `NewRand`; detcheck will flag the `time` import, which is the point of a different exercise, so ignore it here). Confirm the old script reports "skipped" and "determinism gate passed". Run the current script and confirm it fails. Revert.
6. **Allow-list exactness.** Add `hash/fnv` to a scratch copy of `allow.txt`, pass it with `-allow`, and rewrite the server's `hasher` to use `hash/fnv`. Does the trace hash for seed 1 change? (It should not, if the hand-written FNV-1a is correct. This is a nice end-to-end test of the hasher.) Revert.
7. **The `Chance(0)` trap.** Set `cfg.Disturbances.ReadCorruptProb = 0` in a scratch program and run seed 1. Compare the trace hash and `RandCalls` with the default. Explain the difference in the number of draws from the code of `Chance` and `StorageModel.read`.
8. **Feel the frozen key.** In a scratch copy, change the `CorruptionDetected` case of `telemetryHashKey` to print `CorruptionDetected{%q}` (the "obvious cleanup"). Run `go test ./toys3/sim -run 'TestGolden|TestTelemetryHashKeyFrozen' -v`. Which test fails first, and on which line of the golden file? Find the first `default` row that changed and check, with `go test ./toys3/sim -run TestSeeds -seed=N -v`, that its `corruptdet` is non-zero. Explain why every row with `corruptdet=0` is unaffected. Revert.
9. **Reorder an enum.** Swap `modeRecovering` and `modeStartup` in the `mode` const block (keep the names, change their values). Which tests fail? Does `TestStateHash` notice? Does `TestGolden`? Explain the difference in what each test pins. Revert.
10. **Replay the log comparison.** Build the tracer of Chapter 16 twice, once with a `replace` directive pointing at a checkout of `0f0aad9` and once at `a4018d5`. Dump seed 132 with both, apply the rename map of §8.12 to the old log, and `diff`. Then add one line to the rename map that is *wrong* (for example, rewrite `Committed{` to `Commit{`) and observe how quickly the comparison tells you.

## Further reading

- Guy L. Steele Jr., Doug Lea and Christine H. Flood, *"Fast Splittable Pseudorandom Number Generators"*, OOPSLA 2014. The origin of SplitMix64.
- Daniel Lemire, *"Fast Random Integer Generation in an Interval"*, ACM Transactions on Modeling and Computer Simulation, 2019. The multiply-shift-reject method used by `Rand.Intn`.
- The Go Programming Language Specification, section "For statements with range clause": map iteration order "is not specified and is not guaranteed to be the same from one iteration to the next".
- `golang.org/x/tools/go/analysis` package documentation, including `singlechecker` and `analysistest`. The framework detcheck is built on.
- Glenn Fowler, Landon Curt Noll and Kiem-Phong Vo, the FNV hash (documented at http://www.isthe.com/chongo/tech/comp/fnv/).
- `docs/lessons.md` (lessons 3 and 12) and `docs/decisions.md` (decision I) in this repository.
- The bash manual, `set -o pipefail`, and any discussion of "SIGPIPE with grep -q / head under pipefail" on the shell tag of your favourite Q&A site; this is a well-trodden trap.
