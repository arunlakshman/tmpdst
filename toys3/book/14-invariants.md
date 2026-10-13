# Chapter 14: Invariants

> *"A simulator without invariants is a very fast way of doing nothing reproducibly."*

Chapters 10 to 13 built a world: a queue, a clock, a hostile filesystem, clients that write self-describing values, and an oracle that can decide whether a history of client operations is linearizable. This chapter is about the **judges**: the small objects that look at that world after every single step and decide whether something has gone wrong.

There are exactly five of them in `toys3/sim/invariant.go`, plus three "pseudo-invariants" built into the simulator loop itself. Each one is short (the longest is about thirty lines). Each one encodes a specific promise the server makes. And each one exists because some broken server, somewhere in `mutants_test.go`, would slip past the others. By the end of this chapter you should be able to look at any of the five and say: what promise does it check, what does it cost, what can it see that the others cannot, and what would be lost if it were deleted.

---

## 14.1 What an invariant is, in this codebase

An invariant is anything that implements this interface:

```go
// toys3/sim/invariant.go:53-60
// Invariant is checked after every simulator step and once at the end of the
// run. Checking every step (not just at the end) reports a bug at the step
// that caused it, which is what makes the event log readable.
type Invariant interface {
	Name() string
	AfterStep(v View) []Failure
	AtEnd(v View) []Failure
}
```

Two hooks:

- **`AfterStep`** runs after every step of the main loop. The simulator calls it from `afterStep` (`toys3/sim/simulator.go:339-354`), which first handles the end of a quiesce and then loops over `s.cfg.Invariants`.
- **`AtEnd`** runs once, after the queue has drained (or `MaxSteps` was hit), but only if no failure has stopped the run already (`toys3/sim/simulator.go:103-109`).

Both return a slice of `Failure` values (`toys3/sim/config.go:177-183`): an invariant name, a step, a virtual time and a message. An empty slice means "all good".

The comment on the interface is worth dwelling on: *checking every step reports a bug at the step that caused it*. Consider the alternative. If you only checked "every acked write is still readable" at the end of a 360 ms run, a failure would tell you that *something*, *somewhere* in 1,600 steps lost a write. Checking after every step means the failure message lands in the event log right next to the action that broke the promise. In the mutant traces in Chapter 15, the `FAILURE` line is almost always within a handful of lines of the bug. That locality is not a nicety; it is what turns a failing seed into a bug report you can read in two minutes.

### The View: read-only access to the world

Invariants never touch the simulator directly. They see it through a narrow, read-only interface:

```go
// toys3/sim/invariant.go:21-35
// View is the read-only window invariants get onto a running simulation.
type View interface {
	Step() int
	Now() control.Time
	Alive() bool // server is booted (it may still be recovering)
	Inc() Incarnation
	Settling() bool // in Settling: no new faults, no crashes
	// GroundTruth is the plant's true durable state, for white-box monitors.
	GroundTruth() GroundTruth
	Root() string // the server's data directory (control.Config.Dir)
	Oracle() *Oracle
	History() *History
	// Telemetry emitted so far, in order. Invariants remember how far they read.
	Telemetry() []Sample
}
```

The `Simulator` implements it with one-line accessors (`toys3/sim/simulator.go:384-397`). Notice what is on this list and what is not:

- **On the list:** the plant's *durable* state, through `GroundTruth()`; the client-visible history; the oracle; and the *telemetry* the controller has emitted, as `Sample`s.
- **Not on the list:** the controller itself. There is no `Controller()` method. An invariant cannot reach into `control.Controller` and inspect `reqs` or `mode`.
- **Not on the list either:** the plant model itself, and in particular its *current* (volatile) view.

The first omission is deliberate. The controller is treated as a black box whose only outputs are Actions. Invariants judge it by what it *did* (replies, filesystem effects) and what it *said it believed* (telemetry), never by its private state.

The second omission is newer, and it is the anti-corruption layer of Chapter 3a. Before the refactor, `View.FS()` returned the concrete `*FakeFS`, so an invariant could call anything on it: the volatile view, the counters, even `Crash()`. Now `View.GroundTruth()` returns an interface with exactly three methods, declared by the verification side in builtin types:

```go
// toys3/sim/invariant.go:37-51
// GroundTruth is the anti-corruption interface between verification and the
// plant model: the only things a monitor may read of the plant are its true
// durable state and a generation counter. It uses only builtin types, so the
// plant model never depends on verification and monitors never reach into
// the plant model's internals. *PlantModel implements it.
type GroundTruth interface {
	// DurableFile returns path's content in the durable view.
	DurableFile(path string) ([]byte, bool)
	// EachDurable calls fn for every durable file, sorted by path.
	EachDurable(fn func(path string, data []byte))
	// DurableGen changes whenever the durable view may have changed.
	DurableGen() uint64
}

var _ GroundTruth = (*PlantModel)(nil)
```

"Ground truth" is the control-theory phrase for the plant's *true state*, which a controller can only estimate and a testbench can see. The white-box invariants are exactly the checks that compare the controller's claims with it. This keeps invariants valid for any `control.Factory`, including the broken wrappers in `mutants_test.go`, which have quite different internals.

### Telemetry: the server's beliefs, on a timeline

The `Telemetry()` method deserves its own note because it is how the controller talks to the judges. Whenever the controller returns an `Emit{Telemetry}` action, the simulator appends it, stamped with step, time and incarnation, as a `Sample`:

```go
// toys3/sim/invariant.go:12-19
// Sample is one control.Telemetry value stamped by the simulator with its
// sample index (Step), time and incarnation.
type Sample struct {
	Step      int
	At        control.Time
	Inc       Incarnation
	Telemetry control.Telemetry
}
```

(Before the refactor this type was `TimedFact`, and `View.Telemetry()` was `View.Facts()`. In control vocabulary a *sample* is a measured value stamped with its sample index and time, which is exactly what this is.)

```go
// toys3/sim/simulator.go:192-199
		case control.Emit:
			s.samples = append(s.samples, Sample{Step: s.step, At: s.now, Inc: s.inc, Telemetry: x.Telemetry})
			switch f := x.Telemetry.(type) {
			case control.CorruptionDetected:
				s.res.Stats.CorruptionsDetected++
			case control.Recovered:
				s.res.Stats.TempsRemoved += f.TempsRemoved
			}
```

The timeline is append-only. Invariants that read it keep a cursor (`next int`) and only look at samples they have not seen. That is the reason three of the five invariant structs have a single integer field: it is their read position in an append-only log.

```go
// toys3/sim/invariant.go:77,85,92,96,105-109
type NoCorruptReads struct{ next int }
type AckedIsDurable struct{ next int }
type ObjNeverTorn struct{ gen uint64 }
type Linearizable struct{}
type Liveness struct {
	next         int
	settlingFrom int // index of the first op invoked in Settling (once settlingSeen)
	settlingSeen bool
}
```

Because invariants carry state, they must be fresh for every run. `DefaultInvariants()` returns new instances every time it is called (`toys3/sim/invariant.go:62-65`), and `firstCatch` in the mutation tests passes the *function* `DefaultInvariants`, not a slice, so each seed gets its own (`toys3/sim/mutants_test.go:333-343`). If you ever share an invariant slice across runs you will get very confusing results: the second run's `NoCorruptReads.next` would start where the first one ended.

## 14.2 The roster: default and black-box

```go
// toys3/sim/invariant.go:62-72
// DefaultInvariants returns fresh instances of every invariant below.
func DefaultInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &AckedIsDurable{}, &ObjNeverTorn{}, &Linearizable{}, &Liveness{}}
}

// BlackBoxInvariants returns only the invariants that judge what clients
// see (no peeking at the plant model's durable state). Useful to learn
// which bugs are visible end to end, and which only to a ground-truth check.
func BlackBoxInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &Linearizable{}, &Liveness{}}
}
```

The split is the most important conceptual distinction in the chapter:

| Invariant | Looks at | Kind |
|---|---|---|
| `NoCorruptReads` | history (GET bodies) | black-box |
| `AckedIsDurable` | telemetry (`Committed`) + the plant's durable view (`GroundTruth`) | **white-box** |
| `ObjNeverTorn` | the plant's durable view (`GroundTruth`) | **white-box** |
| `Linearizable` | history + oracle model | black-box |
| `Liveness` | history, telemetry (`Recovered`), `Alive()` | black-box (mostly; see §14.7) |

A **black-box** invariant could, in principle, be checked against a real deployed toys3 by a client that records requests and replies. A **white-box** invariant needs *ground truth*: it asks the plant model "what would survive a crash right now?", which no real client could ever ask a real disk.

Why keep both? Because they answer different questions:

- Black-box invariants tell you whether a bug is **observable**. If only a white-box invariant catches a mutant, then in some sense the bug is latent: data is at risk, but no client has yet seen anything wrong.
- White-box invariants tell you about bugs **early**. `AckedIsDurable` fires on the very step a write is acked without being durable, with no crash required. The black-box `Linearizable` has to wait for a crash to actually erase the write and a later GET to notice.

The mutation table in `DESIGN.md` reports both columns, and Chapter 15 walks through each row. Here is the one-line version: the white-box checks catch every mutant at seed 1 except d2, e and f; the black-box checks catch them all too, but some later and through a different door (mutant b is invisible to the linearizability oracle entirely and is only caught by liveness).

## 14.3 NoCorruptReads: every 200 GET body was written by some PUT

This is invariant (c) of `DESIGN.md`: *no torn or corrupt bytes are ever returned with 200.*

```go
// toys3/sim/invariant.go:111-136
func (*NoCorruptReads) Name() string { return "no-corrupt-reads" }
func (n *NoCorruptReads) AfterStep(v View) []Failure {
	h := v.History()
	var fails []Failure
	for ; n.next < len(h.Completed); n.next++ {
		op := &h.Ops[h.Completed[n.next]]
		if _, isGet := op.Op.(control.Get); !isGet || op.Status != control.StatusOK {
			continue
		}
		w, ok := ValueWriter(op.Body)
		if !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned bytes no PUT wrote: %s", describeOp(op))})
			continue
		}
		i := sort.Search(len(h.Ops), func(i int) bool { return h.Ops[i].Req >= w })
		if i == len(h.Ops) || h.Ops[i].Req != w {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned the value of unknown request %d: %s", w, describeOp(op))})
			continue
		}
		if _, isPut := h.Ops[i].Op.(control.Put); !isPut {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned a value attributed to non-PUT %s", describeOp(&h.Ops[i]))})
		}
	}
	return fails
}
func (*NoCorruptReads) AtEnd(v View) []Failure { return nil }
```

Line by line:

1. **The cursor walks `h.Completed`**, not `h.Ops`. `Completed` is the list of op indices in *reply order* (`toys3/sim/history.go:48-49`), appended by `Oracle.Return`. So each step only examines replies that arrived since the last step; typically zero or one. The invariant is O(new replies) per step.
2. **Only 200 GETs matter.** A 404 has no body; a 500 promises nothing; PUT, DELETE and COPY replies carry no payload.
3. **`ValueWriter(op.Body)`** (`toys3/sim/value.go:37-49`) parses the 12-byte self-describing header (writer `ClientReq` plus total length) and then regenerates the *entire* expected value with `MakeValue(writer, len(b))` and compares byte by byte. Any torn prefix, any flipped bit, any mixture of two values fails here. This is the payoff of self-describing values from Chapter 12: *the bytes name their author, and the name is checkable*.
4. **The writer must exist.** `h.Ops` is in invoke order, and `ClientReq` ids are assigned in increasing order by `Workload.Issue` (`toys3/sim/workload.go:93-94`), and the oracle appends ops in that same order. So `h.Ops` is sorted by `Req`, and a binary search finds the writer. If the header names a request that was never issued, something fabricated a well-formed value, which should be impossible but costs nothing to check.
5. **The writer must be a PUT.** `MakeValue` is also used for the payload of the "invalid" PUT with an empty key (`toys3/sim/workload.go:110-114`), which always gets a 400 and never touches the disk. If a GET ever returned *that* value, the server would have stored something it rejected. The check catches that too.

Why have this invariant at all, when the linearizability oracle also rejects a GET whose bytes no PUT wrote (`toys3/sim/oracle.go:285-294`)? Two reasons:

- **Timing.** The oracle only checks at quiescent points (§14.6). `NoCorruptReads` fires on the very step the bad reply is delivered.
- **Clarity.** "200 GET returned bytes no PUT wrote" is a much more specific message than "no linearization of N ops". When both fire, you read this one first.

In practice they often fire together. Here is mutant e (skip the CRC check) at seed 18, from the run made for this book:

```
FAIL no-corrupt-reads step 657: 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
FAIL linearizable step 657: GET req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes) returned bytes no PUT wrote
```

Two invariants, same step. How can two fire if "the first failure stops the run"? Look at `afterStep`:

```go
// toys3/sim/simulator.go:349-353
	for _, inv := range s.cfg.Invariants {
		for _, f := range inv.AfterStep(s) {
			s.addFailure(inv.Name(), f)
		}
	}
```

`addFailure` calls `fail`, which sets `s.halted = true` (`toys3/sim/simulator.go:365-375`). But the loop over invariants for *this* step keeps going. The halt takes effect at the top of the main loop: `for !s.halted` (`toys3/sim/simulator.go:84`). So the rule is precisely: **the run stops after the first step that produced any failure, and every invariant gets to report on that step.** Here, GET request 98 was the only outstanding request, so its reply made the system quiescent, so `Linearizable` ran its window check in the same step and reached the same conclusion through the oracle.

### What the correct server does at the same moment

The same seed against the correct server is instructive. `PlantModel.read` flips one bit with probability `ReadCorruptProb` (`toys3/sim/plantmodel.go:398-402`), and at seed 18 that coin comes up on exactly the same read (the RNG stream is identical up to this point, because the mutant only rewrites an event *after* the read has landed). From the correct server's log (`s18.log` in the book's scratch directory):

```
#656    t=153.982439ms inc=7 fs-land     land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657    t=154.136154ms inc=7 deliver     FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptionDetected{"k3"}} Reply{c98 500}
```

`raw(29 bytes)` is how `describePayload` renders bytes that do not decode as an object (`toys3/sim/log.go:30-44`): 16 bytes of header plus a 13-byte payload in which one bit was flipped, so the CRC no longer matches. The correct server's `DecodeObject` notices, emits a `CorruptionDetected` telemetry, and replies 500 (`toys3/control/controller.go:289-292`). The mutant hands the 13 payload bytes straight back as a 200. Same seed, same bit flip, one step, two outcomes. That is the essence of mutation testing, previewed.

## 14.4 AckedIsDurable: the eager form of "acked implies durable"

This is the first half of invariant (b): *acked mutations survive crashes*. The black-box version of (b) lives in the oracle ("crashes erase nothing", Chapter 13). This is the white-box version, and it is much more aggressive.

```go
// toys3/sim/invariant.go:138-166
func (*AckedIsDurable) Name() string { return "acked-is-durable" }
func (a *AckedIsDurable) AfterStep(v View) []Failure {
	samples := v.Telemetry()
	var fails []Failure
	for ; a.next < len(samples); a.next++ {
		c, ok := samples[a.next].Telemetry.(control.Committed)
		if !ok {
			continue
		}
		path := format.ObjPath(v.Root(), c.Key)
		data, present := v.GroundTruth().DurableFile(path)
		switch {
		case c.Deleted && present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: DELETE %q acked but %s is still durable (%s)", c.Client, c.Key, path, describePayload(data))})
		case c.Deleted:
		case !present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but %s has no durable entry", c.Client, c.Key, path)})
		default:
			payload, valid := format.DecodeObject(data)
			if !valid {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s is torn (%d bytes)", c.Client, c.Key, path, len(data))})
			} else if format.Checksum(payload) != c.Sum {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s holds %s", c.Client, c.Key, path, describePayload(data))})
			}
		}
	}
	return fails
}
func (*AckedIsDurable) AtEnd(v View) []Failure { return nil }
```

The mechanism is a contract between the server and the simulator, spelled out in the `Committed` doc comment:

```go
// toys3/control/telemetry.go:14-24
// Committed is emitted immediately before the 200 Reply of a PUT, COPY or
// DELETE, at the moment the controller believes the mutation is durable. The
// simulator checks it against the plant model's durable state on the same
// step ("acked implies durable"), which catches a missing fsync without
// waiting for a crash to expose it.
type Committed struct {
	Client  ClientReq
	Key     string // the key that changed (Dst for COPY)
	Deleted bool
	Sum     uint32 // CRC-32 of the new payload; 0 when Deleted
}
```

The server emits `Committed` in the same `Step` as the 200 (`toys3/control/controller.go:256-262`). The simulator processes the step's actions, then runs invariants. At that moment, `DurableFile` (`toys3/sim/plantmodel.go:581-588`) answers the question "if the power went out *right now*, and the adversary threw away every un-synced change, what would `obj/<key>` contain?" The four cases:

| Committed says | Durable view shows | Verdict |
|---|---|---|
| delete | file present | fail: DELETE acked but still durable |
| delete | absent | ok |
| write | absent | fail: no durable entry |
| write | present, header/CRC invalid | fail: durable file is torn |
| write | present, valid, wrong CRC | fail: durable file holds some other value |
| write | present, valid, matching CRC | ok |

Note the last comparison is by CRC (`c.Sum`), not by bytes. That is enough in practice: different self-describing values have different CRCs with overwhelming probability, and the telemetry stays small.

### Why "eager" matters

Compare two ways of catching mutant a (skip `FSFsyncDir(obj/)`):

- **Eager (this invariant):** On the step where the mutant replies 200, the rename has happened in the *current* view but `obj/`'s directory entry is not durable. `DurableFile` returns `present=false`. Failure, seed 1, step 46, no crash needed:

  ```
  #46     t=7.377477ms   inc=1 deliver     FSRenameDone{io12 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io15 /data/obj/k2}
  #46     t=7.377477ms   inc=1 crash       FAILURE acked-is-durable: c6: write of "k2" acked but /data/obj/k2 has no durable entry
  ```

- **Lazy (the oracle):** wait for a crash, hope the adversary chooses to revert that particular directory entry (50/50 per entry in `PlantModel.Crash`, `toys3/sim/plantmodel.go:479-485`), hope no background writeback persisted it first, and then hope a later GET of `k2` happens before another PUT overwrites it. The black-box column in `DESIGN.md` says that also happens at seed 1 for this mutant, because a is a blatant bug. For subtler bugs the gap between "eager" and "lazy" is the gap between seed 1 and seed several hundred.

There is a second, quieter benefit. `AckedIsDurable` checks *every* ack, including the vast majority that are never followed by a crash. The oracle can only ever check acks whose loss is subsequently observed. Over the 200 seeds in the preface there were 1,691 crashes but about 33,000 200-replies; perhaps a few thousand of them were mutations. `AckedIsDurable` checked each one at the moment it was made.

### What it trusts

This invariant trusts the server to emit `Committed` honestly. A server that replied 200 without emitting `Committed` would sail past it. That is not a hole in practice, because the black-box `Linearizable` invariant would eventually catch any ack that is lost, but it is a property worth stating clearly: **telemetry is the controller's published beliefs, and white-box invariants check those beliefs against ground truth.** (In control terms: an estimate-versus-true-state check.) If the server lies about what it believes, only black-box checks can help. Chapter 15 shows that the mutants are careful about this: `noTrip.rewrite` drops the `Committed` telemetry for a request it turns into a 500 (`toys3/sim/mutants_test.go:210-229`), because a 500 is not an ack and the invariant must not judge it as one.

## 14.5 ObjNeverTorn: obj/ is always clean on disk

This is invariant (e): *every file under `obj/` in the durable view is a complete object.*

```go
// toys3/sim/invariant.go:168-187
func (*ObjNeverTorn) Name() string { return "obj-never-torn" }
func (o *ObjNeverTorn) AfterStep(v View) []Failure {
	g := v.GroundTruth().DurableGen()
	if g == o.gen {
		return nil // nothing durable changed since the last check
	}
	o.gen = g
	prefix := format.ObjDir(v.Root()) + "/"
	var fails []Failure
	v.GroundTruth().EachDurable(func(path string, data []byte) {
		if !strings.HasPrefix(path, prefix) {
			return
		}
		if _, ok := format.DecodeObject(data); !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("durable %s is torn or corrupt (%d bytes)", path, len(data))})
		}
	})
	return fails
}
func (*ObjNeverTorn) AtEnd(v View) []Failure { return nil }
```

The invariant walks the durable view with `GroundTruth().EachDurable`, which visits every durable file in sorted path order. Before the refactor it ranged over `v.FS().Durable().Files`, a full snapshot of the concrete fake filesystem; `EachDurable` was added to the plant model so that the monitor could do the same walk through the narrow ground-truth interface, in the same order, so that a failure with two torn files reports them in the same order as before (the golden test of Chapter 8 would have noticed otherwise).

The claim it checks is stronger than "acked writes are durable". It says: *no matter what has or has not been acked, if the power went out now, every file the adversary might leave in `obj/` would decode.* The rename protocol is designed to guarantee exactly that: only a file whose content has been fsynced is ever renamed into `obj/`, so any directory entry under `obj/` that could survive a crash points at content that is already durable. The `DESIGN.md` phrasing is "a correct protocol never leaves a torn file under `obj/`", and the comment on the type adds "the object CRC is a safety net, not a mechanism" (`toys3/sim/invariant.go:87-92`).

### The DurableGen optimization

Scanning the whole durable snapshot every step would be wasteful; most steps (a client wake, a read landing, a reply being delivered) change nothing durable. So the PlantModel keeps a generation counter that is bumped whenever the durable view *may* have changed:

```go
// toys3/sim/plantmodel.go:458-460
// DurableGen changes whenever the durable view may have changed, so
// invariants over Durable() can skip unchanged steps.
func (fs *PlantModel) DurableGen() uint64 { return fs.durGen }
```

`durGen++` appears in exactly four places: a successful `fsync` (`plantmodel.go:341`), a successful `fsyncDir` (`plantmodel.go:382`), a background `writeback` that found something to persist (`plantmodel.go:231`), and `Crash` (`plantmodel.go:470`). Writes, renames and unlinks only touch the *volatile* versions, so they do not bump it. The invariant remembers the last generation it checked (`o.gen`) and returns immediately if nothing moved.

The optimization is conservative in the right direction: a spurious bump only costs a redundant scan; a missing bump would be a real hole. If you ever add a new way for durable state to change in the PlantModel (say, a "disk firmware writes back on its own schedule" fault), you must bump `durGen` there too, or this invariant goes blind to it. That is a good example of a coupling that a code reviewer would miss and a mutation test (Chapter 15) would find.

### What it catches that nothing else does

Mutant b (skip the temp file's `FSFsync`) is the canonical example. At seed 1:

```
#42     t=6.871157ms   inc=1 deliver     FSRenameDone{io11 OK} => FSFsyncDir{io14 /data/obj}
#43     t=7.182477ms   inc=1 fs-land     land FSRename{io13 /data/tmp/0000000000000009 -> /data/obj/k0} -> FSRenameDone{io13 OK}
#44     t=7.233120ms   inc=1 fs-land     land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k0 is torn or corrupt (0 bytes)
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k2 is torn or corrupt (0 bytes)
```

The moment `fsyncDir(obj/)` lands, both renamed entries become durable, but they point at inodes whose *durable* content is still `nil` (never fsynced), which `file()` returns as an empty byte slice. Zero bytes is shorter than the 16-byte header, so `DecodeObject` rejects it. Notice:

- The failure fires on an **fs-land step**, before the server has even been told the fsync finished, let alone acked anything. `AckedIsDurable` would also have fired a moment later, at the ack. `ObjNeverTorn` got there first.
- One step produced **two** failures from the **same** invariant: one per torn file. The rename of `k0` landed while `k2`'s fsyncDir was in flight, and the single `fsyncDir(obj/)` made both entries durable at once.

The black-box picture for b is much murkier. If a crash then happens, recovery leaves a 0-byte or torn file in `obj/`. A later GET reads it, the CRC fails, the server replies 500. **A 500 promises nothing**, so the linearizability oracle shrugs: the key's value is simply "unknown, failed read". Only `Liveness` (which forbids 500s once faults stop) eventually notices, at seed 2. Section 14.7 explains why.

## 14.6 Linearizable: the oracle, at quiescent points

This is invariant (a) and the black-box half of (b), and it is the thinnest wrapper of the five, because all the work is in the oracle (Chapter 13):

```go
// toys3/sim/invariant.go:189-197
func (*Linearizable) Name() string { return "linearizable" }
func (*Linearizable) AfterStep(v View) []Failure {
	o := v.Oracle()
	if !o.Quiescent() || o.Unchecked() == 0 {
		return nil
	}
	return o.CheckQuiescent()
}
func (*Linearizable) AtEnd(v View) []Failure { return v.Oracle().CheckFinal() }
```

Two conditions gate the check:

- **`Quiescent()`**: no op is `Pending` (`toys3/sim/oracle.go:129-130`). Only at a quiescent point is the window of history closed: every op in it has either replied or been abandoned by a crash. Checking a window with open ops would require guessing their outcome.
- **`Unchecked() > 0`**: there is something new to check (`oracle.go:132-133`). Without this guard, every step of an idle period would re-run an empty check.

When are there quiescent points? Three sources, all from Chapter 10 and 12:

1. **Quiesce periods.** Every `QuiesceEvery` (8 ms in `BaseConfig`), clients stop issuing until all outstanding requests have replied (`simulator.go:154-158`, `338-347`).
2. **Crashes.** `Oracle.Crash` marks every pending op `Abandoned` (`oracle.go:107-118`), so the system is instantly quiescent.
3. **Natural lulls**: whenever all clients happen to be in think time.

`AtEnd` calls `CheckFinal`, which checks whatever is left even if something is still pending, treating a pending mutation as abandoned and ignoring pending reads (`oracle.go:149-151`, `311-317`). In a passing run with a working server there should be nothing pending at the end; `Liveness` checks that separately.

### Cost and when the failure is reported

`Linearizable` is by far the most expensive invariant: each window is a WGL search (Chapter 13), and the 200-seed run reported windows of up to 23 ops. It is also the *latest* to fire: a violation inside a window is only reported when the window closes. In the d2 run (Chapter 7), the GET that observed the impossible value replied at step 365, but the failure is reported at step 380, the next quiescent point:

```
#365    t=84.991187ms  inc=3 deliver     FSReadDone{io5 OK obj(value-of-26)} => Reply{c55 200 value-of-26}
...
#380    t=91.269593ms  inc=3 deliver     FSFsyncDirDone{io11 OK} => Emit{Committed{c56 delete "k0"}} Reply{c56 200}
#380    t=91.269593ms  inc=3 crash       FAILURE linearizable: no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
  req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
```

When you read a linearizability failure, always look *backward* from the reported step to find the op that could not be placed; the message tells you which one (`req 55`) and its invoke/return times (`@82642556..84991187` in virtual nanoseconds) so you can find it in the log.

### Black-box, but carrying memory

`Linearizable` is black-box: the oracle never consults the PlantModel or the server. But it is not memoryless. The oracle carries the *set of possible reference-model states* from one window to the next (its state estimate, `Oracle.estimate`; Chapter 13 §13.11 reads this as a set-membership observer), which is how "acked then lost across a crash" is detected: the ack in window N pins the key's value; the crash adds no possibility of erasure; the read in window N+1 contradicts it. `TestOracleRejectsAckedThenLost` (`toys3/sim/oracle_test.go:96-107`) is the minimal example.

## 14.7 Liveness: the closed loop settles

This is invariant (d), and it is the one that most rewards close reading, because it does three different things and one of them is subtle.

Its doc comment now opens with the control-theory phrase the refactor gave it: *"the closed loop settles"*. After a disturbance, a stable feedback loop goes through a transient and then settles, with no residual error. The Settling phase of a run (Chapter 10, §10.14) is that period, and `Liveness` checks both halves of "settled": the transient has died out (nothing is still pending at the end) and there is no steady-state error (nothing issued during Settling fails). Before the refactor the comment said "at the end of the quiet tail", and the struct's fields were `quietFrom` and `quietSeen`.

```go
// toys3/sim/invariant.go:98-109
// Liveness is invariant (d), "the closed loop settles": at the end of
// Settling (server alive, no faults) no op is Pending, and the final boot
// reached control.Recovered (earlier incarnations may crash or trip
// mid-recovery by design).
// Additionally, a request issued during Settling never gets a 500: with no
// faults injected a correct server has no reason to fail (this is what makes
// a lost-but-acked object visible to clients: it reads back as 500 forever).
type Liveness struct {
	next         int
	settlingFrom int // index of the first op invoked in Settling (once settlingSeen)
	settlingSeen bool
}
```

### Per step: no 500 for an op invoked in Settling

```go
// toys3/sim/invariant.go:199-214
func (*Liveness) Name() string { return "liveness" }
func (l *Liveness) AfterStep(v View) []Failure {
	h := v.History()
	if v.Settling() && !l.settlingSeen {
		l.settlingSeen, l.settlingFrom = true, len(h.Ops)
	}
	var fails []Failure
	for ; l.next < len(h.Completed); l.next++ {
		i := h.Completed[l.next]
		op := &h.Ops[i]
		if l.settlingSeen && i >= l.settlingFrom && op.Status == control.StatusServerError {
			fails = append(fails, Failure{Msg: "500 with no fault injected (Settling): " + describeOp(op)})
		}
	}
	return fails
}
```

On the first step at which `v.Settling()` is true (the `KindSettle` entry at `Chaos` time, `simulator.go:159-163`), it records `settlingFrom = len(h.Ops)`: the index of the first op that will be *invoked* in Settling. From then on, any completed op with index ≥ `settlingFrom` and status 500 is a failure.

Why "invoked in Settling" rather than "replied in Settling"? Because an op invoked during Chaos may legitimately fail in Settling: its filesystem operations were *planned* during Chaos, and `PlantModel.Plan` decides at issue time whether an action will fail (`plantmodel.go:146-152`), so an EIO can land after the Settling boundary. An op invoked after the boundary has all of its filesystem actions planned while the plant model is quiet (`fs.quiet == true`, set by `SetQuiet` at `KindSettle`), so none of them can fail with EIO, and `read` will not flip bits (`plantmodel.go:398`). For a correct server, such an op has no reason to return 500.

Except one: a corrupt object that is *already on disk*. And that is the subtle point the comment makes. If a bug has left a torn file in `obj/` (mutant b) or lost the content behind an acked write (mutant d), then in Settling every GET of that key returns 500 forever. The linearizability oracle cannot see this, because 500s are `Failed` outcomes that constrain nothing. `Liveness` turns "the server can no longer serve this key, even though nothing is going wrong" into a failure. Mutant b's black-box catch is exactly this:

```
FAIL liveness step 1719: 500 with no fault injected (Settling): req 284 client 3 GET "k7" [527..529 @303457778..303910931] -> 500
```

The GET was invoked at 303.46 ms, after Chaos ended at 300 ms. The run's stats line shows `corruptdet=34`: the server had been emitting `CorruptionDetected` telemetry all through Chaos as it tripped over torn files in `obj/`, and replying 500 each time, and nothing black-box objected until the Settling phase.

There is a small but real design tension here. If someone adds a new *legitimate* cause of 500 that can arise without injected faults (say, "too many requests in flight"), this invariant will start failing on a correct server. That is by design: such a change *should* force a conversation about whether the invariant still states the right promise.

### At the end: alive, recovered, nothing pending

```go
// toys3/sim/invariant.go:215-235
func (*Liveness) AtEnd(v View) []Failure {
	var fails []Failure
	if !v.Alive() {
		fails = append(fails, Failure{Msg: "server not alive at the end of the run"})
	}
	recovered := false
	for _, f := range v.Telemetry() {
		if _, ok := f.Telemetry.(control.Recovered); ok && f.Inc == v.Inc() {
			recovered = true
		}
	}
	if !recovered {
		fails = append(fails, Failure{Msg: fmt.Sprintf("incarnation %d never emitted Recovered", v.Inc())})
	}
	h := v.History()
	for i := range h.Ops {
		if h.Ops[i].Outcome == Pending {
			fails = append(fails, Failure{Msg: "no reply by the end of Settling: " + describeOp(&h.Ops[i])})
		}
	}
	return fails
}
```

Three checks:

1. **The server is alive.** A server that tripped during Settling and was never restarted would fail. In practice the simulator always schedules a restart after a crash (`simulator.go:305-306`), so this mainly guards against a restart that panicked (`Factory` panics are reported separately, see §14.8).
2. **The final incarnation emitted `Recovered`.** A recovery that hangs forever (waiting for a completion that never comes) would leave the last incarnation without this telemetry. Requests queue behind recovery (Chapter 5), so a stuck recovery would also leave them pending.
3. **No op is `Pending`.** Every request issued must have received a reply or been abandoned by a crash. After `KindSettlingEnd`, clients stop issuing (`simulator.go:164-166`) and the queue drains, so any op still pending at the end is waiting on something that will never happen.

**A documentation discrepancy, since fixed.** In the first edition of the code the doc comment said "every boot reached core.Recovered", and `DESIGN.md`'s table said "every boot recovers". The code checks only the *final* incarnation (`f.Inc == v.Inc()`). Commit `a4018d5` corrected both (the comment now says "the final boot reached control.Recovered (earlier incarnations may crash or trip mid-recovery by design)", and `DESIGN.md:199` says "the final boot recovers"); Chapter 17, Part II §3 has the history. Earlier incarnations frequently do *not* recover, legitimately: in seed 1, incarnation 2 tripped in the middle of recovery after an EIO on unlinking a temp file (`Trip{recovery: unlink temp: EIO}`, step 252 of `seed1.log`), and incarnation 5 did the same. Checking "every boot" literally would fail every seed with a recovery-time EIO. The code's behaviour is the right one; the old comment overstated it. A fully precise wording would be "the last boot recovered, and no earlier boot was stuck in recovery when it died", but the second half is not checked at all: a recovery that hangs in incarnation N is only detected if the run *ends* in incarnation N, or indirectly via pending requests.

### Is Liveness black-box?

Mostly. It reads `Alive()`, which a real client could approximate by "the server answers", and the `Recovered` telemetry, which is a server belief rather than ground truth. A purist would call the telemetry check grey-box. It is in `BlackBoxInvariants` because it never consults the plant's ground truth (`View.GroundTruth()`), and that is the line the code draws.

## 14.8 The pseudo-invariants in the main loop

Not every check lives in `invariant.go`. The simulator itself reports three classes of failure, using invariant names that are not on any list:

| Name | Where | Fires when |
|---|---|---|
| `max-steps` | `simulator.go:89-92` | the run did not drain within `MaxSteps` (2,000,000) steps |
| `panic` | `simulator.go:177-180`, `312-315` | `Step` or the `Factory` panicked |
| `protocol` | `simulator.go:200-210`, `229-233` | `Trip` was not the last action of its step; an unknown action type; a reply to an unknown or already-answered request |

Each of these is a promise about the *runtime contract*, not about storage semantics:

- **`max-steps`** is a liveness backstop. A server that busy-loops (say, re-issuing a failed read forever with no backoff) would never drain. Note the check happens before `s.step++` and reports at the current step.
- **`panic`** turns a crash of *our* code (as opposed to a simulated power loss) into a reported failure rather than a test binary crash. `callStep` uses `recover()` (`simulator.go:219-227`), and the run is stopped: a panicking server cannot be trusted to continue.
- **`protocol`** enforces rules from `control/doc.go` that the server's type signatures cannot. "Trip is always the last action of its Step" is documented in the package doc; the simulator enforces it, and the mutation wrappers enforce it on themselves with `truncAfterTrip` (`mutants_test.go:35-43`). "Exactly one reply per request" is enforced by requiring both the oracle and the workload to recognize the reply (`simulator.go:229-234`); a duplicate reply finds neither.

These failures are recorded through `s.fail` directly, with `Invariant` set in the `Failure` literal, so they appear in failure messages and in `toys3sim`'s output exactly like the real invariants. A run that fails with `protocol` is not a durability bug; it is a broken server *contract*, and usually means a new action type was added to `control` without teaching the simulator about it.

## 14.9 DESIGN.md's invariant table, mapped to code

`DESIGN.md` lists invariants (a) to (e). Here they are with exact locations and the timing of each check:

| | DESIGN.md wording | Code | When it runs |
|---|---|---|---|
| a | Every 200 GET is linearizable against a per-key register model (COPY reads src and writes dst atomically) | `Linearizable` → `Oracle.check` (`oracle.go:363-454`), model in `wop.apply` (`oracle.go:214-261`) | at quiescent points; at end |
| b | Acked mutations survive crashes unless superseded; crashes erase nothing in the model | black-box: same oracle search (crashes add no erasure); white-box: `AckedIsDurable` | oracle: quiescent points; `AckedIsDurable`: same step as the ack |
| c | No torn or corrupt bytes are ever returned with 200 | `NoCorruptReads`; also `Oracle.window` rejects unparseable bodies (`oracle.go:289-293`) | every step |
| d | Every request gets a reply while the server is alive, the final boot recovers, and no request issued in Settling gets a 500 | `Liveness` (+ `max-steps` as a backstop) | 500s: every step; the rest: at end |
| e | Every file under `obj/` in the durable view is a complete object | `ObjNeverTorn` | whenever `DurableGen` changes |

Two observations:

- **(b) is checked twice, by two different mechanisms.** That is not redundancy for its own sake. The white-box check is fast and early but trusts the `Committed` telemetry. The black-box check trusts nothing the server says, but needs a crash and a later read. Mutant d2 shows the value of the black-box form: d2 never emits a `Committed` for its doomed requests, so `AckedIsDurable` has nothing to check, and only the oracle sees the value appear and vanish.
- **(c) is also checked twice**: by `NoCorruptReads` per step and by the oracle when it builds a window. Again, different timing, different message clarity.

## 14.10 Invariants about the test itself

There is a second category of check in `toys3/sim/sim_test.go` that is easy to overlook, because it is not about the server at all. It is about whether the *simulation* is doing its job.

### TestCoverage: a passing run that never crashed proves nothing

```go
// toys3/sim/sim_test.go:83-118 (abridged)
// TestCoverage checks that the fault model actually exercises what it
// claims to across 200 seeds: a passing run that never crashed mid-PUT
// proves nothing.
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
		...
		{"Trips", agg.Trips},
		{"CorruptionDetected telemetry", agg.CorruptionsDetected},
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

Every counter in `Stats` (`toys3/sim/config.go:59-80`) that represents a fault or an interesting path must be non-zero over 200 seeds. This is the "Sometimes" assertion idea from `docs/lessons.md` §11 ("A `Sometimes` that never fires means the simulation isn't reaching that code. Add faults, not excuses."), implemented in the bluntest possible way.

Why does this matter? Imagine a refactor of `PlantModel.Crash` that accidentally makes `TornWriteProb` never fire. Every seed would still pass. The invariants would still run. Nothing would look wrong. But the simulator would have silently stopped testing torn writes, and mutant b's white-box catch might still happen while some future torn-write bug would not. `TestCoverage` turns that silent degradation into a red test.

It is worth noticing what it does *not* check. It does not require `widened` to be non-zero (widening is a precision loss in the oracle, which we would rather never happen), nor `MaxWindowOps`. And it does not check `Writebacks`, which `PlantModel` counts (`plantmodel.go:125`) but `Stats` does not export. Given that background writeback is what made mutant f catchable in a reasonable number of seeds (Chapter 15), that is a gap worth closing; see the exercises.

### TestCorruptionUnreachableWithoutBitFlips: proving the safety net is a safety net

```go
// toys3/sim/sim_test.go:120-133
// TestCorruptionUnreachableWithoutBitFlips documents that for the
// correct server the CRC check is only a safety net: with no read
// corruption injected, torn temp files never reach obj/, so no read ever
// fails its CRC.
func TestCorruptionUnreachableWithoutBitFlips(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Disturbances.ReadCorruptProb = 0
		r := Run(cfg)
		if len(r.Failures) > 0 || r.Stats.CorruptionsDetected > 0 {
			t.Fatalf("seed %d: failures=%v corrupt=%d", seed, r.Failures, r.Stats.CorruptionsDetected)
		}
	}
}
```

This is an invariant *about an invariant's reachability*. It asserts a negative: with read corruption off, the correct server never emits `CorruptionDetected`. Two consequences:

1. It is an independent, end-to-end confirmation of what `ObjNeverTorn` checks step by step: a correct protocol never leaves a bad file under `obj/`, even after hundreds of adversarial crashes.
2. It explains *why* `ReadCorruptProb` exists at all. Without bit flips, the CRC check in `DecodeObject` is dead code for the correct server, and mutant e (which removes it) is unkillable. The book's own run confirms this: mutant e against 1,000 seeds with `ReadCorruptProb = 0` was never caught. A check you cannot reach is a check you cannot test.

### 71 corruptions, 70 detections

The 200-seed run in the preface reported `readcorrupt=71 corruptdet=70`. If every flipped read makes the correct server emit `CorruptionDetected`, why is one missing?

Because a read *lands* and its completion is *delivered* at two different times (Chapter 10). `PlantModel.read` flips the bit and bumps `count.ReadCorruptions` when the `FSRead` lands. The completion is then scheduled for delivery one feedback delay (`Plan.FeedbackDelay`) later. If a crash happens in between, `Queue.DropIncarnation` removes the pending delivery (`queue.go:101-122`): the server never sees the corrupted bytes, so it never emits the telemetry. The counters are measuring two different points in the pipeline, and the gap between them is exactly the "it happened, but nobody was told" window. A difference of one in 71 is a nice sanity check that the window exists and is rare.

If you ever see `corruptdet > readcorrupt` for the correct server, on the other hand, something is very wrong: either a bad file reached `obj/` (and `ObjNeverTorn` should have fired), or the CRC check is rejecting good files.

## 14.11 Writing a new invariant

The pattern is mechanical once you have seen five examples:

1. Decide what promise you are checking and whether it is black-box (history, replies) or white-box (the plant's ground truth via `GroundTruth()`, the controller's telemetry).
2. Decide when it can be checked: every step (cheap, incremental), when something changed (use a generation counter), at quiescent points, or only at the end.
3. Keep a cursor for anything append-only you read.
4. Return `Failure{Msg: ...}`; leave `Invariant`, `Step` and `At` empty, and `addFailure`/`fail` will fill them in (`simulator.go:356-368`).
5. Add it to `DefaultInvariants` (and to `BlackBoxInvariants` if it qualifies).
6. Run `TestSeeds` to confirm the correct server passes, then write a mutant that it alone catches (Chapter 15). An invariant without a mutant is an untested claim.

### Example 1: tmp/ is durably empty right after Recovered

Recovery's promise (Chapter 5) is: list `tmp/`, unlink everything, fsync `tmp/`, *then* emit `Recovered`. So at the step where `Recovered` is emitted, the durable view of `tmp/` must be empty. A sketch:

```go
// TmpCleanAfterRecovery: when Recovered is emitted, no file under tmp/
// survives in the durable view. (White-box.)
type TmpCleanAfterRecovery struct{ next int }

func (*TmpCleanAfterRecovery) Name() string { return "tmp-clean-after-recovery" }

func (t *TmpCleanAfterRecovery) AfterStep(v View) []Failure {
	samples := v.Telemetry()
	var fails []Failure
	for ; t.next < len(samples); t.next++ {
		if _, ok := samples[t.next].Telemetry.(control.Recovered); !ok {
			continue
		}
		prefix := format.TmpDir(v.Root()) + "/"
		v.GroundTruth().EachDurable(func(path string, _ []byte) {
			if strings.HasPrefix(path, prefix) {
				fails = append(fails, Failure{Msg: "Recovered emitted but " + path + " is still durable"})
			}
		})
	}
	return fails
}

func (*TmpCleanAfterRecovery) AtEnd(View) []Failure { return nil }
```

Two things to think about before you trust it. First, `Recovered` is emitted in the same step as the first requests start, and some of those may already issue `FSWrite` to new temp files; but an `FSWrite` only changes the *current* view, and the action has not even landed yet when invariants run, so the durable view of `tmp/` is unaffected at that step. Second, predict which mutant it catches. Mutant f (skip temp cleanup) is the obvious one, and it would catch f at the *first* recovery that finds a leftover, rather than waiting for the alias to be overwritten and read. Does it catch the "g" mutant from Chapter 15 (skip the recovery `FSFsyncDir(tmp)`)? Work it out: the unlinks change the current view only, so without the fsync the durable entries are still present. It should catch g immediately, which makes it a white-box invariant that kills a mutant the black-box ones cannot (Chapter 15 explains why g survives 2,000 seeds of the default invariants). Whether g is a *bug* is a different question, also discussed there.

### Example 2: no two requests on one key in flight

The server promises per-key serialization (Chapter 6). This is invisible to every existing invariant unless it causes a linearizability violation. A black-box approximation from the filesystem's point of view: track FS actions per object path and complain if two different requests have overlapping actions on the same `obj/` path. The View does not expose in-flight actions, so this invariant would need either a new `View` method (for example, the list of `KindFSLand` entries pending) or telemetry emitted by the controller when a request starts and finishes. The second option is cleaner and keeps the View small:

```go
// in control: a new Telemetry, emitted by start() and finish()
type Started struct{ Client ClientReq; Keys []string }   // name it ReqStarted to avoid clashing with the Started event
```

and an invariant that keeps a `map[string]control.ClientReq` of "key → request currently running" and fails on overlap. Note the map: iteration over it would be banned by `detcheck` (Chapter 8), but lookups and deletes are fine, and that is all this invariant needs. Note also that adding telemetry is a change to `control`, and every action (including `Emit`) feeds the trace hash (`trace.go:79-80`), so every seed's trace hash changes: there are new actions in every run. That is expected; determinism is about *same code, same seed*, not about stability across code changes. One more thing to do when you add a telemetry type: give it a case in `telemetryHashKey` (`trace.go:91-104`). Without one it falls through to the `%T` fallback, which hashes the Go type *name*, and Chapter 8 (§8.12) explains why a hash input that depends on a name is a trap.

## Key takeaways

- An invariant is an object with `AfterStep` and `AtEnd`. Checking after every step puts the failure next to its cause in the log.
- Invariants see the world through a read-only `View` that deliberately excludes the controller's internals and the plant model's volatile state. They judge outputs (replies, filesystem effects), beliefs (telemetry `Sample`s) and ground truth (`GroundTruth`, the plant's durable view), never private state.
- **White-box** invariants (`AckedIsDurable`, `ObjNeverTorn`) compare the server's beliefs with ground truth and fire early, often with no crash needed. **Black-box** invariants (`NoCorruptReads`, `Linearizable`, `Liveness`) judge only what clients see and tell you whether a bug is observable.
- `Liveness`'s "no 500 for an op invoked in Settling" is what makes *lost* data visible to a black-box checker, because a 500 otherwise promises nothing.
- The run stops after the first step with any failure, but every invariant reports on that step, so you can see several at once.
- `TestCoverage` and `TestCorruptionUnreachableWithoutBitFlips` are invariants about the simulation itself: if the faults stop happening, the tests must go red.
- Documentation drift happens even here: `Liveness` checks that the *final* incarnation recovered, not every boot, and until commit `a4018d5` its comment and `DESIGN.md` said otherwise.

## Exercises

1. **Delete one invariant and see who notices.** Comment out `&AckedIsDurable{}` in `DefaultInvariants` (`toys3/sim/invariant.go:64`) and run `go test ./toys3/sim -run TestCatchesBugs -v`. Before running, predict for mutants a and c which invariant will catch them and at which seed. Compare with the black-box column of the `DESIGN.md` table. Restore the line afterwards.
2. **Break the DurableGen optimization.** In a scratch copy, remove the `fs.durGen++` from `PlantModel.fsyncDir` (`toys3/sim/plantmodel.go:382`). Predict: does mutant b still get caught by `obj-never-torn` at seed 1 step 44? (Hint: what else bumps `durGen` around that step?) Run `go test ./toys3/sim -run 'TestCatchesBugs/b-skip-file-fsync' -v`. Explain the result.
3. **Implement `TmpCleanAfterRecovery`** from §14.11, add it to `DefaultInvariants`, and run `go test ./toys3/sim -run 'TestSeeds$' -seeds=200`. It should pass. Then run `go test ./toys3/sim -run 'TestCatchesBugs/f-skip-temp-cleanup' -v` and compare the seed and step with the default result (seed 20, step 1750).
4. **Coverage for writeback.** Add a `Writebacks int` field to `Stats`, fill it from `fc.Writebacks` in `Simulator.Run` next to the other FS counters (`toys3/sim/simulator.go:114-117`), add it to `Stats.Add` and `String`, and add a line to `TestCoverage`. Then set `WritebackProb: 0` in `BaseConfig` and confirm `TestCoverage` goes red.
5. **Make Liveness say what it means.** Compare the doc comment on `Liveness` before and after the fix (`git show 0f0aad9:toys3/sim/invariant.go` against `toys3/sim/invariant.go:98-104`). Is the new one fully accurate? Then decide whether you want a stronger check: "no incarnation was still recovering when the run ended *or* when it was killed by an injected crash longer than X ms after boot". Is that a real promise the server makes?
6. **Find the 71st corruption.** Run 200 seeds and find one where `readcorrupt > corruptdet` for a single seed (modify `TestSeeds` locally to print per-seed stats, or use a small program that calls `sim.Run`). Replay it with `-seed=N -v` after making it fail deliberately (for instance by temporarily adding a failing invariant), find the `FSRead` landing with a corrupted payload, and confirm that a crash dropped its completion.

## Further reading

- Leslie Lamport, *"Proving the Correctness of Multiprocess Programs"*, IEEE TSE 1977, which introduced the safety/liveness distinction that `Liveness` versus the other four invariants mirrors.
- Bowen Alpern and Fred B. Schneider, *"Defining Liveness"*, Information Processing Letters, 1985.
- The Antithesis documentation on "Sometimes" assertions, and `docs/lessons.md` §11 in this repository, for the idea behind `TestCoverage`.
- Will Wilson's Strange Loop 2014 talk on FoundationDB's simulator, for the practice of checking invariants continuously rather than at the end.
- Kyle Kingsbury's Jepsen analyses (https://jepsen.io), for many examples of black-box invariants (linearizability, lost writes, stale reads) applied to real systems.
