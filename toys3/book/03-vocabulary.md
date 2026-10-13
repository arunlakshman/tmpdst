# Chapter 3: The Vocabulary

> *"The limits of my language mean the limits of my world."* (Wittgenstein)

Chapter 2 argued that in a sans-I/O design, the set of Events and Actions *is* the interface between the logic and the world. That makes the vocabulary the most important design artifact in the system. Whatever the server cannot say, it cannot do; whatever the world cannot tell it, it cannot react to; and whatever the vocabulary cannot express, the simulator cannot inject.

This chapter is a careful tour of every type in package `toys3/control` outside the controller itself (`controller.go`, `recovery.go`, `hash.go`):

- `types.go`: the scalar types of the controller (`Time`, `Duration`, `ClientReq`, `Status`)
- `event.go`: `Event` and the controller's own `EventBody`s (`Started`, `Request`), including the `Op` set
- `action.go`: the controller's own `Action`s (`Reply`, `Trip`, `Emit`)
- `plant.go`: the **plant port**: the `Actuation` and `Feedback` interfaces, the seven filesystem commands (`FS*`), their seven completions (`FS*Done`), and the two scalars that belong to the plant (`IOID`, `ErrCode`)
- `telemetry.go`: the three kinds of `Telemetry`
- `env.go`: `Env`, `Rand`, `Site`, `Buggify`
- `machine.go`: briefly, since Chapter 2 covered it

The on-disk format (`Magic`, `Checksum`, `EncodeObject`, `EscapeKey`, `ObjPath`, ...) is not in this package at all any more. It lives in its own package, `toys3/format`, and Chapter 4 covers it. Chapter 3a explains why the split into `control`, `format` and a separate plant port is the right one; this chapter only needs the result. The file split itself teaches something: `event.go` and `action.go` now hold only what the *controller* talks about with its clients and its supervisor, while `plant.go` holds everything it says to, and hears from, the disk. When you want to know "what can the disk tell the controller?", you read one file.

For each type we ask three questions: what does it mean, what does the server do with it, and how does it look in a simulator log. The logs are real: they come from replaying seed 1 (and, for one example, seed 18) of the default configuration with logging enabled. Chapter 16 shows how to produce such logs yourself.

---

## 3.1 The closed-interface idiom

Before the types themselves, one Go idiom that the whole vocabulary uses. Go has no sum types (no `enum` with payloads, no sealed classes). toys3 emulates them with an interface containing a single unexported marker method:

```go
// toys3/control/event.go:10-12
// EventBody is a closed set of plain-data structs (no funcs, channels or
// pointers into shared state), so events can be logged, compared and replayed.
type EventBody interface{ isEvent() }
```

```go
// toys3/control/event.go:55-56
func (Started) isEvent() {}
func (Request) isEvent() {}
```

```go
// toys3/control/plant.go:167-173
func (FSWriteDone) isEvent()    {}
func (FSFsyncDone) isEvent()    {}
func (FSRenameDone) isEvent()   {}
func (FSFsyncDirDone) isEvent() {}
func (FSReadDone) isEvent()     {}
func (FSUnlinkDone) isEvent()   {}
func (FSListDone) isEvent()     {}
```

Because `isEvent` is unexported, no type outside package `control` can implement `EventBody`. The set is *closed*: there are exactly nine event bodies, and every one of them is listed in these two places (two in `event.go`, seven in `plant.go`). The same pattern defines `Op` (`isOp`, `toys3/control/event.go:30-53`), `Action` (`isAction`, `toys3/control/action.go:7`, `34-36` and `toys3/control/plant.go:151-157`) and `Telemetry` (`isTelemetry`, `toys3/control/telemetry.go:12` and `40-42`).

What the idiom gives you:

- **A single place to read the whole vocabulary.** When you want to know everything that can happen to the server, you read `event.go`.
- **Type switches as pattern matching.** `switch b := ev.Body.(type) { case Started: ... case Request: ... }` is how `Step` dispatches (`toys3/control/controller.go:113-118`).
- **Sub-interfaces as categories.** A closed set can be carved into named subsets by a second interface that embeds the first. `plant.go` does exactly that: `Actuation` is "an `Action` that also has an `ID() IOID`", and `Feedback` is "an `EventBody` that also has an `ID() IOID`" (§3.4). A single type assertion, `ev.Body.(Feedback)`, then replaces a seven-way type switch.
- **Value receivers.** All marker methods have value receivers, and all bodies are passed by value. An `FSWriteDone` is copied into and out of the interface; there are no pointers to share.

What it does *not* give you: **exhaustiveness checking**. The Go compiler will not complain if a type switch forgets a case. toys3 compensates with defensive defaults. In the controller, a body that is neither `Started`, `Request` nor `Feedback` falls through to "ignore" (`toys3/control/controller.go:119-122`); an unrecognized `Op` gets a 400 (`toys3/control/controller.go:173-174`, exercised by `TestBadRequest` with a nil `Op`, `toys3/control/controller_test.go:259`). In the simulator, an unrecognized action is a protocol failure:

```go
// toys3/sim/simulator.go:208-210
		default:
			s.fail(Failure{Invariant: "protocol", Msg: fmt.Sprintf("unknown action %T", a)})
			return
```

If you add a new action to `action.go` and forget to teach the simulator about it, the first seed that emits it fails loudly instead of silently dropping it. That is the right default for a test harness.

---

## 3.2 Scalar types (`types.go`, and two in `plant.go`)

Four scalars live in `types.go`. Two more, `IOID` and `ErrCode`, live in `plant.go`, because they are part of the plant's vocabulary rather than the controller's: an `IOID` names one command to the disk, and an `ErrCode` is the disk's status word. We cover all six here, since they are all small.

### `Time` and `Duration`

```go
// toys3/control/types.go:3-15
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
```

Why not `time.Time` and `time.Duration`? Because `toys3/control` may not import `time` (it is deliberately absent from `tools/detcheck/allow.txt`; the comment at the top of that file lists it among the forbidden packages). `time.Time` also carries a wall clock, a monotonic reading and a location, none of which belong in a deterministic core.

`Time` is an *instant*, `Duration` a *length*. The simulator converts between them explicitly, for example `s.now + control.Time(plan.DeadTime)` (`toys3/sim/simulator.go:189`). Note that the constants are typed: `Microsecond` is a `Duration` because `Nanosecond` is, so `300 * control.Millisecond` in `BaseConfig` (`toys3/sim/config.go:151`) has type `Duration` with no conversion.

In production, `Time` is `time.Since(start)` converted to nanoseconds (`toys3/prod/loop.go:65`), which uses Go's monotonic clock reading, so it never goes backwards even if the wall clock is adjusted. In simulation it starts at 0 and advances to each popped entry's `At`.

In logs, `fmtTime` renders it as milliseconds with six fractional digits (`toys3/sim/log.go:150-153`): `t=53.112270ms` is 53,112,270 ns.

### `ClientReq`

```go
// toys3/control/types.go:17-20
// ClientReq identifies one client request. The runtime assigns it when the
// request arrives (prod: per HTTP request; sim: per workload operation) and the
// server echoes it on the Reply. Unique within one runtime lifetime.
type ClientReq uint64
```

The server never invents a `ClientReq`; it receives one on each `Request` and copies it onto the matching `Reply` and, for mutations, onto the `Committed` telemetry. In production, `Loop.Submit` assigns them from a mutex-protected counter starting at 1 (`toys3/prod/loop.go:123-126`). In simulation, `Workload.Issue` assigns them from `nextReq` (`toys3/sim/workload.go:93-94`).

"Unique within one runtime lifetime" matters in the simulator: the workload's counter is *not* reset when the server crashes, so request numbers are unique across all incarnations of one run. That is what lets the oracle use `ClientReq` as a key for the whole history (`Oracle.open`, `toys3/sim/oracle.go:42`), and what lets a value's bytes name the PUT that wrote it (Chapter 12): `value-of-26` means "written by request 26", unambiguously, for the whole run.

In logs: `c6` is `ClientReq(6)`.

### `IOID`

```go
// toys3/control/plant.go:22-25
// IOID correlates a filesystem Action with its ...Done completion. The server
// assigns IOIDs from a counter; they are unique within one incarnation, which
// is enough because completions addressed to a dead incarnation are dropped.
type IOID uint64
```

Contrast this with `ClientReq`: IOIDs are assigned by the *server*, from `nextIO`, which starts at 0 in every new `Controller` (`toys3/control/controller.go:135-138`). So `io1` is always recovery's `FSList` of `tmp/`. You can see this at the start of every incarnation in the seed-1 log:

```
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#236    t=55.879722ms  inc=2 deliver     Started => FSList{io1 /data/tmp}
#254    t=60.075667ms  inc=3 deliver     Started => FSList{io1 /data/tmp}
```

The reuse is safe because a completion for incarnation 2's `io1` can never be delivered to incarnation 3: the simulator drops every queued entry of a dead incarnation (`toys3/sim/queue.go:101-122`). But IOIDs leak into one durable place: **temp file names**. `TmpPath` names a temp file after the IOID of the `FSWrite` that creates it (`toys3/format/layout.go:73-79`):

```
#26  ... FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)}
#56  ... FSWrite{io17 /data/tmp/0000000000000011 obj(value-of-9)}
```

(`0x11` = 17.) Since every incarnation restarts at `io1`, incarnation 3 may well create `tmp/0000000000000004` when incarnation 2 already created a file of that name. The design relies on recovery to empty `tmp/` before a new incarnation writes anything (the comment at `toys3/format/layout.go:73-75` says exactly this). Mutant (f) in Chapter 15 removes that cleanup and shows the result: a reused temp name that is secretly a second hard link to a committed object.

### `ErrCode`

```go
// toys3/control/plant.go:27-35
// ErrCode is a filesystem error as plain data, so events stay comparable and
// hashable.
type ErrCode uint8

const (
	OK          ErrCode = iota
	ErrNotFound         // ENOENT: the path (or rename source) does not exist
	ErrIO               // EIO or anything else; the operation may or may not have taken effect
)
```

Three values, deliberately. Why not use Go's `error`?

- **`error` is an interface holding arbitrary values**, often pointers (`*fs.PathError`), often wrapping other errors. Two `error`s describing the same failure are not `==`. They cannot be hashed deterministically. They carry strings with paths and errno numbers that differ between platforms.
- **The server only needs to distinguish what it acts on differently.** `ErrNotFound` drives 404s (GET, DELETE, COPY of a missing source) and is tolerated during recovery's unlinks. Everything else gets the same treatment: a 500, and possibly a `Trip` (Chapter 7). A richer error type would invite the server to make distinctions it has no basis for.
- **`ErrIO` is pessimistic by definition.** The comment says "the operation may or may not have taken effect". That one phrase is the whole philosophy of the error policy. The PlantModel honours it adversarially: a failed rename is applied or not with probability ½ each (`toys3/sim/plantmodel.go:351-366`).

Note that `OK` is the zero value. A completion constructed without an `Err` field, like `FSWriteDone{IO: x.IO}`, is a success. The mutants rely on this to fabricate successes (`fakeOK`, `toys3/sim/mutants_test.go:19-33`).

The production adapter maps OS errors onto these three values in exactly one place:

```go
// toys3/prod/executor.go:101-113
// code maps an os error to a control.ErrCode, logging real I/O errors (the core
// only sees the code).
func code(err error) control.ErrCode {
	switch {
	case err == nil:
		return control.OK
	case errors.Is(err, fs.ErrNotExist):
		return control.ErrNotFound
	default:
		log.Printf("toys3: io error: %v", err)
		return control.ErrIO
	}
}
```

`String()` renders the codes as `OK`, `ENOENT`, `EIO` (`toys3/control/plant.go:37-47`), which is what you see in logs: `FSReadDone{io4 ENOENT nil}`, `FSFsyncDone{io78 EIO}`.

### `Status`

```go
// toys3/control/types.go:22-30
// Status is the HTTP status of a Reply.
type Status uint16

const (
	StatusOK          Status = 200
	StatusBadRequest  Status = 400 // invalid key or object too large; nothing was touched
	StatusNotFound    Status = 404
	StatusServerError Status = 500 // filesystem error or corrupt object; outcome of a mutation is unknown
)
```

Four statuses, each with a precise contract that the oracle relies on (Chapter 13):

| Status | Meaning to the client | Oracle treatment |
|---|---|---|
| 200 | The operation happened, exactly as replied (for GET: this is the value) | `Definite`: must be linearized |
| 400 | Rejected before touching anything | Dropped from the window entirely (`toys3/sim/oracle.go:278-280`) |
| 404 | The key (or COPY source) was absent at the operation's linearization point | `Definite` |
| 500 | Unknown: a mutation may or may not have taken effect; a read says nothing | `Failed`: optional, at most once |

The HTTP handler maps `Status` straight onto the HTTP status code (`w.WriteHeader(int(rep.Status))`, `toys3/prod/http.go:75`). Production can also produce 405 and 503, but those are generated by `prod` itself (unsupported method, loop stopped) and never by the controller.

---

## 3.3 Events (`event.go`, plus the completions in `plant.go`)

### The envelope: `Event`

```go
// toys3/control/event.go:3-8
// Event is the input to Step. The runtime stamps At when it dequeues the event,
// so At never decreases across successive Steps of one incarnation.
type Event struct {
	At   Time
	Body EventBody
}
```

The envelope separates *when* (stamped by the runtime) from *what* (the body). Bodies are created by whoever produces the event (the workload for requests, the filesystem for completions), and they never contain a time. This is why the simulator can schedule a body for delivery at any future instant without rewriting it.

### `Started`

```go
// toys3/control/event.go:16-18
// Started is the first event of every boot, including after a crash. Nothing
// in memory survived; the server begins recovery (see package doc).
type Started struct{}
```

An empty struct: the information is entirely in its arrival. On `Started` the server moves from `modeStartup` to `modeRecovering` and issues `FSList` of `tmp/` (`toys3/control/recovery.go:17-26`). A second `Started` in the same incarnation is ignored (`toys3/control/recovery.go:18-20`; tested at the end of `TestStaleIOIgnored`, `toys3/control/controller_test.go:431`).

Requests can arrive *before* `Started`. In the simulator, a restart schedules `Started` at the current instant and also wakes parked clients (`toys3/sim/simulator.go:322-325`); a client's think time can be zero, so a client's request can be scheduled for the same instant and, depending on the seeded tie-break, be delivered first. The server handles this by queuing requests in `modeStartup` exactly as in `modeRecovering` (`toys3/control/controller.go:179-182`: a request only starts if `s.mode == modeAuto`). Chapter 5 tells the full lifecycle.

### `Request` and the `Op` set

```go
// toys3/control/event.go:22-53
// Request is a client request that has been fully received and decoded. The
// server answers it with exactly one Reply carrying the same Client id (unless
// it crashes first).
type Request struct {
	Client ClientReq
	Op     Op
}

// Op is a closed set of object operations. Keys are opaque, non-empty strings
// (the HTTP layer flattens "bucket/key" into one key); see ValidKey.
type Op interface{ isOp() }

// Put stores Data under Key, replacing any existing object.
type Put struct {
	Key  string
	Data []byte
}

// Get returns the object stored under Key.
type Get struct{ Key string }

// Delete removes Key. Deleting an absent key replies 404.
type Delete struct{ Key string }

// Copy stores the current contents of Src under Dst. Src == Dst is allowed
// (it rewrites the object). An absent Src replies 404 and leaves Dst alone.
type Copy struct{ Src, Dst string }
```

The contract on `Request` is the most important sentence in the file: **exactly one `Reply` per request, unless the server crashes first.** Two invariants police it. If the server ever replies to an unknown or already-answered request, the simulator reports a `protocol` failure (`toys3/sim/simulator.go:229-234`). If a request never gets a reply while the server is alive in the Settling phase, `Liveness` reports it at the end of the run (`toys3/sim/invariant.go:229-234`).

"Fully received and decoded" means the server never sees partial requests. In production the HTTP handler reads the whole body first (`io.ReadAll`, `toys3/prod/http.go:46`). This is a simplification; `docs/lessons.md` §6 explains why the larger Kafka system will instead feed raw bytes into the core so that framing bugs are simulated too.

Op semantics in brief (Chapter 6 traces each protocol step):

| Op | Effect | Replies |
|---|---|---|
| `Put{Key, Data}` | Replace the object | 200, 400 (invalid key or `len(Data) > MaxObjectSize`), 500 |
| `Get{Key}` | Read the object | 200 with body, 404, 400 (invalid key), 500 (I/O error or corrupt file) |
| `Delete{Key}` | Remove the object | 200, 404 (absent), 400, 500 |
| `Copy{Src, Dst}` | Read Src, then the PUT path for Dst | 200, 404 (Src absent; Dst untouched), 400, 500 |

Keys are "opaque, non-empty strings". The HTTP layer turns `/bucket/key` into the single key `bucket/key` (`splitKey`, `toys3/prod/http.go:109-117`). Validity is `ValidKey`: non-empty, and short enough that the escaped file name fits in 255 bytes (`toys3/format/layout.go:58-62`). Key escaping is covered in Chapter 4.

In logs, `DescribeOp` (`toys3/sim/log.go:11-27`) renders each op; for a PUT whose data is a self-describing workload value it prints the writer instead of the bytes:

```
#4      t=0.444359ms   inc=1 deliver     Request{c1 GET "k2"}
#9      t=0.745782ms   inc=1 deliver     Request{c3 PUT "" value-of-3} => Reply{c3 400}
#12     t=0.762895ms   inc=1 deliver     Request{c4 COPY "k2" -> "k0"}
#14     t=1.142951ms   inc=1 deliver     Request{c5 DELETE "k0"}
#21     t=3.397177ms   inc=1 deliver     Request{c6 PUT "k2" value-of-6}
```

Step 9 is the workload's deliberate invalid request (an empty key, `toys3/sim/workload.go:109-114`), answered with 400 in the same step, before the server has even finished recovery. Validation needs no I/O, so the server does it immediately (`toys3/control/controller.go:150-175`); invalid requests never enter the queue.

### The seven completions (feedback from the plant)

Every FS action has exactly one completion type, carrying the same `IO` and an `Err`. They are defined in `plant.go`, because each one is the plant's answer to one command; §3.5 shows the interface, `Feedback`, that names them as a group:

```go
// toys3/control/plant.go:103-149 (abridged to the doc comments and fields)
// FSWriteDone completes FSWrite. On ErrIO the file may hold any prefix of Data.
type FSWriteDone struct { IO IOID; Err ErrCode }

// FSFsyncDone completes FSFsync. On ErrIO nothing is known to be durable.
type FSFsyncDone struct { IO IOID; Err ErrCode }

// FSRenameDone completes FSRename. On ErrIO the rename may or may not have
// happened.
type FSRenameDone struct { IO IOID; Err ErrCode }

// FSFsyncDirDone completes FSFsyncDir.
type FSFsyncDirDone struct { IO IOID; Err ErrCode }

// FSReadDone completes FSRead. Data is the raw file (header included) when
// Err == OK, nil otherwise.
type FSReadDone struct { IO IOID; Data []byte; Err ErrCode }

// FSUnlinkDone completes FSUnlink. ErrNotFound means the path did not exist.
type FSUnlinkDone struct { IO IOID; Err ErrCode }

// FSListDone completes FSList. Names are base names, sorted ascending.
type FSListDone struct { IO IOID; Names []string; Err ErrCode }
```

Each doc comment is a statement about **what the server may assume after an error**, and each is matched by what the PlantModel actually does (Chapter 11):

| Completion | On `ErrIO` the server may assume... | PlantModel behaviour on a planned failure |
|---|---|---|
| `FSWriteDone` | the file holds *any prefix* of the data (including nothing, or not existing) | one of: nothing happened; file created/truncated empty; a random prefix written (`toys3/sim/plantmodel.go:298-307`) |
| `FSFsyncDone` | *nothing* is known to be durable | nothing becomes durable; with `FsyncGate`, the inode is poisoned so later fsyncs lie (`toys3/sim/plantmodel.go:330-335`) |
| `FSRenameDone` | the rename *may or may not* have happened | applied or not, 50/50 (`toys3/sim/plantmodel.go:351-366`) |
| `FSFsyncDirDone` | nothing about the directory is durable | nothing becomes durable (`toys3/sim/plantmodel.go:374-376`) |
| `FSReadDone` | nothing (no data) | no effect (`toys3/sim/plantmodel.go:391-393`) |
| `FSUnlinkDone` | the unlink *may or may not* have happened | applied or not, 50/50 (`toys3/sim/plantmodel.go:411-420`) |
| `FSListDone` | nothing (no names) | no effect (`toys3/sim/plantmodel.go:428-430`) |

`ErrNotFound`, by contrast, is never injected by chance: "Missing paths complete with ErrNotFound (never by chance)" (`toys3/sim/plantmodel.go:164`). The PlantModel returns it only when the path really is absent in the current view, even when the plan said to fail (`TestPlantModelEIOMayOrMayNotApply` checks this at `toys3/sim/plantmodel_test.go:151-154`). That asymmetry matters: the server treats `ErrNotFound` as a *definite* answer (404), so it must be truthful.

Two details about the data-carrying completions:

- **`FSReadDone.Data` is the raw file, header included.** The server, not the filesystem, validates the header and CRC with `DecodeObject` (`toys3/control/controller.go:289-292`). The PlantModel's read may flip one bit in the returned copy (`ReadCorruptProb`), which only the CRC can detect. The doc says `nil` on error; on success with an empty file, the PlantModel returns `[]byte{}`, not `nil` (`toys3/sim/plantmodel.go:394-397`), and `DecodeObject` rejects it as too short.
- **`FSListDone.Names` are sorted.** Sorting is part of the contract so that recovery's unlinks are issued in a deterministic order. The PlantModel lists via its sorted name cache (`toys3/sim/plantmodel.go:431-437`); production relies on `os.ReadDir`, which returns entries sorted by file name (`toys3/prod/executor.go:61`).

In logs, `DescribeEvent` (`toys3/sim/log.go:47-69`) renders completions compactly. A selection from seed 1:

```
#10   ... deliver  FSListDone{io1 OK []} => FSFsyncDir{io2 /data/tmp}
#18   ... deliver  FSReadDone{io4 ENOENT nil} => Reply{c2 404}
#56   ... deliver  FSReadDone{io15 OK obj(value-of-6)} => Reply{c7 200 value-of-6} FSWrite{io17 ...}
#233  ... deliver  FSFsyncDone{io78 EIO} => Reply{c30 500} Trip{fsync temp: EIO}
#246  ... deliver  FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]} => FSUnlink{io2 ...} FSUnlink{io3 ...} FSUnlink{io4 ...}
#252  ... deliver  FSUnlinkDone{io2 EIO} => Trip{recovery: unlink temp: EIO}
```

`obj(value-of-6)` is `describePayload` (`toys3/sim/log.go:29-44`) recognizing a well-formed object file whose payload is the value written by request 6. If the file failed its header or CRC check, it would print `raw(N bytes)` instead. Here is that case, from seed 18, where a read came back with one bit flipped:

```
#656    t=153.982439ms inc=7 fs-land     land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657    t=154.136154ms inc=7 deliver     FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptionDetected{"k3"}} Reply{c98 500}
```

29 bytes is a 16-byte header plus a 13-byte payload. The bytes on "disk" were fine; the read lied. The server's CRC check caught it, emitted a `CorruptionDetected` telemetry, and replied 500. Chapter 15 shows the same seed and step with mutant (e), which skips the CRC check and serves the garbage with a 200.

---

## 3.4 Actions (`action.go`, plus the filesystem commands in `plant.go`)

```go
// toys3/control/action.go:3-7
// Action is an output of Step: a request for the runtime to do something.
// Every filesystem action carries an IOID; its result returns as the matching
// ...Done event. Actions in one returned slice are started in order, but the
// filesystem actions among them may complete in any order.
type Action interface{ isAction() }
```

"Started in order" is a real guarantee both runtimes provide: production's `dispatch` loops over the slice (`toys3/prod/loop.go:79`), the simulator's `deliver` likewise (`toys3/sim/simulator.go:185`). It matters most for the `Emit`/`Reply`/`Trip` ordering below.

### `Reply`

```go
// toys3/control/action.go:11-17
// Reply answers the Request with the same Client id. Body is the object
// payload for a 200 GET and nil otherwise.
type Reply struct {
	Client ClientReq
	Status Status
	Body   []byte
}
```

The body is the *payload* (header stripped), aliasing the read buffer. In production, `dispatch` hands the `Reply` to the HTTP handler waiting on that `ClientReq` through a buffered channel (`toys3/prod/loop.go:81-88`). In the simulator, `reply` records it in the oracle, frees the client, and schedules the client's next wakeup (`toys3/sim/simulator.go:229-238`).

In logs, a reply with a body shows the writer: `Reply{c7 200 value-of-6}`.

### The seven filesystem actions (actuations)

These are defined in `plant.go` too; §3.5 introduces `Actuation`, the interface they share. The paragraph that introduces them is a compact statement of POSIX durability, and Chapter 4 is an extended commentary on it:

```go
// toys3/control/plant.go:49-54
// ---- filesystem ----
//
// Paths are slash-separated and absolute under Config.Dir (see ObjPath,
// TmpPath). Changes are volatile until made durable: file contents by FSFsync
// of the file, directory entries (create, rename, unlink) by FSFsyncDir of the
// directory that holds them.
```

Two separate durability domains: **file contents** (made durable by `FSFsync` of the file) and **directory entries** (made durable by `FSFsyncDir` of the directory containing them). Every action's doc comment says which domain it touches:

```go
// toys3/control/plant.go:56-101 (abridged)
// FSWrite creates Path, or truncates it if it exists, and writes Data as the
// whole file content. Creating the entry is a directory change (needs
// FSFsyncDir to be durable); the content needs FSFsync.
type FSWrite struct { IO IOID; Path string; Data []byte }

// FSFsync makes the content of the file at Path durable.
type FSFsync struct { IO IOID; Path string }

// FSRename atomically replaces To with From (POSIX rename). Both paths must be
// on the same filesystem. Durable only after FSFsyncDir of To's directory (and
// of From's, for the old name to be durably gone).
type FSRename struct { IO IOID; From, To string }

// FSFsyncDir makes the directory entries of Dir durable.
type FSFsyncDir struct { IO IOID; Dir string }

// FSRead reads the whole file at Path.
type FSRead struct { IO IOID; Path string }

// FSUnlink removes the directory entry Path.
type FSUnlink struct { IO IOID; Path string }

// FSList lists the entry names in Dir (used by recovery).
type FSList struct { IO IOID; Dir string }
```

Some observations that matter later:

- **`FSWrite` is whole-file, create-or-truncate.** There is no append and no offset. This narrows what the simulator needs to model (a write either lands entirely in the volatile view or, on failure, lands as a prefix) at the cost of not modelling partial overwrites in the middle of a file. It also means that writing to an *existing* name overwrites that inode's content in place. That is exactly how POSIX `open(O_TRUNC)` behaves, and it is the mechanism behind mutant (f) in Chapter 15.
- **`FSRename` has two directories.** The doc is careful: the rename is durable only after an `FSFsyncDir` of `To`'s directory, *and* of `From`'s for the old name to be durably gone. The server fsyncs `obj/` after each rename (to make the new name durable), but never fsyncs `tmp/` after a rename. So after a crash, the old temp name can come back as a second name for the committed object's inode. You can see this happen in seed 1: incarnation 2's recovery listing contains `0000000000000031`, the temp file of `c23`'s PUT, which had been renamed to `obj/k1` back at step 160:

  ```
  #159  ... deliver  FSFsyncDone{io51 OK} => FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1}
  #160  ... fs-land  land FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1} -> FSRenameDone{io53 OK}
  ...
  #233  ... crash    CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
  ...
  #246  ... deliver  FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]} => FSUnlink{io2 /data/tmp/0000000000000031} ...
  ```

  This is harmless for the correct server (recovery unlinks the extra name, which leaves `obj/k1` intact) and fatal for mutant (f). Chapters 5, 11 and 15 return to it.
- **`FSFsyncDir` applies to every entry in the directory**, not just the one you care about. In the PlantModel it copies the current version of every entry to durable (`toys3/sim/plantmodel.go:377-381`). On real filesystems this is the semantics of `fsync` on a directory file descriptor.
- **`FSList` exists only for recovery.** No client operation lists anything (there is no ListObjects).

In logs, paths appear in full: `FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2}`. The simulator's data directory is `/data` (`toys3/sim/config.go:173`).

### `Trip`

```go
// toys3/control/action.go:21-26
// Trip is a safety trip: it asks the runtime to kill this incarnation and
// restart it (fail-stop), after executing the actions that precede it in the
// same slice. Use it when in-memory state can no longer be trusted, e.g. after
// an fsync error. Cause says why. In simulation it is an ordinary crash; in
// production the process exits and a supervisor restarts it.
type Trip struct{ Cause string }
```

`Trip` is how the server says "I no longer trust my own view of the disk; kill me." It is not an error return: it is an instruction to the runtime, sequenced with the other actions. The 500 that precedes it is delivered first; then the incarnation dies. The server guarantees `Trip` is the last action of its slice (`trip` appends it last and sets `modeTripped`, `toys3/control/controller.go:143-146`), and the simulator enforces that as a protocol invariant:

```go
// toys3/sim/simulator.go:200-207
		case control.Trip:
			if i != len(acts)-1 {
				s.fail(Failure{Invariant: "protocol", Msg: "Trip is not the last action of its step"})
				return
			}
			s.res.Stats.Trips++
			s.crash("trip: " + x.Cause)
			return
```

The reason string appears verbatim in logs: `Trip{fsync temp: EIO}`, `Trip{recovery: unlink temp: EIO}`. In seed 1, 9 of the 10 crashes were trips. Chapter 7 is entirely about when to trip and why.

### `Emit`

```go
// toys3/control/action.go:30-32
// Emit publishes Telemetry. The simulator's invariants read it; production
// logs it. Emit has no effect on behavior.
type Emit struct{ Telemetry Telemetry }
```

`Emit` is observability as an action. Because the server cannot do I/O, it cannot log either; instead it returns `Emit{Telemetry}` and the runtime decides what to do with it. "No effect on behavior" is a promise worth keeping: if behaviour depended on whether telemetry was recorded, turning on logging would change runs. In production, each telemetry value becomes a log line (`toys3/prod/loop.go:89-90`):

```
toys3: telemetry control.Committed {Client:1 Key:photos/cat.jpg Deleted:false Sum:222957957}
```

In the simulator, each telemetry value is appended to a timeline, stamped with the step, time and incarnation; a stamped value is a `Sample` (`toys3/sim/invariant.go:12-19`, appended at `toys3/sim/simulator.go:192-199`), and invariants read the samples incrementally (Chapter 14).

---

## 3.5 The plant port (`plant.go`)

The fourteen filesystem types above have something in common that the type system, until the refactor, did not say: every one of them has an `IO IOID` field, and every command is paired with exactly one completion carrying the same `IO`. Before the refactor, code that needed "the IOID of this thing, if it is a filesystem thing" had to spell out a seven-way type switch. There were five such switches: one in the controller (`ioOf`), one inside the controller's `issue` helper, one in each of two test harnesses (`ioOfAction`), and one in the simulator's `deliver` that listed all seven command types in a `case`. Adding an eighth filesystem operation meant finding and editing all five.

`plant.go` now names the two groups:

```go
// toys3/control/plant.go:8-20
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
```

and gives every filesystem type a one-line `ID()` method:

```go
// toys3/control/plant.go:159-165
func (a FSWrite) ID() IOID    { return a.IO }
func (a FSFsync) ID() IOID    { return a.IO }
func (a FSRename) ID() IOID   { return a.IO }
func (a FSFsyncDir) ID() IOID { return a.IO }
func (a FSRead) ID() IOID     { return a.IO }
func (a FSUnlink) ID() IOID   { return a.IO }
func (a FSList) ID() IOID     { return a.IO }
```

(and the same seven lines for the `...Done` types at `toys3/control/plant.go:175-181`). Two compile-time assertions at the end of the file (`toys3/control/plant.go:183-186`) make sure at least one type of each kind satisfies its interface, so a typo in a method signature fails the build rather than silently dropping a type out of the group.

Why a method called `ID` and not `IO`? Because the field is already called `IO`, and Go does not allow a field and a method with the same name on one type. Renaming the field to `ID` would have changed every struct literal in the book, the tests and the logs (`FSWrite{IO: ...}`); naming the method `ID()` changed nothing that already existed. That is a small example of a principle the refactor followed throughout: *add names, do not move data*.

Where the port is used:

| Before (a type switch) | After (one interface) |
|---|---|
| `ioOf(ev.Body)` in `Step` | `fb, ok := ev.Body.(Feedback); io := fb.ID()` (`toys3/control/controller.go:119-123`) |
| a switch in `issue` re-deriving the IOID from each action type | `issue(r, step, a Actuation)` records `a.ID()` (`toys3/control/controller.go:235-240`) |
| `case control.FSWrite, control.FSFsync, ... , control.FSList:` in the simulator | `case control.Actuation:` (`toys3/sim/simulator.go:187`) |
| `ioOfAction` in two test harnesses | a three-line type assertion to `Actuation` (`toys3/control/controller_test.go:147-152`, `toys3/sim/mutants_test.go:269-274`) |

Why "actuation" and "feedback"? Chapter 3a develops the metaphor in full; the short version is that the controller never touches the disk, it only *commands* it, and the disk *reports back*. In control engineering the commands a controller sends to the thing it controls are actuations, the thing being controlled is the plant, and what comes back is feedback. The word "port" is from hexagonal architecture (ports and adapters): the controller owns the interface, and two adapters implement it, `prod.OSExecutor` on a real disk (Chapter 9) and `sim.PlantModel` in simulation (Chapter 11). Note who owns what: the port is defined in package `control`, not in the packages that implement it. The adapters conform to the controller's language, not the other way round.

The POSIX names (`FSWrite`, `FSFsync`, `FSRename`, ...) were deliberately *not* renamed. They are the plant's own language; "actuation" is the category, `FSFsync` is the command. A control engineer does not rename a valve's "open" command to "actuate".

---

## 3.6 Telemetry (`telemetry.go`)

```go
// toys3/control/telemetry.go:3-12
// Telemetry is a closed set of structured signals the controller publishes
// for supervisory monitoring. They describe what the controller believes and
// have no effect on behavior. Any number of subscribers may read them:
// production logs them; the simulator's monitors (invariants) check those
// beliefs against ground truth (the plant model's durable state and the
// client-visible history), and its coverage stats count them. A telemetry
// value may carry fields only a monitor needs (Committed.Sum).
//
// Every type is a domain event, named in the past tense.
type Telemetry interface{ isTelemetry() }
```

"They describe what the controller believes and have no effect on behavior." That framing is the key to understanding why telemetry exists. A telemetry value is the controller asserting something about the world; the simulator, which knows ground truth, checks it.

Three phrases in that comment were added by the refactor, and each one carries a design decision:

- **"for supervisory monitoring ... Any number of subscribers"**. In process control, *telemetry* is the set of signals a controller publishes for whoever supervises it: an operator's screen, a historian that records everything, an alarm system. The controller does not know or care who listens. toys3 has two subscribers: the production log (a historian) and the simulator's monitors (the invariants, and the coverage counters). Before the refactor these signals were called *facts*, and the package doc described them in terms of their one simulated customer ("the simulator checks those beliefs"). The new name says what they are from the controller's side: a published signal, not a test hook.
- **"A telemetry value may carry fields only a monitor needs (Committed.Sum)"**. Nothing in production reads `Sum`; it exists so that a monitor can compare the controller's claim with the disk's true contents. Saying so in the type's doc stops a future reader from "cleaning up" an apparently unused field.
- **"Every type is a domain event, named in the past tense."** In domain-driven design a *domain event* records something that has happened, and is named accordingly: `Committed`, `Recovered`. The third type used to be called `CorruptObject`, a noun describing a thing rather than an event. It is now `CorruptionDetected`, which is both past tense and honest about what happened: the controller did not *corrupt* anything, it *detected* corruption (fault detection, in control vocabulary).

### `Committed`

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

This is the most powerful telemetry signal in the system. Without it, the only way to detect "acked but not durable" would be to wait for a crash that happens to lose the change and a later read that happens to notice. With it, the `AckedIsDurable` invariant can look at the PlantModel's *durable* view at the instant of the ack (`toys3/sim/invariant.go:139-165`): if `obj/k2` has no durable entry, or its durable content does not decode to a payload with CRC `Sum`, the server lied, and the run fails *on that step*, without any crash at all. That is why mutants (a) and (c) are caught at seed 1, step 46 and 42 respectively (Chapter 15).

The server emits it in `commit`, right before the reply, in the same `Step` (`toys3/control/controller.go:255-262`). In logs: `Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200}`; for a delete, `Committed{c56 delete "k0"}` (`DescribeTelemetry`, `toys3/sim/log.go:102-116`).

Because `Committed` is a white-box check (it compares the server's belief against the fake disk's hidden state), it has a black-box counterpart: the linearizability oracle eventually notices lost acked writes from the clients' point of view, after a crash. Chapter 14 compares the two.

### `Recovered`

```go
// toys3/control/telemetry.go:26-30
// Recovered is emitted once per boot when recovery finishes and the
// controller starts serving queued requests.
type Recovered struct {
	TempsRemoved int
}
```

Emitted at the end of recovery, as the first action of the step that also starts the queued requests (`toys3/control/recovery.go:80-83`):

```
#271 ... deliver  FSFsyncDirDone{io4 OK} => Emit{Recovered{temps=2}} FSWrite{io5 /data/tmp/0000000000000005 obj(value-of-35)} FSRead{io6 /data/obj/k1} FSRead{io7 /data/obj/k2}
```

The `Liveness` invariant checks at the end of a run that the final incarnation emitted it (`toys3/sim/invariant.go:220-228`), and `TempsRemoved` feeds the `tempsrm` statistic. In the run of mutant (f) in Chapter 15, `tempsrm=0` over 13 crashes is the first clue that recovery is not doing its job.

### `CorruptionDetected`

```go
// toys3/control/telemetry.go:32-38
// CorruptionDetected is emitted when an object file fails its header or CRC
// check (fault detection). A correct controller with an honest filesystem
// never emits it; the simulator counts it (coverage) and the replies around
// it must be 500.
type CorruptionDetected struct {
	Key string
}
```

Emitted by GET or COPY when `DecodeObject` rejects the file (`toys3/control/controller.go:289-292`). For a COPY, `Key` is the *source* key (the one that was read, `readKey`). The comment's claim, "a correct server with an honest filesystem never emits it", is checked by `TestCorruptionUnreachableWithoutBitFlips` (`toys3/sim/sim_test.go:124-133`): with `ReadCorruptProb = 0`, 200 seeds produce zero `CorruptionDetected` telemetry. The CRC is a safety net, not a mechanism. In the 200-seed run from the preface, `readcorrupt=71 corruptdet=70`: every bit flip that reached the server was caught. (Why one fewer? A read can land and then have its completion dropped by a crash before the server sees it. Exercise 4.)

---

## 3.7 The environment (`env.go`)

```go
// toys3/control/env.go:3-26
// Env is injected into the server at construction. It is the only way
// nondeterminism enters the control. Time is NOT here: it arrives on each Event.
type Env interface {
	Rand
	Buggify
}

// Rand is a source of randomness. In simulation it draws from the run's single
// seeded stream; in production it may be any generator.
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). It panics if n <= 0.
	Intn(n int) int
}

// Site is the stable identity of a buggify point. Never interpolate values.
type Site string

// Buggify is FoundationDB-style cooperative fault injection: code asks "should
// I take the rare-but-legal path here?" (e.g. process the queue in a different
// legal order) so the simulator can explore it. Production always says no.
type Buggify interface {
	Maybe(site Site) bool
}
```

Three implementations exist:

| Implementation | `Uint64` / `Intn` | `Maybe` |
|---|---|---|
| `prod.Env` (`toys3/prod/env.go:9-14`) | `math/rand/v2` | always `false` |
| `sim.env` (`toys3/sim/simulator.go:399-408`) | the run's single `sim.Rand` | `e.buggify && e.rng.Chance(0.05)` |
| `control.testEnv` (`toys3/control/controller_test.go:14-19`) | constants (4 and 0) | always `false` |

The simulator's `Maybe` deserves a careful read:

```go
// toys3/sim/simulator.go:408
func (e *env) Maybe(control.Site) bool { return e.buggify && e.rng.Chance(0.05) }
```

Because `&&` short-circuits, **it consumes a random draw only when buggify is enabled**. Buggify is switched off when Chaos ends (`s.env.buggify = false`, `toys3/sim/simulator.go:162`), so in the Settling phase a buggify site would neither fire nor perturb the random stream. The `Site` argument is ignored: every site fires with the same flat 5% per call. That is simpler than the scheme described in `docs/lessons.md` §7 (each site activated once per run with 50% probability, then firing per call at 25%), which is designed to avoid every site being "a little bit on" all the time; the toys3 implementation has not needed that sophistication because, as the next paragraph explains, nothing calls it.

### The surprise: the server never uses `Env`

`Controller.env` is assigned in `New` (`toys3/control/controller.go:104-106`) and never read. There is no `s.env.` anywhere in `controller.go`, no call to `Maybe` anywhere in `toys3/control`, and no `Uint64`/`Intn` either. The unit-test environment admits it: "a fixed control.Env (the server does not draw randomness today)" (`toys3/control/controller_test.go:14`). `DESIGN.md` says "Randomness and buggify come from the injected `Env`" (`toys3/DESIGN.md:84`), which is true of the plumbing and not yet of the behaviour.

Why keep it? Because adding a buggify site later should be cheap and should not require changing every runtime. And it is a good exercise to think about *where* one would go. The `Buggify` doc suggests an example: "process the queue in a different legal order". In toys3, `startPermitted` starts every runnable request in arrival order (`toys3/control/controller.go:205-213`); a buggify point could start them in reverse order, which is equally legal because runnable requests touch disjoint keys. Exercise 5 asks you to add exactly that and see what it does to the trace hashes.

One consequence to be aware of: the moment the server calls `Maybe` during Chaos, it consumes draws from the shared stream, which shifts every later random decision in the run. Every trace hash changes, and every "caught at seed N" number in `DESIGN.md` may change. That is not a bug; it is what "one seeded stream" means. It is why the canary compares two runs of the *same* code, never a run against a stored hash.

---

## 3.8 `machine.go`, revisited

Chapter 2 covered `StateMachine`, `Hashable`, `Machine`, `Factory` and `NewMachine`. One more observation in light of this chapter: `Factory` takes an `Env` and a `Config`, and those are the *only* inputs to a new incarnation besides the events it will receive. There is no "previous state" argument. A factory cannot pass anything from a dead incarnation to a new one, which is the type-level expression of rule 9 in `loop.md` ("anything in memory is treated as disposable").

---

## 3.9 The vocabulary as the attack surface

Put all of this together and you get a precise answer to the question "what can the simulator do to the server?". It can:

- choose *when* each event arrives (`Event.At`) and in *which order* (queue ties);
- choose each FS operation's outcome among the outcomes its completion type allows (`OK`, `ErrNotFound` only when truthful, `ErrIO` with the documented "may or may not" semantics);
- choose what bytes a successful read returns (correct, or one bit flipped);
- choose to never deliver a completion (crash) after its effect did or did not land;
- choose what survives a crash, entry by entry and file by file;
- send a `Started` to a brand-new object built by the `Factory`.

And it can check:

- every `Reply` against the client-visible history (the oracle),
- every `Committed` against ground truth (the durable view),
- every `Trip` for placement,
- every `Emit{Recovered}` for liveness.

Anything outside the vocabulary (a slow `Step`, a server that allocates unbounded memory, an HTTP framing bug, a real kernel's journaling behaviour) is outside the simulator's reach. Chapter 17 lists those gaps explicitly.

```
          what the world can say                       what the server can say
      ┌───────────────────────────────┐            ┌──────────────────────────────┐
      │ Started                       │            │ Reply{Client, Status, Body}  │
      │ Request{Client, Op}           │            │ FSWrite / FSFsync / FSRename │
      │ FS*Done{IO, Err [, Data|Names]}│  ──Step──► │ FSFsyncDir / FSRead          │
      │   Err ∈ {OK, ENOENT, EIO}     │            │ FSUnlink / FSList            │
      │   At (when)                   │            │ Trip{Cause}                  │
      └───────────────────────────────┘            │ Emit{Committed|Recovered|    │
             (FS*Done = Feedback)                  │   CorruptionDetected}        │
                                                   └──────────────────────────────┘
                                                      (FS* commands = Actuation)
```

## Key takeaways

- The vocabulary is closed (unexported marker methods) so the whole interface can be read in three files; Go does not check type switches for exhaustiveness, so both server and simulator have defensive defaults, and the simulator's default is to fail loudly.
- `ClientReq` is unique for a whole run and names writers; `IOID` is unique only within an incarnation, which is safe for routing but leaks into temp file names.
- `ErrCode` has three values on purpose. `ErrIO` always means "may or may not have taken effect", and the PlantModel realizes both possibilities. `ErrNotFound` is never injected by chance.
- File contents and directory entries are separate durability domains, made durable by `FSFsync` and `FSFsyncDir` respectively. The server never fsyncs `tmp/` after a rename, so temp names can resurrect after a crash.
- `Trip` is an action, ordered after the 500 it accompanies, and must be last.
- The filesystem commands and completions are the plant port: `Actuation` and `Feedback`, each with an `ID() IOID`. The port is owned by the controller and implemented by two adapters, one real and one simulated.
- Telemetry is the controller's published beliefs (domain events, past tense); `Committed` lets the simulator check "acked implies durable" at the moment of the ack, without waiting for a crash.
- `Env` is plumbed through but unused by the server today; adding the first buggify site will change every trace.

## Exercises

1. **Exhaustiveness by hand.** Add a new action `FSTruncate{IO IOID; Path string}` to `toys3/control/plant.go` (with its `isAction` marker but *without* an `ID()` method) in a scratch copy, make the server emit it at the start of every PUT, and run `go test ./toys3/sim -run TestSeeds -seed=1 -v`. Predict which invariant fails and with what message before running. (Answer: the `protocol` default in `deliver`, because without `ID()` the action is not an `Actuation`.) Now add the `ID()` method and run again. What changes? (Now `deliver` hands it to the plant model, and `PlantModel.Land` panics with "not a filesystem action": the plant adapter, not the port, is where an unimplemented command is discovered.) Revert.
2. **ErrNotFound must be honest.** In a scratch copy, change `PlantModel.read` so that a planned failure on an existing file returns `ErrNotFound` instead of `ErrIO`. Predict which invariant catches this (think about what a 404 means to the oracle), then run `go test ./toys3/sim -run 'TestSeeds$' -seeds=50`. Revert.
3. **Read the log fluently.** `TestSeeds -seed=N` prints the event log only when the seed fails, so to read a passing seed's log you need a few lines of Go that set `cfg.Log = true` and print `sim.FormatLog(r.Log)` (Chapter 16 shows a ready-made program; any package that imports `manifester/toys3/sim` will do). Produce the log for seed 1. Find the first `Trip` in the log. Write down: the request that got the 500, the IOID that failed, which protocol step it was, and how many queued entries the crash dropped.
4. **The missing corruption.** In the 200-seed run, `readcorrupt=71` but `corruptdet=70`. Using the simulator's code (`KindFSLand` handling at `toys3/sim/simulator.go:134-145` and `crash` at `toys3/sim/simulator.go:276-307`), explain how a corrupted read can be counted without the server ever seeing it. Then find a seed where it happens: loop over seeds with the scratch tracer and compare the two counters per seed.
5. **Your first buggify site.** In a scratch copy, change `startPermitted` (`toys3/control/controller.go:205-213`) to iterate `s.reqs` in reverse when `s.env.Maybe("startPermitted/reverse")` is true. (You will need to collect runnable requests first, since `start` does not change membership of `s.reqs`.) Run `go test ./toys3/sim -run TestDeterminismCanary` and `go test ./toys3/sim -run 'TestSeeds$' -seeds=200`. Does determinism still hold? Do any seeds fail? Did the trace hash of seed 1 change? Explain why it must have.
6. **Telemetry ordering.** `Committed` is emitted *immediately before* its `Reply`, in the same step. In a scratch copy, swap them in `commit` (`toys3/control/controller.go:255-262`) so the `Emit` comes after the `Reply`. Does any test or invariant notice? Why or why not? (Look at when `AckedIsDurable` runs relative to the actions of a step.)

## Further reading

- Rob Pike, *"Go Proverbs"* (2015), especially "Make the zero value useful", which `ErrCode`'s `OK = 0` embodies.
- The Go specification, "Type switches" and "Method sets", for why an unexported method closes an interface to other packages.
- Jonathan Corbet, *"Ensuring data reaches disk"*, LWN.net (2011), a lucid account of the file-content versus directory-entry durability split that `action.go` encodes. (Chapter 4 goes deeper.)
- Will Wilson's Strange Loop 2014 talk and the FoundationDB documentation on `BUGGIFY`, for the origin of the `Buggify` interface.
- `docs/lessons.md` in this repository, §1 (errors that say what they prove: `NotApplied` / `MaybeApplied` / `Applied`) and §7 (buggify as a first-class interface). toys3's `ErrIO` is the `MaybeApplied` case without the other two being distinguished.
