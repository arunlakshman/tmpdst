# Deterministic simulation testing in Go (Part 1)

This is part 1 of 3 on **deterministic simulation testing (DST)** in Go. It stands alone: you do not need the rest of the series or the repo's other packages. Companion code for every snippet lives in [minikv](minikv/). (When this post is published outside the tree, that path becomes a GitHub URL.)

We will teach the idea first--what must be controlled, how to wrap it for production and simulation, why Go forces a single event loop--then walk a thin example: a key-value store with PUT and GET, file-backed values, and `fsync`. The goal is not perfect code. The goal is that a Go engineer finishes knowing what DST is and how to start one.

One motivating bug, kept short: a client PUTs `v2` over `v1`, the server renames after writing a temp file, a directory fsync fails, the server replies 500 and keeps serving, a GET still sees `v2` from the page cache, then power loss restores `v1`. An acked-or-at-least-observed write vanished. Unit tests of the error path pass. Integration tests cannot fail fsync on cue or cut power between observations. Stress tests might hit it once a year with no replay. DST is how you force and replay that class of failure.

---

## 1. Why do you need DST?

Distributed and storage bugs live in **interleavings**: request arrival, I/O completion order, and crashes. Ordinary tests cannot force a chosen interleaving, and they cannot replay one after a flake.

DST runs the **same server code** in a **controlled world**. One integer--the seed--decides delays, tie-breaks, and (in later posts) faults. Same seed, same run. A failure is a number you re-run, not a ghost in a log.

---

## 2. What is deterministic vs nondeterministic in code?

A function is deterministic when the same inputs always produce the same outputs. In Go services, "inputs" quietly include the wall clock, the disk and network, the process RNG, and the runtime scheduler. Those are the usual leaks. Naming them is enough for now; the next sections say why each one breaks replay, and later sections show how to box them.

---

## 3. Why each source is nondeterministic

### Time

`time.Now`, timers, and sleeps read a wall clock and jitter that the OS and runtime do not freeze for you.

```go
func deadline() time.Time {
    return time.Now().Add(50 * time.Millisecond) // different every run
}
```

Two runs of "the same" test can schedule timeouts in different orders, or expire one that the other does not.

### I/O

Disk and network latency, failures, and completion order are decisions of the kernel and devices. You do not seed them.

```go
go writeFile(pathA, dataA) // which Sync returns first?
go writeFile(pathB, dataB)
```

A bug that needs "B's rename before A's fsync" is invisible to a test that only checks return values on a quiet machine.

### Random

`math/rand`'s global generator is shared and, unless you own the seed end-to-end, not part of your test's contract. Unseeded use is different every process.

```go
n := rand.Intn(100) // global source; not "your" seed
```

Any branch that depends on that draw will disagree across runs even when every argument you passed in matches.

---

## 4. The solution pattern: wrap nondeterminism

Put every nondeterministic call behind a **common interface** (or plain events and actions) that production and simulation both implement. Do not build a separate simulation model of the server. Models drift; they miss the policy bug in the code you ship. Test that server.

- **Time.** Give the runtime a clock, or stamp virtual time on each event (`Event.At`). The server never calls `time.Now`.
- **I/O.** The server returns actions (`Write`, `Fsync`, `Rename`, ...) instead of calling `os`. Production performs them with real syscalls. The simulator performs them on a fake disk and feeds `*Done` events back.
- **Random.** One RNG, seeded once per run. Every delay and tie-break draws from it.

Production and simulation share the server. They do not share the disk or the clock. That split is the whole architecture.

---

## 5. Go-specific: the goroutine scheduler

Any ready goroutine can run next. That choice lives in the runtime. You cannot seed it or replay it. `select` among several ready cases is the same class of problem: the language picks uniformly at random.

Hiding the filesystem behind an interface is not enough if each request still blocks in its own goroutine. Completions still interleave however the scheduler wakes those goroutines. Determinism at the library level, in Go, means the code under test does not leave ordering to the runtime.

---

## 6. Go-specific solution: single event loop

One goroutine owns the business logic. In [minikv](minikv/) that method is `Step`. Concurrent work is **data**--a pending request, queue entries--not many logic goroutines.

The loop is:

1. Pop the next event from a priority queue.
2. Call `Step`.
3. For each returned action, schedule a completion at `now + seeded_delay`.

Handwritten lists of events only replay stories you already imagined. The loop closes the gap: the next events come from the actions `Step` returned, so the interleaving is a function of the seed, not of a script you typed.

---

## 7. Worked example: minikv

[minikv](minikv/) is a tiny PUT/GET store written for this series. Values live as files under `obj/`, with the usual temp-write / fsync / rename / directory-fsync care. This post's disk never fails; Part 2 will teach it to crash. Below are the shapes that matter. Full files stay in the module.

### Shape: `Step(Event) []Action`

Everything the server learns arrives as an event. Everything it causes leaves as an action. Between the two there is no I/O, no clock, and no other goroutine.

```go
type Event struct {
    At   Time
    Body EventBody
}

type Action interface{ isAction() }

func (s *Server) Step(ev Event) []Action
```

Key event and action types (names only; fields in [kv.go](minikv/kv.go)):

- Events in: `Request`, `WriteDone`, `FsyncDone`, `RenameDone`, `FsyncDirDone`, `ReadDone`
- Actions out: `Write`, `Fsync`, `Rename`, `FsyncDir`, `Read`, `Reply`

### PUT phases (why four steps)

```
Request PUT k=v2
  -> Write  tmp/<id>       data in a new file; old obj/k still visible
  -> Fsync  tmp/<id>       data will survive power loss
  -> Rename tmp -> obj/k   readers see v1 or v2, never a mix
  -> FsyncDir obj          the name obj/k -> v2 will survive power loss
  -> Reply 200
```

Short why: fsync on the file makes the **bytes** durable; fsync on the directory makes the **name** durable. Skip either, and an ack can lie after a crash. This post's perfect disk treats both fsyncs as no-ops; the server still issues them so Part 2 can honor them without rewriting `Step`.

### Queue order `(At, Tie, Seq)`

Earlier virtual time wins. Same instant: a tie-break drawn from the seed. Still tied: insertion sequence.

```go
func (q *Queue) less(i, j int) bool {
    a, b := &q.items[i], &q.items[j]
    if a.At != b.At {
        return a.At < b.At
    }
    if a.Tie != b.Tie {
        return a.Tie < b.Tie
    }
    return a.Seq < b.Seq
}
```

### Perfect disk as a map

```go
type Disk struct {
    files map[string][]byte
}

// Fsync / FsyncDir: return *Done, change nothing.
// Write / Rename / Read: clone bytes in or out; never range the map.
```

Fsync is a no-op here on purpose. Crashes and forgotten writes are Part 2.

### Seeded RNG and the `Run` loop core

One SplitMix64 stream owns every draw. The loop pops, steps, and schedules completions with a seeded delay (1..500 us in this harness):

```go
func Run(seed uint64) Result {
    rng := NewRNG(seed)
    q := &Queue{}
    disk := NewDisk()
    srv := &Server{}
    // ... schedule the fixed request script ...

    for /* until queue drains */ {
        e, _ := q.Pop()
        now := e.At
        ev := /* deliver request, or apply(disk, action) -> *Done */
        actions := srv.Step(Event{At: now, Body: ev})
        for _, a := range actions {
            if _, ok := a.(Reply); ok {
                continue
            }
            delay := Time(rng.Intn(500)+1) * Microsecond
            q.Schedule(rng, now, Entry{
                At: now + delay, Kind: kindComplete, Action: a,
            })
        }
    }
    // ...
}
```

### Checkpoint

From [minikv](minikv/):

```
go test
go test -v -run TestDeterminism
```

`go test` passes. Seed 1 prints hash `c43a5572d697e036` (16 steps, 27 RNG draws). Run it twice; the hash does not move. That fingerprint is a practical check that the run is a function of the seed--not the thesis of DST, just proof the harness is sealed.

---

## 8. Checklist and next posts

To DST your own Go service:

1. List every use of time, I/O, and random in the core path.
2. Express the core as `Step(event) []action` (or an equivalent single-owner API). Ban `time.Now`, blocking `os`, and unseeded `rand` inside it.
3. One goroutine owns `Step`. Waiting work is structs in queues, not parked logic goroutines.
4. Implement a production runtime (real clock, real disk) and a sim runtime (virtual clock, fake disk, seeded delays).
5. Drive the sim from one seed; hash or log the trace; assert seed N run twice agrees.
6. Add faults only after the loop is deterministic (crashes, torn writes--Part 2).
7. Check correctness with an oracle or model (Part 3), not only self-agreement of the hash.

**Part 2** replaces the perfect disk with a crashing one. **Part 3** checks invariants and plants bugs to prove the oracle can fail.

### Optional exercises

- **DELETE.** Add `Unlink` / `UnlinkDone`, reply 200 or 404, extend the script, keep one outstanding op. On a perfect disk you can skip a directory fsync; on a crashing disk you will want one for the same reason PUT does.
- **Lint.** Parse the module with `go/parser` and fail on an import of `"time"` in the deterministic core. The hash catches mistakes that already changed a run; the lint catches the import before you take the branch.

---

Companion module: [minikv](minikv/). Relative links work in-repo; on publish, point them at the GitHub tree for that directory.
