# Chapter 3. The deterministic core takes events and returns actions

Chapter 2 shows the system from outside. You see a Kafka broker, an object store, and a control plane (CP). The CP assigns offsets. This chapter explains why `core/` sits apart from that work.

> All decisions live in a pure function. All effects live outside it. The two sides speak only in plain data.

The pure function is `Step(Event) []Action` (`core/machine.go`). You give `core.Machine` one `Event`. The machine changes its own state and returns a slice of `Action` values. The machine does no work in the outside world.

A driver does the outside work for the machine. The production runtime (`sys/run.go`) is one driver. The simulator (`sim/`) is another driver. Each driver feeds an event in, runs the returned actions, and feeds the results back as new events.

The split exists so that you can replay an execution. If a machine reads a clock, opens a file, or talks to the network, the run depends on inputs that no one recorded. You cannot replay the run from a seed. If a machine takes only events and returns only actions, you control every input. The same seed and the same event sequence give the same execution. You can run a failing invariant again, step by step, until you see the step that breaks the invariant.

This design costs you work. You must name each input as an `Event` and each output as an `Action`. You must write a driver that runs the actions and reports the results. The cost is worth the gain. A bug that you can replay is a bug that you can fix and then prove fixed. A bug that depends on a clock read or a network result may not occur again on demand.

## 3.1 The core holds the contracts and leaves the work to other packages

`core/` holds contracts and terms. The package does not hold the main logic. The table below lists the files and the contract that each file defines.

| File | Defines |
|---|---|
| `core/doc.go` | The package rules in one comment block |
| `core/types.go` | Time, IDs, `ErrCode`, `Outcome`, shared scalars |
| `core/event.go` | `Event` / `EventBody`, every possible input |
| `core/action.go` | `Action` / `IOAction`, every possible output |
| `core/machine.go` | `StateMachine`, `Hashable`, `Machine`, `Factory` |
| `core/env.go` | `Env` = `Rand` + `Buggify` + `Assertions` |
| `core/fact.go` | `Fact`, structured and append-only observations |
| `core/inbox.go` | Event `Class`, `Queue`, `InboxPolicy` contract |
| `core/policy.go` | `ControlFirst`, the one inbox policy |
| `core/kafka.go` | `Framer` / `Codec`, Kafka wire work as pure functions |
| `core/controlplane.go` | `CPOp` / `CPResp`, with invariants attached |

You start with the tests. `core/core_test.go` pins the classification table and the frozen `ErrCode` spellings. `core/policy_test.go` pins inbox priority with a slice-backed queue. Neither test needs a broker, a clock, or a network. The tests need no clock or network because `Step` takes time and results as data. That fact shows the split at work.

## 3.2 The Step contract lets one machine run under two drivers

```go
// core/machine.go
type StateMachine interface{ Step(Event) []Action }
type Hashable interface{ StateHash() uint64 }
type Machine interface{ StateMachine; Hashable } // hashing is not optional
type Factory func(env Env, self NodeRef) Machine
```

The contract has four clauses, and the clauses follow.

1. `Step` is the only place where state changes. If a value comes from outside the machine, the value re-enters as an event. The machine sees no value that does not re-enter as an event.
2. `Step` never blocks and `Step` never does `I/O`. The machine changes private state and returns requests (`core/machine.go:3-6`). The driver runs the requests. Section 3.11 covers the driver.
3. Hashing is mandatory. The simulator folds `StateHash()` into a trace hash after every step. You can check the rule that the same seed gives the same hash sequence. The broker version (`broker/hash.go:42`) folds each field that changes behaviour. The broker folds the fields in a fixed order. The fields are counters, queue lengths, per-connection slot state, and segment and op state. The hash omits timing fields that come from event times and that behaviour does not read. One example is `sealedAt` (`broker/segment.go:68-73`).
4. The runtime rebuilds machines and the runtime does not resume machines. `Factory` runs on first boot and after every crash. `Factory` uses a fresh `Incarnation` each time (`core/machine.go:23-25`). No memory content survives a crash. Recovery reads data from the CP and the object store.

Clause 2 explains why the runtime can swap the driver without a change to the machine. If `Step` did `I/O` itself, the production path and the simulation path would run different code. The test would not test the code that runs in production. Because `Step` returns requests, both drivers run the same machine. The drivers differ. The decisions do not differ.

`core/event.go:14-20` states a fifth clause. A completion answers an `I/O` action. If a completion has an unknown `ReqID`, the machine ignores the completion. If a completion has an already-finished `ReqID`, the machine ignores the completion. If a completion has a wrong-kind `ReqID`, the machine ignores the completion. The state does not change and the hash does not change.

Retries explain the fifth clause. A retry can produce a duplicate completion. A completion can arrive after the machine gave up on the request. If such a completion changed state, the result would depend on the order in which outside work finished. That order is not part of the recorded event sequence in the form the machine sees. Replay would lose its fixed result.

The broker handlers scan for a segment or an op in the expected state with that `ReqID`. The handlers return `nil` when the handlers find no match (`broker/broker.go:216-242`). `TestIgnoresStrayCompletions` (`broker/broker_test.go:517`) asserts a bit-identical hash after stray `CPDone`, `ObjPutDone`, `ObjGetDone`, `SendDone`, and `TimerFired` events. If you add a machine, you write the same test for that machine.

## 3.3 Plain data and closed sets keep every input visible

Events and actions use plain data. The data types are numbers, strings, booleans, byte slices, and nested plain structs. The data does not include functions, channels, runtime pointers, or Go `error` values. Errors use the `ErrCode` enum "so events stay comparable and hashable" (`core/types.go:57`).

Plain data matters because the driver and the simulator must record, compare, and hash what passes between the two sides. A function value or a channel hides work and state. You cannot hash the hidden work or compare the hidden state. Plain data makes the whole input visible in one place.

The event set and the action set are closed sets. Unexported marker methods seal the sets. The methods are `isEvent()` and `isAction()` (`core/event.go:11`, `core/action.go:8`). Only `core` can add a member. A new `EventBody` also needs a row in `ClassOf` (section 3.11). `ClassOf` panics on an unclassified type. The code calls that case "a programming error" (`core/inbox.go:31-33`).

A closed set costs you a central change for each new kind of input or output. An open set would let a package add a kind without a change to `core`. The code accepts the central cost. A closed set lets a reader list every input and every output, and the simulator can check that list. That check has more value here than local ease of extension.

Two rules follow from the closed sets, and the rules follow.

- Byte slices do not change after a machine sends the byte slices (`core/doc.go:9`). If you change a `Send` payload or an `ObjPut` payload after you send the payload, you can change data that code has hashed or stored.
- Names do not change after code hashes the names. `ErrCode.String()` spellings "are never changed" (`core/types.go:90`). `Fact` names are "frozen, past-tense constants, never derived from Go type names. Renaming one re-baselines the golden hash" (`core/fact.go:5-9`). Enums are append-only. `ErrCode` keeps a retired slot (`core/types.go:71`) so later numbers do not move.

## 3.4 Time arrives on the event so that replay controls time

The core has no clock, and the absence is the point. If a machine reads a clock inside `Step`, two runs of the same event sequence read two clock values. The runs can take two branches. No seed can fix the branch, because no one recorded the clock read.

Each event carries two stamps instead (`core/event.go:5-9`). `At Time` holds node-local monotonic nanoseconds. Code uses `At` for deadlines, leases, batch windows, and backoffs. `Wall WallTime` holds UTC nanoseconds. Code uses `Wall` for stored values such as Kafka `LogAppendTime` and credential expiry. The runtime and the simulator stamp both fields at dequeue. The stamp order keeps `At` from decreasing in Step order. The production loop reads `b.Clock.Mono()/Wall()` at that point (`sys/run.go:40-43`).

A machine treats the current event `At` value as the current time. The broker caches `b.now = ev.At` at the start of `Step` (`broker/broker.go:89`). The broker derives segment ages, deadlines, and fact timings from stamps in events that the broker has seen. If a machine needs a wake-up, the machine requests the wake-up with `SetTimer`. The machine then waits for `TimerFired`. `core/env.go:2-3` states the rule for `Env`. "Time is NOT here: it arrives on each Event".

The other choice is a clock that a test can replace. A fake clock still lets code read time at a point that the event sequence does not show. The reader must find each read to know the input. An event stamp puts the time in the one value that the machine already takes. The reader sees the time input without a search.

The list below gives the main scalars from `core/types.go`, and the list follows.

- `ID [16]byte` comes from the seeded stream. The code states "drawn from the seeded stream (never `uuid.New`)" (`core/types.go:23`). Replay cannot see unseeded randomness.
- `NodeRef{Node, Inc}` names one boot of a node. A restarted node is a new machine with empty memory.
- `ReqID` links an action to a completion. The code states "unique within one incarnation only; it must never reach a durable name (object keys come from `Env.NewID()`)" (`core/types.go:30-33`).
- `ErrCode` has rules for each value. `ErrNotFound` and `ErrConflict` report the result without change. Engines inject only `ErrTimeout`, `ErrUnavailable`, `ErrIO`, and `ErrConnClosed`. `ErrCorrupt` comes only from the core decoder (`core/types.go:60-63`).
- `Outcome` states what a completion proves. The values are `NotApplied`, `MaybeApplied`, and `Applied` (`core/types.go:80-88`). Retry rules use `Outcome`. A lost response gives `ErrTimeout+MaybeApplied`. A lost response does not give silence. Code retries an operation only when the operation is idempotent.

## 3.5 Env names each source of nondeterminism that replay must control

```go
// core/env.go
type Env interface{ Rand; Buggify; Assertions }
```

`Factory` injects `Env` once. `Env` lists each source of nondeterminism that a simulation must control for replay. Before you use a part of `Env`, you check the part against both implementations.

Random draws, fault choices, and assertion records can change a run. If code takes the inputs from a hidden source, the simulator cannot set the inputs or record the inputs. `Env` puts the three sources behind one interface. The simulator gives the machine a seeded form. Production gives the machine a live form. The machine code stays the same.

`Rand` is defined at `core/env.go:11-18`. `Rand` provides `Uint64`, `Intn(n) ∈ [0,n)`, and `NewID`. In sim, all machines of a seed share one run stream (`sim/assert.go:127-133`). Draw order forms part of the replayed execution. Production seeds ChaCha8 from `crypto/rand` (`sys/prod/env.go:22-31`). Production uses the same shapes and production does not promise replay. The broker draws `NewID()` for segment keys (`broker/segment.go:166`). The broker draws `Intn` for commit-retry jitter (`broker/segment.go:348`). Replay requires a record of each draw. Replay does not require few draws.

`Buggify` is defined at `core/env.go:20-34`. `Buggify` provides FoundationDB-style fault injection. `Maybe(site)` covers paths that occur with low probability and stay valid. A run activates a site first. The default probability after activation is p=0.25 (`sim/assert.go:104,115`). `MaybeP(site, p)` takes a probability from the caller. `Fault(site, p)` makes an operation fail. The code states that `Fault` "never fires once the simulator has entered recovery mode". Sim gates `Fault` on the chaos phase (`sim/assert.go:121`). `Site` is a stable string. Code must not put values in a `Site` string. Production returns `false` in each case (`sys/prod/env.go:51-53`).

> Caveat. A grep check verifies this point. No `broker/` code calls `Maybe`, `MaybeP`, or `Fault` today. Sim engines fail, delay, or drop completions around the broker. The broker receives the results as `Err*` completions and timer expiries. Production gives the broker the same event forms. Sim implements Buggify. Other engines use fault contexts. A new broker site needs a stable name. A new broker site must not include values. A new broker site must return `false` in production.

`Assertions` is defined at `core/env.go:45-57`. `Assertions` provides `Always`, `AlwaysOrUnreachable`, `Sometimes`, `Reachable`, and `Unreachable`. Each method records a result and continues. No method panics. Campaign rules differ by method, and the rules follow. `Always` must hold each time code reaches the call. A campaign must reach `Always` at least once. `Sometimes` and `Reachable` must occur at least once across a campaign. `AlwaysOrUnreachable` limits only executions that reach the call. Sim records a violation and runs on. Sim records the first violation per property per seed (`sim/assert.go:30-72`). Prod logs a violation and continues (`sys/prod/env.go:55-77`). Prod treats `Sometimes` and `Reachable` as no-ops. The broker calls record facts that you can check in code. One call is `Reachable("broker: segment sealed by timer")` (`broker/segment.go:133`). One call is `Unreachable("broker: commit key reused with a different request")` (`broker/segment.go:251`). One call is `AlwaysOrUnreachable(shape, "broker: commit answers every batch in order")` (`broker/segment.go:237`).

## 3.6 The event and action tables list the whole contract

The two tables in this section are reference apparatus. The explanation in the sections before this section points at the tables. You read the tables to check which inputs and outputs exist.

The list is complete as of this chapter. The source lists are `core/event.go:114-126` and `core/action.go:110-124`. If you find a diff against those lists, you have a bug in this chapter or an unreviewed core change.

### Events (`EventBody`, wrapped in `Event{At, Wall, Body}`)

| Event | Class | Answers / source | Meaning |
|---|---|---|---|
| `Started{Self}` | Control | first event of every boot | Rebuild from CP + store; nothing survived |
| `Stop{Deadline}` | Control | runtime shutdown | Stop admitting, finish/hand off, `Exit` by deadline |
| `ConnOpened{Conn, Peer}` | Data | listener | New connection; `Peer` informational only |
| `BytesReceived{Conn, Data}` | Data | reader | Arbitrary TCP chunk; framing is the core's job |
| `ConnClosed{Conn, Abort}` | Data | reader/network | EOF (graceful) or reset (abort) |
| `SendDone{Req, Conn, Err}` | Control | `Send` | `Err != OK`. Bytes may be partially sent |
| `DialDone{Req, Conn, Err}` | Control | `Dial` | `Conn` valid only if `Err == OK` |
| `TimerFired{ID, Gen}` | Control | `SetTimer` | Ignored if `Gen` is stale for `ID` |
| `ObjPutDone{Req, Key, Err, Outcome}` | Control | `ObjPut` | Upload verdict |
| `ObjGetDone{Req, Data, Err}` | Control | `ObjGet` | Download verdict + bytes |
| `ObjDeleteDone{Req, Err, Outcome}` | Control | `ObjDelete` | Delete verdict |
| `ObjListDone{Req, Keys, Err}` | Control | `ObjList` | `Keys` sorted |
| `CPDone{Req, Resp, Err, Outcome}` | Control | `CPCall` | `Resp` nil unless `Err == OK` |

The seven `*Done` bodies implement `Completion` (`ReqID()`). The other bodies are primary inputs. A machine does not need a case for each event. The broker has no case for `SendDone`, `DialDone`, `ObjDeleteDone`, or `ObjListDone` (`broker/broker.go:90-111`). Those events fall through without a state change. Section 3.2 states the rule.

### Actions (one slice per `Step`)

| Action | IO? | Completed by | Meaning |
|---|---|---|---|
| `SetTimer{ID, Gen, After}` | no | `TimerFired{ID, Gen}` | Wake after a node-local duration |
| `CancelTimer{ID}` | no | none | Optimisation only; correctness relies on `Gen` |
| `Send{Req, Conn, Data}` | yes | `SendDone` | Bytes, ordered with earlier `Send`s on the conn |
| `CloseConn{Conn, Abort}` | no | none | FIN after queued bytes, or RST |
| `PauseReads{Conn}` / `ResumeReads{Conn}` | no | none | Admission control; received bytes still process |
| `Dial{Req, Addr}` | yes | `DialDone` | Outbound conn (sim workloads; brokers use `CPCall`) |
| `ObjPut{Req, Key, Data}` | yes | `ObjPutDone` | Key unique per upload ⇒ `MaybeApplied` PUT safe to retry |
| `ObjGet{Req, Ref}` | yes | `ObjGetDone` | Fetch a `SegmentRef` byte range |
| `ObjDelete{Req, Key}` | yes | `ObjDeleteDone` | Delete object |
| `ObjList{Req, Prefix}` | yes | `ObjListDone` | List keys under prefix |
| `CPCall{Req, Op}` | yes | `CPDone` | One control-plane op (§3.10) |
| `Emit{Fact}` | no | none | Append a fact (section 3.8) |
| `Trip{Cause}` | no | none | Kill this incarnation; memory untrusted |
| `Exit{}` | no | none | Last action of a graceful shutdown |

`core/action.go` states the ordering rules for one returned slice. Actions in one slice start in slice order. `I/O` actions can complete in any order (`core/action.go:5-7`). An `IOAction` gets one completion. The code states the completion rule in full. "a live incarnation always gets that completion: a lost response surfaces as `ErrTimeout+MaybeApplied`, never as silence; completions addressed to a dead incarnation are dropped" (`core/action.go:10-13`). Runtimes must keep that rule. `Trip` and `Exit` are each "the last action of its Step" (`core/action.go:94-100`). `Trip` returns `ErrTripped` in prod for the supervisor. `Exit` does not count as a fault.

Timers use generations because a cancel can race with a firing. If correctness rested on the cancel, the race would decide the result. The race sits outside the machine, so the machine cannot record the race as an input. `CancelTimer` is only an optimisation. Each firing carries `Gen` and the machine ignores a stale generation. The code describes timers as "matched by generation, not cancelled" (`core/event.go:70-71`). The broker raises one counter for each arming (`broker/broker.go:79-82`). The broker checks `s.gen != gen` before the broker acts (`broker/segment.go:357`). The runtime can cancel the host timer (`sys/run.go:113-117`). The model does not include a cancel and fire race as a state input.


## 3.7 One Step in the broker shows the split in code

```go
func (b *Broker) Step(ev core.Event) []core.Action {
    if b.exited { return nil }
    b.now = ev.At                      // time comes from the event, §3.4
    var acts []core.Action
    switch e := ev.Body.(type) {      // closed set: only these cases exist
    case core.Started:   b.started = true
    case core.Stop:      acts = b.stop()
    case core.ConnOpened:    acts = b.onOpen(e)
    case core.BytesReceived: acts = b.onBytes(e)
    case core.ConnClosed:    acts = b.onClosed(e)
    case core.TimerFired:    acts = b.onTimer(e)
    case core.ObjPutDone:    acts = b.onPut(e)
    case core.ObjGetDone:    acts = b.onGet(e)
    case core.CPDone:        acts = b.onCP(e)
    }
    return b.finish(acts)              // drain ready replies, flow control, maybe Exit
}
```

`Broker.Step` (`broker/broker.go:85-120`) shows four points, and the points follow. The switch has no `default` case with side effects. Unhandled bodies fall through without a state change. `SendDone` and `DialDone` in section 3.6 are examples. The common `finish` call runs after the switch. `finish` does not create work for an event that the machine ignored. Each handler returns actions and no handler performs the actions. The response drain in `finish` appends `Emit` and `Send` actions to the slice. Handlers check the correlation before the handlers act. `onPut` and `onCP` scan segments for `s.req == e.Req && s.state == segPut/segCommit` (`broker/broker.go:216-242`). The scan makes the stray-completion rule part of the handler code.

The handlers use no lock, no channel, no goroutine, no clock read, and no map range. The handlers need none of those controls. The runtime gives one thread to `Step` at a time and the runtime states "this goroutine alone owns the machine" (`sys/run.go:24`). One owner at a time removes the need for a lock inside the machine.

If you add a machine, you follow the same form. You cache `ev.At`. You dispatch on the closed `EventBody` set. You change only your own fields. You match completions by `ReqID` and by expected state. You return actions. You fold state that changes behaviour into `StateHash`. If a design needs a background goroutine or a `time.Sleep`, you use a timer event instead. Section 3.12 gives an example.

A background goroutine would break the form. The goroutine would run between steps and the goroutine could change state or read a clock between steps. The event sequence would not show that work. A timer event keeps the wake-up in the event sequence, where the simulator can order the wake-up and record the wake-up.

## 3.8 A machine returns facts because a log line is an effect

A machine cannot log from `Step`. A log call writes output, and the write is an effect inside the decision step. Log calls also have no fixed order that code can hash. A machine returns `Emit{Fact}` instead. The driver writes the fact, and the machine keeps the decision in data. The code follows.

```go
// core/fact.go
type Fact struct{ Name string; Fields []Field } // Name: frozen past-tense constant
```

Field values use only `I64/U64/Str/Bool/Bytes` (`core/fact.go:20-35`). Facts use values that code can hash and compare. Prod treats `Emit` as tracing. Sim invariants read the timeline (`core/action.go:107`). One ordering rule controls ack checks. The code states the rule in full. "an ack fact is emitted in the same Step, immediately before the `Send` that acks" (`core/fact.go:7-9`). The broker queues `produce_acked` on the request slot at commit time (`broker/segment.go:370-385`). `drain` emits queued facts before the `Send` for the request (`broker/conn.go:187-191`). If an ack fact sits at another point, the check for the "acked ⇒ committed" invariant loses the fact order that the check needs. `BugEarlyAck` in section 3.14 shows the failure.

## 3.9 The bans on clocks, goroutines, and map order protect replay

Each ban in this section removes one input that a seed cannot control. A clock read gives a value that depends on the moment of the read. A goroutine gives an order that depends on how the Go runtime schedules the goroutine. A map range gives an order that Go changes between runs. Any one of the three inputs can send two runs of the same seed down two paths. The bans trade ease for control, and the trade is right here. Code inside a machine is short and runs often. A recorded input costs little at each step. A run that no one can replay costs a long search each time a test fails.

`core/doc.go` states the rules. `tools/detcheck` and `scripts/ci-determinism.sh` enforce the rules over `core/ sim/ format/ kwire broker/ veritas` and more packages. The rules follow.

1. Deterministic packages do not import `time`, `math/rand`, `crypto/rand`, `os`, `net`, `sync`, or SDK packages. The check uses an exact-path allow-list (`tools/detcheck/allow.txt`). Kafka types come from the franz-go `kmsg` package. The `kgo` client "is never used here" (`core/kafka.go:1-3`).
2. Deterministic packages do not use goroutines, channels, or `select`. Detcheck reports each use. Concurrency stays outside the machine. The machine runs one `Step` for one event at a time.
3. Map iteration does not decide behaviour. Go changes map iteration order between runs. Detcheck flags `range` over maps and `reflect` map iteration. The broker keeps slice state. The state holds `conns`, `segs`, `ops`, and `topics []core.TopicInfo` (`broker/broker.go:46-58`). Code scans the slices in insertion order.
4. Deterministic packages do not use package-level mutable vars. Global state can carry data between sim runs in one process.
5. Machines use plain data only. Byte slices do not change after a machine sends the byte slices (section 3.3).

The slice in rule 3 is the other choice to a map. A slice has an order that code sets and code can hash. The scan costs more than a map lookup in some cases. The code pays the scan cost because the order stays fixed across runs. A sorted map key list would also fix the order. The code in the broker uses slices, and a new machine follows that form.

If a reported use is correct, a code comment can allow the one use. The form is `//detcheck:ok <reason>` on the same line or the previous line. The reason is required (`tools/detcheck/detcheck.go:13-17`). State after n steps is a function of the seed and the event sequence. `TestStateHash` requires bit-identical hash sequences across two runs (`broker/broker_test.go:482-503`) for that reason. CI runs each seed twice.

## 3.10 Wire work and control plane work stay inside the same contract

Kafka wire handling uses `core/kafka.go`. The code states that wire handling "runs inside the deterministic path (decision A)". `Framer` puts length-prefixed frames back together from chunks. Each connection has one `Framer`. The `Framer` forms part of machine state and the `Framer` uses plain data. `Codec` decodes requests and encodes responses in both directions. Chunk boundaries cannot change behaviour because the partial frame is state. The broker hashes `Buffered()` and a running receive hash (`broker/hash.go:62-64`). `TestStateHash` shows that partial frames with different bytes hash to different values.

If wire work sat outside the machine, the driver would decide where a frame starts and ends. Two drivers could split the same bytes in two ways. The machine would see two inputs for one client request. The code keeps the `Framer` in machine state so that the chunk split cannot change the decision.

The control plane uses `core/controlplane.go`. The file defines three ops and three responses. The ops are `CommitSegment`, `Lookup`, and `DescribeTopics`. Both sets are closed sets. The Produce trace in section 3.12 uses the rules in this section. Brokers hold no registration, epoch, or fencing state. Brokers do not use idempotent producers. The CP assigns offsets and the broker does not propose an offset (`core/controlplane.go:7-11`). `CommitSegment` assigns offsets for a multi-partition segment in one operation. The operation assigns all batches or no batches. Offsets stay contiguous per partition. The CP does not reuse offsets (`core/controlplane.go:16-24`). `CommitSegment` must be idempotent per `SegmentKey`. A replay with the same key and the same request returns the original ranges. The same key with a different request returns `ErrConflict`. The code calls this case "the only use of `ErrConflict`" (`core/controlplane.go:25-28`).

## 3.11 One policy picks the next event so that Go cannot pick the order

Machines do not pick the next event with `select`. If a machine uses `select`, Go picks one ready case without a fixed order. That choice adds scheduling order from Go to the model, and a seed cannot control the choice. The next event then depends on the runtime and not on the recorded input.

Each event has a class instead (`core/inbox.go:8-18`). `Control` covers completions, timers, and boot. `Control` has strict priority. The state machine caps in-flight work, and that cap bounds Control traffic. `Data` covers connection bytes. `Data` traffic has a bound and `PauseReads` gives backpressure. `ClassOf` (`core/inbox.go:22-35`) holds a fixed table. `ConnClosed` has class `Data` so "a client's EOF can never overtake its last request."

Strict priority can starve `Data` in a design without bounds. The design here has bounds on both queues, and the machine caps in-flight work. The bound is the reason that strict priority is safe here. Without the bound, a fair policy would be the safer choice.

The policy function is pure and stateless. The code states "the SAME code in production and simulation" (`core/inbox.go:45-56`). The code follows.

```go
// core/policy.go — ControlFirst.Next(control, data Queue)
if control.Len() > 0 { return control.Pop() }
if data.Len() > 0 { return data.Pop() }
return nil, false // caller sleeps until either queue is non-empty
```

Production uses `sys.Inbox` for the queues. `sys.Inbox` uses a mutex and slices (`sys/inbox.go`). Sim and tests use a plain slice (`core/policy_test.go:5-15`). Both forms use the same interface, the same policy, and the same step order. The runtime can sleep only while both queues are empty. The rule is "Go's `select` must never decide priority." Bounded in-flight work bounds control traffic. Data events run after the machine processes the results of prior work. Starvation does not occur under the bound.

## 3.12 One Produce request shows each decision as a step

The trace shows one batch to topic `t`, partition 0. The trace shows events and actions only. That limit is the point of the trace. You can read each decision without work from the driver between the decisions. Chapter 4 covers encoding. Chapter 5 covers segment format. Broker behaviour comes from `broker/produce.go`, `broker/segment.go`, and `broker/conn.go`.

```mermaid
sequenceDiagram
    participant C as Client (TCP)
    participant R as Runtime / Sim kernel
    participant B as Broker (core.Machine)
    participant S as Object store
    participant P as Control plane
    C->>R: TCP bytes (Produce request)
    R->>B: Step(Event{At, Wall, BytesReceived}) [Data]
    Note over B: frame, decode, validate, append to open segment
    B-->>R: [SetTimer{timerFlush, Gen=g, After=BatchWindow}]
    R->>B: Step(Event{At', Wall', TimerFired{timerFlush, g}}) [Control]
    Note over B: first batch's window expired — seal
    B-->>R: [CancelTimer, Emit(segment_sealed), ObjPut{Req=r1, Key=seg/…, Data}]
    R->>S: execute ObjPut r1
    S-->>R: ObjPutDone{Req=r1, Err=OK, Outcome=Applied}
    R->>B: Step(Event{…, ObjPutDone r1}) [Control]
    B-->>R: [Emit(segment_put_done), CPCall{Req=r2, CommitSegment{Key, Batches}}]
    R->>P: execute CPCall r2
    P-->>R: CPDone{Req=r2, Resp=CommitSegmentResp{Assigned}, Err=OK}
    R->>B: Step(Event{…, CPDone r2}) [Control]
    Note over B: take Base from Assigned; queue produce_acked; drain response
    B-->>R: [Emit(segment_committed), Emit(produce_acked), Send{Req=r3, Conn, Data}]
    R->>C: TCP bytes (Produce response)
    S-->>R: SendDone{Req=r3, Err=OK} (later)
    R->>B: Step(Event{…, SendDone r3}) [Control — no case, ignored]
```

The points below check the trace against the contracts, and the points follow.

- No step does `I/O`. You can swap the driver between the prod executor and a sim engine, and the steps stay the same.
- The ack waits for the CP. Step 1 admits the batch. The broker answers after an OK `ObjPutDone` and an OK `CPDone`. The answer uses `Assigned[i].Base` and the broker does not guess the base.
- Retries use normal steps. A transient `ObjPutDone` leads to `SetTimer{Gen=g'}`. The matching `TimerFired` issues the same `ObjPut` under the same key (`broker/segment.go:315-328`). A transient `CPDone` or a `MaybeApplied` `CPDone` uses the same `SegmentKey`. The broker caps jittered backoff at `CommitDeadline` (`broker/segment.go:330-354`). Section 3.10 states the idempotency rule that allows this retry. Only the head segment commits. CP commits occur in seal order. A partition receives offsets for the batches of the partition in admission order (`broker/segment.go:202-209`).
- Facts follow a fixed order. The order is `segment_sealed`, `segment_put_done`, `segment_committed`, and `produce_acked`. The `produce_acked` fact comes before the `Send` for the fact.
- The final `SendDone` has no handler case. The event does not change state.

A seal can occur inside the `BytesReceived` step. Size caps, partition caps, and batch caps can cause the seal (`broker/segment.go:88-118`). That step then includes `ObjPut` in the returned slice of that step. The simulator tests different interleavings of the same machine. The invariants must hold in each interleaving.

## 3.13 The invariants state what replay must preserve

1. The same seed and script give the same `StateHash` sequence and the same fact timeline.
2. State changes only in `Step`. State changes occur in dequeue order. `At` never decreases.
3. An unknown, finished, or wrong-kind `ReqID` does not change the hash. A stale `TimerGen` does not change the hash.
4. A live incarnation gets one completion for each `IOAction`. A lost response gives `ErrTimeout+MaybeApplied`. A lost response does not give silence. A dead incarnation gets no completion.
5. Actions start in slice order. Completions can arrive in any order. Correctness does not depend on completion order. Correctness does not depend on `CancelTimer`.
6. The broker emits `produce_acked` in the same step as its `Send`. The fact comes before the `Send`. The broker emits the fact only after a successful `CommitSegment`.
7. Offsets stay contiguous. The CP does not reuse offsets. One commit assigns offsets in one operation. One `SegmentKey` gives idempotent commit. `ErrConflict` means the same key with a different request. No other case uses `ErrConflict`.
8. Control events run before data events. Both queues have bounds. `ControlFirst` decides priority. `select` does not decide priority.
9. Event sets, action sets, enum numbers, enum spellings, fact names, and fact field order are append-only. A change needs a stated hash re-baseline.
10. A step uses only the event, machine state, and `Env` draws. A reader checks the inputs in that order.

## 3.14 Mutants test whether an invariant catches a wrong machine

`core/` has no bug flags. `core/` has `Buggify` (section 3.5). The broker has a second mechanism named `Bug*`. The type is `type Bug uint8` inside `Config` (`broker/config.go:6-37`). The code marks the type with a comment. "TEST ONLY: the zero value is the real broker and production code must never set it." A mutant selects one wrong behaviour at construction. Code selects the behaviour through `broker.New(cfg)` and `cfg.Bug`. A failing seed reproduces bit-for-bit. `StateHash` includes the `Bug` value first (`broker/hash.go:44`). Honest traces and mutant traces do not collide. The table below lists the nine mutants.

| Mutant | Wrong behaviour | Invariant that catches it |
|---|---|---|
| `BugEarlyAck` | Ack after PUT, before `CommitSegment` (guessed offset, `broker/segment.go:193`) | acked range is committed in the CP |
| `BugFreshKeyOnRetry` | Retry lost commit under a fresh key | a stored batch is committed at most once |
| `BugNoPartitionCap` / `BugNoBatchCap` | Ignore segment caps | sealed segment spans/holds at most the cap |
| `BugWrongOffset` | Fetch patches base offset + 1 | consumer sees every offset in order, none skipped |
| `BugServeCorrupt` | Skip CRC check on fetch | consumer never receives a corrupt batch |
| `BugSkipProduceCRC` | Admit batch without CRC (`broker/produce.go:75`) | committed batch is intact |
| `BugTimerFromLastBatch` | Flush window restarts per batch | segment sealed within first batch's window |
| `BugSealOffByOne` | Size rule one batch late | overshoot ≤ last batch |

`TestMutantsAreCaught` enforces the mutant rule (`sim/kafka/kafka_test.go:92-135`). Each mutant must fail within 20 seeds. The expected bound is 8 seeds or fewer. The invariant that names the mutant must catch the mutant. If no invariant catches a mutant, the mutant shows a missing invariant. The test does not pass such a mutant. If you add a broker rule, you add a mutant and an invariant in the same change. `sim/engines/logengine` keeps a second set under the same rule. The set holds `BugWriterOffsets`, `BugTrimDelete`, `BugPerReplicaWindow`, and `BugHoleSkip`.

Buggify and mutants change different code, and you keep the two apart. Buggify changes the world around a correct machine, and a buggify test checks the response of the machine to faults. A mutant changes the machine, and a mutant test checks that an invariant catches wrong machine behaviour. If a test mixes the two kinds of change, the test can inject a fault without an invariant that checks the result.

## Key takeaways

- `Step(Event) []Action` defines the split in the code. Machines make decisions without `I/O`. Drivers run the effects. Machines and drivers pass plain data.
- Time is data on the event. You use `At` for deadlines. You use `Wall` for stored UTC values. Randomness, faults, and assertions enter through `Env`. Other inputs must already be state.
- Code and CI enforce determinism. The code uses closed sets, frozen names, and frozen numbers. Deterministic packages do not use `I/O`, goroutines, or map iteration for behaviour. Detcheck checks the rules in CI.
- Priority is a pure function. `ControlFirst` gives the priority rule. Timers use generation matching. Stray completions cannot change state.
- Facts give an ordered record. The broker emits `produce_acked` before its `Send`. Mutants check that invariants catch wrong fact order and wrong ack order.

## Checkpoint

1. A completion arrives for an already-finished `ReqID`. What must happen to `StateHash`, and where is that written?
   <details><summary>Answer</summary>Nothing changes. The machine ignores the completion. The hash does not change. The contract is at `core/event.go:14-20` and `core/machine.go:3-6`. `TestIgnoresStrayCompletions` covers the case (`broker/broker_test.go:517`).</details>
2. Why does `CancelTimer` exist if correctness never relies on it?
   <details><summary>Answer</summary>`CancelTimer` gives an optimisation only (`core/action.go:27`). The call ends the host timer at an earlier time. Correctness uses generation matching. The machine ignores a `TimerFired` with a stale `Gen` (`core/event.go:70-71`). A lost cancel-vs-fire race does not change the result.</details>
3. Your machine needs a random retry delay. Which interface do you use, and what must you never do?
   <details><summary>Answer</summary>Use `Env.Rand` through `Intn` or `Uint64` (`core/env.go:11-18`). `Env.Rand` gives one seeded stream. The broker commit jitter uses that stream (`broker/segment.go:348`). Do not import a clock source or a random source into a deterministic package. The detcheck allow-list does not include those packages (`tools/detcheck/allow.txt`).</details>
4. Why is `ConnClosed` class `Data`, not `Control`?
   <details><summary>Answer</summary>Events that must stay in order share a queue. A client EOF cannot pass the last request from that client (`core/inbox.go:20-21`). Control priority covers results of work on the node.</details>
5. You rename fact `"segment_committed"` to `"segmentCommit"`. What breaks?
   <details><summary>Answer</summary>Fact names are frozen past-tense inputs to the trace hash. Code does not derive fact names from Go type names (`core/fact.go:5-9`). The rename changes the golden hash. The rename breaks comparison with recorded traces. A change needs a stated and reviewed re-baseline. Append-only enums use the same path (`core/types.go:57`).</details>

## Try it

```sh
go test ./core/ -count=1 -v                                   # the core contracts
go test ./broker/ -run 'TestStateHash|TestIgnoresStrayCompletions' -count=1
grep -n 'func ClassOf' -A 14 core/inbox.go                     # the fixed class table
go run ./tools/detcheck ./core/... ./broker/...                # determinism gate, as CI
go test ./sim/kafka/ -run TestMutantsAreCaught -count=1 -v     # mutants vs invariants
grep -rn 'env\.Maybe\|env\.Fault\|\.MaybeP(' broker/ --include='*.go'  # verify §3.5's caveat: no hits
```

## Further reading

- `core/doc.go`, `core/event.go`, and `core/action.go` in full. Each file takes two screens. Read each file in full.
- `broker/broker.go`, `broker/segment.go`, and `broker/conn.go`. The files give the section 3.12 trace in source order.
- `sys/run.go` and `sys/inbox.go`. The files give the production driver, dispatch, host timers, and queue storage.
- `sim/assert.go`. The file gives the simulator `Env`, seeded `Rand`, per-site `Buggify`, and record-and-continue assertions.
- `docs/lessons.md` (lessons 3, 7, 10-12), `docs/decisions.md` (A, G, H, I, 41-44), and `docs/flow-control.md` sections 2-3. The files state why each rule exists.
