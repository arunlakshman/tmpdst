# dst v2: design

Status: design for task T1.1 of `MONOREPO-PLAN.md`. No code is written from this file
until the user approves it (plan section 3.5).

Audience: the people who build T1.2 to T1.16, and the user who approves.

## 0. How to read this file

Sections 1 and 2 give the goals and the package layout. Sections 3 to 12 answer
points 1 to 10 of the T1.1 card, one section per point, with Go signatures.
Section 13 names the one file where type erasure is allowed. Section 14 is point
11 (the mapping table). Section 15 says where the starting proposals of the plan
do not work in Go, and what this design does instead. Section 16 is point 12
(migration order and golden hashes). Section 17 lists risks. Section 18 lists the
decisions the user must make.

Words used in this file (more words are in the plan, section "Words used"):

- **machine**: a sans-IO state machine. "Sans-IO" means: it does no IO itself. It
  takes one input value and returns output values as plain data. Somebody else
  (the simulation kernel, or the production loop) does the real work.
- **node**: one simulated process that runs one machine. It has a name, and it has
  one incarnation at a time.
- **incarnation**: one boot of a node. A crash ends it. A restart starts a new one
  with a new boot number. Nothing in memory survives.
- **service**: a simulated outside system (object store, control plane, network,
  shared log). Today the broker calls it `FakeService` and dst calls it `Engine`.
- **host code**: the product code that builds one simulation run: it creates the
  nodes and the services, drives the clients and the faults, and checks the result.
- **sum type** (or **sealed sum**): a Go interface with an unexported marker method,
  so only its own package can add cases. A `switch x := v.(type)` over it is
  **exhaustive** when it lists every case. The `gochecksumtype` linter checks this.
- **type erasure**: a typed value is stored where its type is no longer visible,
  for example in an `any`, in a non-generic interface, or inside a closure.
- **lane**: an ordering group in the event queue. Entries on the same lane at the
  same time run in the order they were added (FIFO).
- **tie**: a random number drawn for each queued entry. Entries at the same time
  run in tie order, unless they share a lane.
- **catalog**: the list of every assertion and every buggify site that the code
  declares, built when the program starts.
- **buggify site**: a named place in the code where a fault may be injected on
  purpose (dst `fault.Injector.Maybe`).

## 1. Goals and limits

1. **v2 holds what the existing kernels already do, nothing more** (plan section 9).
   Every feature of the broker kernel, of dst `sim`/`eng` and of the control plane
   host code that some scenario or test uses has a place in v2. A feature that no
   scenario and no product code uses is deleted, and the mapping table (section 14)
   says so.
2. **No Roadmap features.** No network model in the kernel (the broker network stays
   a service, as today), no disk model, no linearizability checker.
3. **Type safety.** No value of type `any` in product code paths. Sending a message
   of the wrong type to a node does not compile. Output switches are exhaustive and
   checked by `gochecksumtype`.
4. **One kernel, one production loop, one spec toolkit, one campaign tool**, used by
   both products.
5. **Plain names.** Simple English in names and comments. ASCII only.

## 2. Package layout

All packages are in the `dst` module. None of them has a product word.

    dst/
      machine/     NEW. The machine contract: Machine[In, Out], Message, Now, Env,
                   time types, node names, boot numbers, Fact. Small. Product machine
                   code imports only this package, never the kernel.
      kernel/      NEW. The simulation world: nodes, services, addresses, routers,
                   timers, lanes, inboxes, phases, incarnations, crash / restart /
                   stop / pause, attrition, cost model, timeline, trace hash.
      live/        MOVED from controlplane/internal/system/live, made generic. The
                   production loop that runs the same machines with real time and
                   real IO.
      check/       EXTENDED. Spec[Op, Res], Lockstep, SelfCheck, planted bugs, and the
                   log Replay helpers that exist today. dst/model merges into it.
      campaign/    EXTENDED. Scenario registry, seed runs, until-stable, determinism
                   canary, report (text and JSON), replay one seed.
      rng/         KEPT, plus Derive.
      assert/      KEPT, plus the catalog and two more kinds.
      fault/       KEPT, plus the site catalog.
      trace/ sched/ clock/ golden/ realclock/ syncclock/   KEPT as they are.
      mutant/      MERGED into check (planted bugs), deleted in T1.16.
      sim/ eng/ model/   DELETED in T1.16.

The plan (section 7.2) names `kernel`, `live`, `check`, `campaign`. This design adds
`dst/machine`. Reason: product machine code (broker `core`, control plane
`runtime`, `replica`, `rebalance`) needs the contract types but must not depend on
the whole simulator. `live` also needs them. A small shared package keeps both
`kernel` and `live` out of the product machine code.

Import order inside dst: `trace`, `rng` <- `assert`, `fault` <- `machine` <-
`kernel`, `live` <- `check`, `campaign`.

## 3. Point 1: the machine contract

### 3.1 What exists today

- Broker: `core.StateMachine.Step(core.Event) []core.Action`, with
  `core.Event{At NodeTime, Wall RealTime, Body EventData}`. Randomness, buggify and
  assertions come from `core.Environment`, given to the factory at boot.
  `core.MachineFactory func(env Environment, self NodeInstance) Machine`.
- Control plane: `replica.Replica` and `rebalance.Machine` implement dst
  `sim.Process`: `Step(cx sim.Context, inbox any) []any`. They read `cx.Now()`,
  `cx.Wall()` and (rebalance only) `cx.Rand()`. The replica also gets
  `runtime.Environment` at construction.

### 3.2 The v2 contract

The plan proposed `Step(now Time, in In) []Out`. That works, with one change: the
time argument must carry **two** clocks. Both products read the node clock (with the
node's clock skew, for deadlines and leases) and the UTC wall clock (Kafka
LogAppendTime, rebalance `wallMs`). So `Now` is a small struct, not one number.
Randomness, buggify and assertions are not in `Step`; they come from `Env` at
construction, as in the broker today. The rebalance machine moves from
`cx.Rand()` to its `Env`; it is the same random stream, so the draws do not change.

    package machine

    // NodeTime is node-local monotonic time in nanoseconds. In a simulation it
    // includes the fixed clock skew of the node.
    type NodeTime int64

    // RealTime is UTC, in nanoseconds since the Unix epoch.
    type RealTime int64

    // Duration is a length of time in nanoseconds.
    type Duration int64

    const (
        Nanosecond  Duration = 1
        Microsecond          = 1000 * Nanosecond
        Millisecond          = 1000 * Microsecond
        Second               = 1000 * Millisecond
    )

    type ID [16]byte          // drawn from the seeded stream, never uuid.New
    type NodeID string        // the stable name of a node
    type BootNumber ID        // new for every boot of a node
    type RequestID uint64     // matches an IO call with its completion
    type TimerID uint64
    type TimerVersion uint64  // a TimerFired with an old version is ignored

    // NodeInstance names one boot (one incarnation) of one node.
    type NodeInstance struct {
        Node NodeID
        Inc  BootNumber
    }

    // Now is the time a machine sees when it takes one input.
    type Now struct {
        Mono NodeTime // node clock, with skew
        Wall RealTime // UTC
    }

    // Message is every input and every output of a machine. Fold writes the value
    // into the trace hash: a fixed tag number first, then the fields. It never
    // uses %T or reflection, so renaming a Go type does not move a golden hash.
    type Message interface {
        Fold(h *trace.Hash)
    }

    // Machine is a sans-IO state machine. Step is the only place where its state
    // changes. Step never blocks and never does IO. In and Out are sealed sum
    // types owned by the product.
    type Machine[In, Out Message] interface {
        Step(now Now, in In) []Out
        StateHash() uint64
    }

    // Factory builds a fresh machine for one boot. It is called on the first boot
    // and after every crash, with a new boot number each time.
    type Factory[In, Out Message] func(env Env, self NodeInstance) Machine[In, Out]

    // Env is the only way that randomness, buggify and assertions reach a machine.
    // Time is not here: it comes with each Step.
    type Env interface {
        Rand
        Faults
        Assertions
    }

    type Rand interface {
        Uint64() uint64
        Intn(n int) int                 // [0, n)
        Between(lo, hi int64) int64     // [lo, hi], both ends included (see 9.2)
        Chance(p float64) bool
        NewID() ID
    }

    type Faults interface {
        Maybe(site fault.Site) bool
        MaybeP(site fault.Site, p float64) bool
        Fault(site fault.Site, p float64) bool // never fires outside PhaseFaults
    }

    type Assertions interface {
        Always(cond bool, p assert.Always, details ...assert.Pair)
        AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, details ...assert.Pair)
        Sometimes(cond bool, p assert.Sometimes)
        Reachable(p assert.Reachable)
        Unreachable(p assert.Unreachable, details ...assert.Pair)
    }

    // End says how an incarnation ended.
    type End uint8

    const (
        Crash End = iota // killed from outside (nemesis, attrition, stop deadline)
        Trip             // the machine asked for a restart: its memory is not trusted
        Exit             // the machine finished a graceful stop
    )

    // Class picks one of the two inboxes of a node. Control is always taken
    // before Data. (Broker flow control, docs/flow-control.md section 3.)
    type Class uint8

    const (
        Control Class = iota
        Data
    )

    // Clock is time for code that is not a machine (run loops, adapters).
    type Clock interface {
        Mono() NodeTime
        Wall() RealTime
        AfterFunc(d Duration, f func()) Timer
    }

    type Timer interface{ Stop() bool }

    // Fact is a named, structured observation for the timeline (broker lesson 10).
    type Fact struct {
        Name   string
        Fields []Field
    }

    func (f Fact) Field(key string) FieldValue
    func (f Fact) Fold(h *trace.Hash)

    type Field struct {
        Key   string
        Value FieldValue
    }

    //sumtype:decl
    type FieldValue interface {
        isFieldValue()
        Fold(h *trace.Hash)
    }

    type Int64Value int64
    type Uint64Value uint64
    type StringValue string
    type Bool bool
    type Bytes []byte

What each product keeps: only its own messages, as sealed sums with a `Fold`
method on every case. For example broker `core.EventData` and `core.Action`,
control plane `runtime.EventData` and `runtime.Action` (new: today the replica
returns `[]any`), and `rebalance.Input` and `rebalance.Output` (new: today `any`).
`core.Event` (the struct with At, Wall, Body) is deleted: `Now` plus the input
replace it.

What moves out of `core` and `runtime` into `dst/machine` in T1.6: `NodeTime`,
`RealTime`, `Duration` and its units, `ID`, `NodeID`, `BootNumber`, `NodeInstance`,
`RequestID`, `TimerID`, `TimerVersion`, `Environment` (as `Env`), `Rand`,
`FaultInjector` (as `Faults`), `Assertions`, `PropertyID` (replaced by the typed
handles of section 10), `ContextPair` (as `assert.Pair`), `Clock`, `Timer`,
`Fact`, `Field`, `FieldValue` and its five cases, `Hasher` (as `trace.Hash`; the
two are the same FNV-1a code today), `StateMachine`, `HasStateHash`, `Machine`,
`MachineFactory`. `InboxKind` becomes `machine.Class`. `ErrCode`, `Outcome`,
`SegmentRef`, `LogPosition`, `Offset`, `TopicPartition` stay in the products:
they are product words.

### 3.3 The production loop on the same contract (`dst/live`)

T1.11 moves `controlplane/internal/system/live` to `dst/live` and makes it generic.
It then runs any `machine.Machine` with real time and real IO.

    package live

    type Config[In, Out machine.Message] struct {
        Self      machine.NodeInstance
        New       machine.Factory[In, Out]
        Env       machine.Env                            // production Env: no faults
        Boot      func(self machine.NodeInstance) In     // first input; nil: none
        StopMsg   func(deadline machine.NodeTime) In     // graceful stop request
        StopGrace machine.Duration
        Clock     machine.Clock
        Inbox     *Inbox[In]
        Class     func(in In) machine.Class              // nil: every input is Data
        Route     func(l *Loop[In], out Out) error       // exhaustive switch, product code
        Shutdown  <-chan struct{}
        Inject    <-chan func() []Out                     // work for the loop goroutine
        AfterStep func()
    }

    func Run[In, Out machine.Message](ctx context.Context, cfg Config[In, Out]) error

    type Loop[In machine.Message] struct{ /* unexported */ }

    func (l *Loop[In]) Self() machine.NodeInstance
    func (l *Loop[In]) SetTimer(id machine.TimerID, after machine.Duration, msg In)
    func (l *Loop[In]) CancelTimer(id machine.TimerID)
    func (l *Loop[In]) SendSelf(msg In)
    func (l *Loop[In]) End(how machine.End, cause string)

    type Inbox[In machine.Message] struct{ /* unexported */ }

    func NewInbox[In machine.Message](dataCap int) *Inbox[In] // 0: Data has no bound
    func (i *Inbox[In]) Deliver(c machine.Class, msg In)      // Data blocks at dataCap
    func (i *Inbox[In]) Len() int
    func (i *Inbox[In]) Close()
    func (i *Inbox[In]) Closed() <-chan struct{}

    var (
        ErrStoppedForRestart = errors.New("tripped") // End(Trip, cause), wrapped with cause
        ErrStopped           = errors.New("stopped") // the context ended
        ErrKilled            = errors.New("killed")  // a graceful stop missed its deadline
    )

`End(Exit, ...)` makes `Run` return nil. This one loop covers both production loops
of today: the control plane `live.Run` (urgent stop value = Control class, all other
values = Data class with no bound, Inject channel, AfterStep hook) and the broker
`internal/system.Run` (two inboxes, Data bounded by `dataCap`, `ControlFirstPicker`).
`Loop` has no way to send to another node, so the control plane check "only the
process itself can be reached" becomes a compile-time fact. Whether the broker
moves to it is decision D2 (section 18).

## 4. Point 2: typed routing

### 4.1 The problem

dst `sim` routes `[]any` outputs: first its own control structs, then each engine
in turn until one does not return `eng.ErrNotMine`. The broker kernel asks every
engine `Handles(action)` and requires exactly one yes. Both are runtime checks on
`any`. A new action with no handler is found only when a test hits it.

### 4.2 The design

Each node has a typed router. The product writes one route function per node kind.
Its body is one exhaustive type switch over the product's `Out` sum (checked by
`gochecksumtype`). Each case calls typed kernel functions.

    package kernel

    // Router is lent to a route function for the outputs of one Step. In is the
    // input type of the node that stepped, so operations on the node itself are
    // methods.
    type Router[In machine.Message] struct{ /* unexported */ }

    func (r *Router[In]) Self() Ref[In]   // the incarnation that stepped
    func (r *Router[In]) Now() Time
    // SetTimer delivers msg to this incarnation after `after` of node time. A
    // pending timer with the same id is replaced: the old one never fires.
    func (r *Router[In]) SetTimer(id machine.TimerID, after machine.Duration, msg In)
    func (r *Router[In]) CancelTimer(id machine.TimerID)
    // SendSelf delivers msg to this incarnation at delay 0. It is dropped if the
    // incarnation ends first.
    func (r *Router[In]) SendSelf(msg In)
    // Fact appends f to the timeline with this incarnation as the source.
    func (r *Router[In]) Fact(f machine.Fact)
    // End ends this incarnation (Trip or Exit). It must be the last output of the
    // Step; an output after it is a protocol failure.
    func (r *Router[In]) End(how machine.End, cause string)

    // Send delivers msg to the current incarnation of node `to` at delay 0, with a
    // fresh random tie. Go methods cannot have their own type parameters, so this
    // is a package function: In is the sender's input type, To the receiver's.
    func Send[In, To machine.Message](r *Router[In], to Addr[To], msg To)

    // SendOn is Send on a lane: messages on one lane at one time keep their order.
    func SendOn[In, To machine.Message](r *Router[In], lane Lane, to Addr[To], msg To)

    // Call hands req to a service. The service answers (later, through its
    // ServiceContext) with values of the caller's own input type In.
    func Call[Req, In machine.Message](r *Router[In], s ServiceAddr[Req, In], req Req)

A broker route, as an example (T1.12 writes the real one):

    func (h *host) routeBroker(r *kernel.Router[core.EventData], out core.Action) {
        switch x := out.(type) {
        case core.SetTimer:
            r.SetTimer(x.ID, x.After, core.TimerFired{ID: x.ID, Gen: x.Gen})
        case core.CancelTimer:
            r.CancelTimer(x.ID)
        case core.RecordFact:
            r.Fact(x.Fact)
        case core.StopForRestart:
            r.End(machine.Trip, x.Cause)
        case core.ShutdownDone:
            r.End(machine.Exit, "exit")
        case core.ObjectCall:
            kernel.Call(r, h.objects, x)
        case core.ControlPlaneCall:
            kernel.Call(r, h.controlPlane, x)
        case core.Send:
            kernel.Call(r, h.network, core.NetCall(x)) // NetCall: the sum of the network cases
        case core.Connect:
            kernel.Call(r, h.network, core.NetCall(x))
        case core.CloseConn:
            kernel.Call(r, h.network, core.NetCall(x))
        case core.PauseReads:
            kernel.Call(r, h.network, core.NetCall(x))
        case core.ResumeReads:
            kernel.Call(r, h.network, core.NetCall(x))
        }
    }

Two Go rules show in this example. (1) One case per type: in a case with several
types, `x` has the interface type, not the concrete one. (2) The explicit
`core.NetCall(x)`: Go infers `Req` from both the service address and the request,
and both must give the same type, so a concrete `core.Send` must be converted to the
service's request sum first.

There is no "unrouted action" failure and no "two engines handle it" failure any
more: both become compile errors (a missing case is a `gochecksumtype` error; a
service of the wrong request type does not compile).

### 4.3 Addresses

    // Addr names a node, whatever incarnation is up. Only Spawn makes one. A
    // message for an Addr goes to the incarnation that is up when it arrives.
    type Addr[In machine.Message] struct{ /* unexported: world, node index */ }

    func (a Addr[In]) ID() machine.NodeID

    // Ref names one incarnation of a node. A message for a Ref is dropped if that
    // incarnation has ended. Services answer callers by Ref.
    type Ref[In machine.Message] struct{ /* unexported: Addr, boot number */ }

    func (r Ref[In]) Addr() Addr[In]
    func (r Ref[In]) Instance() machine.NodeInstance

    // Node is any Addr or Ref, for lifecycle calls that do not need the type.
    // Only this package implements it (unexported method).
    type Node interface{ node() nodeKey }

`Addr[In]` and `Ref[In]` are comparable, so they can be map keys. Go cannot forbid
the zero value `Addr[In]{}`; the kernel treats a zero address as a protocol failure
(`FailProtocol`, "zero address"). An address also remembers its world, so using it
in another run is a protocol failure too.

Messages that carry a node name as plain data (`core.NodeStarted{Self}`,
`ControlPlaneDeliver{From}`) keep `machine.NodeInstance`. When host or service code
must deliver to such a name, it looks it up in its own table of `Addr` values (it
spawned them) and asks `CurrentRef` for the incarnation. The kernel never turns a
plain name back into a typed address, so it needs no runtime type check.

### 4.4 Spawning nodes

    // NodeSpec says how to run one node. In and Out are the machine's sum types.
    type NodeSpec[In, Out machine.Message] struct {
        Name  machine.NodeID
        New   machine.Factory[In, Out]               // called on every boot
        Route func(r *Router[In], out Out)            // exhaustive switch over Out
        Boot  func(self machine.NodeInstance) In      // first input of every boot
        // StopMsg makes the graceful stop request. Nil: Stop is not allowed.
        StopMsg func(deadline machine.NodeTime) In
        // Class picks the inbox of an input. Nil: every input is Data.
        Class func(in In) machine.Class
        // Cost is how long the node is busy after one Step. Zero: no time.
        Cost CostParams
        // Restart says when the next incarnation starts after one ends.
        // ok=false or nil: the node stays down until host code calls Boot.
        Restart func(how machine.End, r *rng.Rand) (after machine.Duration, ok bool)
        // Attrition lets the kernel kill this node during PhaseFaults (7.2).
        Attrition bool
        // Driver marks a workload (a test client). A driver boots after every
        // other node, never ends (an End or a panic stops the run with a failure),
        // and hears phase changes through OnPhase.
        Driver  bool
        OnPhase func(p Phase) (msg In, ok bool)
        // AfterStep runs after each Step, after the outputs are routed. Host
        // code uses it for checks on one node (control plane checkNode).
        AfterStep func(self Ref[In], in In, out []Out)
    }

    func Spawn[In, Out machine.Message](w *World, s NodeSpec[In, Out]) Addr[In]

    // CurrentRef returns the incarnation of a that is up now.
    func CurrentRef[In machine.Message](w *World, a Addr[In]) (Ref[In], bool)

    // Inject delivers msg from host code (a client request, a nemesis) to the
    // current incarnation of `to`, after a delay.
    func Inject[In machine.Message](w *World, to Addr[In], after machine.Duration, msg In)
    func InjectOn[In machine.Message](w *World, to Addr[In], lane Lane, msg In)

## 5. Point 3: services

### 5.1 What exists today

- Broker `FakeService`: `Handles`, `Execute(ctx, from, action)` at issue time (all
  fault coins and delays are drawn here, in issue order), `OnInternal(ctx, body
  any)` for its own later steps, `OnCrash(ctx, ref)` (effects already applied
  stand; only entries addressed to the dead incarnation are dropped),
  `EnterRecovery()` (no new faults for calls issued after it), `StateHash()`.
  `Deliver` to a dead incarnation is dropped with no random draw. The network
  service also delivers to nodes that did not call it (`ConnOpened`,
  `BytesReceived`) and finds the live incarnation with `NodeCurrent.Current`.
- dst `eng.Engine`: `Handle(cx, act any) error` (with `ErrNotMine`),
  `OnSelf(cx, body any)`, `StateHash()`. The control plane `storage` and
  `replyEngine` are engines.

### 5.2 The v2 service

    // Service simulates one outside system. Req is the sum of the requests it
    // takes. In is the input type of the nodes it talks to: it answers with In
    // values, and may also send In values to other nodes (the network does).
    type Service[Req, In machine.Message] interface {
        Name() string
        StateHash() uint64
        // Handle starts one request at issue time. Draw every fault coin and every
        // delay here, so that draws happen in issue order. Never complete anything
        // at once: every answer is a scheduled Deliver.
        Handle(cx *ServiceContext[In], from Ref[In], req Req)
        // OnCrash says that an incarnation ended. Effects already applied stand.
        // Entries the service scheduled for itself (Later) are not dropped, so a
        // write already on the wire can still land after the caller died.
        OnCrash(cx *ServiceContext[In], who machine.NodeInstance)
        // EnterRecovery: no new faults for requests issued after this call.
        // Plans already made stand.
        EnterRecovery()
    }

    func AddService[Req, In machine.Message](w *World, s Service[Req, In]) ServiceAddr[Req, In]

    type ServiceAddr[Req, In machine.Message] struct{ /* unexported */ }

    type ServiceContext[In machine.Message] struct{ /* unexported */ }

    func (c *ServiceContext[In]) Now() Time
    func (c *ServiceContext[In]) Rand() *rng.Rand
    func (c *ServiceContext[In]) Faults() *fault.Injector
    func (c *ServiceContext[In]) Assert() machine.Assertions
    // Deliver schedules msg for one incarnation. If `to` is not up now, it is
    // dropped at once and draws nothing. If `to` ends before `at`, it is dropped
    // then.
    func (c *ServiceContext[In]) Deliver(to Ref[In], at Time, lane Lane, msg In) EntryID
    // Later runs fn as a later step of this service. The closure keeps its types,
    // so no opaque body and no type switch are needed (replaces OnInternal/OnSelf).
    func (c *ServiceContext[In]) Later(at Time, lane Lane, fn func(cx *ServiceContext[In])) EntryID
    func (c *ServiceContext[In]) Cancel(id EntryID)
    // Current is the live incarnation of a node (the network uses it for Connect).
    func (c *ServiceContext[In]) Current(a Addr[In]) (Ref[In], bool)
    // Fault records an injected fault: a "sim_fault" fact with kind and service name.
    func (c *ServiceContext[In]) Fault(kind string, fields ...machine.Field)

Rules kept from the broker kernel: every call into a service clears its cached state
hash; the kernel folds the service state hash after every queue pop; a panic in a
service stops the run with `FailPanic`.

Why the service speaks the node input type `In`, and not a typed response: see
section 15, row `Call`.

Host code that is not a node and not a service (the control plane clients and
nemesis) uses host tasks (`World.At`, `World.After`, section 6.4). The control
plane `replyEngine` is deleted: the replica route calls the client pool directly
for a `ControlPlaneReply`, in the same step, as the engine does today.

A later `Faulty(spec, faults)` (T1.15, stretch) fits this shape: it is a
`Service[Req, In]` built from a `check.Spec[Op, Res]` and a function that turns
(Res, error code, outcome) into the caller's `In`. This design does not define it.

## 6. Point 4: ordering

### 6.1 One queue

The kernel has one event queue. It is the broker scheduler (`schedimpl.go`), moved:

- Order is (time, tie, sequence).
- Adding an entry clamps a past time to now, takes the next sequence number, and
  draws **exactly one** tie from the run stream (so the draw count never depends
  on lane history).
- If the entry has a lane other than `NoLane`, and a pending entry on the same lane
  has the same time, the new entry takes that entry's tie instead. Equal (time,
  tie) falls through to sequence, which is the order of adding. So one lane at one
  time is FIFO; different lanes at one time are in seeded random order.
- `NoLane` entries never share a tie (dst `sched.After` behavior, used by the
  control plane today).
- Cancel and "drop everything for this incarnation" mark entries dead; dead
  entries are thrown away when they reach the top.

    type Time int64      // global virtual time in ns since the run started
    type Lane uint64
    type EntryID uint64

    const (
        NoLane   Lane = 0       // no FIFO group: a fresh random tie
        HostLane Lane = 1 << 62 // the kernel's own lane (restarts, kills, phases)
    )

    func (w *World) Cancel(id EntryID)

Lanes at or above `HostLane` belong to the kernel: per node, one lane for timers
and one for wake-ups (as `nodeLane` today). Product lanes are below it.

### 6.2 Typed lane keys

The broker asks for lanes by string key (`"net/misc"`, a connection pair key). In v2
the key is any comparable product type, with no `any`:

    // Lanes gives one lane per key, in first-use order, from a counter shared by
    // every Lanes of the world (so two key types never share a lane). Allocation
    // draws no randomness. The same key gives the same lane for the whole run.
    type Lanes[K comparable] struct{ /* unexported: world, map[K]Lane */ }

    func NewLanes[K comparable](w *World) *Lanes[K]
    func (l *Lanes[K]) Get(key K) Lane

Example: `type connLane struct{ pair uint64; dir uint8 }` in the network service.
Because allocation is in first-use order, as today, a 1:1 key change keeps the
lane numbers.

### 6.3 Control plane wake tokens become lanes

The control plane host (`groups.go`) keeps two FIFO queues per node and sends a
"wake token" for each value, because dst `sched` breaks same-time ties at random.
In v2 the host sends the values themselves on two lanes per node
(`kernel.NewLanes[groupLane](w)` with `type groupLane struct{ node int; toReplica
bool }`), with `InjectOn` / `SendOn`. The tokens `machineWake`, `replicaWake`, the
`queue` type and `lastToReplica` are deleted.

### 6.4 Host tasks

Host code schedules plain closures on the same queue:

    func (w *World) At(t Time, lane Lane, fn func()) EntryID
    func (w *World) After(d machine.Duration, fn func()) EntryID // NoLane

The control plane clients, consumers, nemesis and `s.after(...)` calls use these.
The broker phase changes and quiesce points use them on `HostLane`.

### 6.5 Inboxes and the cost model

Each node has the broker inbox model: two FIFO inboxes (Control, Data), picked
control first, and a "busy until" time.

- An input that arrives goes into its inbox (`NodeSpec.Class`, default Data).
- If the node is not busy, it takes one input at once (in the same queue pop).
- After a Step, the node is busy for `Cost.Sample(rng)`; if inputs wait, a wake
  entry is queued at "busy until" on the node's wake lane.
- With zero cost (the control plane), a node is never busy, so every arrival is
  one Step at once: exactly the dst `sim` behavior. One model serves both.

    // CostParams is the node CPU model (broker docs/flow-control.md section 4.1).
    type CostParams struct {
        PerEvent  machine.Duration
        PauseProb float64           // stands in for GC pauses and CPU contention
        Pause     FastWithRareSlow
    }

    func (p CostParams) Sample(r *rng.Rand) machine.Duration // zero params draw nothing

    // FastWithRareSlow is a delay that is mostly fast, with a rare slow tail.
    type FastWithRareSlow struct {
        FastMin, FastMax machine.Duration
        SlowMin, SlowMax machine.Duration
        SlowProb         float64
    }

    func (t FastWithRareSlow) Sample(r *rng.Rand) machine.Duration

The broker `CostParams.PerByte` is deleted: no scenario sets it, and without it the
cost does not depend on the input type.

### 6.6 Running

    type Config struct {
        Seed         uint64
        WallStart    machine.RealTime   // UTC at time zero
        End          Time               // stop before the first entry after End; 0: none
        MaxSteps     uint64             // 0 means 10_000_000; more is FailMaxSteps
        ClockSkewMax machine.Duration   // per-node skew, drawn once at Spawn
        Attrition    Attrition
        Log          bool               // keep every step line in Result.Log
        Tail         int                // keep the last Tail step lines for a failure
        HaltOnFirst  bool
        VerifyHashes bool               // recompute cached state hashes every step
    }

    func New(cfg Config) *World

    type World struct{ /* unexported */ }

    func (w *World) Now() Time
    func (w *World) Wall() machine.RealTime
    func (w *World) Steps() uint64
    func (w *World) Rand() *rng.Rand
    func (w *World) Env() machine.Env           // the one Env every machine shares
    func (w *World) Faults() *fault.Injector
    func (w *World) Assert() *assert.Log
    func (w *World) Logging() bool
    func (w *World) Note(text string)           // a log line; never hashed
    func (w *World) Fail(kind FailKind, property, detail string)

    // Start boots every spawned node: first the nodes that are not drivers, in
    // spawn order, then the drivers.
    func (w *World) Start()

    // RunUntil takes entries until stop returns true (checked before each
    // entry), the queue is empty, the next entry is after Config.End, the step
    // limit is hit, or the run halted.
    func (w *World) RunUntil(stop func() bool) Stopped

    // Finish runs Rule.AtEnd and returns the result.
    func (w *World) Finish() Result

    type Stopped uint8

    const (
        StopAsked Stopped = iota
        Drained
        TimeUp
        StepLimit
        Halted
    )

    // FailKind says what kind of failure a seed had. Not folded into the trace.
    type FailKind uint8

    const (
        FailViolation     FailKind = iota // an Always, AlwaysOrUnreachable or Unreachable broke
        FailProtocol                      // output after End, zero address, first Step not the boot input
        FailPanic                         // recovered from a Step, a factory, a service or a rule
        FailMaxSteps
        FailNotRepeatable                 // the determinism canary (set by campaign)
    )

    // Failure is the first failure of one property in a run. Later failures of
    // the same property only raise Repeats (control plane rule).
    type Failure struct {
        Seed     uint64
        Kind     FailKind
        Property string
        Detail   string
        Step     uint64
        At       Time
        Repeats  int
    }

    type Counter struct {
        Name string // node_steps, engine_steps, sim_steps, deliveries, ... (broker names, same order)
        N    uint64
    }

    type LogEntry struct {
        Step uint64
        At   Time
        Text string // formatted when the Result is built, from the kept message; never hashed
    }

    // Result is one run. (Steps, RandCalls, Hash, Stats) is what a golden pin holds.
    type Result struct {
        Seed, Steps, RandCalls, Hash uint64
        Crashes, Trips, Exits        int
        Stats                        []Counter
        Failures                     []Failure
        Coverage                     map[string]assert.Coverage
        Sites                        map[string]fault.SiteCount
        Log                          []LogEntry // every step, only with Config.Log
        Tail                         []LogEntry // the last Config.Tail steps, only on failure
    }

    func (r Result) OK() bool

The control plane drives its phases with `RunUntil` and predicates (as today).
The broker calls `kernel.SchedulePhases` (section 7.3), which sets the end time.

## 7. Point 5: incarnations, crash, restart, exit, pause, attrition, cost, phases

### 7.1 Lifecycle calls

    func (w *World) Up(n Node) bool
    func (w *World) Current(n Node) (machine.NodeInstance, bool)
    // Down reports how the last incarnation ended, while the node is down.
    func (w *World) Down(n Node) (how machine.End, down bool)
    // Boot starts a new incarnation now, if the node is down and not broken.
    func (w *World) Boot(n Node)
    // Crash ends the incarnation now (for a Ref: only if it is still current).
    func (w *World) Crash(n Node, cause string)
    // Stop sends StopMsg(deadline) in the Control inbox and arms a kill at
    // now+grace, addressed to this incarnation (an Exit first drops the kill).
    func (w *World) Stop(n Node, grace machine.Duration)
    // Pause: the node takes no Step until Resume. Inputs keep arriving and wait
    // in its inbox. What it started keeps going.
    func (w *World) Pause(n Node)
    func (w *World) Resume(n Node)

### 7.2 Mapping

| Today | v2 |
|---|---|
| Broker `boot`: new boot number from the run stream, fresh machine from the factory, `NodeStarted` in the Control inbox, a wake entry | `World.Boot`: same steps; the first input is `NodeSpec.Boot(self)`; "the first Step must take the boot input" stays a protocol check |
| dst `Spawn` over an old process, `Restart{Who, New, Boot}` | `World.Boot`; the factory gives the new machine; `NodeSpec.Boot` gives the boot value |
| Broker `end`: drop entries for the incarnation, tell every service `OnCrash`, emit `sim_crashed` / `sim_tripped` / `sim_exited`, schedule a restart after `RecoveryDelay` | `World.Crash` and `Router.End`: same steps; the restart delay comes from `NodeSpec.Restart` (broker: `RecoveryDelay` for every end) |
| dst `Crash{Who}` / `Exit{}` outputs | `Router.End(Trip / Exit)` for the node itself; `World.Crash(n)` for another node (host code) |
| Control plane trip: host draws 100..500 ms, controller restarts replica and rebalance machine together | the replica route crashes the rebalance node, calls `End(Trip)`, and schedules `World.After(d, boot both)`; the replica `Restart` policy is nil |
| Control plane nemesis `killNode` | host task: `w.Crash(replica)`, `w.Crash(rebalance)`, `w.After(d, boot both)` |
| Control plane `shutDown` (replica Exit) | `End(Exit)`; the node stays down (`Restart` returns ok=false for Exit) |
| Broker graceful stop (`stopNode`, 5 s kill deadline, `sim_stop_requested`) | `World.Stop(n, grace)`; attrition uses `Attrition.StopGrace` (default 5 s) |
| Timers die with the incarnation; a reply to a dead incarnation is dropped | same: timers and `SendSelf` belong to a `Ref`; `ServiceContext.Deliver` takes a `Ref` |
| A message to a process name goes to the current incarnation (dst `Inject`, `Send`) | `Addr`-based `Inject` / `Send`: same |
| Broker freeze (`FreezeProb`, `frozenUntil`) | deleted from attrition (no scenario sets `FreezeProb`); the same effect is `Pause` + a host task that calls `Resume` |
| Control plane pause (`held` list, re-injected on resume) | `World.Pause` / `World.Resume`; the inbox holds the inputs in order |
| Control plane final-phase `freeze` of the rebalance machine | `World.Pause` with no `Resume` |
| Broker attrition (`NodeKills`): one roll after each node Step and each service step during PhaseFaults; `MaxDead`; crash or graceful stop by weight | `kernel.Attrition` (below), same rolls, only for nodes with `NodeSpec.Attrition` |
| Broker workload (never crashes, boots last, gets the broker list) | `NodeSpec.Driver`; the host passes the broker addresses in the factory closure |
| `HiddenFromClients`, `NoAttrition` | deleted: nothing sets them; the host decides which addresses a driver gets, and `NodeSpec.Attrition` is opt-in |
| Broker cost model (`CostModel`, `NodeInbox`, `BusyUntil`) | section 6.5 |
| Broker per-node clock skew (from `NetworkFaults.ClockSkewMax`) | `Config.ClockSkewMax`, drawn at Spawn; `Now.Mono` = time + skew |
| Panic in Step: node crashes ("panic"); in a driver, a service or a rule: run halts | same, `FailPanic` |

    // Attrition kills nodes at random during PhaseFaults (broker NodeKills).
    type Attrition struct {
        MaxDead        int     // nodes down or stopping; 0 means attrition is off
        CrashProb      float64 // rolled after each node Step and each service step
        CrashWeight    float64 // hard crash ...
        GracefulWeight float64 // ... or graceful Stop, by weight
        StopGrace      machine.Duration // 0 means 5 s
    }

### 7.3 Phases

    type Phase uint8

    const (
        PhaseFaults     Phase = iota // faults on
        PhaseRecovering              // faults off, nodes alive; liveness is checked
        PhaseRecover                 // draining the rest
    )

    func (w *World) Phase() Phase
    // SetPhase appends the "sim_phase" fact, makes buggify Fault() quiet outside
    // PhaseFaults, calls EnterRecovery on every service when PhaseRecovering
    // starts, then gives each live driver NodeSpec.OnPhase(p), in node order, as
    // one Step that takes no node time and skips the inbox.
    func (w *World) SetPhase(p Phase)

    // Phases is the broker schedule: Chaos, then Settling, then Settle.
    type Phases struct{ Chaos, Settling, Settle machine.Duration }

    // SchedulePhases adds the two phase changes as host tasks on HostLane and
    // sets Config.End to the sum.
    func SchedulePhases(w *World, p Phases)

The control plane "quiet tail" (`s.quiet`, `env.setQuiet(true)`) is
`w.SetPhase(PhaseRecovering)`. The control plane has no drivers and its services
have no recovery mode, so only the quiet flag matters there.

### 7.4 Quiescent

The broker kernel emits a `sim_quiesce` fact every `QuiesceEvery`, with
"no workload has an op in flight". In v2 this is broker host code: a host task on
`HostLane` that asks its own workload objects (it holds them from the factory
closure) and calls `w.Fact(...)`. The kernel does not know "workload ops".

## 8. Point 6: randomness

One SplitMix64 stream per run, with a call counter: dst `rng.Rand`. It gets one new
method, from the broker `CountedRand`:

    // Derive returns an independent stream from (seed, label). It reads only the
    // seed and draws nothing, so adding a label never moves the run stream.
    // Formula (broker today): New(mix(seed ^ mix(fnv1a(label)))).
    func (r *Rand) Derive(label string) *Rand

`rng.Rand` stores its seed for this. Everything else of `CountedRand` is already in
`rng.Rand` (`Uint64`, `Intn`, `Float64`, `Chance`, `Between`, `Calls`, `Bytes`).
`NewID` is two `Uint64` draws, little-endian: the broker `NewID` and the control
plane `env.NewID` (`rand.Bytes(id[:])`) already give the same bytes. `Reseed` and
`Fingerprint` are deleted (no caller outside the kernel tests). The `CountedRand`
interface is deleted: the kernel uses `*rng.Rand`, machines see `machine.Rand`.

### 8.1 Two traps

- `Between`: dst includes both ends (`[lo, hi]`); the broker excludes `hi`
  (`[lo, hi)`). v2 keeps the dst meaning. T1.12 rewrites the 7 broker call sites
  (`grep -rn '\.Between(' broker`) to `Between(lo, hi-1)` and checks each with a
  test, so the ranges do not change.
- `Intn(n <= 0)`: dst returns 0; the broker panics. v2 keeps the dst meaning.

### 8.2 Swarm and buggify

The broker swarm (per-seed knobs) draws from `w.Rand().Derive("swarm")`, as today.
The product calls it while it builds the run, so the kernel has no `SeedConfig`.

Buggify uses dst `fault.Injector` (activation from the run stream with p=0.5, fire
p=0.05, `Fault` quiet outside PhaseFaults). The broker kernel has its own injector
(activation from `Derive("buggify/"+site)`, fire p=0.25), but no broker code calls
`Maybe`, `MaybeP` or `Fault`, so it is deleted with no change of behavior.

## 9. Point 7: trace hash, tags, timeline

### 9.1 What is folded

The trace hash is dst `trace.Hash` (the same FNV-1a code as broker `core.Hasher`).
The kernel folds, in this order (the broker kernel order, extended to typed values):

1. For each queue pop: time, entry kind, lane, target (node name and boot number,
   or service name), then the body: `In.Fold` for a delivery, tag 901 for a wake,
   902 and the phase for a phase change. Host tasks and service `Later` steps fold
   kind and lane only (today engine bodies are not folded either).
2. For each Step: node name, then `in.Fold`. For `OnPhase`: node name, 903, phase.
3. For each output, before it is routed: `out.Fold`. For each service request:
   `req.Fold` (it is an output of the caller).
4. For each fact, when it reaches the timeline: `Fact.Fold` (tag 501, name, fields).
5. After each pop: for every node: up or down; if up: state hash, Control length,
   Data length, busy-until. Then every service state hash. Cached hashes are
   recomputed only after the kernel called into the node or service;
   `VerifyHashes` recomputes all and fails the seed on a mismatch.

`Result.Hash` is the hash at the end. The control plane `finalHash` extras (steps,
acked writes, log slots, random calls) become one host fact "sim_end" with those
fields, appended before `Finish`, so they are folded by rule 4.

### 9.2 Tags without a big hand table

Today `tags.go` (615 lines) has one hand-written switch that gives every broker
event, action, control plane op and response, and fact value a frozen number.

In v2 every case of a product sum has a `Fold(h *trace.Hash)` method, next to its
type, that writes the **same** tag number and the **same** fields in the same
order as `tags.go` today. The sum interface includes `Fold`, so a new case without
it does not compile. This replaces both the "untagged type panics" rule and the
`tags_test.go` scan of `core/`.

Because the bytes are the same, this move does not move any golden hash. That is
why T1.6 does it while the broker still runs on its old kernel: `tags.go` just calls
`b.Fold(&h.Hash)`, and the golden test proves nothing changed.

To keep tag numbers unique, each product sum also gets `Tag() uint64`, and `Fold`
starts with `h.Uint64(x.Tag())`. A product test folds one value of every case and
checks that no two tags are equal. The list of values in that test is built by a
helper with an exhaustive switch, so `gochecksumtype` reports a case that is
missing from it.

### 9.3 Timeline

The broker timeline moves to the kernel, unchanged:

    type Timeline struct{ /* unexported */ }      // was MemTimeline

    func NewTimeline() *Timeline
    // Append stamps and records a fact. In a run only the kernel calls it
    // (tests call it to build a timeline by hand).
    func (t *Timeline) Append(source machine.NodeInstance, at Time, f machine.Fact) uint64
    func (t *Timeline) Len() int
    func (t *Timeline) Since(name string, c *Cursor) []TimedFact
    func (t *Timeline) Snapshot(name string) []TimedFact

    type Cursor struct{ /* unexported */ }

    type TimedFact struct {
        Seq    uint64
        At     Time
        Source machine.NodeInstance // zero for kernel and service facts
        Fact   machine.Fact
    }

    func (w *World) Timeline() *Timeline
    func (w *World) Fact(f machine.Fact) // a host fact, source zero

Kernel facts keep their names: `sim_restarted`, `sim_crashed`, `sim_tripped`,
`sim_exited`, `sim_phase`, `sim_stop_requested`, `sim_fault`. `sim_frozen` goes with
freeze. `sim_quiesce` becomes broker host code (7.4).

### 9.4 Rules (invariants)

    // Rule checks one property. A fresh Rule per run, so there is no Reset.
    type Rule interface {
        Name() string
        Observe(v View) // after every queue pop
        AtEnd(v View)   // once, after the run
    }

    type View interface {
        Step() uint64
        Now() Time
        Phase() Phase
        Timeline() *Timeline
        Assert() machine.Assertions
    }

    func (w *World) AddRule(r Rule)

`View.Objects`, `View.CP` and `View.History` are deleted. A broker rule gets typed
pointers to the fake object store, the fake control plane and the history when the
host builds it. `View` stays an interface so tests can stub it (they do today).

## 10. Point 8: assertions and buggify sites as a catalog

### 10.1 Assertions

Each assertion is declared once, as a package-level value. The declaration puts it
in a catalog. The value has a type per kind, so a `Sometimes` property cannot be
used as an `Always`.

    package assert

    type Always struct{ /* unexported: name */ }
    type AlwaysOrUnreachable struct{ /* ... */ }
    type Sometimes struct{ /* ... */ }
    type Reachable struct{ /* ... */ }
    type Unreachable struct{ /* ... */ }

    // Declare these only in package-level var blocks. A name declared twice
    // panics at program start.
    func NewAlways(name string) Always
    func NewAlwaysOrUnreachable(name string) AlwaysOrUnreachable
    func NewSometimes(name string) Sometimes
    func NewReachable(name string) Reachable
    func NewUnreachable(name string) Unreachable

    func (p Always) Name() string // and the same for each kind

    type Declared struct {
        Name string
        Kind Kind
    }

    // Catalog returns every declared assertion, sorted by name.
    func Catalog() []Declared

    const (
        KindAlways              Kind = "always"
        KindAlwaysOrUnreachable Kind = "always_or_unreachable" // NEW (broker kind)
        KindSometimes           Kind = "sometimes"
        KindReachable           Kind = "reachable"
        KindUnreachable         Kind = "unreachable"           // NEW (broker kind)
    )

Use, in a product package:

    var (
        epochClaimed  = assert.NewReachable("logcp-sim: epoch claimed")
        oneWriter     = assert.NewAlways("sim: one writer per epoch")
    )

    env.Reachable(epochClaimed)

`assert.Log` records five kinds (today it records three: AlwaysOrUnreachable is
stored as "always" and Unreachable is not counted). It also records, per name, the
step of the first interesting evaluation (a failed safety check or a passed
coverage check), as the broker `AssertionStat.FirstStep` does. The control plane
never calls `AlwaysOrUnreachable` or `Unreachable`, so its report lines do not
change.

Dynamic names (built with `+` at run time) are not allowed. Today they are: the
control plane `s.violation("...")` adds `"sim: "`, `check.Checker` adds its
`Prefix`, and the broker `FaultCoverageCheck(prefix)` adds `"echo:"` or `"kafka:"`.
Each such name becomes a declared value. `check.NewChecker(prefix)` declares its own
names (`prefix + "log has no hole"`, `prefix + "log entries decode"`) and must also
be called at package level.

### 10.2 Buggify sites

    package fault

    type Site struct{ /* unexported: name */ }

    func NewSite(name string) Site // package-level only; a duplicate panics
    func (s Site) Name() string
    func Sites() []string          // the catalog, sorted

`simulator.SiteNames()` and `rebalance.SiteNames()` are deleted.

### 10.3 How the campaign uses it

- "never reached" = every declared `Sometimes` or `Reachable` whose total passes
  over the campaign is zero (it may have no coverage entry at all).
- "never checked" = every declared `Always` with zero checks (broker rule: an Always
  must be reached at least once).
- "never fired" = every declared site with zero fires.

`expectedReachable` in `cmd/cpsim/campaign.go` and the broker
`Config.ExpectReachable` are deleted. T1.11 prints the old list and the catalog
list before it deletes the old one. Known difference today: the control plane
declares 30 `Reachable` names, and `expectedReachable` lists 27. Not in the list:
"logcp-sim: idempotent replay after takeover", "logcp-sim: put refused at or below
the floor", "logcp-sim: replayed key answered from the window". See decision D5.

## 11. Point 9: the spec toolkit (`dst/check`)

`dst/model` and `dst/mutant` merge into `dst/check`. The log-replay helpers stay as
they are. New parts are built in T1.3.

    package check

    // Spec is a small, fault-free program that says what the right answer is.
    type Spec[Op, Res any] interface {
        Apply(op Op) Res
        StateHash() uint64
    }

    // Subject is the code we compare with the spec. The error means "no answer at
    // all" (infrastructure), not a business error.
    type Subject[Op, Res any] interface {
        Apply(op Op) (Res, error)
    }

    // HasStateHash is optional on a Subject: when both sides have a state hash,
    // Lockstep compares them after every op.
    type HasStateHash interface{ StateHash() uint64 }

    // Reboots are the restart ops a subject may have (control plane conformance
    // Target). A nil func is never drawn.
    type Reboots struct {
        Reboot      func()
        DirtyReboot func(keep int) // keep: 0 all checkpoints, 1 all but the newest, 2 none
        Checkpoint  func()
        MaxKeep     int            // DirtyReboot draws keep in [0, MaxKeep]
    }

    type Lockstep[Op, Res any] struct {
        Name       string                          // Go type of Op, for the replay text
        NewSpec    func() Spec[Op, Res]
        NewSubject func(t *rapid.T) Subject[Op, Res]
        Ops        *rapid.Generator[Op]            // build it with SimplestFirst
        Equal      func(want, got Res) error
        Invariant  func() error                    // nil: none
        Reboots    Reboots
        HasFailed  func() bool                     // dst/model CheckAllowFailed flag; nil: never
        Format     func(op Op) string              // one op as Go source
        Replay     Replay                          // how to print the rerun command
    }

    // Check runs rapid.Check. On failure it prints the seed, the shrunk op list
    // as Go code, and the command to run it again.
    func (l Lockstep[Op, Res]) Check(t *testing.T)

    // Steps is the body, for a caller that owns rapid.Check.
    func (l Lockstep[Op, Res]) Steps(t *rapid.T)

    // SimplestFirst picks among gens; rapid shrinks toward the first ones.
    func SimplestFirst[V any](gens ...*rapid.Generator[V]) *rapid.Generator[V]

    // SelfCheck compares a spec with a naive model (for example a sorted slice).
    func SelfCheck[Op, Res any](t *testing.T, spec, naive func() Spec[Op, Res],
        ops *rapid.Generator[Op], equal func(a, b Res) error)

    // Replay builds the rerun command. EnvPrefix is set by the product (for
    // example "MDS"), because dst has no product words. The variables are
    // <prefix>_SEED, <prefix>_SEEDS and <prefix>_CAMPAIGN (broker runsettings).
    type Replay struct {
        EnvPrefix string
        Command   func(seed uint64, test string) string
    }

    // Planted bugs (was dst/mutant).
    type Bug string
    func RequireCaught(t testing.TB, bugs []Bug, budget int, run func(bug Bug, seed uint64) (caught bool))
    func RequireClean(t testing.TB, budget int, run func(seed uint64) (failed bool))

    // Kept from dst/model: Write, CheckPersistence, CheckForwardProgress, DirtyReboot.
    // Kept from dst/check: Slot, Log, Checker, Verdict, NoHole, Replay (the log
    // replay, renamed ReplayLog to free the name Replay), Holders, Prefixes,
    // Monotonic, Node, Converged, AllEqual.

    // NewChecker declares the checker's own assertions with the prefix. Call it
    // at package level.
    func NewChecker(prefix string) Checker
    func (c Checker) Verdict(ok bool, p assert.Always, format string, args ...any) bool

Note on `args ...any` in `Verdict`: it is a format argument list, read only by
`fmt.Sprintf` when the check fails. It is not a stored value and not a product path.

Shrinking is rapid's. The broker generator, shrinker and comparator are deleted in
T1.14 (plan).

## 12. Point 10: the campaign API (`dst/campaign`)

    package campaign

    // Scenario is one named simulation. Run builds and runs one seed.
    type Scenario struct {
        Name   string
        Detail string
        Run    func(seed uint64, o RunOptions) Result
    }

    type RunOptions struct {
        Log         bool // keep every step line
        HaltOnFirst bool // stop at the first failure (replay)
    }

    type Registry struct{ /* unexported */ }

    func (r *Registry) Add(s Scenario) // a duplicate name panics
    func (r *Registry) Names() []string
    func (r *Registry) Get(name string) (Scenario, bool)

    // Result is one seed. Fields that exist today are kept.
    type Result struct {
        Seed, Hash       uint64
        Steps, RandCalls uint64            // NEW: for the determinism canary
        OK               bool
        Reproduce        string
        Failures         []Failure         // NEW: Property, Detail, Step, AtMs, Repeats
        Coverage         map[string]Coverage
        Counters         map[string]uint64
        Sites            map[string]fault.SiteCount
        Tail             []string          // last step lines of a failed run
    }

    // FromKernel turns a kernel result into a campaign result.
    func FromKernel(r kernel.Result, reproduce string) Result

    type Options struct {
        Scenarios   []string // "all": every registered one
        First       uint64
        Seeds       int
        Workers     int      // 0: GOMAXPROCS
        UntilStable bool
        StableAfter int
        MaxSeeds    int
        CheckEvery  int      // determinism canary: run every Nth seed twice; 0: off
        MinSeedsForCoverage int // below this many seeds, coverage gaps do not fail
        Report      bool
        JSONPath    string
    }

    // Config is what a product gives the standard main.
    type Config struct {
        Tool      string // command name, for replay lines ("cpsim")
        EnvPrefix string // "MDS": reads MDS_SEEDS, MDS_SEED, MDS_CAMPAIGN
    }

    // ParseOptions reads the standard flags (-seeds, -start/-seed, -scenario,
    // -until-stable, -stable-after, -max-seeds, -report, -json, -j) and the
    // environment variables. Flags win over variables. One place parses both.
    func ParseOptions(c Config, args []string, getenv func(string) string, stderr io.Writer) (Options, error)

    // Run runs the campaign. Seeds run in parallel; results fold in seed order.
    // Every CheckEvery-th seed runs a second time with Log flipped; a difference
    // in (Hash, RandCalls, Steps) is a FailNotRepeatable for that seed.
    func Run(reg *Registry, o Options) Report

    // Replay runs one seed with Log and HaltOnFirst and prints its log.
    func Replay(reg *Registry, scenario string, seed uint64, w io.Writer) int

    // Main is the whole main function of a product simulator command.
    func Main(c Config, reg *Registry, args []string, stdout, stderr io.Writer) int

    // Report: moved from controlplane/cmd/cpsim/report.go, same fields and text.
    type Report struct { /* Seeds, Runs, Passed, Failed, FailedRuns, Assertions,
        NeverReached, NeverChecked (NEW), Sites, NeverFired, Curve, LastGrowth,
        LastSeed, UntilStable, Nondeterminism (NEW, from the broker) */ }

    func (r Report) WriteText(w io.Writer)
    func (r Report) WriteJSON(w io.Writer) error
    func (r Report) Failed() bool

    // Kept: Coverage, SiteTotal, Counts, Summary, Summary.Merge, Missing,
    // NeverFired. The old Run(first, seeds, workers, run) is renamed RunSeeds.

Golden pins: `dst/golden` is kept. T1.4 adds one helper for the broker style (a
text file of lines, rewritten with `-update-golden`):

    func Lines(t testing.TB, path string, got []string, update bool)

Report text: the cpsim format is kept where the two old formats differ (plan
T1.4). The broker per-seed lines (`-v`) and its `kind` words ("violation",
"protocol", ...) become one more section of the text, listed in the T1.4 report.

## 13. The one file with type erasure

**`dst/kernel/erase.go`** is the only file in dst (and in every product) where a
typed value is stored in a way that hides its type. It holds:

- `type nodeSlot interface{ ... }` (unexported methods): the kernel keeps
  `[]nodeSlot`; each is a `*nodeHost[In, Out]`.
- `type serviceSlot interface{ ... }`: `[]serviceSlot`; each is a
  `*serviceHost[Req, In]`.
- `type entry struct{ ...; body machine.Message; run func() }`: a queue entry. The
  closure `run` was made by a generic function that knew the types; `body` is kept
  only to fold it into the trace and to print it in a log line.

No value of type `any` (or `interface{}`) exists in `dst/kernel` at all; the design
does not need one. T1.2 adds a test in `dst/kernel` that loads the package with
`go/types` and fails if any field, variable, parameter or result in the package
has type `any`, or if a file other than `erase.go` declares an unexported
interface type or a field of type `func()` (the two ways to hide a generic node,
service or entry). The public interfaces of section 4 to 9 (`Node`, `Service`,
`Rule`, `View`) are not erasure: they are the API. (Type parameter constraints such
as `[K comparable]` are not values and are allowed.)

Other uses of `any` that stay, and why they are not product paths:
`assert.Pair.Value` (failure detail text only), the `args ...any` of format
functions (formatted at once), and `trace.Ring[T any]` (a constraint).

## 14. Point 11: mapping table

Every exported name of these five packages has one row:

- `broker-simulator`: `broker/internal/verify/simulator` (the broker kernel)
- `broker-campaign`: `broker/internal/verify/simulator/internal/campaign` (kernel
  presets; T1.12d deletes this package)
- `dst-sim`: `dst/sim`
- `dst-eng`: `dst/eng`
- `cp-simulator`: `controlplane/internal/verify/simulator` (host code)

"Exported name" means: every exported top-level const, var, func and type, every
exported method of an exported type as `Type.Method`, and every method of an
exported interface as `Interface.Method`. Struct fields are not rows; where fields
go is said in the row of their struct. The lists were made by a small `go/ast`
program (non-test files only) and checked against `go doc -all`.

Rows have the form `| package | Name | v2 place |`. "Product" means the name stays
in (or moves to) product code, not dst.

### 14.1 broker-simulator (148 names)

| package | name | v2 place |
|---|---|---|
| broker-simulator | AssertionStat | `campaign.Report.Assertions` rows; per seed `assert.Coverage`; FirstStep is recorded by `assert.Log` (10.1) |
| broker-simulator | Config | split: kernel knobs to `kernel.Config` (Seed, MaxSteps, Log, HaltOnFirst, VerifyHashes, Attrition, Phases via `SchedulePhases`); Processes and Workloads to `kernel.Spawn` calls; Invariants and InvariantsFor to `World.AddRule`; Engines to `kernel.AddService`; fault knobs, Swarm, Params to product scenario code; ExpectReachable deleted (catalog, 10.3); Scripted deleted (see PlannedFault); CheckDeterminism to `campaign.Options.CheckEvery`; QuiesceEvery to broker host task (7.4); Cost to `NodeSpec.Cost` |
| broker-simulator | ControlPlaneFaults | product: broker `fakecontrolplane` package (its config) |
| broker-simulator | ControlPlaneRealState | deleted from the kernel: rules get a typed pointer to the fake control plane (9.4) |
| broker-simulator | ControlPlaneRealState.Committed | stays a method of the broker fake control plane, called directly |
| broker-simulator | ControlPlaneRealState.Gen | stays a method of the broker fake control plane, called directly |
| broker-simulator | ControlPlaneRealState.HighWatermark | stays a method of the broker fake control plane, called directly |
| broker-simulator | CostModel | deleted: one implementation; `kernel.CostParams.Sample` (6.5) |
| broker-simulator | CostModel.Sample | `kernel.CostParams.Sample` |
| broker-simulator | CostParams | `kernel.CostParams`; field PerByte deleted (no scenario sets it) |
| broker-simulator | CountedRand | `*rng.Rand` (dst/rng); machines see `machine.Rand` |
| broker-simulator | CountedRand.Between | `rng.Rand.Between`; note: v2 includes hi (8.1) |
| broker-simulator | CountedRand.Calls | `rng.Rand.Calls` |
| broker-simulator | CountedRand.Chance | `rng.Rand.Chance` |
| broker-simulator | CountedRand.Derive | `rng.Rand.Derive` (new method, same formula) |
| broker-simulator | CountedRand.Fingerprint | deleted: no caller outside kernel tests; the canary compares hash, calls and steps |
| broker-simulator | CountedRand.Float64 | `rng.Rand.Float64` |
| broker-simulator | CountedRand.Reseed | deleted: no caller outside kernel tests |
| broker-simulator | Counter | `kernel.Counter` (same counter names, same order) |
| broker-simulator | Cursor | `kernel.Cursor` |
| broker-simulator | DeliveryLane | `kernel.Lane` |
| broker-simulator | DynamoMaxCommitItems | product: broker `fakecontrolplane` package |
| broker-simulator | Entry | unexported `entry` in `dst/kernel/erase.go`; only the kernel makes entries |
| broker-simulator | EntryCrash | unexported entry kind; host code calls `World.Crash` |
| broker-simulator | EntryDeliver | unexported entry kind (still folded into the trace) |
| broker-simulator | EntryFakeServiceStep | unexported entry kind for `ServiceContext.Later` |
| broker-simulator | EntryID | `kernel.EntryID` |
| broker-simulator | EntryKind | unexported entry kind type (still folded into the trace) |
| broker-simulator | EntryPhase | deleted: a phase change is a host task (`SchedulePhases`) |
| broker-simulator | EntryRecover | deleted: the quiesce point is a broker host task (7.4) |
| broker-simulator | EntryRestart | unexported entry kind; host code calls `World.Boot` |
| broker-simulator | EntryStop | unexported entry kind; host code calls `World.Stop` |
| broker-simulator | FailKind | `kernel.FailKind` |
| broker-simulator | FailMaxSteps | `kernel.FailMaxSteps` |
| broker-simulator | FailNotRepeatable | `kernel.FailNotRepeatable` (set by `campaign.Run`) |
| broker-simulator | FailPanic | `kernel.FailPanic` |
| broker-simulator | FailProtocol | `kernel.FailProtocol` |
| broker-simulator | FailUndecided | deleted: no code sets it (only `cmd/mdssimulator` prints its name) |
| broker-simulator | FailViolation | `kernel.FailViolation` |
| broker-simulator | Failure | `kernel.Failure` (Prop becomes Property string; adds Repeats from the control plane) |
| broker-simulator | FakeService | `kernel.Service[Req, In]` (5.2) |
| broker-simulator | FakeService.EnterRecovery | `Service.EnterRecovery` |
| broker-simulator | FakeService.Execute | `Service.Handle` |
| broker-simulator | FakeService.Handles | deleted: the route switch picks the service at compile time |
| broker-simulator | FakeService.Name | `Service.Name` |
| broker-simulator | FakeService.OnCrash | `Service.OnCrash` |
| broker-simulator | FakeService.OnInternal | deleted: `ServiceContext.Later` takes a typed closure |
| broker-simulator | FakeServiceContext | `kernel.ServiceContext[In]` |
| broker-simulator | FakeServiceContext.Assert | `ServiceContext.Assert` |
| broker-simulator | FakeServiceContext.Cancel | `ServiceContext.Cancel` |
| broker-simulator | FakeServiceContext.Deliver | `ServiceContext.Deliver` (takes `Ref[In]`) |
| broker-simulator | FakeServiceContext.Fault | `ServiceContext.Fault` |
| broker-simulator | FakeServiceContext.Lane | `kernel.NewLanes[K]` and `Lanes.Get` (typed keys, 6.2) |
| broker-simulator | FakeServiceContext.Now | `ServiceContext.Now` |
| broker-simulator | FakeServiceContext.Rand | `ServiceContext.Rand` |
| broker-simulator | FakeServiceContext.Self | `ServiceContext.Later` |
| broker-simulator | FastWithRareSlow | `kernel.FastWithRareSlow` |
| broker-simulator | FastWithRareSlow.Sample | `kernel.FastWithRareSlow.Sample` (takes `*rng.Rand`) |
| broker-simulator | History | product: broker simulation `history` package, generic over op and result types (no `any`) |
| broker-simulator | History.Invoke | product: `history.History.Invoke` |
| broker-simulator | History.Ops | product: `history.History.Ops` |
| broker-simulator | History.Return | product: `history.History.Return` |
| broker-simulator | HistoryOperation | product: `history.Operation[Op, Res]` |
| broker-simulator | Latency | deleted: one implementation, no other user |
| broker-simulator | Latency.Sample | `kernel.FastWithRareSlow.Sample` |
| broker-simulator | LogEntry | `kernel.LogEntry` |
| broker-simulator | LogParams | product: broker `fakelogservice` (or deleted with it, decision D4) |
| broker-simulator | LogStoreFaults | deleted: no code reads it |
| broker-simulator | MemTimeline | `kernel.Timeline` |
| broker-simulator | MemTimeline.Append | `kernel.Timeline.Append` |
| broker-simulator | MemTimeline.Len | `kernel.Timeline.Len` |
| broker-simulator | MemTimeline.Since | `kernel.Timeline.Since` |
| broker-simulator | MemTimeline.Snapshot | `kernel.Timeline.Snapshot` |
| broker-simulator | NetworkFaults | product: broker `fakenetwork` package; field ClockSkewMax to `kernel.Config.ClockSkewMax` |
| broker-simulator | NewCostModel | deleted: `kernel.CostParams.Sample` |
| broker-simulator | NewNodeInbox | deleted: the inbox is internal to the kernel; tests use a `World` |
| broker-simulator | NewRand | `rng.New` |
| broker-simulator | NewRunner | deleted: `campaign.Run` |
| broker-simulator | NewScheduler | deleted: the queue is internal to the kernel; tests use a `World` |
| broker-simulator | NewTimeline | `kernel.NewTimeline` |
| broker-simulator | NodeAlive | deleted: no caller; host code uses `World.Up` |
| broker-simulator | NodeAlive.Alive | `World.Up` |
| broker-simulator | NodeCurrent | `ServiceContext.Current` |
| broker-simulator | NodeCurrent.Current | `ServiceContext.Current` (takes `Addr[In]`, returns `Ref[In]`) |
| broker-simulator | NodeInbox | internal kernel inbox (6.5) |
| broker-simulator | NodeInbox.BusyUntil | internal (folded into the trace as today) |
| broker-simulator | NodeInbox.Control | internal; class from `NodeSpec.Class` |
| broker-simulator | NodeInbox.Data | internal; class from `NodeSpec.Class` |
| broker-simulator | NodeInbox.Push | internal; delivery puts the input in its inbox |
| broker-simulator | NodeKills | `kernel.Attrition` (MaxDead, CrashProb, CrashWeight, GracefulWeight); RecoveryDelay to `NodeSpec.Restart`; FreezeProb and FreezeDur deleted (no scenario sets them; `Pause` covers it) |
| broker-simulator | ObjectStoreFaults | product: broker `fakeobjectstore` package |
| broker-simulator | ObjectStoreRealState | deleted from the kernel: rules get a typed pointer to the fake object store |
| broker-simulator | ObjectStoreRealState.EachObject | stays a method of the fake object store, called directly |
| broker-simulator | ObjectStoreRealState.Gen | stays a method of the fake object store, called directly |
| broker-simulator | ObjectStoreRealState.Object | stays a method of the fake object store, called directly |
| broker-simulator | OpGivenUp | product: broker `history` package |
| broker-simulator | OpOutcome | product: broker `history` package |
| broker-simulator | OpPending | product: broker `history` package |
| broker-simulator | OpResultKnown | product: broker `history` package |
| broker-simulator | OpResultUnknown | product: broker `history` package |
| broker-simulator | Phase | `kernel.Phase` |
| broker-simulator | PhaseFaults | `kernel.PhaseFaults` |
| broker-simulator | PhaseRecover | `kernel.PhaseRecover` |
| broker-simulator | PhaseRecovering | `kernel.PhaseRecovering` |
| broker-simulator | Phases | `kernel.Phases` and `kernel.SchedulePhases`; field MaxQuietLatency to broker product presets (only product rules read it) |
| broker-simulator | PlannedFault | deleted: test only; a test calls `w.At(t, HostLane, func() { w.Crash(n, "planned") })` |
| broker-simulator | Process | `kernel.NodeSpec[In, Out]` and `kernel.Spawn` (Count: the host calls Spawn Count times; Factory and FactoryFor: `NodeSpec.New`, a closure over the typed per-seed params; NoAttrition and HiddenFromClients deleted, nothing sets them; Freezable deleted with freeze) |
| broker-simulator | RandomSettingsPicker | deleted: the product swarm function, called by the scenario with `w.Rand().Derive("swarm")` |
| broker-simulator | ReceivesPhaseChange | `NodeSpec.OnPhase` |
| broker-simulator | ReceivesPhaseChange.OnPhase | `NodeSpec.OnPhase` |
| broker-simulator | Report | `campaign.Report` |
| broker-simulator | RunSeed | the product scenario `Run` function on a `kernel.World` |
| broker-simulator | Runner | deleted: `campaign.Run` and `campaign.Replay` |
| broker-simulator | Runner.Replay | `campaign.Replay` |
| broker-simulator | Runner.Run | `campaign.Run` |
| broker-simulator | SafetyRule | `kernel.Rule` |
| broker-simulator | SafetyRule.AtEnd | `kernel.Rule.AtEnd` |
| broker-simulator | SafetyRule.Name | `kernel.Rule.Name` |
| broker-simulator | SafetyRule.Observe | `kernel.Rule.Observe` |
| broker-simulator | Scheduler | internal kernel queue (6.1) |
| broker-simulator | Scheduler.Cancel | `World.Cancel`, `ServiceContext.Cancel` |
| broker-simulator | Scheduler.DropNode | internal: done when an incarnation ends |
| broker-simulator | Scheduler.Len | internal |
| broker-simulator | Scheduler.Now | `World.Now` |
| broker-simulator | Scheduler.Pop | internal: `World.RunUntil` |
| broker-simulator | Scheduler.Schedule | internal; public ways in: `Inject`, `Send`, `SetTimer`, `Deliver`, `Later`, `At`, `After` |
| broker-simulator | SeedConfig | deleted: the product keeps typed per-seed params; no `Params any` |
| broker-simulator | SeedResult | `kernel.Result` (same counters; plus `campaign.FromKernel`) |
| broker-simulator | ServiceOutage | product: broker `fakeservices/internal/fakecompletions` |
| broker-simulator | Target | internal entry target |
| broker-simulator | Time | `kernel.Time` |
| broker-simulator | TimedFact | `kernel.TimedFact` |
| broker-simulator | Timeline | `kernel.Timeline` (a concrete type) |
| broker-simulator | Timeline.Since | `kernel.Timeline.Since` |
| broker-simulator | Timeline.Snapshot | `kernel.Timeline.Snapshot` |
| broker-simulator | View | `kernel.View` |
| broker-simulator | View.Assert | `kernel.View.Assert` |
| broker-simulator | View.CP | deleted: typed pointer in the rule (9.4) |
| broker-simulator | View.History | deleted: typed pointer in the rule (9.4) |
| broker-simulator | View.Now | `kernel.View.Now` |
| broker-simulator | View.Objects | deleted: typed pointer in the rule (9.4) |
| broker-simulator | View.Phase | `kernel.View.Phase` |
| broker-simulator | View.Step | `kernel.View.Step` |
| broker-simulator | View.Timeline | `kernel.View.Timeline` |
| broker-simulator | Workload | `kernel.NodeSpec` with `Driver: true` |
| broker-simulator | Workload.Name | `NodeSpec.Name` |
| broker-simulator | Workload.Quiescent | stays a method of the broker workloads, read by the broker quiesce host task (7.4) |
| broker-simulator | WorkloadFactory | `NodeSpec.New` (a closure that gets the broker addresses from the host) |

### 14.2 broker-campaign (8 names)

| package | name | v2 place |
|---|---|---|
| broker-campaign | FaultCoverageCheck | product: broker presets package, a `kernel.Rule` with two declared `Sometimes` per scenario |
| broker-campaign | Millis | deleted: write `n * machine.Millisecond` |
| broker-campaign | New | product: broker presets package (`internal/campaign` is renamed `internal/preset` so it does not clash with `dst/campaign`) |
| broker-campaign | PhaseStart | product: broker presets package, reads `sim_phase` facts from `kernel.Timeline` |
| broker-campaign | Phases | product: broker presets package, returns `kernel.Phases` plus MaxQuietLatency |
| broker-campaign | ProgressGraceTime | product: broker presets package |
| broker-campaign | ScenarioPreset | product: broker presets package; Attrition to `kernel.Attrition`, Cost to `kernel.CostParams`, CheckDeterminism to `campaign.Options.CheckEvery` |
| broker-campaign | Swarm | product: broker presets package; takes `*rng.Rand` |

### 14.3 dst-sim (36 names)

| package | name | v2 place |
|---|---|---|
| dst-sim | CancelTimer | `Router.CancelTimer`; in production `live.Loop.CancelTimer` |
| dst-sim | Config | `kernel.Config` (Seed, WallStart, MaxPops as MaxSteps); Steps: host uses `RunUntil`; OnStep: `NodeSpec.AfterStep` |
| dst-sim | Context | deleted: time comes as `machine.Now`, the rest from `machine.Env` at construction |
| dst-sim | Context.Assert | `machine.Env` assertion methods |
| dst-sim | Context.Faults | `machine.Env` Maybe, MaybeP, Fault |
| dst-sim | Context.Now | `machine.Now.Mono` (argument of `Step`) |
| dst-sim | Context.Rand | `machine.Env` Rand methods |
| dst-sim | Context.Wall | `machine.Now.Wall` |
| dst-sim | Crash | `Router.End(machine.Trip, cause)` for the node itself; `World.Crash(n, cause)` from host code |
| dst-sim | ErrProtocol | deleted: an unhandled output cannot exist; routes are exhaustive switches (`gochecksumtype`) |
| dst-sim | Exit | `Router.End(machine.Exit, cause)`; in production `live.Loop.End` |
| dst-sim | Failure | `kernel.Failure` |
| dst-sim | New | `kernel.New` |
| dst-sim | Process | `machine.Machine[In, Out]` plus `kernel.NodeSpec[In, Out]` |
| dst-sim | Process.ID | `NodeSpec.Name`; `Addr.ID` |
| dst-sim | Process.StateHash | `machine.Machine.StateHash` |
| dst-sim | Process.Step | `machine.Machine.Step` |
| dst-sim | ProcessID | `machine.NodeID` (typed `Addr[In]` to send) |
| dst-sim | Restart | `World.Boot` plus `NodeSpec.New` (field New) and `NodeSpec.Boot` (field Boot) |
| dst-sim | Result | `kernel.Result` |
| dst-sim | Runner | `kernel.World` |
| dst-sim | Runner.Assert | `World.Assert` |
| dst-sim | Runner.Crashed | `World.Up` (not up) and `World.Down` |
| dst-sim | Runner.Exited | `World.Down` (how == `machine.Exit`) |
| dst-sim | Runner.Faults | `World.Faults` |
| dst-sim | Runner.Inject | `kernel.Inject` |
| dst-sim | Runner.Rand | `World.Rand` |
| dst-sim | Runner.Register | `kernel.AddService` |
| dst-sim | Runner.Run | `World.RunUntil` then `World.Finish` |
| dst-sim | Runner.RunUntil | `World.RunUntil` |
| dst-sim | Runner.Sched | deleted: one queue owned by the world; host code uses `World.At` and `World.After` |
| dst-sim | Runner.Spawn | `kernel.Spawn` |
| dst-sim | Runner.Steps | `World.Steps` |
| dst-sim | Send | `kernel.Send` / `kernel.SendOn`; to itself `Router.SendSelf` |
| dst-sim | SetTimer | `Router.SetTimer`; in production `live.Loop.SetTimer` |
| dst-sim | StepInfo | `NodeSpec.AfterStep(self, in, out)`, typed |

### 14.4 dst-eng (16 names)

| package | name | v2 place |
|---|---|---|
| dst-eng | ActorID | `machine.NodeID`; to deliver, `Ref[In]` |
| dst-eng | Context | `kernel.ServiceContext[In]` |
| dst-eng | Context.Assert | `ServiceContext.Assert` |
| dst-eng | Context.Deliver | `ServiceContext.Deliver` |
| dst-eng | Context.Faults | `ServiceContext.Faults` |
| dst-eng | Context.From | the `from Ref[In]` argument of `Service.Handle` |
| dst-eng | Context.Now | `ServiceContext.Now` |
| dst-eng | Context.Rand | `ServiceContext.Rand` |
| dst-eng | Context.Self | `ServiceContext.Later` |
| dst-eng | Engine | `kernel.Service[Req, In]` |
| dst-eng | Engine.Handle | `Service.Handle` |
| dst-eng | Engine.ID | `Service.Name` |
| dst-eng | Engine.OnSelf | deleted: `Later` takes a typed closure |
| dst-eng | Engine.StateHash | `Service.StateHash` |
| dst-eng | ErrNotMine | deleted: each request type goes to one `ServiceAddr`, chosen at compile time |
| dst-eng | ID | `Service.Name` (a string) |

### 14.5 cp-simulator (21 names)

| package | name | v2 place |
|---|---|---|
| cp-simulator | Bug | product (stays); used with `check.RequireCaught` |
| cp-simulator | BugClaimAlwaysWins | product (stays) |
| cp-simulator | BugLogOverwrite | product (stays) |
| cp-simulator | BugNone | product (stays) |
| cp-simulator | Config | product (stays): Seed, Scenario, Nodes, Steps, Bug; Trace to `kernel.Config.Log` plus `World.Note` |
| cp-simulator | DefaultSteps | product (stays) |
| cp-simulator | Failure | deleted: `kernel.Failure` (same fields, plus Kind) |
| cp-simulator | Faults | product (stays): scenario knobs |
| cp-simulator | Report | product (stays) for the control plane stats lines; the failure and tail lines come from `kernel.Result` |
| cp-simulator | Result | `kernel.Result` plus the product `Stats` (`simulator.Result` keeps only Stats and embeds the kernel result) |
| cp-simulator | Result.OK | `kernel.Result.OK` |
| cp-simulator | Result.Reproduce | `campaign` builds the replay command from `campaign.Config.Tool`, scenario and seed |
| cp-simulator | Run | product (stays): the `Run` of each registered `campaign.Scenario` |
| cp-simulator | Scenario | product (stays); each one is registered as a `campaign.Scenario` |
| cp-simulator | ScenarioByName | deleted: `campaign.Registry.Get` |
| cp-simulator | ScenarioNames | deleted: `campaign.Registry.Names` |
| cp-simulator | SiteLogShortPage | product: a declared `fault.NewSite` value (10.2) |
| cp-simulator | SiteLogSlowAnswer | product: a declared `fault.NewSite` value |
| cp-simulator | SiteNames | deleted: `fault.Sites()` (catalog) |
| cp-simulator | SiteObjectsSlowAnswer | product: a declared `fault.NewSite` value |
| cp-simulator | Stats | product (stays) |

### 14.6 Unexported host code of the control plane simulator

Not rows of the check, but every host piece has a place:

| Today | v2 |
|---|---|
| `simNode`, `nodeProc` (node.go) | `kernel.Spawn` of the replica; `simNode` keeps only product state (cut, the replica pointer for checks) |
| `controller` process and its messages `bootNode`, `tripped`, `shutDown`, `killNode` | deleted: `World.Boot`, `World.Crash`, host tasks (7.2) |
| `replyEngine` | deleted: the replica route calls the client pool and the group host directly |
| `nodeOf` (parses "cp-2") | deleted: host code keeps a slice of addresses |
| `groupHost`, `groupProc` (groups.go) | `kernel.Spawn` of the rebalance machine; host keeps calls, terms and notes |
| `machineWake`, `replicaWake`, `queue` | deleted: lanes (6.3) |
| `storage` engine (services.go) | `kernel.Service[runtime.IOCall, runtime.EventData]` |
| `env` (env.go) | deleted: `World.Env()` |
| `sim.after`, `s.runner.Inject` | `World.After`, `kernel.Inject` |
| `onStep`, `onGroupsStep`, `observe`, trace ring | `NodeSpec.AfterStep`; `kernel.Config.Tail` keeps the last lines |
| `sim.hash`, `finalHash` | the kernel trace hash; extras as the "sim_end" fact (9.1) |

### 14.7 How to check this table

The name lists are in the scratchpad of the T1.1 session, one file per package
(`broker-simulator.txt`, `broker-campaign.txt`, `dst-sim.txt`, `dst-eng.txt`,
`cp-simulator.txt`), made by `listnames.go` in the same folder. To check again from
the repo root (needs the lists):

    for p in broker-simulator broker-campaign dst-sim dst-eng cp-simulator; do
      grep "^| $p | " dst/docs/v2-design.md | awk -F'|' '{gsub(/ /,"",$3); print $3}' \
        | LC_ALL=C sort > /tmp/rows-$p.txt
      diff "$LISTS/$p.txt" /tmp/rows-$p.txt && echo "$p ok"
    done

## 15. Where the starting proposals do not work in Go, and what this design does

| Proposal | Problem in Go or in the code | This design |
|---|---|---|
| `Step(now Time, in In) []Out` | Works, but one number is not enough: both products read the skewed node clock and the UTC wall clock; the rebalance machine also reads `cx.Rand()` | `Step(now machine.Now, in In)`, `Now{Mono, Wall}`; randomness from `Env` at construction (3.2) |
| `Route[Out any] func(r *Router, from Addr, out Out)` with a non-generic `Router` | Timers and self-sends must carry the node's own `In` type; with a plain `Router` they need an extra `self Addr[In]` argument everywhere; `from` repeats what the router knows | `Router[In]` is generic on the node's input type; self operations are methods; route is `func(r *Router[In], out Out)` (4.2) |
| `Send[In any](r *Router, to Addr[In], msg In)` | With `Router[In]` generic, a send to a node of another type needs a second type parameter; Go methods cannot have their own | `Send[In, To](r *Router[In], to Addr[To], msg To)`, a package function; callers write `kernel.Send(r, to, msg)` and inference fills both |
| `SetTimer[In any](r *Router, self Addr[In], ...)` | A timer belongs to one incarnation, not to the node name; a timer of an old incarnation must never fire | `Router[In].SetTimer(id, after, msg)` (method, bound to `Self()`, a `Ref`) |
| `Call[Req, Resp, In](r, svc ServiceAddr[Req, Resp], req, reply func(Result[Resp]) In, caller Addr[In])` | (1) The fake services build the completion themselves, with error code and `Outcome` (`MaybeApplied` after a lost reply); those are product rules, a generic `Result[Resp]` cannot hold them. (2) The network service sends to nodes that did not call (`ConnOpened`, `BytesReceived`) and some requests have no reply (`CloseConn`, `PauseReads`). (3) A reply closure per call would be a stored closure per request for no gain. (4) `caller` repeats the router | `Service[Req, In]` speaks the caller's input type; `Call[Req, In](r *Router[In], s ServiceAddr[Req, In], req Req)` (5.2) |
| `Addr[In]` made only by the kernel | Go cannot forbid the zero value `Addr[In]{}`, and an address can leak into another run | the kernel treats a zero address or an address of another world as `FailProtocol` (4.3) |
| Sealed sums checked by `gochecksumtype` | (1) The linter needs an interface with an unexported method and a `//sumtype:decl` comment. The control plane `runtime.ControlPlaneOperation` has an exported marker (`IsControlPlaneOperation`) so that `contract` can add cases: it is open and cannot be checked. (2) A sum cannot get a case from another package (plan trap). (3) The linter checks `switch x := v.(type)` only, not `if v, ok := x.(T)` chains, and by default a `default:` branch does not count | (1) T1.8 makes `contract.Operation` and `contract.Response` sealed sums inside `controlplane/contract`; `runtime` then imports `contract` (today it is the other way), and `contract` gets its base types from `dst/machine` instead of `runtime`, which breaks the cycle. (2) A case from another package is wrapped: broker `core.ObjectCall{Call objectstore.Call}` (T1.7) whose `Fold` writes the same bytes as today. (3) Set `default-signifies-exhaustive: false` (the default) in every `.golangci.yml`; product code uses type switches for sums |
| A hand tag table for the trace | 615 lines, panics at run time when a row is missing | `Fold` (and `Tag`) methods on every case; a missing method does not compile (9.2) |
| "Type erasure only in one kernel file" with `any` | Not needed: closures and unexported interfaces erase types without `any` | `dst/kernel/erase.go`, no `any` at all (13) |
| Lane keys typed "if possible without any" | Possible | `Lanes[K comparable]`, one map per key type, one shared counter (6.2) |

## 16. Point 12: migration order and golden hashes

### 16.1 The golden pins that exist today

| Pin | File | Values today |
|---|---|---|
| broker kafka, seeds 1..5 (steps, rand calls, hash) | `broker/internal/verify/simulator/kafka/testdata/golden_hashes.txt` | seed 1: steps=14154 rand=67851 hash=a332c735306406a3 ... seed 5: hash=a71871f39ccdc7de |
| control plane, with groups | `controlplane/internal/verify/simulator/golden_test.go` `goldenHashes` | basic/17 = 0xb8160450304fcc49, chaos/42 = 0xfe0e8c6763239d00 |
| control plane, without groups | same file, `goldenWithoutGroups` | basic/17 = 0x08bfa86385d8e8ae, chaos/42 = 0x45d210abda0e2bed |
| control plane campaign summary hashes (2000 seeds, printed by cpsim) | progress file / baseline | basic, log-faults, crash-restart, leader-fencing, chaos: aaf01c86 4032d6fb f1e59c13 0609269d 951ef86c |
| dst `trace` FNV vectors | `dst/trace/hash_test.go` | never move |

The echo scenario and dst `sim` tests have no golden pin.

### 16.2 Order

Stage P1a (no product code changes; no product golden moves):

1. **T1.2 kernel.** `dst/machine`, `dst/kernel` (with `erase.go` and its test),
   new methods in `rng` (Derive), `assert` (catalog, two kinds, typed handles; the
   old string methods stay until T1.16), `fault` (site catalog; old string `Site`
   API stays until T1.16). Port the kernel tests (old -> new table in the report).
   New copies of `dst/examples/kv` and `kvchaos` on v2, with new golden pins (new
   files, not moves). `gochecksumtype` on in every module. `dst/live` is NOT written
   here; T1.11 moves it.
2. **T1.3 check** and **T1.4 campaign** (may run beside T1.2, as the plan says).
3. **T1.5** stage gate.

Stage P1b (types and ownership; no golden moves expected):

4. **T1.6**, in separate commits:
   - a) base types from `dst/machine` replace the copies in `core` and `runtime`
     (type aliases first, then the copies are deleted). No hash move.
   - b) `Fold` and `Tag` methods on every broker `core` event, action, op, response
     and fact value, writing the bytes of `tags.go`; `tags.go` calls them. The
     broker golden must not move: this is the proof that the bytes are the same.
   - c) delete the dead `Log*` actions and their tags from broker `core`. Expected:
     no move, because the kafka and echo scenarios never produce them, so their
     tags are never folded. If the golden moves anyway, it is a RE-PIN commit of
     its own, as the plan says.
   - d) declare every assertion and buggify site as a catalog value and change the
     call sites to the typed handles (about 158 assertion calls). No hash move:
     assertion names are not hashed, and site names do not change, so the control
     plane buggify draws do not change.
5. **T1.7** objectstore: the broker object actions become one wrapper case
   (`core.ObjectCall`) whose `Fold` writes the old bytes. No move.
6. **T1.8** control plane public surface: `contract.Operation` and `Response`
   become sealed sums with `Fold` (the broker tag numbers 200..399). No move.
7. **T1.9**, **T1.10**. No move.

Stage P1c (RE-PIN):

8. **T1.11 control plane**, three steps:
   - a) the replica and the rebalance machine take the v2 contract
     (`machine.Machine[In, Out]`, sums with `Fold`). A small adapter in the control
     plane simulator wraps them as dst `sim.Process` for now. The rebalance machine
     draws from its `Env` (the same run stream, same order). **No hash move**: this
     proves that no machine rule changed.
   - b) `live` moves to `dst/live` (generic). No simulation hash is involved.
   - c) the host code moves to `dst/kernel` (nodes, storage service, lanes instead
     of wake tokens, host tasks, Pause). **RE-PIN**: `goldenHashes` basic/17 and
     chaos/42, `goldenWithoutGroups` basic/17 and chaos/42, and the five campaign
     summary hashes. One commit, message "RE-PIN: kernel moved to dst v2", old and
     new values. Then `cmd/cpsim` becomes a thin main over `dst/campaign`;
     `expectedReachable` is deleted after the catalog comparison (decision D5).
9. **T1.12 broker** (decision D3: side by side):
   - a) the fake services as `kernel.Service` values, in a new tree
     `broker/internal/verify/simulation/...`. The old tree and its goldens do not
     change.
   - b) the broker machine, the workloads, the rules and the oracle in the new tree;
     `History` moves to `simulation/history`; the 7 `Between` call sites are
     rewritten (8.1).
   - c) `cmd/mdssimulator` becomes a thin main over `dst/campaign`. **RE-PIN**: the
     new tree gets its own `testdata/golden_hashes.txt` (kafka seeds 1..5, steps,
     rand, hash) in one commit with the old and new lines in the message. Side by
     side, old and new campaigns give the assertion counts for the acceptance
     rule.
   - d) delete the old tree (kernel files, `internal/campaign` renamed to
     `internal/preset` and moved, `internal/session` moved: the workloads use it),
     then rename `simulation` back to `simulator` (a pure move, no hash change).
   - e) only if decision D2 says so: the broker production loop
     (`internal/system/run.go`, `inbox.go`) moves to `dst/live`. No hash.
10. **T1.13** stage gate, with old and new hashes side by side.

Stage P1d:

11. **T1.14** broker spec checks on `dst/check`. No hash.
12. **T1.15** (stretch) `Faulty(spec)`: **RE-PIN** of the broker kafka pins again
    if the fault draws change order. Only if it runs.
13. **T1.16** delete `dst/sim`, `dst/eng`, `dst/model`, `dst/mutant`, the old
    examples and the old string APIs of `assert` and `fault`. No hash.

### 16.3 Summary: which pins move where

| Task | Pins that move |
|---|---|
| T1.2 to T1.10 | none (T1.6c only if the dead `Log*` removal changes folds; expected: none) |
| T1.11c | control plane `goldenHashes` (2), `goldenWithoutGroups` (2), cpsim campaign hashes (5) |
| T1.12c | broker kafka seeds 1..5 (new file in the new tree; the old file is deleted in T1.12d) |
| T1.15 | broker kafka seeds 1..5 again, only if T1.15 runs |
| T1.16 | none |

### 16.4 Behavior that changes in a RE-PIN (the acceptance rule must still hold)

These changes move hashes but not what is checked:

1. Control plane: wake tokens become lanes; pause holds inputs in the inbox; trace
   hash folds inputs and outputs; replies to a dead incarnation draw nothing.
2. Broker: `SetTimer` with a pending id replaces the old timer (today the old one
   still fires and the machine ignores it by `Gen`); the inbox, cost and attrition
   logic is the same code, moved.
3. Both: kernel entries are closures; draw counts per entry stay one tie per entry.

If a seed range that passed before fails after the move, it is either a kernel bug
(fix it) or a real product bug that the new interleaving found. In the second case
the task stops and asks the user (plan 3.5).

## 17. Risks

1. **Unifying two kernels can hide a semantic change.** Mitigation: T1.11a and
   T1.6b are no-move steps that prove the machines and the tags did not change
   before any kernel move; then the P1c acceptance rule (same names, same seeds
   pass, same assertion and site counts, planted bugs caught).
2. **`Between` means two things today** (8.1). A silent off-by-one would change
   every delay range of the broker. Mitigation: one test per rewritten call site.
3. **Typed assertion handles touch about 158 call sites** and forbid dynamic names.
   Mechanical, but wide. Mitigation: its own commit in T1.6; no hash move expected.
4. **Open sum in the control plane contract** (15, row "sealed sums"): T1.8 must
   reverse the `runtime` / `contract` import direction. Mitigation: listed in the
   T1.8 card as a forced signature change.
5. **Closures per queue entry allocate.** The broker kafka seeds have 9k to 20k
   steps. Mitigation: T1.12 records the campaign time before and after; if it is
   much slower, the entry can keep a small typed payload instead (still in
   `erase.go`).
6. **`dst/live` for the broker** changes the production path; the e2e test covers
   only the main path. That is why D2 is a separate, optional sub-task.
7. **Environment variable names** `MDS_SEED`, `MDS_SEEDS`, `MDS_CAMPAIGN` are product
   words. dst takes a prefix (`EnvPrefix`), so dst stays free of product words.

## 18. Decisions I need from the user

Each has my recommendation. Please answer yes, no, or the other option.

- **D1. New package `dst/machine`** for the contract types, beside `kernel`, `live`,
  `check`, `campaign` (section 2). Recommend: **yes**.
- **D2. Broker production loop on `dst/live`** (T1.12e, optional, after the
  simulation move is green). Recommend: **yes, as its own sub-task**; skip it if
  the week runs out.
- **D3. Broker simulation moves side by side** (new tree, then delete the old one)
  instead of in place. Recommend: **side by side**; it gives the before/after
  assertion counts for free and keeps every step green.
- **D4. `fakelogservice` (the log control plane fake)**: no scenario uses it
  (kafka-log is deferred; only its own test imports it). Port it in T1.12a, or
  delete it with `LogParams`. Recommend: **delete**; git history keeps it.
- **D5. Catalog vs `expectedReachable`**: the catalog makes all 30 declared control
  plane `Reachable` names "expected"; the hand list has 27. Recommend: **all 30 are
  expected**; if one of the 3 is never reached in a 2000-seed campaign, T1.11
  stops and asks.
- **D6. Delete unused broker kernel features**: freeze (`FreezeProb`, `FreezeDur`,
  `Freezable`), `LogStoreFaults`, `NoAttrition`, `HiddenFromClients`,
  `CostParams.PerByte`, `Reseed`, `Fingerprint`, `PlannedFault` (test only),
  `NodeAlive`, `FailUndecided`, the broker buggify injector. Recommend: **delete**
  (none is used by a scenario or by product code).
- **D7. Timer rule**: `SetTimer` with a pending id replaces the old timer (dst,
  control plane and both production loops do this; only the broker simulator lets
  the old one fire). Recommend: **replace**.
- **D8. Assertion kinds**: `assert.Log` records the broker's five kinds. The
  control plane report does not change (it uses only three). Recommend: **yes**.

## 19. Changes found while building

Notes from the tasks that build this design. Each one says what did not work as
written, and the smallest change that was made.

### T1.2 (kernel)

1. **`fault.Site` cannot be the new typed handle yet.** Section 10.2 names the new
   handle `fault.Site`, and section 16.2 keeps the old string type `fault.Site`
   until T1.16. Go cannot have two types with one name, and the control plane
   uses the string type today (`fault.Site(site)`). The typed handle is named
   `fault.NamedSite` (made by `fault.NewSite`, as designed). `machine.Faults`
   takes `fault.NamedSite`. T1.16, which deletes the string type, may rename
   `NamedSite` to `Site`.
2. **Type inference needs the sum type for every generic call, not only `Call`.**
   Section 4.2 says this for `Call`. It is the same for `Send`, `SendOn`, `Inject`
   and `InjectOn`: Go infers `In` from the address and from the message, and both
   must give the same type. Write `kernel.Send(r, to, serverIn(putRequest(x)))` or
   `kernel.Inject[tin](w, a, 0, note{})`. Methods (`Router.SetTimer`,
   `ServiceContext.Deliver`) are not generic functions, so they take a case as it is.
3. **No `any` in the kernel, also for `recover()`.** `recover()` returns `any`, so
   `x := recover()` would be a variable of type `any`. The kernel writes
   `fmt.Sprint(recover())` inside the deferred function instead, and uses a "done"
   flag to know that a panic happened.
4. **FirstStep needs a step counter.** `assert.Log` gets
   `SetStepCounter(func() uint64)`, and `assert.Coverage` gets the field
   `FirstStep` (section 10.1). The kernel gives it `World.Steps`.
5. **The determinism gate forbids package-level variables.** The catalog of
   `assert` and `fault` is a package-level map, written only while packages
   initialize; it has a `//determinismcheck:ok` comment. Note for T1.6: the
   typed handles in product packages (`var x = assert.NewAlways(...)`) are
   package-level variables too, and broker `core` and control plane `runtime` are
   in their determinism gates. T1.6 needs a `//determinismcheck:ok` comment on
   those var blocks, or a small change to `determinismcheck` that accepts
   variables of the handle types.
6. **A one-case sum in tests.** `gocritic` (singleCaseSwitch) asks to replace a
   type switch with one case by an `if`, and `gochecksumtype` checks only
   switches. The kernel tests give their request sum a second case (`idle`)
   instead of turning the linter off.
7. **Small choices the design left open** (no behavior that a scenario uses):
   - the queue entry kinds keep the broker numbers (deliver 0, service step 1,
     kill 2, restart 3, phase 5) and add host task 7;
   - the counter "freezes" is gone with freeze; the other counter names and their
     order are the broker ones;
   - a timer that fired is removed from the table of pending timers, so a later
     `CancelTimer` of its id is not counted;
   - the kill at the end of a graceful stop has the cause "stop deadline"; the
     `sim_fault` fact names the service with the key "service";
   - `Crash` or `Stop` of a driver, a second node or service with one name, a
     `Stop` of a node without `StopMsg`, and using a `Router` after its Step are
     protocol failures;
   - `Spawn` every node before `Start`; a node spawned later stays down until
     `Boot`. `RunUntil` calls `Start` if it was not called;
   - a pause ends with the incarnation (as the control plane pause does today);
   - `machine.End` has a `String` method for log lines.
