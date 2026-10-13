# Chapter 3a: The domain model: toys3 through domain-driven design and control theory

> *"There are only two hard things in Computer Science: cache invalidation and naming things."* (Phil Karlton)

Chapter 3 listed every word of the vocabulary: every event, every action, every telemetry type. This chapter steps back and asks a different question: **what are the parts of this system, what is each part called, and why?**

That might sound like a question about taste. It is not. toys3 was refactored, after the first edition of this book, by a sequence of commits (`9b65410..a4018d5`) whose only purpose was to give the code a *domain model*: a set of named concepts, each with a clear home, a clear boundary and a clear relationship to the others. Not one behaviour changed. Every trace hash quoted in this book is the same before and after (Chapter 8, §8.12 shows how that was proven, commit by commit). And yet a reader of the old code and a reader of the new code learn different things from it, because the names now *say* what the old code only *did*.

Two bodies of ideas were used to do it:

- **Domain-driven design** (DDD), Eric Evans's method for organizing software around the concepts of its problem domain. DDD gives you tools for drawing boundaries: subdomains, bounded contexts, context maps, aggregates, domain events, ports and adapters.
- **Control theory**, the engineering discipline of feedback loops. toys3's architecture, `Step(Event) []Action` driven by completions from a disk, turns out to be a discrete-time controller in a feedback loop around a plant. Control engineering has had precise words for every part of such a loop for eighty years, and toys3 borrows them where they fit.

The chapter is organized in that order. §3a.1 to §3a.8 apply DDD: what the subdomains are, how the code splits into bounded contexts, how those contexts relate, what the aggregate is, what the domain events are, and where the ports are. §3a.9 to §3a.11 develop the control-theory reading end to end, with a block diagram mapped onto the code, and then, just as carefully, show where the metaphor was *not* applied and why. §3a.12 lists what the refactor deliberately left alone.

The source documents for this chapter are `toys3/REFACTOR-PROPOSAL.md` (the full analysis, including a glossary with a verdict and a reason for every name) and the "Domain model and vocabulary" section of `toys3/DESIGN.md` (`toys3/DESIGN.md:22-67`). Read them alongside; this chapter explains them.

---

## 3a.1 Why a toy needs a domain model

A fair objection: toys3 is under 5,000 lines of non-test Go. Its author knows what every function does. Who benefits from subdomains and bounded contexts?

Three answers.

**First, names are the interface between code and reader.** This book is about learning DST from a working system. A reader meets `FakeFS`, `Fact` and `QuietTail` and has to build their own mental model of what those are. A reader who meets `PlantModel`, `Telemetry` and `Settling` is handed one. If the names are drawn from a coherent model, every name the reader learns makes the next one easier: once you know there is a *plant*, a *plant model* explains itself, and so do *disturbances*, *dead time* and *feedback delay*.

**Second, names expose design decisions that were made implicitly.** Before the refactor, the per-key FIFO queue was a correct, carefully reasoned mechanism with no name except "per-key FIFO". It is now the **key interlock**, and the condition under which a request may start is its **start permissive**. Those are not cosmetic synonyms: the words come from industrial control, where an interlock is a mechanism that *prevents* two conflicting operations from running at once. That sentence is exactly the specification of the mechanism. Similarly, `Halt` did the right thing, but `Trip` tells you *why* it is the right thing: it is a safety trip, the controller driving itself to a safe state because it can no longer trust its own view of the plant.

**Third, boundaries let you reason about change.** The old `toys3/sim` package mixed four different jobs in one namespace (running the simulation, modelling the disk, generating traffic, judging the results), and nothing stopped an invariant from reaching into the fake disk's internals. After the refactor you can tell, from the types alone, which invariants look only at what clients saw (black-box) and which also look at the disk's true state (white-box), and the white-box ones see the disk only through a three-method interface. That matters the day you add a second plant (a simulated network) or a second controller (a replica): the boundaries tell you what has to change and what does not.

A domain model is not a framework. Nothing in toys3 imports a DDD library, inherits from an `AggregateRoot` base class or registers a repository. DDD here is a way of *thinking about* what is already there, and then naming it.

---

## 3a.2 Domain-driven design in one page

DDD has a strategic half and a tactical half.

**Strategic design** is about the big picture:

- A **domain** is the problem space the software serves. For toys3 it is "durable object storage, and the testing of it".
- A **subdomain** is one part of that problem space. Subdomains are classified as **core** (where the system's distinctive value lies, and where the best people and the most care should go), **supporting** (necessary and specific to this system, but not where it competes) and **generic** (necessary, but a solved problem; buy it or copy it).
- A **bounded context** is a part of the *solution* (the code) inside which every term has one precise meaning. The same word can mean different things in different contexts, and that is fine, as long as each context is explicit about its own meaning. "Model" means one thing inside the plant model and another inside the oracle; each context should say which.
- The **ubiquitous language** is the vocabulary shared by the people and the code within a context. The rule is that the code, the documentation and the conversation use the *same* words. A ubiquitous language is not a glossary that lives in a wiki: it is the identifiers.
- A **context map** draws the bounded contexts and the relationships between them. DDD names a handful of relationship patterns: **Shared Kernel** (two contexts share a small model and change it together), **Customer/Supplier** (the downstream context's needs drive the upstream's design), **Conformist** (the downstream simply adopts the upstream's model), **Anti-Corruption Layer** (ACL: the downstream translates the upstream's model into its own so it does not leak), **Open Host Service** and **Published Language** (the upstream offers a well-documented protocol for anyone to use), and **Separate Ways** (two contexts deliberately do not integrate).

**Tactical design** is about the inside of one context:

- An **entity** has an identity that persists while its attributes change. A **value object** is defined entirely by its attributes and is immutable.
- An **aggregate** is a cluster of entities and values that is treated as one unit for consistency: every invariant inside the aggregate holds at the end of every operation on it. The aggregate's **root** is the one entity outsiders may hold a reference to. The aggregate is the *consistency boundary*.
- A **domain event** records something that happened in the domain, named in the past tense (`OrderPlaced`, not `PlaceOrder`).
- A **repository** loads and saves aggregates.
- **Ports and adapters** (hexagonal architecture, Alistair Cockburn) is not DDD proper but travels with it: the domain defines **ports** (interfaces it owns), and the outside world plugs in through **adapters**. A *driving* port is how the world calls the domain; a *driven* port is how the domain calls the world.

Hold on to those definitions. The rest of the DDD half of this chapter applies them one at a time.

---

## 3a.3 The subdomains of toys3

| Subdomain | Type | Why | Where it lives |
|---|---|---|---|
| **Durable object control**: the write protocol, the key interlock, the trip policy, recovery | **Core** | It is the system under test. Every interesting decision is here: the order of fsync and rename, when to trip, which requests may run together. | `toys3/control` |
| **Verification**: the reference model, the linearizability search, the invariants, the coverage checks | **Core** (for *this* repository) | toys3 exists to learn DST. The oracle and the invariants are where the learning value is, and they are modelled as carefully as the controller. | `toys3/sim`: `oracle.go`, `history.go`, `invariant.go` |
| **Plant model**: a filesystem with crash and fault semantics | Supporting | It must be realistic (Chapter 11 and Chapter 17 show that its realism decides which bugs are found), but it models POSIX; it invents nothing. | `toys3/sim/plantmodel.go` |
| **Simulation testbench**: event queue, virtual clock, seeded RNG, incarnations, trace hash | Supporting | Necessary, but the same machinery would drive any `Step` machine. | `toys3/sim`: `simulator.go`, `queue.go`, `rand.go`, `hash.go`, `trace.go`, `log.go`, `config.go` |
| **Object format**: header, CRC, key escaping, directory layout | Supporting | A small, stable contract between incarnations and between the controller and the monitors. | `toys3/format` |
| **Stimulus**: the simulated clients and their self-describing values | Supporting | Its value encoding is what lets the oracle check reads exactly (Chapter 12). | `toys3/sim`: `workload.go`, `value.go` |
| **Production runtime and transport**: event loop, OS executor, HTTP S3 subset | Generic | A thin shell. "It contains no business logic" (`toys3/prod/doc.go:2-5`). | `toys3/prod`, `toys3/cmd/toys3` |
| PRNG, hashing, priority heap | Generic | Commodity algorithms: SplitMix64, FNV-1a, a binary heap. | `sim/rand.go`, `sim/hash.go`, `control/hash.go`, `sim/queue.go` |

The interesting row is the second. In a product, verification would be a supporting subdomain at best: tests serve the product. In toys3 the relationship is inverted. The S3 server is deliberately small so that the simulator, the oracle and the invariants can be studied on it, and the oracle (Chapter 13) is the most intricate piece of code in the repository. DDD says to put your best modelling effort into your core subdomains, and toys3 does: verification got the same naming care as the controller (the oracle's internal names, `refState` and `estimate`, were part of the refactor).

The classification also explains the last row. The FNV-1a hasher used to sit at the bottom of `core/server.go`, the file holding the control law. A commodity algorithm in the middle of the core subdomain's most important file is a small smell; the refactor moved it to `control/hash.go` along with `StateHash`, the one function that uses it.

---

## 3a.4 Bounded contexts

The subdomains are the problem. The bounded contexts are how the solution is cut. toys3 has six:

| | Context | Responsibility | Package / files |
|---|---|---|---|
| A | **Storage Control** | Turns requests and plant feedback into actuations, replies and telemetry. Owns the durability protocol, the key interlock, the trip policy and recovery. | `toys3/control` |
| B | **Durable Medium (the plant)** | The thing being controlled: files, directory entries, fsync, crash. Two implementations, one real and one modelled. | the port in `control/plant.go`; adapters `prod.OSExecutor` and `sim.PlantModel` |
| C | **Object Format** | Byte layout of an object file, key-to-name escaping, `obj/` and `tmp/` paths. The only thing that survives a crash. | `toys3/format` |
| D | **Verification** | Judges a run: the client-visible history against a sequential reference model (black box), and controller telemetry against the plant's true state (white box). | `sim/oracle.go`, `sim/history.go`, `sim/invariant.go` |
| E | **Simulation Testbench** | Runs the controller against the plant model under a seeded schedule, injects crashes, folds the trace hash; includes the stimulus. | `sim/simulator.go`, `queue.go`, `rand.go`, `hash.go`, `trace.go`, `log.go`, `config.go`, `workload.go`, `value.go` |
| F | **Production Runtime and Transport** | Real I/O, goroutines, and the HTTP mapping to S3. | `toys3/prod`, `toys3/cmd/toys3` |

Contexts B, D and E all live in package `sim` and are separated by *file* and by *naming*, not by package. The package doc says so explicitly:

```go
// toys3/sim/doc.go:6-12
// The package holds four bounded contexts, grouped by file:
//
//	testbench     simulator.go queue.go rand.go config.go hash.go trace.go log.go
//	plant model   plantmodel.go (PlantModel, Disturbances, ActuationPlan)
//	stimulus      workload.go value.go (self-describing values, shared with verification)
//	verification  oracle.go history.go invariant.go (monitors read the plant only
//	              through the GroundTruth interface)
```

(The doc counts the stimulus as a fourth context inside `sim`; this chapter folds it into the testbench, E, because it has no language of its own beyond "workload" and "value". Either reading is defensible. What matters is that the boundary is written down.)

### What was mixed before

The proposal catalogues ten places where the old code mixed contexts (`toys3/REFACTOR-PROPOSAL.md`, §1.3). Four of them are worth knowing because they show what a boundary buys:

1. **The format lived inside the controller.** `core/format.go` held the object header, the CRC and the path functions. Everyone who only needed to read bytes (the invariants, the log renderer, the plant model's hash, the production `PrepareDir`) had to import the whole controller package. The package doc spent twenty lines on byte offsets in the middle of the description of the control law. Now `toys3/format` is its own package with its own doc (`toys3/format/doc.go:1-31`) and, for the first time, its own unit tests (`toys3/format/format_test.go`): round-trip, damage detection, escaping injectivity, key validity and paths. Before the refactor, no test referenced `EscapeKey` at all.

2. **Facts served two masters, and said so in terms of one.** The controller's beliefs (`Committed`, `Recovered`, the corruption signal) were read by production (as log lines) and by the simulator (as invariant input and coverage counters). That is legitimate: one published signal with two subscribers. But the doc comments described them as things "the simulator checks", and the type was called `Fact`. The rename to `Telemetry` (§3a.6) names the published signal from the publisher's side.

3. **The plant's vocabulary was decoded by type switches in five places.** Chapter 3 (§3.5) told this story: adding a filesystem operation meant editing five switches. The port types `Actuation` and `Feedback` replaced them.

4. **One `sim` package held four contexts, and nothing kept monitors out of the plant.** `View` handed every invariant a concrete `*FakeFS`, so a monitor could call anything on the fake disk, including methods that described its *volatile* state, which no real observer could see. Now `View.GroundTruth()` returns an interface with exactly three methods (§3a.5).

The other six findings were smaller: the test seams (`Hashable`, `Factory`) were documented in terms of their one customer; `Stats` is a single struct fed by four contexts (left as is, but grouped by comment); the plant model knows the controller's directory layout (acceptable once the format is a shared kernel); the workload builds controller requests directly (a deliberate Conformist relationship); the testbench checks a controller contract ("Trip is the last action of its step"); and a handful of comment-level language drifts recorded in Chapter 17.

---

## 3a.5 The context map

Here is how the six contexts relate. Arrows point from upstream (the side whose model is used) to downstream (the side that uses it):

```
                                 S3 clients
                                     │ HTTP
                       ┌─────────────▼──────────────┐
                       │ F1 TRANSPORT  prod/http.go │  generic
                       └─────────────┬──────────────┘
                                     │ ACL: S3 REST ─► control.Op
                       ┌─────────────▼──────────────┐
                       │ F2 PROD RUNTIME  prod/     │  generic
                       │ Loop · OSExecutor · Env    │──── conformist to POSIX (the kernel)
                       └───┬──────────────────▲─────┘
           Step(Event)     │ CONFORMIST       │ Feedback
           []Action        │ (implements the  │
                           │  actuation PL)   │
   ╔═══════════════════════▼══════════════════╧══════════════════════════╗
   ║ A  STORAGE CONTROL   control/                    CORE               ║
   ║    Controller aggregate · key interlock · trip · recovery           ║
   ║    OPEN HOST SERVICE + PUBLISHED LANGUAGE:                          ║
   ║      Step(Event) []Action  (shared with loop.md)                    ║
   ║      Actuation / Feedback  (outbound port: the plant interface)     ║
   ║      Telemetry             (outbound signal: any subscriber)        ║
   ╚══════╤═══════════════════════════╤══════════════════════════════════╝
          │ uses                      │ CUSTOMER/SUPPLIER (Hashable, Factory)
          ▼                           ▼
   ┌───────────────┐       ┌────────────────────────┐  owns   ┌─────────────────────────┐
   │ C OBJECT      │◄──SK──│ E TESTBENCH  sim/      │────────►│ B PLANT MODEL           │
   │   FORMAT      │       │ Simulator·Queue·Rand   │         │ sim/plantmodel.go       │
   │   format/     │       │ Workload (stimulus)    │         │ CONFORMIST: to control's│
   │ SHARED KERNEL │◄──SK──│ trace hash · log       │         │ actuation PL + to POSIX │
   │               │       └───────────┬────────────┘         └────────────▲────────────┘
   │               │                   │ drives                            │
   │               │                   ▼                                   │
   │               │       ┌────────────────────────┐  ACL: GroundTruth    │
   │               │◄──SK──│ D VERIFICATION         │──────────────────────┘
   └───────────────┘       │ oracle.go history.go   │  (durable view only)
                           │ invariant.go           │◄── PL: Telemetry, Op, Reply, Status
                           └────────────────────────┘    (SEPARATE WAYS from controller
                                       ▲                   internals)
                                       └── SK: value encoding (value.go) with the stimulus
```

(Redrawn from `toys3/REFACTOR-PROPOSAL.md` §3.) Every relationship on it is visible in the code. Let us take them one by one.

**A → F2 and A → E: Open Host Service with a Published Language.** The controller's interface, `Step(Event) []Action` with its closed sets of events and actions, is a protocol anyone can drive: the production loop, the simulator, the unit-test harness and the mutant decorators all do. It is also *published* in the DDD sense: it is the same shape that `loop.md` prescribes for the larger Manifester system, which is why the refactor kept the names `Event`, `Action` and `Step` even where a control-flavoured name was available (§3a.10).

**A → B: Published Language, Conformist adapters.** The plant port (`control/plant.go`) is owned by the controller. Both plant adapters conform to it exactly: `prod.OSExecutor.Execute` maps each `FS*` command onto an `os` call and each result back onto the matching `FS*Done` (`toys3/prod/executor.go:37-72`), and `sim.PlantModel.Land` does the same against its in-memory model. This is hexagonal architecture in its pure form: the port lives in the core, not in a shared "plant vocabulary" package. The proposal considered a separate package for the plant vocabulary and rejected it: with the port owned by the core, the dependency arrows already point the right way (§3a.8).

**A → E: Customer/Supplier.** `Hashable` and `Factory` exist because the testbench needs them (`toys3/control/machine.go:9-25`): the simulator folds `StateHash()` into the trace after every step, and it builds every incarnation through a `Factory` so it can substitute a mutant. The downstream (the testbench) drives what the upstream (the controller) provides. Mutants are decorators on the `Machine` port: each wraps a real controller and rewrites its inputs or outputs (Chapter 15).

**A → D: Published Language, and Separate Ways from internals.** The black-box invariants judge only the client-visible history: requests (`Op`), replies (`Reply`, `Status`). The white-box ones add the controller's published telemetry. None of them reads the controller's private state. "Nothing about the server's internals is consulted," says the oracle's doc comment (`toys3/sim/oracle.go:19-21`). That is Separate Ways: the verification context could judge any controller that speaks the published language, including a completely different implementation.

**C → A, B, D, F2: Shared Kernel.** The object format is used by the controller (to write and validate object files), by the plant model (to fold content checksums into its state hash), by the monitors (to decode durable files) and by production (to create the directories). It is small, stable and versioned by its magic number `"TOY3"`. Above all, it is the contract between incarnation *N* and incarnation *N+1*: the only thing that crosses a crash. That is why it deserves its own package, and why changes to it must be made with every user in mind. The shared kernel is the one relationship in DDD where two teams agree to change a model *together*.

**B → D: Anti-Corruption Layer.** This one is new. Monitors need to read the plant's true durable state; they do not need anything else, and they must not see the plant's volatile state (no real-world observer could). So verification declares exactly what it needs, using only builtin types:

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

`View.GroundTruth()` returns this interface, not the plant model (`toys3/sim/invariant.go:28-29`, implemented at `toys3/sim/simulator.go:391`). The interface uses no plant types, so a future refactor that moves the plant model and the monitors into separate packages (the "Full" layout, §3a.12) needs no change to either side. Note the cost discipline: to support it, `PlantModel` gained one method, `EachDurable` (`toys3/sim/plantmodel.go:567-578`), which iterates in exactly the order the old code's `Durable().Files` did, so `ObjNeverTorn` reports failures in the same order.

**Stimulus ↔ D: Shared Kernel.** The workload writes self-describing values; the oracle and `NoCorruptReads` read them. The encoding (`MakeValue`, `ValueWriter`, `ValueHeader`) was split out of `workload.go` into its own file, `value.go`, whose comment names the relationship: "the shared kernel between the stimulus (Workload, which writes them) and verification (the oracle and NoCorruptReads, which read them)" (`toys3/sim/value.go:9-11`).

**Clients → F1: Anti-Corruption Layer.** `prod.Handler` and `ParseRequest` translate S3 REST (methods, paths, the copy-source header) into `control.Op` (`toys3/prod/http.go:81-107`). Buckets are flattened into keys. Nothing of HTTP crosses into the controller.

**POSIX → F2, B: Conformist.** Both plant adapters accept POSIX semantics as given. toys3 does not wrap the kernel in a model of its own; it conforms, and the plant model imitates what it conforms to.

**Dependency direction.** Imports always point toward `control` and `format`:

```
cmd/toys3    ──► prod ──► control ──► format
                   └─────────────────► format     (PrepareDir: ObjDir, TmpDir)
cmd/toys3sim ──► sim  ──► control ──► format
                   └─────────────────► format
```

Nothing imports `sim` or `prod` except the commands. `format` imports nothing from toys3 at all, which is why `format.TmpPath` takes a `uint64` and not a `control.IOID` (`toys3/format/layout.go:73-79`): a shared kernel must not depend on any of the contexts that share it.

---

## 3a.6 The aggregate: one Controller, and why not one per key

**Aggregate root: `control.Controller`.** Its doc comment says it outright: "It is the single aggregate of the domain: one Step is its consistency boundary" (`toys3/control/controller.go:77-78`). Everything the controller knows changes atomically inside a single `Step`, and nothing outside can observe a half-applied step. The runtime calls `Step`, gets back a slice of actions, and only then executes them; there is no callback into the controller in the middle.

That is the easy part. The interesting question is the one a DDD reader asks next: *the per-key FIFO queues look like little aggregates; why is there not one aggregate per key?* An "ObjectLoop" per key, each serializing its own operations, is a natural design, and in a larger system it might be the right one. In toys3 it would be wrong, for five reasons that are all visible in the code:

1. **COPY spans two keys.** It must hold the interlock on both its source and its destination, in arrival order (`interlocked`, `toys3/control/controller.go:68-72`, and `permitted`, `189-201`). With an aggregate per key, a COPY would be a cross-aggregate operation: a saga, with its own failure modes and its own ordering. That would change the order of actions, and therefore the behaviour and every trace hash.
2. **The queues are derived, not stored.** There is no per-key state at all. A key's queue is the subsequence of `reqs` that touches it (`toys3/control/controller.go:80-86`). There is no per-key identity to root an aggregate on.
3. **Some state is global.** `nextIO` and `nextSeq` are controller-wide counters. Recovery gates *all* keys (the mode is controller-wide). A trip stops *all* keys.
4. **`StateHash` is defined over the whole controller** (`toys3/control/hash.go:5-32`): one fingerprint, one consistency boundary.
5. **Ordering across keys is observable.** Inside one `Step`, `startPermitted` starts newly permitted requests in global arrival order (`toys3/control/controller.go:203-213`). That order appears in the action list, the trace hash and the log.

So the per-key queue is not an aggregate. It is a **named invariant of the aggregate**: the key interlock.

> A request may start only when its start permissive holds: no earlier unfinished request is interlocked with it (shares a key).

It is enforced in exactly one place:

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

Before the refactor this function was called `canRun`, and `interlocked` was `conflicts`, and `startPermitted` was `startRunnable`. The behaviour is identical. What changed is that the three words now belong to one vocabulary (§3a.9).

### Entities and value objects

| Kind | Members |
|---|---|
| Aggregate root | `Controller`. Its identity is one incarnation; it is rebuilt from nothing on every cold start. |
| Entity inside the aggregate | `request` (identity: its arrival `seq`). Its lifecycle is: waiting (start permissive false) → protocol steps → finished. |
| Value objects | `Event`, `Action` and all their bodies; the ops `Put`, `Get`, `Delete`, `Copy`; every `Telemetry`; `IOID`; `ClientReq`; `ErrCode`; `Status`; `Time`; `Duration`; `Config`; keys; object file bytes |
| Plant-model entities (context B) | inodes (`InodeID`) and directory entries |
| Verification entities (context D) | `HistOp` (identity: its `Req`), and the carried estimate groups |

The proposal considered renaming `request` to `procedure` (an ISA-88 term for an ordered sequence of steps, each waiting on a transition) and `reqStep` to `procStep`. It fits, and it would have freed the overloaded word "step" (§3a.11). It was not done: the gain was small and "request" is what every storage engineer calls it.

### No repository, on purpose

A DDD reader will look for the repository that loads and saves the `Controller`. There is none, and the absence is a design statement. Memory is disposable (`toys3/control/doc.go:106`). The controller is never saved; on every boot it starts empty. **Recovery reconstitutes nothing**: `obj/` needs no recovery, because the rename protocol only ever puts complete files there, and the controller does not need to know what is in `obj/` to serve requests (it reads it on demand). Recovery's only job is to empty `tmp/`. In control terms recovery is *homing* (establishing a known initial condition), not reconstitution. The plant is the source of truth, and the plant is not inside the aggregate.

---

## 3a.7 Commands, feedback and domain events

DDD distinguishes things that *ask for* something to happen (commands) from things that *record* that something happened (events). toys3's vocabulary sorts cleanly:

| Direction | Kind | Types |
|---|---|---|
| in | operator command | `Request{Client, Op}` |
| in | lifecycle | `Started` |
| in | feedback from the plant | `FS…Done`, each implementing `Feedback` |
| out | actuation commands to the plant | `FS…`, each implementing `Actuation` |
| out | reply to the operator | `Reply` |
| out | safety | `Trip{Cause}`, always the last action of its step |
| out | **domain events** | `Telemetry`: `Committed`, `Recovered`, `CorruptionDetected` |

The last row is where the naming changed. In DDD a domain event is past tense: it records a fact about the domain that other parts of the system may react to. `Committed` and `Recovered` already were. The third was `CorruptObject`, a noun phrase that describes a *thing* rather than an *event*, and reads like an instruction ("corrupt the object"). It is now `CorruptionDetected`: past tense, and precise about what happened. The controller did not corrupt anything; it *detected* corruption. In control vocabulary that is *fault detection*, the first half of FDI (fault detection and isolation).

The package doc of `telemetry.go` now states the rule: "Every type is a domain event, named in the past tense" (`toys3/control/telemetry.go:11`).

There is no event sourcing, and none is needed. Telemetry is published for observers; it is not the system of record. The plant is.

---

## 3a.8 Ports and adapters

Put the context map and the vocabulary together and you get toys3's hexagon:

| Port | Direction | Defined in | Adapters |
|---|---|---|---|
| `Machine.Step(Event) []Action` | driving (inbound) | `control/machine.go` | `prod.Loop`, `sim.Simulator`, the harness in `controller_test.go`, the mutant decorators |
| `Actuation` / `Feedback` (the plant port) | driven (outbound) | `control/plant.go` | `prod.OSExecutor` (the real plant), `sim.PlantModel` via the simulator (the plant model), the map-backed harness in `controller_test.go` |
| `Env` (`Rand`, `Buggify`) | driven | `control/env.go` | `prod.Env`, `sim.env`, `testEnv` |
| `Telemetry` (via `Emit`) | output signal | `control/telemetry.go` | the production log line; the simulator's `Sample` stream, feeding monitors and stats |
| `Reply` | output | `control/action.go` | the production waiter (the HTTP handler); the simulator's oracle and workload |
| S3 REST | driving, at the edge | `prod/http.go` | `Handler`, the ACL to `Op` |

Two things about this table are worth dwelling on.

**The driven port is data, not an interface with methods.** In the textbook hexagon, a driven port is an interface like `type Disk interface { Write(path, data) error }`, and the domain calls it. In toys3 the domain never calls anything; it *returns* commands. The "port" is a vocabulary of values, and the adapter is whatever interprets those values. Chapter 2 explained why that is strictly more powerful for simulation (an asynchronous vocabulary can express "the effect landed but the caller never learned"). DDD does not need the port to be a Go interface; it needs it to be owned by the domain and expressed in the domain's language. `Actuation` and `Feedback` are Go interfaces, but only as *category markers* over the values; nothing implements them except the plain structs.

**There are three adapters for the plant, not two.** The unit-test harness in `controller_test.go` is a third: a map from path to bytes that executes each actuation synchronously and returns its feedback. It is the simplest possible plant, with no crashes and no faults, and it is enough to pin down exact action sequences (Chapter 6). The simulator is the most adversarial. Production is the real thing. All three conform to the same port, and the controller cannot tell them apart.

---

## 3a.9 The control-theory reading, end to end

Now the second body of ideas. Here is the claim, stated precisely: **toys3's controller is a discrete-time controller in a feedback loop around a plant, and the simulator is a software-in-the-loop testbench for it.** The names in the code follow from that claim. This section builds the picture term by term and maps each term onto code.

### The block diagram

```
     reference (client requests)                         disturbances: EIO, torn writes,
              │                                          bit flips, writeback, power loss
              ▼                                                    │
     ┌──────────────────┐   actuation (FS* commands)      ┌────────▼────────────┐
     │   CONTROLLER     │ ───────────────────────────────►│        PLANT        │
     │ control.         │   dead time: command → effect   │ (disk + page cache) │
     │  Controller      │                                 │ prod: OSExecutor    │
     │ Step(Event)      │ ◄───────────────────────────────│ sim:  PlantModel    │
     │  []Action        │   feedback (FS*Done events)     └─────────────────────┘
     └──┬────────┬──────┘   feedback delay: effect → completion
        │        └──► telemetry (Committed, Recovered, CorruptionDetected)
        │                 ├──► prod: log line (a historian)
        │                 └──► sim: Sample stream ──► monitors (invariants), stats
        └──► Reply to the operator, or Trip (fail-stop) when its own state is untrustworthy

  testbench (sim) = software-in-the-loop: real controller + plant model + stimulus + monitors
```

And the same picture with the code locations filled in:

| Block or signal | Control-theory meaning | In toys3 |
|---|---|---|
| Controller | computes outputs from inputs and internal state | `control.Controller` (`toys3/control/controller.go:74-101`) |
| Control law | the function the controller implements | `Step` (`toys3/control/controller.go:108-133`) and the protocol handlers it calls |
| Sampling instant | the time `k` at which a discrete controller reads its inputs | `Event.At` (`toys3/control/event.go:3-8`) |
| Controller state `x[k]` | everything the controller remembers between samples | the `Controller` fields, fingerprinted by `StateHash` (`toys3/control/hash.go:7-32`) |
| Reference / operator input | what the controller is asked to achieve | `Request{Client, Op}` |
| Plant | the process being controlled | the disk and the page cache |
| Actuator, actuation | the device and the command that act on the plant | the `FS*` commands (`Actuation`, `toys3/control/plant.go:8-13`) |
| Sensor, feedback | what the plant reports back | the `FS*Done` completions (`Feedback`, `toys3/control/plant.go:15-20`) |
| Dead time | delay from command to effect | `Disturbances.DeadTime`, drawn per op into `ActuationPlan.DeadTime` (`toys3/sim/plantmodel.go:26-31`, `146-152`) |
| Feedback (measurement) delay | delay from effect to the controller seeing it | `Disturbances.FeedbackDelay`, `ActuationPlan.FeedbackDelay` |
| Disturbance | an input the controller cannot command | `sim.Disturbances`: `EIOProb`, `TornWriteProb`, `ReadCorruptProb`, `WritebackProb`, `FsyncGate`; plus crashes (power loss) |
| Plant model | a simulation of the plant used in place of the real one | `sim.PlantModel` (`toys3/sim/plantmodel.go:86-118`) |
| True state | the plant's actual state, which the controller cannot see directly | the plant model's durable view, exposed to monitors as `GroundTruth` |
| Telemetry | signals the controller publishes for supervision | `control.Telemetry` via `Emit` |
| Monitors | supervisory checks on telemetry and outputs | the invariants (`toys3/sim/invariant.go`) |
| Safety trip | drive to a safe state when the controller cannot trust itself | `control.Trip{Cause}`, `modeTripped` |
| Interlock | prevents conflicting operations from running together | the key interlock: `interlocked`, `permitted` |
| Start permissive | all conditions required to start are true | `permitted(r)`, `startPermitted()` |
| Operating modes | Startup / Auto / Tripped, etc. | `modeStartup`, `modeRecovering`, `modeAuto`, `modeTripped` |
| Settling | the closed loop converges after a disturbance | the Settling phase of a run (`Config.Settling`, `KindSettle`, `KindSettlingEnd`) |
| Software-in-the-loop (SIL) | test the real controller code against a plant model | the simulator (`toys3/sim/doc.go:1-4`) |
| Stimulus generator | produces test inputs | the `Workload` |

### Walking one PUT around the loop

Seed 1, request `c6`, a PUT of `"k2"` (the same lines Chapter 2 showed, read now as a control engineer would). Each row is one sample of the controller, and between rows the plant has its dead time and its feedback delay:

| Actuation issued | at | lands on the plant | dead time | feedback delivered | feedback delay |
|---|---|---|---|---|---|
| `FSWrite{io7 tmp/…07}` | 4.822820 ms (#26) | 6.109640 ms (#33) | 1.287 ms | 6.531543 ms (#38) | 0.422 ms |
| `FSFsync{io10 tmp/…07}` | 6.531543 ms (#38) | 6.676498 ms (#40) | 0.145 ms | 6.871157 ms (#42) | 0.195 ms |
| `FSRename{io12 tmp/…07 → obj/k2}` | 6.871157 ms (#42) | 7.233120 ms (#44) | 0.362 ms | 7.377477 ms (#46) | 0.144 ms |
| `FSFsyncDir{io14 obj}` | 7.377477 ms (#46) | 8.030661 ms (#49) | 0.653 ms | 8.242306 ms (#50) | 0.212 ms |

At #50 the controller emits `Emit{Committed{c6 "k2" crc=9b35c638}}` and `Reply{c6 200}`. Three things are visible in this table that the old names hid:

- **Every actuation is issued at the instant the previous feedback is delivered.** The controller is purely reactive: no timers, no polling. Its "sampling" is event-driven; a sample happens whenever there is something to sample.
- **The dangerous window is the feedback delay.** If power fails at 7.3 ms, between the rename landing (7.233) and its feedback arriving (7.377), the rename has *happened* on the plant but the controller never learned. That is the "it happened, but nobody was told" case of Chapter 10, and it now has a precise name: a crash inside the feedback delay. A crash inside the dead time is the benign case: the command never reached the plant.
- **`Committed` is a claim about the plant's true state.** At #50 the controller believes `obj/k2` durably holds a payload with CRC `9b35c638`. The monitor `AckedIsDurable` checks that claim against the plant model's durable view on the same step. In control language, it is an **estimate-versus-true-state check**: the controller's internal estimate of the plant compared with ground truth that only the testbench can see.

### Controller, plant, and why the controller never does I/O

The first sentence of the `control` package doc used to say "the server never performs I/O". It now says the package "is written as a controller in a feedback loop around a plant (the disk)" (`toys3/control/doc.go:5-10`). These say the same thing, but the second explains it. A controller does not reach into the plant; it has no hands. It emits commands, and actuators carry them out. If you have ever wondered why the sans-I/O pattern feels so natural once you have used it, this is why: it is how every physical control system has always been built, because a physical controller *cannot* do anything else.

### Dead time and feedback delay

These two used to be `Latency` and `ReplyDelay`. "Latency" was ambiguous (latency of what, measured from where to where?), and "reply" collided with `control.Reply`, the controller's answer to a client, which is a completely different thing. The new names are standard, and each one measures one specific interval:

```go
// toys3/sim/plantmodel.go:26-31
	// DeadTime is the [min, max] delay from issuing an actuation to its effect
	// landing on the plant; FeedbackDelay is the further [min, max] delay until
	// the completion (feedback) is delivered. A crash in between leaves the
	// effect applied but unacked.
	DeadTime      [2]control.Duration
	FeedbackDelay [2]control.Duration
```

### Disturbances, and the plant model

A **disturbance** is an exogenous input: it acts on the plant, and the controller can neither command nor prevent it. Wind on an aircraft. A cold draft on a thermostat. An EIO from a failing disk, a torn write, a bit flip on read, the kernel's background writeback, the fsyncgate lie, and power loss. Every one of these is something the toys3 controller must be robust to and cannot cause. The struct that configures them was called `FSFaults`; it is now `Disturbances`, and its doc says what it is:

```go
// toys3/sim/plantmodel.go:12-16
// Disturbances configures the plant model: its dynamics (dead time and
// feedback delay) and the disturbances it injects, the exogenous inputs the
// controller cannot command (EIO, torn writes, bit flips, background
// writeback, fsyncgate). Zero value: an honest, instant filesystem that still
// loses un-synced changes on crash.
```

The **plant model** is the simulation of the plant that stands in for the real one. In model-in-the-loop and software-in-the-loop testing it is always called that, and the rename from `FakeFS` makes the simulator's structure read like the textbook: real controller code, plant model, stimulus, monitors. "Fake" also undersold it. A fake, in testing vocabulary, is a simplified working implementation; the plant model is *more* adversarial than the real thing, on purpose (Chapter 11).

Note that the crash, too, is a disturbance: a power-loss disturbance. It is configured in the testbench (`Config.CrashProb`), not in `Disturbances`, because the testbench has to coordinate it with the queue and the oracle; but conceptually it belongs with the others.

### Trip, interlock, permissive, modes

These four come from industrial process control and safety engineering, and they fit toys3 almost uncannily.

**Trip.** In a safety-instrumented system, a *trip* drives the process to a safe state when a monitored condition shows that normal control can no longer be trusted. A reactor SCRAM is a trip. Crucially, a trip is not an error report; it is an *action*, and recovery from it is a deliberate restart, not a retry. That is exactly what `Halt` was: after an fsync error the controller's view of the page cache is unknowable (fsyncgate), so it replies 500 and then fail-stops; the runtime restarts it, and the new incarnation re-establishes a known state from disk. The doc now says so: "Trip is a safety trip: it asks the runtime to kill this incarnation and restart it (fail-stop)" (`toys3/control/action.go:21-26`). Chapter 7 is about when to trip.

**Interlock.** An interlock prevents two operations that would conflict from running at the same time: you cannot open a pressure vessel's door while it is pressurized. The key interlock prevents two requests that share a key from running at the same time. `interlocked(a, b)` is the condition.

**Start permissive.** In PLC programming, a *permissive* is a condition that must be true for an operation to be allowed to start, and the start permissive of a motor is the AND of all of them. `permitted(r)` is the start permissive of request `r`: no earlier unfinished request is interlocked with it. `startPermitted()` starts every waiting request whose permissive has become true. The permissive is evaluated, not latched: it is recomputed whenever something finishes.

**Modes.** Industrial controllers have operating modes (Hand / Off / Auto is the classic three-position switch), plus Tripped. Chapter 5 maps toys3's four modes onto this. The rename from `phase` freed the word "phase" for the testbench's Chaos and Settling phases.

### Settling

When a disturbance hits a feedback loop, the loop's output goes through a transient and then, if the loop is stable, *settles* into a tolerance band. Engineers specify loops by their settling time and their steady-state error. The simulator's run is shaped the same way: a Chaos phase full of disturbances, then a Settling phase in which no new disturbances arrive and the closed loop must settle: every outstanding request gets a reply (the transient dies out), and no request issued during Settling gets a 500 (zero steady-state error). The `Liveness` invariant checks both; its doc calls it "the closed loop settles" (`toys3/sim/invariant.go:98-104`). This used to be the "quiet tail", which described what the simulator did (nothing) rather than what the system under test had to achieve.

### The verification side: reference model, estimate, observer

The control vocabulary extends, carefully, into the oracle.

- **Reference model.** The sequential specification that the closed loop's behaviour must match is, in control language, a *reference model*: the model whose behaviour the system should track (the term is from model-reference adaptive control). The oracle's internal type for one state of that model was `modelState`; it is now `refState` (`toys3/sim/oracle.go:164-167`). This also resolved an ambiguity: "model" had come to mean both the plant model and the reference model, and the rule now is to always qualify the word.
- **State estimate.** The oracle cannot see the system's true state; it reconstructs the *set of states* the reference model could be in, given everything clients have observed. In estimation theory that is a *set-membership state estimate*. The oracle's field that holds it was `groups`; it is now `estimate` (`toys3/sim/oracle.go:46-50`). The oracle widens the set when it gets too large, like an interval observer that over-approximates; an empty set means a fault has been detected (the history is not linearizable).
- **True state.** The plant model's durable view, `GroundTruth`.

Put together, the oracle is a *set-membership state observer* for the reference model, and `AckedIsDurable` is an estimate-versus-true-state check for the controller. Chapter 13 develops the observer reading of the oracle in more depth. The type itself is still called `Oracle`, for reasons in the next section.

---

## 3a.10 Where the metaphor stops, and why

A metaphor applied everywhere stops being a model and becomes a costume. The proposal that drove the refactor classified every candidate name as RENAME (the control term fits and is clearer), OPT (it fits, but the gain is marginal) or KEEP (the current name is better, or it belongs to a language shared with something outside toys3). The KEEP decisions are as instructive as the renames.

**Requests are not setpoints.** In control theory, the reference signal is the *setpoint*: the value the controller should drive the plant toward. A PUT is, arguably, a setpoint change ("make `k` hold `v`"); so are DELETE and COPY. But a GET is not: it asks for a reading and changes nothing. Calling a quarter of the operations "setpoints" would be wrong, and calling the rest "setpoints" and GET something else would split one concept in two. `Request`, `Op`, `Put`, `Get`, `Delete`, `Copy`, `Reply` and `Status` are also the S3 vocabulary that the HTTP adapter translates, a published language toys3 does not own. They stayed.

**`Linearizable` is about linearizability, not linearization.** In control theory, *linearization* means approximating a nonlinear system by a linear one around an operating point. *Linearizability* is a correctness condition for concurrent objects (Herlihy and Wing). The words are close and the concepts unrelated. The rule adopted is: write "linearizability (concurrency)" in docs when there is any doubt, and never coin a control-flavoured name containing "linear".

**`Oracle` is not renamed `Observer`.** The oracle *is* a state observer, as §3a.9 showed, and the fit is remarkable. But "Observer" collides with the Gang-of-Four Observer pattern (publish/subscribe), which every Go programmer will read first; and "test oracle" is the established DST term. The type kept its name; its insides use observer language (`estimate`, `refState`).

**`Committed` is not `Settled`.** Settling is asymptotic convergence within a tolerance band. A commit is a discrete, atomic durability point: before it, a crash may lose the write; after it, it may not. Using "settle" for both the end of a transient and an atomic commit would have blurred two very different things, one of which the run's phase structure now uses.

**`Event`, `Action` and `Step` stay.** `Step` already *is* the control word: a discrete-time controller's update `x[k+1] = f(x[k], u[k])` is a step. `Event` and `Action` are the published language shared with `loop.md` and the larger Manifester system; renaming them in the toy would make the toy and its specification disagree.

**The format keeps plain names.** The CRC is not an "integrity sensor" and the magic number is not a "calibration constant". Context C has no control analogue, and none is needed; forcing one would only obscure.

**The POSIX names stay.** `FSWrite`, `FSFsync`, `FSRename`: these are the plant's own language. Actuation is the category; `FSFsync` is the command.

**Crashes stay crashes.** A crash is a power-loss disturbance; a trip is a controller action. They have the same effect on the disk, and the simulator handles them with the same code path, but they are different *concepts*, and the vocabulary keeps them apart. (`Stats.Crashes` counts both, documented as "crashes, including Trips"; `Stats.Trips` counts the controller's own.)

**OPT names not taken.** `Started` was not renamed `ColdStart` (a cold start retains no state, which is exactly what `Started` means, but the word appears in the book and logs about a hundred times and the gain is small). `request` was not renamed `procedure`. `KindFSLand` was not shortened to `KindLand`. `FSCounters` was not renamed `Counters`.

**DST terms stay.** `Quiesce`, `QuiesceEvery`, `KindQuiesce` and `Quiescent` are linearizability-checker terms and keep their meaning. `Incarnation`, `Chaos`, `Buggify`, `Rand`, `Fold`, `TraceHash` are the DST community's language.

**Two names that look stale on purpose.** `sim.Config.Server` (the controller's configuration inside the simulator's config) and `Simulator.halted` (the run stopping after an invariant failure) kept their names. The first is the field the whole book and every test call `cfg.Server`; renaming it would have bought nothing. The second is not a trip at all: it is the testbench stopping, and calling it `tripped` would have *created* the confusion the Trip rename was meant to remove. Likewise the mutant labels `d-no-halt-on-error` and `d2-no-halt-on-dir-fsync-error` kept their spelling, because they are test names quoted in the golden file and in this book (their Go identifiers became `mutNoTripOnError` and `noTrip`).

---

## 3a.11 Collisions and double meanings

Even a careful vocabulary has words that mean two things. The useful move is to list them, so that a reader who hits one knows to check which meaning is intended.

| Term | Meanings | Resolution |
|---|---|---|
| linearizability / linearization | concurrency correctness / small-signal approximation | Keep `Linearizable`; never use "linearize" for anything else. |
| model | the plant model / the reference model | Always qualify: `PlantModel`, `refState`. |
| step | `Step()` / the simulator's step counter (`Result.Steps`, `Failure.Step`, log `#N`) / a request's protocol step (`reqStep`) / "step response" | Prose says "sim step #N" and "protocol step". |
| phase | (formerly) the server lifecycle / the testbench timeline / ISA-88 phase | The controller has *modes*; "phase" is reserved for Chaos and Settling. |
| loop | event loop / control loop / `loop.md` | Aligned: in `prod`, the event loop *is* the control loop (`toys3/prod/doc.go:14-15`). |
| quiet / quiesce / quiescent | no new faults / clients pause / no pending ops | `QuietTail` became `Settling`; the other two keep their meanings; `PlantModel.SetQuiet` keeps "quiet" as a plant-side word. |
| observer | state observer / GoF Observer | That is why `Oracle` was kept. |
| trip / crash | self-initiated fail-stop / external power loss | Different words, same disk effect; `Stats.Crashes` counts both. |
| seq | arrival seq (`request.seq`) / `Entry.Seq` / `InvSeq`, `RetSeq` | Prose qualifies each one. |
| bench | testbench / `go test -bench` | No package is named `bench`. |

---

## 3a.12 What the refactor did not change

**Behaviour.** Not one run changed. The trace hash of seed 1 is `1ca97eb77a9044e1` before and after; every one of 500 `DefaultConfig` seeds, 50 `BaseConfig` seeds and 14 mutant catches reproduces the same step count, random-draw count, trace hash and statistics. How that was proven (a golden file, a frozen hash encoding for telemetry, and a log comparison through a rename map) is a DST lesson in its own right, and Chapter 8 tells it in §8.12.

**The package count, almost.** The proposal offered two layouts. The **Minimal** layout, which was implemented, has four packages: `control`, `format`, `sim`, `prod`. The **Full** layout would split `sim` further into `plant/`, `verify/` and `internal/mix` (for the shared hash helpers), making the contexts compiler-enforced. It was not done, for a reason worth remembering: in the Full layout three of the seven packages exist only to break import cycles that a single package does not have. The proposal's recommendation is to move to Full when a **second plant** (a simulated network, Chapter 17) or a **second controller** (a replica) arrives. Then `plant/` and `verify/` will each have two users, and the boundaries will pay for themselves. The `GroundTruth` ACL was built now precisely because it is the cheap first step toward that split.

**Stats layout.** `Stats` is still one flat struct fed by four contexts. Its fields were renamed (`Halts` → `Trips`, `CorruptObjects` → `CorruptionsDetected`, the misspelt `Indetermined` → `Indeterminate`), and so were the printed keys (`trips=`, `corruptdet=`), so that the ubiquitous language reaches the output too.

### A scorecard

The proposal ends with a self-assessment, reproduced here because it shows where the effort went:

| Criterion | Before | After |
|---|---|---|
| Ubiquitous language in code | 6/10: good protocol words; "core", "Fact" and "FakeFS" are technical | 9/10 |
| Explicit bounded contexts | 4/10: four contexts in `sim`; format inside core | 7/10 (file-level in `sim`) |
| Aggregate design | 8/10: a single-Step consistency boundary, already clean | 9/10: the interlock invariant is named |
| Ports and adapters | 7/10: the Step port is excellent; the plant port is implicit | 9/10 |
| Domain events | 6/10: exist, unnamed as a concept, one not past tense | 9/10 |
| ACLs at boundaries | 7/10: the HTTP ACL is good; monitors use the concrete fake FS | 9/10 with the `GroundTruth` ACL |

The highest "before" score is aggregate design. The architecture was right from the start; the refactor mostly *named* what was there. That is the usual outcome when a design grew from good principles (here, `loop.md`), and it is the reason a behaviour-preserving refactor was possible at all.

---

## Key takeaways

- A domain model is a set of names with homes and boundaries. Choosing them well is design work, and in toys3 it was done without changing a single behaviour.
- toys3 has two core subdomains: durable object control (the controller) and, because the repository exists to teach DST, verification (the oracle and invariants).
- Six bounded contexts: storage control (`control`), the plant (a port in `control`, two adapters), the object format (`format`, a shared kernel), verification, the testbench (both in `sim`, separated by file), and production (`prod`).
- The context map names real relationships: Open Host Service and Published Language for `Step`, Conformist adapters for the plant, Customer/Supplier for the test seams, Shared Kernels for the format and the value encoding, ACLs for HTTP and for the monitors' view of the plant (`GroundTruth`).
- The `Controller` is the only aggregate, and one `Step` is its consistency boundary. The per-key queue is not an aggregate but a named invariant of it: the key interlock, enforced by the start permissive in `permitted`.
- Telemetry is the controller's domain events, named in the past tense, published to any subscriber.
- The control-theory reading is genuine: controller, plant, actuation, feedback, dead time, feedback delay, disturbances, plant model, telemetry, monitors, trip, interlock, permissive, modes, settling, reference model and state estimate all map onto existing code.
- The metaphor stops where it would mislead: requests are not setpoints, linearizability is not linearization, `Oracle` and `Committed` keep their names, the format and POSIX names stay plain, and the published language shared with `loop.md` is untouched.

## Exercises

1. **Draw your own context map.** Without looking at §3a.5, list every `import` of every non-test file in `toys3/control`, `toys3/format`, `toys3/sim` and `toys3/prod` (`go list -f '{{.ImportPath}}: {{.Imports}}' ./toys3/...`). Draw the package graph. Then mark which arrows are Shared Kernel, Conformist or Customer/Supplier relationships. Which relationships of §3a.5 are *not* visible as imports at all, and why? (Hint: three of the contexts share one package.)
2. **Find the boundary violations the compiler cannot see.** Contexts B, D and E share package `sim`, so an invariant *could* call `v.(*Simulator).plant.Current()`. Write a short `go vet`-style check, or just a `grep`, that would flag any use of `PlantModel` methods from `invariant.go` or `oracle.go` other than through `GroundTruth`. Run it. Then explain what it would take to make the compiler enforce the same rule (the Full layout).
3. **Per-key aggregates, on paper.** Design the "ObjectLoop per key" alternative of §3a.6 in enough detail to handle COPY: what does a COPY of `a` to `b` do when `a`'s loop is busy and `b`'s is idle? Write down the messages between the two loops. Then identify the exact point where your design produces a different *action order* from the current controller for some input. That difference is why the refactor could not take this path while keeping the trace hash.
4. **Name a missing concept.** The controller's `request` struct has a lifecycle (waiting → protocol steps → finished) but its "finished" state is implicit: the request is simply removed from `reqs`. Is there a control-theory or DDD name for the moment a request finishes? Would naming it (for example as a domain event) change anything observable? Argue both sides.
5. **Try the forced metaphor.** Rename, in a scratch copy, `Request` to `Setpoint` and `Op` to `SetpointChange`. Read `onRequest` (`toys3/control/controller.go:150-183`) and the oracle's `apply` (`toys3/sim/oracle.go`) with the new names. Write down every sentence in the code or this book that becomes false or awkward. That list is the argument for KEEP.
6. **Dead time versus feedback delay.** In a scratch copy, set `Disturbances.FeedbackDelay` to `[0, 0]` in `BaseConfig`, leaving `DeadTime` as is. Before running anything, answer: can a crash still fall between an effect landing and its feedback being delivered? (Read the `KindFSLand` handler, `toys3/sim/simulator.go:134-145`, and note where `maybeCrash` is called relative to the scheduling of the delivery.) Then run `go test ./toys3/sim -run TestCatchesBugs -v` and compare the catching seeds with the defaults. Which mutants moved, and why did *any* move if the dangerous window still exists? (Hint: a zero delay changes which other entries can be interleaved in the window, and it changes the random stream.) Revert.
7. **The observer, made explicit.** The oracle's `estimate` is a set of reference-model states, factored by key groups. Using Chapter 13, write a two-paragraph explanation of the oracle as a set-membership observer for a reader who knows control theory but not linearizability: what is the state, what are the measurements, what is the update, what does an empty set mean, and what does widening correspond to?
8. **Audit the ubiquitous language.** Grep the code (`toys3/`) for the words "server", "core" and "fake" in comments. For each hit decide: is it the outside view (the S3 server, which is fine), the generic English word, or a stale name? Chapter 17 (Part II) and the final report of the refactor list a few that were left; see whether you find others.

## Further reading

- Eric Evans, *Domain-Driven Design: Tackling Complexity in the Heart of Software* (2003). Part IV (strategic design) for bounded contexts and the context-map patterns used in §3a.5.
- Vaughn Vernon, *Implementing Domain-Driven Design* (2013), chapters 10 (aggregates: "design small aggregates", and when not to) and 8 (domain events).
- Alistair Cockburn, *"Hexagonal Architecture"* (2005), the original ports-and-adapters article.
- Karl J. Åström and Richard M. Murray, *Feedback Systems: An Introduction for Scientists and Engineers* (2nd ed., 2021, free online). Chapters 1 and 2 for the vocabulary of controllers, plants, disturbances and feedback; the sections on time delay for dead time.
- ISA-88 (batch control) and IEC 61511 (safety instrumented systems), for the industrial meanings of *procedure*, *phase*, *interlock*, *permissive* and *trip*.
- Maurice Herlihy and Jeannette Wing, *"Linearizability: A Correctness Condition for Concurrent Objects"* (1990), to keep linearizability and linearization firmly apart.
- `toys3/REFACTOR-PROPOSAL.md` in this repository: the full glossary with a verdict and a reason for every name, the evidence for each context-mixing finding, and the migration plan that Chapter 8 (§8.12) walks through.
