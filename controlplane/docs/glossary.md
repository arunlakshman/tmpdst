# Glossary

One line per term. Plain words. For the layout of the code see
`internal/controlplane/doc.go`.

## The log and the state

- **command**: a change that a client asks for (CreateTopic, CommitSegment, GroupInstall, GroupAssign, GroupRemove, OffsetCommit). Commands are what the log stores.
- **event**: a fact that `Decide` makes when it runs a command (for example SegmentCommitted). Events are made at apply time on every replica, written by `Apply`, and never stored.
- **log**: the shared, ordered list of commands. It is the truth. Implementations: `executor/dynamolog`, `executor/sqllog`, and the in-memory log of tests.
- **slot**: a numbered place in the log (`runtime.LogPosition`). A slot is written once. Nodes apply slots in order.
- **entry**: what one slot holds: a protobuf `Entry` (an id and one command), or a group of commands that the writer packed together, stamped with the writer epoch and name. Heartbeat and fence entries hold no command.
- **Decide**: the pure rule function of a schema: `Decide(view, command) -> events, error code`. It reads the state before the command and writes nothing.
- **Apply**: `Apply(txn, event)`: writes one event to the tables.
- **view**: the read-only picture of the state that `Decide` reads (an interface, so a test can pass plain maps).
- **schema**: one context (`domain/topic`, `domain/group`) as metadb sees it: a row per command and per read, and the tables it owns.
- **table**: a key prefix in the engine that one schema owns (topic: T, E, R, K, S; group: G, M, O).
- **engine**: the ordered key-value store under metadb (`metadb/engine`, with `memory` and `pebble`). Like InnoDB. It need not be durable: it can be rebuilt.
- **metadb**: the database on top of the engine. Like the MySQL server. It runs commands on schemas, and owns the entry format, the checkpoint and the state hash.
- **checkpoint**: a snapshot of every key, taken at a slot and stored in the object store (`checkpoint/<slot>`). A node restores from the newest one and then reads the log after it. The log before it can be trimmed.
- **state hash**: a 64-bit digest of the state (`metadb.StateHash`). It leaves out the idempotency window. Replicas that applied the same slots must have the same hash.
- **segment**: a batch of records for several topic partitions, stored outside the control plane. `CommitSegment` records it and gives out offsets; `FindSegments` lists the runs of a partition.
- **commit key** (segment key): the id a client puts on a `CommitSegment`. A repeat of the same key inside the window gets the first answer; a different request with the same key is `ErrConflict`.
- **idempotency window**: how long commit keys are remembered (`WindowSlots`, and until the log is trimmed). After it, the same key commits again as a new segment.
- **offset**: the position of a record in a partition. The log decides it when the commit is applied. In a group, it is also the offset a member committed for a partition.
- **trim**: delete old log slots that a checkpoint covers (`KeepSlots` stay behind it).

## The nodes

- **replica**: one node's copy of the log: it reads, applies, checkpoints, and writes when it is the writer. A `sim.Process`.
- **role**: writer (the active appender) or standby. Only the writer appends. A standby still reads and serves reads.
- **epoch**: the number of the current writer. A node claims a new epoch by compare-and-swap (`LogClaimEpoch`). Every entry carries it.
- **fence**: how an old writer is stopped. A put into a taken slot fails, or it sees a newer epoch, and it steps down to standby.
- **takeover**: a standby claims the epoch after `TakeoverAfter` of silence from the writer.
- **term** (rebalance machine): the time one node serves as coordinator. The memory of the machine is for one term. It ends when the role is lost, or when a log write that may have changed the log fails.
- **fresh read**: a read that waits until the node has applied the log tail (`ControlPlaneDeliver`). A `LocalRead` answers at once from local state.
- **lease**: a broker is alive while its heartbeat lease is fresh (`domain/broker`). It is kept in memory and never logged.

## Consumer groups

- **group**: consumers that share the partitions of topics, by the classic Kafka group protocol.
- **member**: one consumer in a group, with a member id.
- **generation**: the number of a finished join round. Every `GroupInstall` raises it by 1. A call with an old generation is refused.
- **leader**: the member that gets the member list and chooses the assignment for the others.
- **assignment**: the bytes that the leader gives each member in `GroupAssign`.
- **session timeout**: how long a member may be silent (no heartbeat) before it is removed.
- **rebalance timeout**: how long a round waits for known members to join again.
- **initial delay**: how long a new group waits for more members before its first round.
- **new member timeout**: how long a new member may wait for its first round.
- **machine** (rebalance machine): `domain/rebalance.Machine`, the group coordinator as a pure state machine. It is Standby, Loading or Serving.
- **input / output**: what a machine takes in `Step` (a call, a timer, a reply from the replica) and what it returns (a reply, a log write, a log read).

## Simulation and tests

- **sim.Process**: the dst interface of a thing that does no IO: `Step(cx, inbox)` returns outputs. The replica and the rebalance machine have this shape.
- **dst**: the deterministic simulation testing library (module `github.com/manifesterio/dst`). It has the scheduler, the random stream, fault injection and the runner.
- **simulator**: `internal/verify/simulator`: runs replicas and consumers on a virtual clock with a fake log and fake objects, one seed per run. `cmd/cpsim` runs campaigns of it.
- **seed**: the one number that decides a whole run. Same seed, same run.
- **scenario**: a named fault set for the simulator (basic, log-faults, crash-restart, leader-fencing, chaos).
- **oracle**: the checks of the simulator on a finished run. They compare answers with the model and check the other properties.
- **quiet tail**: the end of a run: faults stop, nodes heal and boot, and the run waits until all nodes and groups have settled.
- **buggify site**: a named place where a fake or the machine may do something odd now and then (a late reply, a short page). It is on in about half of the runs. Reports count how often it fired.
- **trace hash**: the digest of every step of a run. Equal seeds must give equal hashes.
- **golden hash**: a trace hash pinned in a test (`internal/verify/simulator/golden_test.go`), or printed by `make sim-campaign`. A change that moves one is a change of behaviour and must be re-pinned on purpose.
- **determinism gate**: `make determinism` (`ops/tools/determinismcheck`): the deterministic packages may import only an allow-list, and use no goroutines, wall clock or map ranges.
- **model / spec**: `internal/spec`: the small independent reference program that gives the correct answer. It shares types with the code, never logic.
- **conformance**: tests in `test/conformance` that draw random operations (rapid), send them to the model and to the real code, and require equal answers.
- **mutant**: a bug planted by one exact text replacement in the real code (`ops/mutants.txt`). `make mutants` requires that a named test fails for each.
- **test double**: a fault of the environment, not of the code (a crash that loses answered writes). A test wraps the target with it and expects the harness to fail.
- **campaign**: many seeds of each scenario in one run (`make sim-campaign`, `cpsim -seeds N`).
