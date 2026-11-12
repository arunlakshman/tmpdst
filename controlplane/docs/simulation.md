# Deterministic simulation of the control plane

`cpsim` runs the replicated control plane (3 replicas by default) inside one
goroutine, on a virtual clock, with a fake shared log and a fake object store.
It sends random client work, injects faults, and checks the answers against the
control-plane spec. A run is made from one number, the seed. The same seed gives
the same run, bit for bit. So a failure found with seed 4711 can be run again
and again until it is understood.

## Run it

    make sim                                   # the tests of the simulator
    make sim-campaign                          # 300 seeds for each scenario, about 15 s
    make sim-campaign SIM_SEEDS=5000           # a long campaign
    go run ./cmd/cpsim -scenario chaos -seed 17          # one seed, with a trace of every step
    go run ./cmd/cpsim -scenario chaos -seeds 1000 -first 5000

Flags: `-seeds N` (seeds per scenario), `-first S` (first seed of a campaign),
`-seed S` (one seed, verbose), `-scenario NAME|all`, `-nodes N`, `-steps N`
(events in the fault phase, default 4000), `-bug NAME` (plant a bug, see below), and the report flags `-report`, `-json`,
`-until-stable`, `-stable-after`, `-max-seeds` (next section).

## The report

The lines that `make build` prints do not change. Three more flags add a report
after them:

    go run ./cmd/cpsim -seeds 2000 -report                  # report after the campaign
    go run ./cmd/cpsim -until-stable                        # run until no new assertion shows up
    go run ./cmd/cpsim -until-stable -stable-after 100 -max-seeds 2000
    go run ./cmd/cpsim -seeds 300 -json                     # only the report, as JSON

- `-report` prints the report after the normal lines.
- `-until-stable` runs seeds in steps of 50 numbers (all scenarios each time)
  until `-stable-after` seed numbers in a row (default 200) brought no new
  assertion. `-max-seeds` (default 5000) is the limit for each scenario. If the
  limit comes first, the report ends with a line that starts with `WARNING`:
  the campaign is not known to be stable. It replaces `-seeds`. It implies
  `-report`.
- `-json` prints the report as JSON and nothing else (no scenario lines). It
  implies `-report`. The exit code is still 1 when a seed fails.

What the report holds:

1. Counts: seed numbers run, runs (seeds times scenarios), passed, failed.
2. Failed runs, first, each with the command that replays it.
3. Every assertion by name: kind (`always`, `sometimes` or `reachable`), how
   many times it was checked and how many times it held.
4. Never reached: `sometimes` assertions that never held, and the paths in
   `expectedReachable` (`cmd/cpsim/campaign.go`) that no seed reached.
5. Buggify sites by name: in how many runs the site was asked and switched on,
   how many calls it saw and how many times it fired. Never fired: the sites
   that fired in no run, and the names in `simulator.SiteNames()` that no run
   asked. A site that never fires tests nothing, so it is worth a look.
6. The coverage curve: for each seed number that saw an assertion for the first
   time, how many different assertions had been seen by then. The last line
   says at which seed the curve stopped growing. A curve that is still growing
   at the end means the campaign is too short.

An assertion that is never evaluated leaves no trace in the counts. That is why
the hand-kept `expectedReachable` list stays: only a list can name a path that
no seed reached. When a path is added or removed in the replica or in the group
host (`groups.go`), change the list.

## What a run does

1. Boot: the replicas are built with `replica.New(config)`, the constructor that
   `cmd/mdscontrolplane` uses. The config has the same shape as `machineConfig`
   there, with times scaled down (catch-up every 50 ms, heartbeat 250 ms,
   takeover after 1.5 s, checkpoint every 1 s, log trim 8 slots behind it) so a
   run is short in virtual time.
2. Fault phase: 3 clients send random operations (create topic, commit segment,
   find segments, describe topics, and the group storage operations: install,
   assign, remove, offset commit, offset fetch, describe group, list groups, on
   the groups `g0` and `g1`). Each operation goes to a random node. On
   `ErrUnavailable`, a timeout or an IO error the client retries the same
   operation with a new request id, up to 8 times, then gives up. At the same
   time the consumers use the consumer groups `cg0` to `cg2` through the
   rebalance machine of each node (see "Consumer groups"). Faults of the
   scenario are on.
3. Quiet tail: all faults stop, partitions are healed, paused nodes resume, dead
   nodes boot. The run waits until every node is ready and has applied every
   slot that changes state, and the consumer groups have settled (at least 3 s,
   at most 40 s of virtual time).
4. Final reads: the consumers stop and the rebalance machines are frozen, so
   that the log no longer changes. Then one client per node reads all the state
   from that node.
5. Judgement: the oracle checks everything (next section) and the run returns
   its result with the trace hash.

Time, order and chance: the scheduler keeps a queue ordered by (virtual time,
random tie, sequence number). Events at the same virtual time run in an order
that the seed decides, so races between a timer, a reply and a client request
are explored. Every random choice (latency, fault, workload, node choice) draws
from one stream (`Rand`, SplitMix64). The replicas draw their ids from the same
stream through `runtime.Environment`.

## What is simulated

| Real thing | In the simulator |
|---|---|
| Run loop | `simNode`: stamps `At` and `Wall` on every event, runs the returned actions, handles `SetTimer`, `StopForRestart`. |
| Log (`executor.Log`) | The real `executor.MemoryLog` behind a fault layer. Calls take effect at one instant; the reply comes later. |
| Object store | The real `executor.MemoryObjects`, with `List` sorted like S3 (the memory store returns unsorted keys). |
| Executor error mapping | `executor.Classify` for errors, and the same completions as `Executor.logIO`. |
| Timers | `SetTimer` and `CancelTimer` on the virtual clock, matched by token, like the run loop. |
| gRPC server | Not used. The client sends `runtime.ControlPlaneDeliver` and gets `ControlPlaneReply`, the simulator bridge path of the replica. |
| Rebalance loop of `server.Node` | A second dst process per node (`rb-0` beside `cp-0`, `groups.go`) that hosts `rebalance.Machine` as `server.Node` does. |
| gRPC group handlers | The group host in `groups.go`: a call id per call, the wait for the reply, `CallAbandoned` at the deadline, and `OffsetFetch` behind the role bit. |

Faults:

- Log and object calls: error reply with nothing applied, timeout with nothing
  applied, applied but the reply is lost (a timeout, `MaybeApplied`), slow
  replies, corrupted checkpoint reads (one byte changed).
- Nodes: crash at any step and boot later with a new boot number (all memory
  lost, the node restores from the log and the checkpoint), pause for a while,
  cut off from the log and the object store (every call times out), so another
  node can claim the epoch and the old writer is fenced when it comes back. The
  replica's own cooperative faults (`StallProb`, `CrashProb`) are on in
  `crash-restart` and `chaos`.
- Buggify sites (dst `fault.Injector.Maybe`). Each site is switched on in about
  half of the runs, and when on it fires on about 1 call in 20. They are in
  every scenario, also in the quiet tail, because the real log and object store
  may do these things at any time. The names are in `simulator/sites.go`:
  `sim.log.slow-answer` and `sim.objects.slow-answer` (the call takes effect and
  the reply comes 50 to 400 ms late), and `sim.log.short-read-page` (a `LogRead`
  returns the first part of the page, at least one entry; the replica reads again
  from its applied slot). The object store has no short page site: `ObjectList`
  has no page limit or continuation, so a short list would not be legal.
  The rebalance machine has five sites of its own (`domain/rebalance/sites.go`),
  and gets the fault injector of the run, so they fire and are counted too:
  `rebalance.timer.late`, `rebalance.timer.stale`, `rebalance.reply.duplicate`,
  `rebalance.queue.late-drain` and `rebalance.load.retry`.

The log has no epoch check on append. Fencing in this design is a conditional
put (a slot can be written once) plus the writer epoch stored in every entry,
and `LogClaimEpoch` is a compare-and-swap. The fake log keeps those semantics
because it is the real in-memory log.

## What is checked (the oracle)

Every check is reported with `env.Always` (a broken one is a failure) and has a
property name.

| Property | How |
|---|---|
| acked write is in the log, with the spec answer | After the run the log history is replayed slot by slot into the spec model. Every acked commit must be in it and must have the answer the spec gives. Acked topic creates and group installs must be in the final spec state. |
| reads agree with the spec | Every read records how many slots the node had applied when it answered. The spec, replayed to that prefix of the log, must give the same answer. |
| acked write is visible, offsets and segment lists never go backwards | A read must show what clients had been told before the read was sent: end offsets, committed ranges, topics, group generation. Needs no order of the writes. |
| one writer per epoch | Every epoch has one claim winner, one writer name in the log entries, and at most one node that holds it. |
| replicas agree on every slot | After every step, a node that has applied N slots must have the same state hash as every other node (and the same node before a crash) at N slots. Checkpoint restores are covered, too. |
| live replicas converge | At the end of the quiet tail every node is ready and misses no slot that changes state, and answers the final reads. |
| log has no hole, log entries decode | The history of the log is contiguous. |
| assertions of the replica | The `Always` calls inside the replica (for example "a Put never wins a slot below the trim floor") are recorded as failures too. |
| the consumer groups | See "Consumer groups" below. |

Because the log is the truth about the order of writes, an operation whose
answer was lost ("maybe applied") needs no special case: it is in the log or it
is not. The model (`internal/spec`) lives behind a one-method interface (`spec.Model`).
`spec.go` in the simulator is the only file that imports it.

Coverage: a campaign prints which replica and rebalance paths no seed reached
(claims, take-over, fencing, restore from checkpoint, trimmed log, lost slot,
bad checkpoint CRC, replica crash and stall, and the rebalance paths below). It
is a hint, not a failure.

## Consumer groups

The rebalance machine (`domain/rebalance`, docs/rebalance.md) runs in the
simulator beside each replica, and a few consumers use it.

The rebalance process. Each node gets a second dst process, `rb-N`, that
crashes, pauses and restarts with the replica `cp-N`. A restart takes a new
machine, so it starts with no memory, in Standby. The host (`groups.go`) does
what `server.Node` does in production, and nothing else:

- `SubmitCommand` goes to the replica as a `ControlPlaneDeliver` from
  `rebalance`; `SubmitRead` as a `LocalRead`, or as a `ControlPlaneDeliver`
  when it is Fresh.
- A `ControlPlaneReply` to `rebalance` comes back to the machine as a
  `ReplicaReply`. The node looks at `To` first, as `Node.reply` does.
- After each step of the replica, a change of "active appender and Ready" is
  sent to the machine as `RoleChanged` (only the edge), after the other outputs
  of the step, as the `AfterStep` hook does.
- A group call gets an id and waits for its `Reply`. If its deadline passes
  first, the call is forgotten, `CallAbandoned` goes to the machine, and a later
  reply is dropped. When the node goes down, every waiting call gets
  `ErrUnavailable`, as a closed connection.
- `OffsetFetch` is not a machine call: behind the role bit it is a `LocalRead`
  of the replica, and the answer is judged against the spec like any read.
- The machine gets the fault injector of the run, so its buggify sites fire.

A real inbox keeps the order in which values were put in it, but the dst
scheduler breaks ties of the same instant at random. So the host keeps a queue
for the machine and one for the replica, and sends a wake token for each value:
a token takes the value at the head of its queue, whatever token comes first.
The times are scaled down like the replica config: initial delay 300 ms, write
timeout 2 s, new member timeout 4 s.

The consumers (`consumers.go`). Each scenario runs 5 or 6 consumers over three
groups. A consumer joins (session 1.5 to 2.5 s, rebalance timeout 2 to 3 s),
syncs (the leader gives each member bytes that name the group, the generation
and its place), then heartbeats about four times per session, commits offsets
(each offset is new in the run), fetches them, describes and lists groups. It
follows the answers as a Kafka client does: an unknown member joins again as a
new member, a rebalance in progress or another generation makes it join again.
Each call goes to the node that answered it last; on `ErrUnavailable` or a
timeout it tries the next node, up to 8 times. In the fault phase a consumer
also leaves now and then, abandons a call on purpose (a short deadline), or goes
silent for good, sometimes right after its join and before its sync. A new
consumer takes the place of a silent one a few seconds later. In the quiet tail
the consumers do not leave, abandon or go silent, and a watcher reads all the
groups again and again. The quiet tail ends only when that read shows the groups
settled.

What is checked, with the property names:

| Property | How |
|---|---|
| no two leaders of one generation | Two join answers for one group and generation name the same leader. |
| a generation never goes down | The join answers of one member never go down in generation. A known member keeps its id (`a known member keeps its id`). |
| an answered generation is in the log | After the run, every join answer matches the logged `GroupInstall` of its generation: the same leader, and the member is in it. |
| a sync answer is the logged assignment | The bytes a member got are the ones a logged `GroupAssign` of its generation gave it. |
| an acked offset commit is in the log, and survives | Every offset acked OK applied in the log, and the final state has an offset for the partition from a commit at the same slot or later. A commit answer has one code per offset. |
| a silent member is removed, consumer groups settle | The last read of the watcher, at the end of the quiet tail: no member of a silent consumer is left, and every group is Stable with exactly its live consumers as members. |
| every call gets an answer, and one answer | A call sent in the quiet tail is answered before its deadline. A reply to a call that does not wait (a second reply) is a failure; a reply to an abandoned call is dropped. |
| request ids never repeat | A machine never uses a request id twice in one run (`Node.reply` depends on it). |
| a machine answers only with the role | No call is answered OK after the machine was told that it lost the role, until it is told that it has it again. |

The log may get a write that the machine gave up (its term ended first) after
the machine read the log again (docs/rebalance.md section 8). Such a write gives
no answer, so the checks of answers need no special case. But it can leave the
log with other members than the memory of the machine: the host marks the group
"late" when a given-up write is answered OK, and the members of a late group are
not compared when the groups settle (its state still is).

The rebalance paths in the coverage list (the machine has no assertions of its
own, so the host marks what it sees in the inputs and outputs): a join round
completes, a round completes with many members, a member gets its assignment,
an offset commit is acked, a leave removes a member, a heartbeat is told to join
again, a silent member is removed, a member that never synced is removed, a
takeover loads a group with members, a failed write ends the term, the write
timer ends the term, groups are read again from a fresh tail, a refused load
read is sent again, the role is lost while calls wait, a call is abandoned, and
a late reply to an abandoned call is dropped.

`Config.noGroups` (tests only) leaves out the rebalance processes and the
consumers. A run is then the run of before they were added, step for step:
`TestGoldenWithoutGroups` checks the old trace hashes.

## Trace hash

After every step the run folds the virtual time, the node, the replica's
`StateHash()` and the number of actions into one hash (and the same for each
step of a rebalance machine, with its own `StateHash()`). The hash is in the
report and the campaign line. `TestSameSeedSameHash` runs each seed twice (and
once on 8 goroutines) and requires the same hash. Running `make sim-campaign`
twice prints the same hash for each scenario; this also holds across processes,
which have different map iteration orders. The engine is in the determinism
gate (`ops/scripts/ci-determinism.sh`): no goroutines, no map ranges, no
imports outside the allow-list (plus `context`). The campaign runner
(`cmd/cpsim`, on `dst/campaign`) uses goroutines to run seeds in parallel
and the wall clock only to print the duration; it is not in the gate.

Golden pins. `TestGoldenTraceHashes` pins the hash of `basic/17` and
`chaos/42` (`golden_test.go`), and `TestGoldenWithoutGroups` pins the same seeds
with the rebalance processes left out. The campaign line of `make sim-campaign`
is a third pin: the hash of 300 seeds of each scenario. A change that moves any
of them changed the behaviour of a run. That is allowed only on purpose: say
why, and re-pin in a commit of its own. At 300 seeds (`-seeds 300`, first seed 1)
the hashes are: basic `aaf01c86`, log-faults `4032d6fb`, crash-restart
`f1e59c13`, leader-fencing `0609269d`, chaos `951ef86c`.

## Read a failure

    FAIL seed=7 scenario=chaos step=2123 t=19133ms: sim: replicas agree on every slot: detail=after 22 slots, cp-1 has state b2243c2c93cc1170 but cp-0 had 515376c0e35c0fae (and 3 more)
    last steps:
      step 2119 t=19130ms cp-1 <- runtime.LogReadDone{...} (2 actions)
      ...
    reproduce: go run ./cmd/cpsim -scenario chaos -seed 7

- `seed` and `scenario` are all you need to run it again. The command on the last
  line does it, with a line for every step on the output.
- `step` is the scheduler event count and `t` the virtual time when the
  property first broke. `(and N more)` counts later hits of the same property.
- `last steps` are the last 48 steps before the end of the run: event, node,
  and how many actions the node returned.
- The first failure is usually the cause. Later ones (for example a read that
  disagrees with the spec) often follow from it.

## Planted bugs

A simulator that cannot find a planted bug cannot be trusted. `-bug` plants one:

- `writer-offsets`: the replica's own mutant, the writer chooses offsets (found
  at once, replicas disagree).
- `log-overwrite`: the log says "stored" for a put into a taken slot.
- `claim-always-wins`: a claim of the writer epoch wins when it should lose.

`TestPlantedBugsAreFound` requires that each is found within 300 seeds, and
`TestCleanRunHasNoFalseAlarm` that the same seeds pass without the bug.

Three bugs of the rebalance machine are text mutants in `ops/mutants.txt`, run
by `make mutants`, and `TestScenariosPass` kills each. They show only under
faults (the basic scenario passes with each of them):

- `rebalance-request-ids-repeat`: request ids start again at each load (found
  by "request ids never repeat").
- `rebalance-role-loss-ignored-while-loading`: a role loss while the machine
  loads is ignored, and it serves without the role (found by "a machine answers
  only with the role").
- `rebalance-refused-write-applied`: a group write refused with
  `ErrUnavailable` is taken as applied (found by "an answered generation is in
  the log", "a sync answer is the logged assignment" and "no two leaders of one
  generation").

The unit tests of the machine find none of them, and the conformance target
(`TestRebalanceMachine`) only the third: it answers every write and read at
once, so a late answer or a role change during a load never happens there.

## Add a scenario

1. Add a `Scenario{Name, Detail, Faults, Consumers}` in `allScenarios`
   (`scenario.go`). `Faults` has per-call probabilities and the mean gap of each
   node fault; `Consumers` is how many consumers use the consumer groups.
2. If you need a new kind of fault, add a field to `Faults`, act on it in
   `services.go` (calls) or `scenario.go` (nodes), and add a counter to `Stats`.
3. Add its name to `TestFaultsAreInjected` so it cannot go silent.
4. Run `go run ./cmd/cpsim -scenario NAME -seeds 500`.

To add a client operation, add it to `workload.next`. To add a check, record
what is needed in `oracle.record` and judge it in `oracle.verify` (after the
run) or `oracle.checkRead` (during the run).

## Not covered yet

- The gRPC server is not run. The group host in `groups.go` plays its part for
  the group calls, and `TestGRPCRebalance` (test/conformance) checks the real
  one.
- The reads of the plain clients go through the simulator bridge path
  (`ControlPlaneDeliver`: wait for a fresh tail). The reads of the rebalance
  machine and the `OffsetFetch` of the consumers are `LocalRead`s, as in the
  server.
- A call sent in the fault phase may hit its deadline without a failure: under
  faults the time a correct machine takes to answer has no simple bound, so only
  calls sent in the quiet tail must be answered in time.
- The idempotency window is off (`WindowSlots` 0), so forgetting old commit
  keys is not tested. Clients retry quickly, so it would not be hit anyway.
- Committed group offsets may legitimately go down (a late retry of an old
  commit), so only the spec comparison checks them, not a monotone rule.
- No corruption of log entries, no clock skew between nodes, no loss of
  acknowledged log writes, no network faults between clients and nodes.
- The log is the in-memory log. The DynamoDB and PostgreSQL logs are checked by
  their own conformance tests (`executor/logtest`), not here.
- Liveness is checked only at the end (the cluster converges when faults stop).
  There is no check of how long a take-over may take.
