# Consumer groups design

Status: slices 1 to 5 are built. Slice 6 is still to do. Date: 2026-10-31.

Read these first:

- [decisions.md](decisions.md) for the numbered decisions this design uses.
- [read-consumer-gaps-for-kafka-clients.md](read-consumer-gaps-for-kafka-clients.md) for what a Kafka client can do today.

WarpStream facts come from [arun-warpstream-agent-reverse-engineering.md](arun-warpstream-agent-reverse-engineering.md). Where that file is not sure, this doc says so.

## Goal

Stock upstream OpenMessaging Benchmark (OMB) with the Kafka driver must run a produce and consume workload.

- No source patches to OMB. Only the YAML config changes.
- Today our OMB build has four patches. Each one is a workaround:
  - maniBalance pin
  - static assign
  - produce-only wait
  - affine partitioner
- The goal of this design is to remove the need for these patches.

We check the result with the Kafka CLI scripts: `kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh` and `kafka-consumer-groups.sh`. We do not use franz-go for this check. We do not use Docker.

## Scope

In scope:

- CreateTopics (Kafka API 19).
- A Metadata answer that lists all live brokers.
- Broker liveness in the control plane (CP).
- Fetch that waits for new data (long poll).
- Group APIs: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch.
- ListOffsets and OffsetFetch as plain reads.

Out of scope for now:

- The KIP-848 group protocol (see Q9).
- Idempotent and transactional producers. Decision 41 stays. YAML may still set `enable.idempotence=false`.
- Retention and log compaction.

## Current state in one paragraph

The broker serves five APIs. A FindCoordinator request gets a closed connection, so a group client fails at its first step. Metadata lists the live brokers (slice 2), but every broker still names itself as leader. Fetch never waits. The CP has three operations: CommitSegment, Lookup and DescribeTopics. There is no group state and no committed offset anywhere. See [read-consumer-gaps-for-kafka-clients.md](read-consumer-gaps-for-kafka-clients.md) for the details.

## Decisions

Each decision has four parts: the question, the decision, the reason, and the WarpStream comparison.

### Q1. Bar for this work

**Question.** What must work before this is done?

**Decision.** Stock upstream OMB, Kafka driver, YAML config only, produce and consume. Checked with the Kafka CLI scripts (see Goal). `enable.idempotence=false` stays allowed in YAML. Decision 41 stays.

**Why.** The four patches show that OMB needs more than the broker has today. A YAML-only run is the test that tells us the gap is closed for real users.

**WarpStream.** WarpStream supports stock clients with no client changes. This design aims at the same thing for our own broker.

### Q2. Metadata and leaders

**Question.** Which broker does a client talk to, and what does Metadata say?

**Decision.** Every broker still names itself as the leader of every partition for Produce and Fetch (decision 29). Brokers stay stateless and interchangeable. But Metadata `brokers[]` lists all live brokers. So a client can connect to the broker that FindCoordinator names.

**Why.** A group client must reach its coordinator. With a one-broker Metadata answer, the client has only its seed list when that broker is down. A full broker list fixes that without making any broker a leader.

**WarpStream.** WarpStream agents are stateless front ends. The CP answers `Discover` and `DiscoverBatch` calls, which give agents the list of their peers (reverse-engineering doc, section on service discovery).

### Q3. Coordinator and group RPCs

**Question.** Where does group state live, and which broker answers group calls?

**Decision.**

- FindCoordinator(group) returns a broker picked by hash(groupId) over the live broker set.
- Group RPCs (JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch) are accepted on any broker.
- Each broker forwards these calls to the CP.
- No broker keeps group state.

If two brokers have different live sets for a short time, they may name different coordinators. This is safe, because any broker forwards to the CP. The only effect is that a client may need one more FindCoordinator call.

**Why.** No broker owns a group, so a broker crash does not lose group state. The group state stays in the CP, where the log protects it (Q5).

**WarpStream.** The reverse-engineering doc reads the group RPC list as a sign that the group coordinator lives in the CP, and that agents are stateless front ends. It cites a flag named `KAFKA_HANDLE_CONSUMER_GROUPS_IN_BACKEND`. The comparison doc (`warpstream-architecture-comparison.md`) says this is still a hint, not proof. We take the design from the reverse-engineering doc and keep the comparison doc's doubt in mind.

### Q4. Live broker set

**Question.** How does the CP know which brokers are alive?

**Decision.** Brokers register with the CP and send heartbeats. The CP keeps a lease for each broker. A broker whose lease expires is removed from the live set. The lease length is a config value. Pick it in slice 2.

**Why.** The FindCoordinator hash and the Metadata list both need the live set. A lease is the simplest way to get it.

**WarpStream.** WarpStream has a `Heartbeat` call for agent liveness and a `Discover` call for finding peers. Both do the same job as the lease and the list in this design.

### Q5. Group state durability

**Question.** Which group facts must survive a CP failover?

**Decision.**

- These are commands in the CP log. The state updater folds them (`internal/controlplane/stateupdater`):
  - Join
  - Leave
  - Sync assignment
  - OffsetCommit
  - Member expiry
- Heartbeats and session timers are memory only on the active CP.
- After a CP failover, each group gets its own session timeout as a grace window. A member that is still silent after that window is expired by a logged entry. That entry bumps the group generation and forces a rebalance.

**Why.** Membership, assignments and offsets must not be lost. Timers and heartbeats can be rebuilt. The grace window lets live members send a heartbeat to the new CP before they are expired.

**WarpStream.** WarpStream has a CP-side `TimeoutGroupHeartbeats` call. That is the same job as the logged member expiry here.

### Q6. Heartbeats are not logged

**Question.** Should each heartbeat be a log command?

**Decision.** No. The log holds decisions, not observations. Heartbeats are not logged.

**Why.** One heartbeat every 3 seconds per member is too many log slots. With 64 consumers that is about 21 slots per second, only for liveness. The log would grow fast, and most entries would say nothing new.

**WarpStream.** WarpStream's heartbeats go to the CP as calls. The reverse-engineering doc does not show them as a log.

### Q7. One log, not two

**Question.** Should group state use its own log?

**Decision.** One log. One appender, one fence, one checkpoint and one lockstep oracle (speccheck).

- Group data uses its own key prefix in the state updater KV store:
  - `G` for groups, members and assignments.
  - `O` for committed offsets.
  - These sit next to the existing prefixes `T`, `E`, `R`, `K` and `S`.
- Group commands have their own command kinds.
- Splitting the log later is a mechanical change.

**Cost.** OffsetCommit shares the slot budget with CommitSegment. At 64 consumers that commit every 5 seconds, that is about 13 commits per second. Before beta, check that the appender can pack several commands in one slot. This is not verified yet (see Open items).

**Why.** After Q6 the group rate is low. Two logs would double the operations work for a small load. One log also lets the fold check an OffsetCommit against the topic and end offsets, in the same step, with no cross-log lookup.

**WarpStream.** WarpStream keeps group state in the CP. The reverse-engineering doc does not say whether that state shares a log with segment commits.

### Q7b. Broker live set is memory only

**Question.** Should the broker live set be logged?

**Decision.** No. The live set is memory only on the active CP. After a failover, brokers register again within one heartbeat interval.

**Why.** A broker's liveness is an observation, like a heartbeat (Q6). A short gap after failover is harmless.

**WarpStream.** Not compared. This is internal to our CP.

### Q8. Fetch waiting

**Question.** How does a Fetch wait for new data?

**Decision.**

- The broker polls CP `Lookup` on a fixed interval until data arrives or `MaxWait` ends.
- The broker honors `MinBytes`. It counts the bytes of all partitions of the Fetch together. A partition with data holds it while the Fetch is short. Only partitions with no data are polled again.
- `MaxWait` is capped at 30 s. `MaxWait` 0 or `MinBytes` 0 answers at once.
- Config knob: `FetchPollInterval`, default 200 ms, flag `-fetch-poll-interval`. Tune it later.
- With the poll, each idle Fetch costs one `Lookup` per partition per interval. Later, `Lookup` should take many partitions at once, so one Fetch costs one CP call (see Open items).

**Why.** A fixed poll is the smallest change. It needs no wake-up signal from the commit path. The cost is extra CP calls for idle consumers, which the interval controls.

**WarpStream.** All 125 `Stream` RPCs are unary. A flag named `-fetch-quick-retry` retries the fetch with a 50 ms maximum duration when there is no new data. WarpStream also has batch RPC variants (`GetRecordsBatch`, `JoinGroupBatch`, `HeartbeatGroupBatch`) and a file cache of 0.5 GiB per vCPU by default.

### Q9. Group protocol: classic only

**Question.** Which group protocol do we support?

**Decision.**

- Classic only: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit and OffsetFetch.
- The group leader client computes the assignment. The CP only stores it.
- KIP-848 is rejected for now. KIP-848 is "The Next Generation of the Consumer Rebalance Protocol". In that design the broker computes the assignment, with one ConsumerGroupHeartbeat call and no stop-the-world barrier. A request with `group.protocol=consumer` is refused.
- KIP-848 can be added later as new command kinds.

**Why.** The classic protocol is the default in Kafka 3.9 clients. KIP-848 needs a server-side assignor. That is a large piece of work and is not needed for the first goal.

**WarpStream.** WarpStream supports both the classic protocol and the newer "group-type consumer" protocol (reverse-engineering doc, consumer group section).

### Q10. CreateTopics

**Question.** How are topics created through Kafka?

**Decision.**

- Add CreateTopics (key 19) to the broker.
- Accept any replication factor and ignore it. Object storage is the replication.
- Ignore unknown configs. Log one warning per request for them.
- An existing topic gets TOPIC_ALREADY_EXISTS.
- The check and the create are one control plane step: `core.CreateTopic` with `FailIfExists`. The control plane answers ErrTopicExists and changes nothing. So two creates of one new name cannot both succeed.
- Honor `validateOnly`. It only reads (DescribeTopics) and creates nothing.
- Pick the supported version range from what the Kafka CLI 3.9.1 uses. Check it in slice 1.

**Why.** The Kafka CLI and OMB both create topics first. Without CreateTopics they need the CP HTTP API, which is not a Kafka path.

**WarpStream.** WarpStream advertises CreateTopics in the range 0 to 7 (reverse-engineering doc, API table).

### Q11. Defaults and group rules

**Question.** What do the group rules look like in detail?

**Decision.**

- OffsetCommit checks the member id and the generation against the logged membership. A wrong generation gets ILLEGAL_GENERATION. An unknown member gets UNKNOWN_MEMBER_ID.
- An OffsetCommit with generation -1 is accepted. Manual assign clients send -1, so they must still be able to commit.
- Every logged group command carries the timestamp of the active CP. The fold never reads a clock. This keeps the fold deterministic.
- A rebalance in progress is memory only. Only the final Sync assignment is logged.
- If the CP fails over in the middle of a rebalance, the rebalance starts again.
- ListOffsets and OffsetFetch are plain CP reads. They need no new commands.

**Why.** These rules keep the fold deterministic, so the simulator and speccheck can check it. They also keep manual assign clients working.

**WarpStream.** WarpStream's `JoinGroupRequest` carries `timestamp_nanos` (reverse-engineering doc, protobuf section). That matches the timestamp rule here.

## Build order (slices)

Each slice ends with a check that can fail. A slice that does not pass is not done.

0. **Harness.** Download Kafka CLI 3.9.1 into `.tools/`. Git-ignore `.tools/`. Write `ops/scripts/e2e-kafka-cli.sh`. Run one broker with `-objects mem` and `manicp -store mem`. Run the Kafka CLI produce and consume against it.
1. **CreateTopics.** Add the API (Q10). Check it with `kafka-topics.sh`.
2. **Broker leases and Metadata brokers list. Done.** Add the lease (Q4) and the full broker list in Metadata (Q2). This needs a shared object store for 3 brokers, so all brokers see the same segments.
3. **Fetch long poll.** Add `FetchPollInterval` and `MinBytes` and `MaxWait` honoring (Q8). Done.
4. **Group log commands, fold and coordinator.** Done. 4a is the state, the log commands, the reads and the tests in the control plane. 4b is the coordinator (timers, rebalance wait, takeover) and the gRPC group calls (see "Group coordinator"). Add the group command kinds, the `G` and `O` keys and the fold (Q5, Q7, Q11). Add them first to the speccheck oracle and the simulator, and test there. The simulator has the trace tags but no group traffic, so its hashes did not change (see "Group log commands").
5. **Group RPCs and FindCoordinator on brokers. Done.** Forward group calls to the CP (Q3). Test with `kafka-console-consumer.sh --group` and `kafka-consumer-groups.sh --describe`. See "Group APIs on the broker".
6. **OMB run.** Run OMB with YAML only (Q1). **Skipped on the maintainer laptop**, because it needs more compute. On a laptop, run only a very small case.

**Status.** Slice 1 is done. CreateTopics (Q10) is served for versions 0 to 7, and the broker checks it with `ops/scripts/e2e-kafka-cli.sh`. `kafka-topics.sh --describe` also calls DescribeConfigs, so that is served for topics too, with empty configs. The control plane gets one new operation, `core.CreateTopic`. Its `FailIfExists` flag makes the create exclusive: the control plane checks that the topic is new and creates it in one step, and an existing topic gets ErrTopicExists. Each create is one control plane call, with no describe first, so the race is closed. Two creates of one new name cannot both succeed. One case is left: a retry after a lost reply that finds the topic counts as success, because the first try probably made it. A create by another client at the same moment is the same case, and then both can report success.

**Status of slice 2.** Done. Each broker sends a heartbeat every `-lease-interval` (1s). The control plane (`BrokerHeartbeat` RPC) keeps the leases in memory on the active node only. A lease lives for `-broker-lease-ttl` (5s, 3 intervals). The call returns the live set, sorted by node id. The broker core gets it as a `core.LiveBrokers` input event. Metadata lists the live brokers and always lists the broker itself. If the set is stale (no heartbeat worked for 3 intervals), the shell sends an empty set, so Metadata is self only, as before. Every broker still names itself as leader (decision 29). The simulator never sends `LiveBrokers`, so its traces do not change. The 3-broker check uses a local minio (mise) as the shared object store: `ops/scripts/e2e-kafka-cli.sh` checks that 3 brokers are listed, produces through broker 1 and reads through broker 3, and checks that a killed broker leaves the list after the TTL.

**Status of slice 3.** Done. Fetch long poll (Q8). A Fetch part that finds no data is parked. It polls again every `FetchPollInterval`, and each poll is one `Lookup` for that partition. A part with data holds it while the Fetch is short of `MinBytes`. The Fetch answers when `MinBytes` is reached or `MaxWait` ends. The waits are the broker's own timers, so the core stays deterministic. A parked Fetch ends at once on a connection close or a stop. The simulator consumers send `MaxWait` 100, so their trace hashes changed. With the long poll off, the hashes match the old file, so the cause is this change alone. The hashes were updated. `ops/scripts/e2e-kafka-cli.sh` checks the wake-up time (5 lines within 2 s) and the idle `Lookup` rate (29 in 5 s from one idle consumer, limit 60). No new decision row: this follows Q8.

**Status of slice 4a.** Done. The group state and its log commands are in `internal/controlplane/stateupdater` (keys `G`, `M`, `O`), with the oracle in `internal/verify/speccheck/groups.go`. The checks are a table test per command, a random lockstep run with a checkpoint restore in the middle, and a replay test. Not done here: the broker forwarding (slice 5). See "Group log commands".

**Status of slice 4b.** Done. The group coordinator is in `internal/controlplane/groupcoordinator`. It runs on the active control plane node only. The gRPC group calls are in the control plane API, and a standby answers them Unavailable. See "Group coordinator".

**Status of slice 5.** Done. The broker serves the classic group APIs and FindCoordinator, and forwards each group call to the control plane. See "Group APIs on the broker". `ops/scripts/e2e-kafka-cli.sh` checks the Kafka CLI 3.9.1 group tools against the 3-broker stack. The golden trace hashes changed only because the ApiVersions list grew, as checked in that slice.

## Group log commands

Slice 4a. The control plane keeps the group state in the log. Brokers and gRPC do not use it yet.

Writes (log entries, kinds 3 to 6 of the entry codec):

- `GroupInstall`: the join result. It starts generation `Generation`, which must be the current one + 1 (a new group is at 0). It replaces the members and clears all assignments. Offsets stay.
- `GroupAssign`: the leader's assignment. The generation must be the current one, and the leader must be the header leader. Each member gets its bytes, or none. A repeat gives the same result.
- `GroupRemove`: removes members. Unknown members are ignored. With no member left, the header stays (generation kept), with no leader and `assignedGeneration` 0.
- `OffsetCommit`: stores offsets, with one error per partition. A managed commit (generation 0 or more) needs a member at the current generation. A manual commit (generation -1, no member) is allowed only when the group has no members.

Reads (not logged): `OffsetFetch` (offset or -1 per partition), `DescribeGroup` (state, members, assignments) and `ListGroups` (names with state, sorted). The later coordinator rebuilds its memory with `ListGroups` and then `DescribeGroup` for each name. It needs no separate snapshot call.

Errors: `ErrUnknownMember`, `ErrIllegalGeneration` and `ErrUnknownPartition` are new codes 11 to 13 (wire enum too). Old numbers did not change. Every group write over 256 KiB is `ErrTooLarge`. The size counts fields: 16 for the command, 4 + length for each string or byte field, and 10 for each number and list length. It is an upper bound of the stored bytes.

Keys (state updater KV, next to `T`, `E`, `R`, `K` and `S`):

- `G group`: generation, protocolType, protocol, leader, assignedGeneration.
- `M` + uvarint(len group) + group + member id: clientId, clientHost, groupInstanceId, sessionTimeoutMs, rebalanceTimeoutMs, joinedTimeMs, metadata, assignment.
- `O` + uvarint(len group) + group + topic + partition (4 bytes, big endian): offset, leaderEpoch, metadata, commitTimeMs.

Numbers in values are zigzag varints. Strings and byte fields are a uvarint length, then the bytes.

Checkpoints: the format `APLR1` holds any key, so the `G`, `M` and `O` keys are in it with no format change. Old checkpoints still restore.

Simulator: the trace tags for the new types are in `internal/verify/simulator/tags.go`. No scenario sends group traffic, so the golden hashes did not change.

## Group coordinator

Slice 4b. The coordinator is the part that makes groups work over time. It runs on the active control plane node. It is I/O code: it uses clocks and timers. It is not in the deterministic packages.

What the log holds: only decisions. A join result (`GroupInstall`), the leader's assignment (`GroupAssign`), a member that left or expired (`GroupRemove`) and offset commits (`OffsetCommit`). Each one carries the time from the coordinator clock.

What is memory only, on the active node:

- The phase of each group.
- The JoinGroup and SyncGroup calls that wait.
- Heartbeats and session timers.
- The initial delay and the round deadline.

Phases of a group:

- **Empty.** No member.
- **PreparingRebalance.** Members join. The round ends when every known member has rejoined, or at the deadline (the largest rebalance timeout). A new group first waits `InitialRebalanceDelay` (default 3 s). Each new member restarts that wait, but never past the deadline.
- **CompletingRebalance.** The join answer is out. The leader must send the assignment. Followers wait in SyncGroup. If the leader never syncs, the round deadline sends the group back to PreparingRebalance.
- **Stable.** The leader's assignment is in the log. Heartbeats get OK.

Who waits: a JoinGroup call waits until its round ends. A SyncGroup call from a follower waits for the leader. Both stop early when their context ends, and then they return that context error. A member that leaves or expires gets ErrUnknownMember on its waiting calls.

Errors that matter:

- A join with a protocol type that differs from the group gets ErrInconsistentGroupProtocol. So does a round where no protocol is common to all members. The round stays open, so a retry can finish it.
- A heartbeat or sync during a rebalance gets ErrRebalanceInProgress, and the member must rejoin.
- An unknown member gets ErrUnknownMember. A wrong generation gets ErrIllegalGeneration.

Failed writes: a failed log write means the memory may not match the log. The coordinator then stops serving, and its waiting calls get ErrUnavailable. The node rebuilds from the log a moment later.

Takeover: when this node becomes the active appender, the coordinator reads every group from the log. Each member gets a session timer from now. That is the grace window of Q5. A member that stays silent after it is expired by a logged `GroupRemove`. A group whose stored assignment is for an older generation starts in PreparingRebalance, so its members must rejoin.

Standby: a standby answers every group call Unavailable, so the client tries the next address. It drops its memory.

Locks: one mutex per group. A log write runs while that group's mutex is held. The decision and the memory change must not be split. Every write has a timeout (10 s), and Close or a stop cancels it.

Offsets: OffsetCommit goes to the log without the group mutex. A commit from a member counts as that member's heartbeat.

Not in this slice: the broker forwarding (slice 5). Empty groups stay in memory until the next rebuild.

## Group APIs on the broker

Slice 5. The broker holds no group state. Each group call is one control plane call (`core.GroupJoin`, `core.GroupSync`, `core.GroupHeartbeat`, `core.GroupLeave`, `core.DescribeGroups`, and the existing `core.OffsetCommit`, `core.OffsetFetch` and `core.ListGroups`). The full rules and the list of limits are in `read-consumer-gaps-for-kafka-clients.md`, section "Consumer groups". The short version:

- The versions advertised are OffsetCommit 0 to 8, OffsetFetch 0 to 8, FindCoordinator 0 to 4, JoinGroup 0 to 7, Heartbeat 0 to 4, LeaveGroup 0 to 5, SyncGroup 0 to 5, DescribeGroups 0 to 5 and ListGroups 0 to 4. There is no KIP-848 API (Q9).
- FindCoordinator hashes the group name (FNV-1a 32 bit) over the live broker list. ListGroups lists the groups that hash to the answering broker, so each group is listed once.
- JoinGroup and SyncGroup wait for the rebalance. The wait is a control plane call with its own deadline (`core.ControlPlaneCall.Timeout`), because the executor's 10 s default is too short. The broker parks only that connection.
- A failed or late group call gets COORDINATOR_NOT_AVAILABLE. A broker never retries a join.

## Open items

- **Appender slot packing.** Check that the appender packs several commands in one log slot. This is not verified yet. Q7 depends on it. Check before beta.
- **Lookup for many partitions.** Make `Lookup` take many partitions in one call, so one Fetch costs one CP call (Q8). Not scheduled.
- **OMB run on a bigger machine.** Slice 6 runs only on a machine with enough compute.
- **DescribeGroups.** Done in slice 5 (key 15, versions 0 to 5). `kafka-consumer-groups.sh --describe` uses it.
- **DeleteGroups and OffsetDelete.** Not served (keys 42 and 47). `kafka-consumer-groups.sh --delete` needs DeleteGroups. Not planned yet.
- **Coordinator hash on partial live sets.** Q3 says a short disagreement is safe. Test it in the simulator with brokers that join and leave.
