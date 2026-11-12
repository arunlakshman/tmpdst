# Kafka differences: our group coordinator against Apache Kafka

This is a reference. It lists every place where our consumer-group coordinator (`domain/rebalance`, the
machine in `docs/rebalance.md`) differs from the classic group protocol of Apache Kafka, and every
behaviour choice made in the greenfield refactor. Rule numbers (R n) are the rule table in
`docs/rebalance.md` section 5. Decisions Q1 to Q10 are in its section 2.

Kafka source references are to the open-source files `GroupMetadataManager.java`, `ClassicGroup.java`,
`ClassicGroupMember.java` and `OffsetMetadataManager.java` (trunk, as cached during the refactor). They
name the class and method; line numbers are left out because they move. A claim marked "not verified"
could not be confirmed from that cached source.

Code paths are under `internal/controlplane/domain/rebalance` unless another path is given.

## 1. Principle

The user decided: we mimic the behaviour of public open-source Apache Kafka for the classic consumer
group protocol. Where we differ, it is listed here, with the reason and the status. Classic protocol
only: no cooperative rebalance, no KIP-848 consumer groups. The log holds decisions only; phases,
parked calls and timers live in memory only.

Status words: DONE (we follow Kafka), KEEP (differs on purpose; the user decided to keep today's
behaviour and document it), NOT DONE (Kafka has it, we do not), OURS (a design choice that is not a
Kafka question).

## 2. Where we follow Kafka (deliberate changes from the older code)

| Point | Kafka | What we do | Why | Where | Status |
|---|---|---|---|---|---|
| R30 session timer of a waiting member | `ClassicGroupMember.hasSatisfiedHeartbeat`: a new member is never satisfied; an existing member awaiting join or sync is. `GroupMetadataManager.expireClassicGroupMemberHeartbeat` removes a member only if not satisfied. | An existing member with a parked join or sync is not removed by its session timer; the timer starts again when the call ends. A new member (joined with an empty id, until its first done round) gets the new member timeout (5 min) and is removed even with a parked join. The join timeout and the sync deadline bound the wait. | Q1. A slow rebalance must not drop members that are only waiting. | `timers.go`, `join.go`; `kafka_test.go` TestWaitingMemberKeepsSession, TestNewMemberTimeout; model: `spec/rebalance_test.go` TestRebalanceWaitingMemberDoesNotExpire, TestRebalanceNewMemberExpires | DONE |
| R25 heartbeat by state | `GroupMetadataManager.classicGroupHeartbeatToClassicGroup`: Empty is UNKNOWN_MEMBER_ID, PreparingRebalance is REBALANCE_IN_PROGRESS, CompletingRebalance and Stable are OK (and reset the timer). | Same. Unknown member, then wrong generation, are checked first (R18). | Q2. A consumer may heartbeat while the group is in CompletingRebalance. | `coordinator_test.go` TestHeartbeatTable; model: `TestRebalanceHeartbeatByState` | DONE |
| R24 sync deadline | `GroupMetadataManager.schedulePendingSync` and `expirePendingSync`: after the install, a timer of the group rebalance timeout; members that did not sync are removed and a rebalance starts. | Sync deadline = install time + the largest rebalance timeout. Members not synced are removed (logged `GroupRemove`, reason expire), then R27. The deadline also runs after Stable while a member has not synced. The leader that never syncs is removed too. | The older code never removed non-syncing members after Stable. | `sync.go`, `timers.go`; `kafka_test.go` TestSyncDeadlineInStable; `rules_test.go` TestLeaderNeverSyncs; model: `TestRebalanceSyncTimeout` | DONE |
| R31 offset commit validation | `ClassicGroup.validateOffsetCommit`: generation < 0 on an Empty group is accepted; otherwise an unknown member is UNKNOWN_MEMBER_ID, a wrong generation is ILLEGAL_GENERATION; CompletingRebalance is REBALANCE_IN_PROGRESS. | Same checks on memory first (`commit.go`); a group the machine does not have is not checked here. Then the write; the log checks again at apply time. A commit with an empty member id and generation -1 on a group with members is ErrUnknownMember (it was ErrIllegalGeneration). A commit keeps the member's session (R29). | Q7. | `rules_test.go` TestOffsetCommitChecks; `kafka_test.go` TestCommitKeepsSession; model: `TestRebalanceOffsetCommit` | DONE |
| R32 Describe and List show Kafka states | `GroupMetadataManager.describeGroups` and the list path read the states from coordinator memory: Empty, PreparingRebalance, CompletingRebalance, Stable, Dead. | State comes from the machine's memory phase. Describe is the log's description with State replaced; Dead for a group the machine does not have. List comes from memory alone. | Q9. The log cannot tell PreparingRebalance from Stable. | `describe.go`; `kafka_test.go` TestDescribeAndList | DONE |

## 3. Where we still differ from Kafka and KEEP today's behaviour

The user decided: keep, but document. Each row is a candidate for a later change (section 6).

| Point | Kafka | What we do | Why | Where | Status |
|---|---|---|---|---|---|
| R9 unchanged follower rejoins in Stable or CompletingRebalance | `GroupMetadataManager.classicGroupJoinExistingMember`: in CompletingRebalance a member whose protocols match, and in Stable a follower whose protocols match, is answered at once with the current generation; no new round. In Stable a leader rejoin does start a round. | Any join on Stable or CompletingRebalance opens a round at once, with no delay. | Today's tested behaviour; simpler. | `join.go`; `coordinator_test.go` TestSecondMemberRebalancesStableGroup | KEEP |
| R13 round ends with no member | `ClassicGroup.initNextGeneration`: the generation is raised even when the group becomes Empty (and an Empty group record is written). | No member left: Empty, generation kept. | Today's behaviour. | `join.go`; R13 in `docs/rebalance.md` | KEEP |
| R14 leader choice | Not verified (leader choice in `ClassicGroup` was not read). | The leader stays if still a member, else the member that joined first. | Today's behaviour. | `join.go` | KEEP |
| R15 protocol choice | `ClassicGroup.selectProtocol`: candidates are the protocols every member supports; each member votes for one; most votes wins. | The first protocol in the leader's list that every member has. None: every parked join gets ErrInconsistentGroupProtocol and the group stays in its round. | Today's behaviour; simpler and deterministic. | `join.go`; `rules_test.go` TestProtocolMismatch | KEEP |
| R15 join whose protocols do not fit | `ClassicGroup.supportsProtocols` (called from `classicGroupJoinNewMember` and `classicGroupJoinExistingMember`): refuses the join at once with INCONSISTENT_GROUP_PROTOCOL when the group has members and no protocol of the joiner is supported by all members. | We check only the protocol type at once (R3). A joiner with no common protocol is parked; the round then ends with ErrInconsistentGroupProtocol for the parked joins (R15). | Today's behaviour. | `join.go`; `rules_test.go` TestProtocolMismatch | KEEP |
| R21 assignment check | `GroupMetadataManager.classicGroupSyncToClassicGroup`: does not check the assignment; members missing from it get an empty assignment. | The leader's sync is checked before any write: an assigned member that is not a member is ErrUnknownMember, a member assigned twice is ErrInvalid. | Today's behaviour; protects the log. | `sync.go`; `rules_test.go` TestSyncRules | KEEP |
| R31 log part: generation below 0 | `ClassicGroup.validateOffsetCommit`: on an Empty group any generation < 0 is accepted, with or without a member id. | The log (`domain/group`) decides at apply time and is stricter (see "memory and log" below). | Today's log rules. | `domain/group`; plan rule 2 | KEEP |
| R31 log part: unknown group | `OffsetMetadataManager.validateOffsetCommit`: generation < 0 creates a "simple group"; generation >= 0 is GROUP_ID_NOT_FOUND from request version 9, ILLEGAL_GENERATION before. | Generation >= 0 on an unknown group is ErrUnknownMember. | Today's log rules. | `domain/group` | KEEP |
| R31 log part: simple group | `OffsetMetadataManager.validateOffsetCommit` calls `getOrMaybeCreateClassicGroup(groupId, true)`; `ClassicGroup.isSimpleGroup` is a group with no protocol type and no members. `GroupMetadataManager` treats it in the list path (shown as Empty; exact list code not verified). | A manual commit creates no group. Describe says Dead; List leaves it out. | Offsets live in the log without a group record. | `describe.go` | KEEP |
| R32 Describe outside Stable | `GroupMetadataManager.describeGroups`: in Stable it gives protocol, member metadata and assignment; in other states it gives no protocol and members without metadata (`describeNoMetadata`), taken from memory. | Describe gives the log's description (protocol, metadata, assignment as logged) with the memory State. | Q6/Q9 resolution. | `describe.go` | KEEP |
| MEMBER_ID_REQUIRED (KIP-394) | `GroupMetadataManager.classicGroupJoinNewDynamicMember`: a join with an empty member id (version 4 and up) is answered MEMBER_ID_REQUIRED with a new id, and the client joins again. | A join with an empty id creates the member at once (R4). | Not needed by our clients. | `join.go` | NOT DONE |
| Static membership (group.instance.id) | `ClassicGroup` and the join paths (`classicGroupJoinNewStaticMember`) handle static members. | The instance id is kept in the member, not used. | Classic dynamic members only. | `join.go` | NOT DONE |
| SyncGroup protocol check | Not verified (the check of protocol type and name in `validateSyncGroup` was not read). | No check. | Not decided. | `sync.go` | NOT DONE |
| Memory and log can answer a commit differently | Kafka has one copy of the state. | The log decides at apply time (plan rule 2). Example: generation -1 with a member id on an Empty group passes the memory check (R31) but the log refuses it with ErrIllegalGeneration. | The machine checks memory only to answer early; the log is the truth. Fully Kafka would need a change in `domain/group`. | `commit.go`, `domain/group` | KEEP |

## 4. Design choices of ours that are not Kafka

These are not wire-answer differences (except where said). They are worth knowing.

| Point | Kafka | What we do | Why | Where | Status |
|---|---|---|---|---|---|
| Write timer (R38, Q3) | Not applicable (the coordinator writes to its own partition). | Each in-flight write has a 10 s timer. If it fires before the reply, the term ends (R36). | We write through a replica and cannot know the outcome. | `write.go`, `timers.go`; `term_test.go` TestWriteTimerEndsTerm | OURS |
| Fresh re-read after a possibly-applied write (Q4) | Not applicable. | After a failed or timed-out write the term ends, memory is dropped, and every group is read again with a ControlPlaneDeliver read (waits for a fresh tail). | Memory may differ from the log. | `load.go`; `rules_test.go` TestStoreFailureBreaksTermAndRebuildRecovers | OURS |
| Load retry (R39, Q5) | Not applicable. | In Loading, a load read refused with ErrUnavailable is sent again after 100 ms. Any failed load read is retried. | The node may not be Ready yet. | `load.go`; `term_test.go` TestLoadRetry | OURS |
| OffsetFetch, Describe, List (Q6, Q9) | Describe and List are served from coordinator memory; OffsetFetch from the offsets state (`OffsetMetadataManager`). | OffsetFetch goes from the handler straight to a local read behind the role bit, not through the machine. Describe and List stay machine calls. If Describe came from the log it could not show PreparingRebalance or CompletingRebalance. | Q6 and Q9 pull apart; this keeps Kafka states. Cheap to change (one input). | `server/groups_handler.go`, `describe.go` | OURS |
| Commit does not queue (Q7) | Not applicable. | A commit does not wait behind a group write; a failed commit write answers ErrUnavailable and does not end the term. Answer codes pass through the log's rule codes, others become ErrUnavailable. | A commit does not change group state. | `commit.go`; `term_test.go` TestCommitFailedWrite | OURS |
| Malformed join on a node that is not the coordinator | Not coordinator is answered first. | The machine answers a malformed join ErrInvalid even when the node is not the coordinator. | The machine does not know the shard owner; the host decides that. | `join.go`; `rules_test.go` TestEmptyMemberIDAndBadJoins | OURS |
| Non-coordinator answer to a group write | Not coordinator (a retriable error). | ErrUnavailable with outcome MaybeApplied. The old coordinator said NotApplied. `Reply` carries no outcome. Clients retry anyway. | Smaller `Reply`. | `server/groups_handler.go` | OURS |
| A join does not restart the session timer | `classicGroupJoinExistingMember` path: not verified. | A join does not restart the session timer. The answer of a parked join does (R29). The old code restarted it on a join. | Follows R29/R30. | `join.go` | OURS |
| Round end after a failed round | Not verified. | The round end does not move after a failed round; the old code could arm it again. | Avoids a round that never ends. | `join.go`, `timers.go` | OURS |
| Removal refused with ErrTooLarge or ErrInvalid | Not applicable. | At round end the parked joins get the code. At session expiry the session starts again. At the sync deadline the deadline is armed again for the longest rebalance timeout. | A refused removal wrote nothing; the term goes on. | `remove.go` | OURS |
| No GroupRemove when all removed members are new | Writes nothing for members that were never logged (matches Kafka). | The same: no write when every removed member is new. The model still writes it and the log ignores unknown members. | Less log traffic. | `remove.go` | OURS (matches Kafka) |
| Describe of many names | One response for the request. | One read per name, one reply when all are back. One failed read fails the whole call. State is taken when all reads are back. | Simple call model. | `describe.go` | OURS |
| State name in the log layer | Kafka names: PreparingRebalance, CompletingRebalance. | The log layer still says `PreparingSync` (in `domain/group`); the machine's Describe uses the Kafka names. | Renaming the log value is a data change. | `domain/group/tables.go` | OURS |
| Round end moves later for a longer rebalance timeout | `GroupMetadataManager.prepareRebalance` sets the join timer once when the round opens. | R10: a later member with a longer rebalance timeout moves the round end later. | Simplification (`docs/rebalance.md` section 7). | `timers.go` | OURS |
| Abandoned call starts the session again | Kafka cannot see that a client went away and keeps the call. | R33: an abandoned parked join or sync gets no reply, and the member's session starts again. | We see the RPC context end. | `join.go`, `sync.go`; `waitjoin_test.go` | OURS |

## 5. Known limits

- The machine heals stale memory by itself. Any mismatch between memory and log gives a business code
  from the log, which ends the term and reloads. So some plausible bugs are invisible to tests (for
  example timers dropped while a group is busy). See `docs/refactor-progress.md`, T6.5 FINDINGS.
- The stale-state window after a fresh re-read: a write still queued in the replica can apply after the
  read. Installs stay safe, because the log checks generation + 1. A late `GroupRemove` or `GroupAssign`
  is seen at the next term. The simulator oracle allows a late write (it marks the group "late" and
  skips member comparison for it). Kafka has no such window; not applicable.
- The conformance model (`internal/spec/rebalance.go`) and the machine disagree in one corner: an
  assignment that is both over 256 KiB and breaks R21. The model says ErrTooLarge (size first); the
  machine says ErrInvalid (R21 first). The machine is right per the note. Not fixed.
- Not drawn by the conformance test: rebalance timeout 0, too-large mixed with R21 errors,
  ErrTooLarge or ErrInvalid write answers, failed reads (R39) and Describe read failures.

## 6. How to change one of these later

| Change | Touch |
|---|---|
| R9 answer an unchanged follower at once | `join.go` (join in Stable and Syncing), R9 in `docs/rebalance.md`, the model `internal/spec/rebalance.go`, `coordinator_test.go` |
| R13 raise the generation on an empty round | `join.go`, `domain/group` (install of an empty group), the model, R13 test in `coordinator_test.go` |
| R14, R15 vote and early refusal | `join.go`, the model, `rules_test.go` TestProtocolMismatch |
| R21 drop the assignment check | `sync.go`, the model, `rules_test.go` TestSyncRules |
| R31 log part (generation < 0, simple group) | `domain/group` commit rules, `commit.go`, `rules_test.go` TestOffsetCommitChecks, the model |
| R32 hide data outside Stable | `describe.go`, `kafka_test.go` TestDescribeAndList |
| MEMBER_ID_REQUIRED, static members, SyncGroup check | `join.go`, `sync.go`, the wire types in `api`, the model |
| Describe or List out of the machine | `describe.go`, `server/groups_handler.go` (one input, Q6/Q9) |
| Any rule | Update `docs/rebalance.md`, the model, the machine test, the mutants in `ops/mutants.txt`, then `make build`, `make mutants`, `make sim-campaign` (hashes may re-pin) |
