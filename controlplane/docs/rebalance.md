# Rebalance: the group protocol as a pure machine

Status: design note for phase P6 (task T6.1), revised to the user's decisions. T6.2 builds from it.

## 1. Purpose and scope

`domain/rebalance` replaced the old coordinator in `groups/` (deleted in T6.6). It is a `sim.Process`-shaped machine,
`Step(cx, input) -> outputs`, with no locks, goroutines, channels or clock; time and randomness come
from `cx`. It runs in production (a second `live.Run`) and in the simulator (a second dst process per
node). The independent model is `internal/spec/rebalance.go` (section 12).

Principle: do what the open-source Apache Kafka classic group coordinator does (`GroupMetadataManager`,
`ClassicGroup`, `ClassicGroupMember` on trunk). Classic protocol only: no static membership (the
instance id is kept, not used), no cooperative rebalance, no KIP-848. The log holds decisions only;
phases, waiting calls and timers are memory only.

## 2. Decisions

Q6 and Q9 pull apart: Q6 moves Describe and List out of the machine, but Q9 wants the Kafka state,
which only the machine's memory has (the log cannot tell PreparingRebalance from Stable, and does not
know a group in its first round). Resolution, for the user to confirm: **OffsetFetch leaves the
machine; Describe and List stay machine calls.** Describe is the log's description (a LocalRead the
machine makes) with State replaced by the memory phase. List comes from memory alone. Both show the
same state, as in Kafka, where both read the coordinator's memory. Mapping the log state to a Kafka
name in the read was the other option; it can never show PreparingRebalance, so List and Describe
would disagree with Kafka during every round.

- Q1: Kafka rule 30. An existing member waiting in a join or sync is not removed by its session timer; a new member is (by the new member timeout); the join timeout and the sync timeout bound the wait.
- Q2: Kafka heartbeat (rule 25): CompletingRebalance and Stable OK, PreparingRebalance ErrRebalanceInProgress, Empty ErrUnknownMember.
- Q3: a timer per in-flight write (today's 10 s WriteTimeout); when it fires, the term ends.
- Q4: after a write that may have changed the log, read again with ControlPlaneDeliver (fresh tail).
- Q5: in Loading, a load read refused because the node is not Ready is tried again after 100 ms.
- Q6: OffsetFetch goes from the handler straight to LocalRead behind the role bit (see above for Describe and List).
- Q7: offset commit does not queue; a failed write is ErrUnavailable and does not end the term; Kafka checks first (rule 31).
- Q8: one input, `ReplicaReply`, instead of CommandDone and ReadDone.
- Q9: Describe and List show the Kafka states from the memory phase; Dead for a group the machine does not have.
- Q10: the model size is accepted; it must not grow (section 12 says where it stands).

## 3. Inputs and outputs

`Call` is a call id (uint64) that the host gives each RPC, unique in one run of the process. `Req` is
a `machine.RequestID` that the machine gives each submit, unique in one run of the process.

| Input | Fields |
|---|---|
| `Join` | Call, Group, MemberID ("" asks for a new member), GroupInstanceID, ClientID, ClientHost, SessionTimeoutMs, RebalanceTimeoutMs (0 means the session timeout), ProtocolType, Protocols (list of Name and Metadata, in the member's order of preference) |
| `Sync` | Call, Group, MemberID, Generation, Assignments (list of Member and Bytes; only the leader's are used) |
| `Heartbeat` | Call, Group, MemberID, Generation |
| `Leave` | Call, Group, Members (list of member ids) |
| `OffsetCommit` | Call, `contract.OffsetCommit` (Group, Member, Generation, Offsets). The machine sets TimeMs. |
| `Describe` | Call, Groups (list of names) |
| `List` | Call |
| `CallAbandoned` | Call. The RPC context ended. The call gets no reply. |
| `TimerFired` | `runtime.TimerFired` (ID, Gen). A Gen that is not the current one for ID is ignored. |
| `RoleChanged` | Active: the replica is the active appender and Ready. |
| `ReplicaReply` | Req, Done (`runtime.ControlPlaneDone`: Err, Outcome, Resp). The machine knows what each Req was. |

| Output | Fields |
|---|---|
| `SubmitCommand` | Req, Op: `GroupInstall`, `GroupAssign`, `GroupRemove` or `OffsetCommit` |
| `SubmitRead` | Req, Op: `ListGroups` or `DescribeGroup`, Fresh. Fresh false: LocalRead (load at takeover, Describe). Fresh true: a ControlPlaneDeliver read that waits for a fresh tail (load after a broken term). |
| `Reply` | Call, Err, and the answer of the call: Join (Generation, Protocol, Leader, MemberID, Members: id, instance id, metadata; leader only), Sync (Assignment), Leave (one code per member), OffsetCommit (one code per offset), Describe (one description per name), List |
| `sim.SetTimer` | ID, After, Body: `runtime.TimerFired` |
| `sim.CancelTimer` | ID |

Expected codes in Reply.Err: OK, ErrInvalid, ErrUnavailable, ErrUnknownMember, ErrIllegalGeneration,
ErrRebalanceInProgress, ErrInconsistentGroupProtocol, ErrTooLarge, and per offset ErrUnknownPartition.

## 4. Phases as data

The machine is `Standby` (not serving), `Loading` (reading every group from the log, after
RoleChanged active or a broken term) or `Serving` (a map from group name to a group). Standby and
Loading answer every call ErrUnavailable.

A group has fields every phase shares (generation, protocol type, protocol, leader, members; a member
has its session settings, protocol list, join order, session timer, a "new" mark and a "synced" mark),
and one phase value that holds only what that phase needs:

| Phase (Kafka state) | Holds | Left by |
|---|---|---|
| `Empty` | nothing (generation kept) | a join: to `Joining`, initial |
| `Joining` (PreparingRebalance) | round start and end, the end of the initial delay (if any), the parked join of each member | round done: `Syncing`; no member left: `Empty`; install refused or no common protocol: stays `Joining` |
| `Syncing` (CompletingRebalance) | sync deadline, the parked sync of each follower | leader's assignment logged: `Stable`; join, leave, expiry or sync deadline: `Joining` |
| `Stable` | the assignment of each member, the sync deadline while a member has not synced | join, leave, expiry or sync deadline: `Joining` |

So a parked sync in `Joining`, or a parked join in `Stable`, cannot be written. Any phase goes to
`Empty` when the last member leaves. `Dead` is not a phase: Describe says Dead for a group the machine
does not have. Waiting for a log write is not a phase either; a busy group has a queue (section 8).

## 5. Rules

Today's tests (before T6.2; the files are deleted and ported to `domain/rebalance`) were in `groups/coordinator_test.go` (C), `groups/rules_test.go` (R) and
`groups/waitjoin_test.go` (W). "code" means today's code has the rule but no test checks it. CHANGED
marks a rule that differs from today (section 6).

| # | Rule | Today's test | Input / phase |
|---|---|---|---|
| 1 | Join with no group, no protocol type, no protocols, or session timeout <= 0: ErrInvalid. | R TestEmptyMemberIDAndBadJoins | Join |
| 2 | Not serving: every call gets ErrUnavailable. | R TestCloseWakesParkedCalls, R TestStoreFailureBreaksTermAndRebuildRecovers | any / Standby, Loading |
| 3 | A group with members refuses another protocol type at once: ErrInconsistentGroupProtocol. | R TestProtocolMismatch | Join |
| 4 | Empty member id: a new member with id ClientID + "-" + random (random alone without a client id). | R TestEmptyMemberIDAndBadJoins | Join |
| 5 | A member id that is not "" and not a member: ErrUnknownMember. | R TestEmptyMemberIDAndBadJoins | Join |
| 6 | Rebalance timeout 0 means the session timeout. | code | Join |
| 7 | A join parks until its round ends. A newer join of the same member answers the older one ErrRebalanceInProgress. | code | Join / Joining |
| 8 | A join on Empty opens an initial round: it waits the initial delay (default 3 s) for more members. Each new member starts the wait again, but never past the round end. | C TestOneMemberJoinsAndSyncs | Join / Empty, Joining |
| 9 | A join on Stable or Syncing opens a round at once, with no delay. Parked syncs get ErrRebalanceInProgress. | C TestSecondMemberRebalancesStableGroup | Join / Stable, Syncing |
| 10 | Round end (the join timeout) = round start + the largest rebalance timeout. A member with a longer one moves it later. | R TestLeaderNeverSyncs, W TestJoinCanceledThenRoundCompletes | Joining |
| 11 | A round is done when every member has a parked join and the initial delay passed. | C TestSecondMemberRebalancesStableGroup | Join / Joining |
| 12 | At the round end, members with no parked join are removed (logged `GroupRemove`, reason expire). | W TestJoinCanceledThenRoundCompletes | TimerFired / Joining |
| 13 | No member left: Empty. The generation is kept. | C TestLeaveRebalances | Joining |
| 14 | The leader stays if it is still a member, else the member that joined first leads. | C TestSecondMemberRebalancesStableGroup | Joining |
| 15 | The protocol is the first in the leader's list that every member has. None: every parked join gets ErrInconsistentGroupProtocol, and the group stays in its round with its members. | R TestProtocolMismatch | Joining |
| 16 | A done round logs `GroupInstall` (generation + 1, members by id, each with the metadata of the chosen protocol), then goes to Syncing. Each parked join gets generation, protocol, leader and its own id; only the leader gets the member list. No member is new any more, none has synced, and the sync deadline starts (rule 24). | C TestOneMemberJoinsAndSyncs, C TestSecondMemberRebalancesStableGroup, R TestConcurrentJoinsOneRound | Joining -> Syncing |
| 17 | An install refused with ErrTooLarge or ErrInvalid gives that code to the parked joins. The round stays open. | code | ReplicaReply / Joining |
| 18 | Sync, heartbeat: unknown group or member is ErrUnknownMember, then another generation is ErrIllegalGeneration. | R TestSyncRules, C TestHeartbeatTable | Sync, Heartbeat |
| 19 | Sync in Joining: ErrRebalanceInProgress. | R TestSyncRules | Sync / Joining |
| 20 | A follower's sync in Syncing parks until the leader's assignment is logged. A newer sync of the same member answers the older one ErrRebalanceInProgress. | C TestSecondMemberRebalancesStableGroup | Sync / Syncing |
| 21 | The leader's sync: a member assigned that is not a member is ErrUnknownMember, a member assigned twice is ErrInvalid, both before any write. | R TestSyncRules | Sync / Syncing |
| 22 | CHANGED. The leader's assignment is logged (`GroupAssign`), then Stable. The leader and every parked follower get their own bytes (none: empty), and their sessions start again. | C TestOneMemberJoinsAndSyncs, C TestSessionExpiryRemovesSilentMember | Syncing -> Stable |
| 23 | CHANGED. Sync in Stable: the member's stored assignment, at once; a follower's Assignments are ignored. The session does not start again. | R TestSyncRules, R TestConcurrentJoinsOneRound | Sync / Stable |
| 24 | CHANGED. Sync deadline = install time + the largest rebalance timeout. A sync that passes rules 18 and 19 (a parked follower, the leader's logged assignment, or any sync in Stable) marks its member synced. When every member is synced the deadline stops. If it fires (Syncing or Stable), the members not synced are removed (logged `GroupRemove`, reason expire), then rule 27. | R TestLeaderNeverSyncs | TimerFired / Syncing, Stable |
| 25 | CHANGED. Heartbeat after rule 18: PreparingRebalance is ErrRebalanceInProgress; CompletingRebalance and Stable are OK. All three start the session again. In Empty no member passes rule 18 (ErrUnknownMember). | C TestHeartbeatTable, C TestLeaveRebalances | Heartbeat |
| 26 | Leave: one code per listed member, in order. Unknown is ErrUnknownMember (all of them for an unknown group). The known ones leave in one logged `GroupRemove` (reason leave), each once. A refused write gives its code to the known ones. | C TestLeaveRebalances | Leave |
| 27 | After members are removed: no member left is Empty; Joining may now be done; Stable or Syncing opens a round. Their parked calls get ErrUnknownMember. | C TestLeaveRebalances, C TestSessionExpiryRemovesSilentMember | Leave, TimerFired |
| 28 | A member whose session timer fires is removed (logged `GroupRemove`, reason expire), then rule 27, unless rule 30 keeps it. | C TestSessionExpiryRemovesSilentMember | TimerFired |
| 29 | CHANGED. The session starts again on: a heartbeat that passes rule 18; a commit that passes rule 31 (then the group is PreparingRebalance or Stable); the answer of a parked join or sync; an abandoned parked call (rule 33); takeover (rule 35). A new member keeps its new member timer (rule 30). | C TestSessionExpiryRemovesSilentMember; commit: code | all calls |
| 30 | CHANGED (Kafka `hasSatisfiedHeartbeat`). An existing member with a parked join or sync is not removed when its session timer fires; the timer starts again when the call ends. A new member (joined with MemberID "", until its first done round) has the new member timeout (5 min) instead of its session, and is removed when it fires even with a parked join, which gets ErrUnknownMember. | none | TimerFired |
| 31 | CHANGED. Offset commit, Kafka's `validateOffsetCommit` on memory first: on a group with no members a Generation < 0 is not checked; else an unknown member (also "") is ErrUnknownMember, another generation is ErrIllegalGeneration, and CompletingRebalance is ErrRebalanceInProgress. A group the machine does not have is not checked. Then it is a write: the log checks again at apply time (plan rule 2) and gives one code per offset. It does not queue. A failed write is ErrUnavailable and does not end the term. | R TestOffsetCommitChecks | OffsetCommit |
| 32 | CHANGED. Describe: the log's description (LocalRead) with State from memory: Empty, PreparingRebalance, CompletingRebalance, Stable; Dead for a group the machine does not have. List: every group in memory with its protocol type and state. OffsetFetch is not a machine call: the handler reads the log; its rules are `domain/group`'s. | R TestOffsetCommitChecks, C TestOneMemberJoinsAndSyncs; fetch: `domain/group` tests, `spec_test.go` | Describe, List |
| 33 | An abandoned parked join or sync gets no reply and is forgotten. The member stays, but it no longer counts as joined. | W TestJoinCtxEndsWhileParked, W TestJoinCanceledThenRoundCompletes, R TestLeaderNeverSyncs | CallAbandoned |
| 34 | Losing the role: every parked call gets ErrUnavailable, every timer is cancelled, memory is dropped. | R TestCloseWakesParkedCalls | RoleChanged |
| 35 | Taking the role: every group is read from the log. Each member's session starts now; no member is new or owes a sync. Stable if the assigned generation is the generation and the leader is a member; Empty with no member; else a round from now, with no delay. Join order: join time, then id. | R TestRebuildKeepsMembersAndExpiresSilent | RoleChanged, ReplicaReply |
| 36 | A write that may have changed the log (failed, or its write timer fired) ends the term (section 8). | R TestStoreFailureBreaksTermAndRebuildRecovers | ReplicaReply, TimerFired |
| 37 | Many joins at the same time make one round with one leader. For the machine these are just inputs in order. | R TestConcurrentJoinsOneRound | Join |
| 38 | NEW (Q3). Each SubmitCommand has a write timer (10 s). If it fires before the reply, rule 36. | none | TimerFired |
| 39 | NEW (Q5). In Loading, a load read answered ErrUnavailable is sent again after 100 ms. | none | TimerFired / Loading |

The model checks rules 1 to 35. It leaves out 36, 38 and 39 (it has no faults or requests), has
nothing to check in 37, and checks OffsetFetch of rule 32 only through its Store. Rule 34 "every timer
is cancelled" and the queue of section 8 are machine details.

## 6. Changes from today's tested behaviour

These follow Kafka on purpose. T6.2 ports today's tests with these changes:

- Rule 25: C TestHeartbeatTable, case "stable and right" (the group is in CompletingRebalance): OK, not ErrRebalanceInProgress; the comment "a heartbeat is refused" goes.
- Rule 24: R TestLeaderNeverSyncs: at the sync deadline the leader that never synced is removed, so its rejoin (`h.rejoin("g", a)`) gets ErrUnknownMember and the second round has one member.
- Rule 31: R TestOffsetCommitChecks: `commit("", -1)` on a group with members is ErrUnknownMember, not ErrIllegalGeneration. New: a commit in CompletingRebalance is ErrRebalanceInProgress.
- Rules 22, 23, 29, 30, 32 (session details; Describe and List states, where the log said PreparingSync): no test today; the port adds them.

## 7. Timers

Each timer has an ID and a Gen, both in `TimerFired`. The machine ignores a Gen that is not current:
in `live`, a timer that fired just before its `CancelTimer` is already in the inbox.

- Session: one per member. An existing member: After = its session timeout, set again by rule 29.
  When it fires for a member with a parked join or sync, nothing happens (rule 30). A new member:
  After = the new member timeout (5 min, Kafka `classicGroupNewMemberJoinTimeoutMs`), set once at its
  join; it fires even while the join is parked. Cancelled when the member is removed.
- Join (round end): one per group in Joining. Set when a round opens, moved later by rule 10. When
  it fires: rule 12, then the round is done.
- Initial delay: one per group in an initial round. Cancelled when the round ends.
- Sync: one per group from the install. Same ID slot as the join timer: a group has one or the other.
  Cancelled when every member is synced or a round opens. When it fires: rule 24.
- Write: one per in-flight SubmitCommand, 10 s. Cancelled by its ReplicaReply. When it fires: rule 36.
- Load retry: 100 ms, in Loading only, after a load read is refused (rule 39).

Simplified against Kafka: the round end moves later for a later member with a longer rebalance
timeout (rule 10; Kafka sets it once when the round opens); an abandoned call starts the session again
(Kafka cannot see a client go away and keeps the call). All timers are cancelled on role loss and on
a broken term.

## 8. Waiting for a log write

- A group with a `SubmitCommand` in flight is busy. Every later input for that group (calls,
  `TimerFired` of its timers, `CallAbandoned` of its calls) waits in a queue, in order. This is what
  the group mutex does today. An offset commit does not wait (rule 31): it uses memory as it is.
- On the `ReplicaReply` the machine applies the answer, then takes the queued inputs one by one. An
  input that submits a new write stops the drain until its own reply.
- An abandoned call that waits in the queue is taken out with no reply. If its write is in flight,
  the answer of the write is still applied; only the reply is dropped.
- `List`, `Describe`, `RoleChanged` and inputs for other groups do not wait.
- Settle. OK: apply. ErrTooLarge or ErrInvalid: nothing was written; the caller gets the code and the
  term goes on. Anything else (ErrUnavailable, ErrTimeout, ErrIO, the write timer, and also a business
  code such as ErrIllegalGeneration, which shows memory and log disagree) may have changed the log or
  shows they differ: the term ends. The caller, every parked call and every queued call get
  ErrUnavailable, every timer is cancelled, memory is dropped, and the machine goes to `Loading` and
  reads every group with `SubmitRead{Fresh: true}` (a ControlPlaneDeliver read, after a fresh tail).
- A fresh read narrows the gap but does not close it: a write still queued in the replica can apply
  after the read. Installs stay safe (the log checks generation + 1); a late `GroupRemove` or
  `GroupAssign` is seen at the next term. Today has the same gap.
- Today's 1 s retry of a failed expiry write (`retryAfter`) never runs (`settle` already broke the
  term). The machine drops it.

## 9. Term and role

- The host sends `RoleChanged{Active}` only when "active appender and Ready" changes.
- Active false: go to `Standby` (rule 34). A late `ReplicaReply` has an unknown Req and is ignored
  (request ids never repeat in one run). ErrUnavailable tells the client to try the next address.
- Active true: go to `Loading`: `SubmitRead(ListGroups, Fresh: false)`, then one
  `SubmitRead(DescribeGroup)` per group. A read refused with ErrUnavailable is sent again after 100 ms
  (rule 39). When all are done, go to `Serving` (rule 35).
- A broken term (section 8) goes to `Loading` too, with fresh reads, without waiting for a role change.

## 10. Time, ids and randomness

- `cx.Now()` (monotonic) for every timer; `cx.Wall()` in milliseconds for `TimeMs` in logged commands.
- A new member id is ClientID, a dash, and 16 bytes from `cx.Rand()` as base32 text (today:
  `crypto/rand.Text`). In `live` the stream is seeded from the OS once. Ids are not secrets.
- Call ids come from the host; request and timer ids from counters in the machine state.

## 11. Hosting

Production (`replica` and `live` do not change):

- `server.Node` starts a second `live.Run` with the machine, its own inbox and the same clock.
- Its `Exec`: `SubmitCommand` becomes `ControlPlaneDeliver{From: rebalance, Req, Op}` to the replica
  inbox; `SubmitRead` becomes `LocalRead{From: rebalance, Req, Op}`, or `ControlPlaneDeliver` when
  Fresh; `Reply` goes to the RPC that waits.
- The replica answers with `ControlPlaneReply{To: rebalance}`. `Node.reply` delivers it to the
  rebalance inbox as `ReplicaReply`. `Node.reply` must look at `To` first: today it deletes
  `waiters[Req]` before it checks `To`, and the two processes count request ids apart.
- The replica loop's `AfterStep` sends `RoleChanged` when `IsActive() && Ready()` changes, and keeps
  that bit for the OffsetFetch handler. This replaces `watchGroups` (the 100 ms poll), `nodeGroups`
  and `groupRole`.
- A gRPC handler gives the call an id, delivers the input and waits for the Reply. When its context
  ends it delivers `CallAbandoned` and answers ErrTimeout with MaybeApplied, as today.
- OffsetFetch: the handler answers ErrUnavailable when the role bit is off, else does a LocalRead.

Simulator: each node gets a second dst process that crashes and restarts with the replica. `simNode`
sends `RoleChanged` after each replica step; a small wrapper turns submits into sends to the replica
and routes replies with `To: rebalance` back.

## 12. The model

`spec.Rebalance.Step(now, input) []Reply`. Inputs: `Join`, `Sync`, `Heartbeat`, `Leave`, `Commit`,
`Describe` (one group), `List`, `Abandon`, `RoleChanged`. OffsetFetch is not an input (Q6); the
generic store call is gone. Deadlines up to `now` fire first, earliest first: initial delay, round
end, sync deadline, sessions (new members with `NewMemberTimeout`, default 5 min). The logged half of
the state is kept in a `spec.ControlPlane` (the metadata model), which also gives Describe its log
part and checks commits again. No faults, no queue, no timers or requests: those are how the machine
gets there, not what it answers. A conformance test (T6.3) sends both the same inputs and matches
replies by call id, because the order inside one step may differ. It should not draw two deadlines
at the same instant, or accept either order.

Size: 607 lines, up from 559: the store call and OffsetFetch went, rules 24, 30, 31 and 32 came. I
could not get back to 559 without dropping one of these rules.

## 13. Left for later

Every difference from Kafka, with its status, is listed in `docs/kafka-differences.md`.

- T6.2: skip the `GroupRemove` write when none of the removed members is in the log (new members
  only); Kafka writes nothing for them. The model writes it and the log ignores unknown members.
- T6.2: Describe of many names: one LocalRead per name, one Reply when all are back.
- T6.4: the log layer still names its state PreparingSync; rename or keep it (the machine replaces it).
- T6.5: the oracle must allow a late write after a fresh re-read (section 8).
- Kafka differences found but not decided (today's rule kept; the user decides):
  rule 9 (Kafka answers an unchanged follower's rejoin in Stable or CompletingRebalance at once, with
  no round); rule 13 (Kafka raises the generation when a
  round ends with no member); rules 14, 15 (Kafka votes on the protocol, and refuses at once a join
  whose protocols do not fit the group); rule 21 (Kafka does not check the assignment); rule 31, log
  part (Kafka takes any Generation < 0, with or without a member id, on an Empty group; an unknown
  group with Generation >= 0 is ILLEGAL_GENERATION or GROUP_ID_NOT_FOUND, here ErrUnknownMember; a
  manual commit makes a Kafka "simple group" shown as Empty, here Dead and not listed); rule 32
  (Kafka's Describe hides protocol, metadata and assignment outside Stable, and shows the members in
  memory). Not done at all: MEMBER_ID_REQUIRED (KIP-394), static membership, SyncGroup's protocol check.
