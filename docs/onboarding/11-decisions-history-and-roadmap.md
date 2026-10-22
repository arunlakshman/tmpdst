# The design changed its mind in public, and two verbs tell you what still holds

You can understand the code without its history. You cannot change the code in safety without that history. You need to know which ideas the team tried, measured, superseded, or left as a proposal.

Two verbs run this file. Supersede and refine.

A later decision supersedes an earlier decision when the later decision removes the earlier one. You must not implement the earlier decision. A later decision refines an earlier decision when the later decision narrows the earlier one. The earlier decision stands except where the later decision narrows it.

Keeping superseded decisions in the repo is right. A superseded decision records the constraint that forced the turn. The new decision alone does not record that constraint. The cost is small. You check for "Superseded" and "Refines" before you quote a row.

`docs/` holds records with different authority. A decision is not a benchmark result. A proposal is not a plan. A dated audit is not a description of today's code.

## 1. A later decision has more authority than the earlier text it names

If you need a rule, you read in this order:

- For a rule that must never change, read `docs/decisions.md` first. Read the topic doc after the decision. The topic docs include `docs/log-cp-service.md` and `docs/flow-control.md`.
- For a measured claim, read the results docs. Read the measured, modelled, and computed labels in `docs/surprises-audit.md`.
- For work that the team accepted but did not build, read `docs/TODO.md`. Read only `docs/TODO.md` for that question.
- For an idea only, check the headers. The headers say PROPOSAL, IDEAS, or "proposal only, nothing applied". The headers appear in `docs/commit-path-redesign.md`, `docs/delos-logdrive-ideas.md`, and `docs/refactoring-proposal.md`.
- For AWS, treat the plan as intent. Treat the results as fact. Compare `docs/stage5-run-plan.md` with `docs/w4-log-store-results.md`.

A later decision has more authority than the earlier text it names. The later decision says "Supersedes", "Refines", or "Under 41-44". The earlier text stays for its reasoning. Dates and `Status:` lines matter more than confident prose.

## 2. A row in `docs/decisions.md` holds only where no later row limits it

`docs/decisions.md` is one table. The table has rows numbered 1-53. The table also has lettered rows A, C, D, F-J, and S from the earliest sessions. Rows 7-22 came from building the predecessor system toys3. `docs/interface-changes-from-toys3.md` expands the evidence for rows 7-22.

You read a row in this way:

- The row is terse by design. The justification lives in the topic doc or commit that the row cites. Follow those references before you argue with the row.
- "Accepted" means the decision is current. Row 28 says "Superseded by 41 and 44". Row 34 says "Superseded by 44". You must not implement from row 28 or row 34.
- "Refines" means the earlier row stands except where the later row narrows it. Row 48 refines 46. Row 50 refines 39 and 42. Row 53 refines 48 and 51.
- Rows 35 and 39 carry notes that say "Under 41-44 ...". The notes redo old arithmetic after a contract change. The original numbers are historical.
- Some rows record staged acceptance. Row 52 sets the partition cap at 4096. Row 52 says that no hash changes in the stage that records it. Row 52 also says the work was "Done in stage 3". Acceptance and landing are different events.

### The ten decisions that set the current architecture

Read these ten rows in full. Together the ten rows describe the current architecture. Most other rows refine these ten rows.

| # | Decision, in one line | Why the decision sets the architecture |
|---|---|---|
| I | Deterministic packages `core/` and `sim/` use an exact-path import allow-list, fail closed, and `tools/detcheck` enforces the list. A third-party module needs a record in `docs/dep-audits/`. | The simulator can act as the oracle because decision I holds. Every dependency question starts here. |
| 21 | `veritas/` holds executable specs. The specs double as test fakes. Conformance C1-C4 checks the specs. The oracle never imports veritas. | Two independent implementations agree. That agreement is the main correctness argument in the project. |
| 35 | The segment flush rule seals at 16 MiB or a 200 ms fixed window. Partition, batch, and item caps also seal a segment. `sys/prod` derives the buffer cap from memory at runtime. | Decision 35 sizes the whole produce path. Later rows 39 and 52 only move the caps. |
| 41 | The system has no idempotent producers. Delivery is at-least-once. Clients must disable idempotence. A retried commit cannot duplicate. A retried produce can duplicate. | Decision 41 supersedes the producer half of decision 28. A case for producer dedupe reopens decision 41. |
| 42 | The system has no high-watermark item. The CP derives the next offset for a partition from its newest run `[Base, Base+Count)`. The CP assigns offsets itself. Compare-and-set is a store detail. | The invariant "contiguous, never reused" rests on decision 42. The invariant does not rest on broker honesty. |
| 44 | The system has no broker fencing. Brokers are stateless. A zombie broker cannot harm the system through a commit because decision 42 assigns the offsets. | Decision 44 supersedes decision 34 and the epoch parts of decision 28. Decision 44 deletes `Register`, epochs, and the fencing `ErrFenced`. |
| 45 | The broker stores and serves compression as opaque bytes. The broker validates the header only. The header checks cover CRC and codec id 0-4. The broker never decompresses. | Decision 45 keeps codec libraries out of the deterministic packages. Decision I still holds. |
| 48 | Design A is the production design. A DynamoDB log holds raw `CommitSegment` commands. Replicas apply the commands deterministically. All commits go to one CP node. Sticky routing sends the commits. K=4 appends stay in flight. | Decision 48 is the current control plane. Decision 48 refines decision 46. Decision 48 replaces the per-partition run-item CAS with the if-absent write on the log slot. |
| 49 | The production state machine is not veritas. The production state machine is an embedded-KV applier in `cp/applier`. Pebble backs the applier. Lockstep tests check the applier against veritas. The tests use 100,000 seeds in `docs/applier-bakeoff.md`. | Decision 49 keeps the spec separate and the production store fast. Veritas keeps every run forever. Veritas would use about 6 GB per hour at the target rate. Veritas cannot act as the store for that reason. |
| 53 | Log entries start with a format-version byte. An undecodable entry stops a node. The node returns 503 and never skips the entry. The idempotency window is a function of slot number. `WindowSlots` is 131,072. The window is identical on all replicas. Trimming leaves tombstones. | Decision 53 gives the upgrade story. Decision 53 also explains why replicas cannot fork when the system forgets a key. |

Other rows still matter. Row 3 sets a separate CP that assigns offsets. Row 3 also makes commit idempotent per segment key. Row 29 sets leaderless metadata. Row 46 sets a single sequencer. Decision 48 refines row 46. Row 51 sets a hot standby. Row 51 says the log slot is the fence. Row 51 says a lease is not the fence.

## 3. Measurements forced the large turns, and the dates show the order

The repository has a compressed recorded history. About three weeks of commits carry two systems. The commits carry toys3 first and manifester after toys3. The phases are approximate.

The timeline has eight dated steps:

| When | What happened | Evidence |
|---|---|---|
| 2026-10-05 | The team built the core interface skeleton, the determinism gate, dependency-audit tooling, and Go 1.24. | commits `1f762c6`, `011144f` |
| 2026-10-12-15 | The team built toys3 end to end as a learning system. The system had a simulator, an oracle, veritas specs, and mutants. The team added a porcupine cross-check and then reverted the cross-check in `9cc93a1`. | toys3 commits and `docs/interface-changes-from-toys3.md` |
| 2026-10-16 | Milestone 1 landed the simulator kernel. The Kafka scenario, the broker, and the CP service followed. The CP service had idempotent producers under decisions 28-31. The team imported decisions 7-22 from toys3. | `17ca3db`, `918eac0`, `176d541`, `af422bd` |
| 2026-10-17-18 | The team built production wiring in `sys/`, `cmd/`, and `it/`. The first AWS run r1 followed. Decision 35 set the segment flush rule. Decisions 36-38 set metrics-as-facts. Decision 39 set CP range compaction. Decision 40 set the CRAP gate. The team discussed the DDD proposal. | `44d41e5`, `b418b0b`, `d7e14a9` |
| 2026-10-18 | Decisions 41-44 made the stateless turn. Decisions 41-44 removed idempotent producers, the high-watermark item, and broker fencing. Decisions 41-44 marked 28 and 34 superseded. The team wrote the second edition of the refactoring proposal. | `04ca77d`, `22958b1`, `fe9afae` |
| 2026-10-19 | Decision 45 added compression. Decision 46 gave the control plane sequencing. The team chose single-CP-host deployment. The team wrote the surprises audit before the next benchmark. | `79f2b21`, `2f75286`, `1a11024` |
| 2026-10-20 | Runs w2 and w3 showed the transaction store reaching only about 240-275k msg per second. The commit-path redesign proposed design L. The Delos and LogDrive ideas proposed design A. The team prototyped A in `cp/logsm`. The team measured A in X1-X5. Decisions 48-53 adopted A. The bake-off chose Pebble. The team built the Log CP service in stage 2. The team built the sim log engine in stage 3. | `aab17c0`, `fcd6228`, `bd6b64d`, `5b2b90c`, `ae027ec`, `392d372`, `c301a20` |
| 2026-10-21 | The team wired the Log CP as `manicp -store dynamolog` in stage 4. The team wrote the stage-5 plan. Run w4 then executed and the team reported run w4. The team wrote the roadmap and the OSS-readiness doc. The team added sample DynamoDB rows. | `8bd349e`, `d4dd5d5`, `513bc67`, `c6da4c5`, `df0e176` |

A measurement forced each large turn. The team built the idempotent version first. Decisions 41-44 came after that build. Run w2 came before decision 48. Several docs describe systems that existed for a day. If you cite a doc, check `git log -- docs/<file>` first.

## 4. `docs/lessons.md` gives the shortest reason for each odd contract

`docs/lessons.md` adapts the Moonpool Sim Book to this Go codebase. The Moonpool Sim Book draws on FoundationDB, TigerBeetle, and Antithesis practice. The lessons doc gives the shortest explanation of why the contracts look odd.

You will cite these sections most:

- Sections 0-1 cover Go scheduler limits. You cannot swap the scheduler in Go. The Action and Event vocabulary is the provider interface for that reason. The codebase uses sans-I/O state machines for that reason. Every completion carries an `Err` and an `Outcome`. The `Outcome` is `NotApplied`, `MaybeApplied`, or `Applied`. The code acks producers only on `Applied`. `CommitSegment` must be idempotent per segment key for that reason.
- Sections 2-3 cover time and random numbers. Two clocks sit on the event. The clocks are `At` and `Wall`. The core has no `Now()` call. One counted RNG stream exists. Derived streams serve the swarm knobs. Decision 17 superseded the "same stream" wording in §9.
- Sections 5-6 cover crashes and network faults. Crashes resolve pending results one by one. A landed PUT with a lost response stands. Network faults act at connection level. Kafka framing runs inside the deterministic path for that reason.
- Sections 8-11 and 13 cover runs and checks. The sections describe run phases. The phases are chaos, recovery, quiet tail, settle, and check. The sections describe swarm testing that can switch fault families fully off. The sections describe invariants that read a fact timeline. The sections describe control-before-data backpressure. `docs/flow-control.md` expands the backpressure part.

Section 4 is historical. Section 4 covers incarnations and fencing. Section 4 opens with "Moot since decisions 41-44". If broker membership returns, you may revisit section 4. Broker membership is a `TODO.md` item today. Do not design fencing from section 4 today.

## 5. An S-id names one claim in the audit, and the audit froze on 2026-10-19

The audit is `docs/surprises-audit.md`. The audit is dated 2026-10-19. The team wrote the audit against branch `toys3` at `4854ad7`. The team was still writing the decision-46 sequencer at that time. The audit ranks 19 surprises, S1-S19, by severity. The severity values are BLOCKER, HIGH, MEDIUM, LOW, and KNOWN. Each item has an evidence label. The labels are verified by experiment, verified by reading code, computed, and suspected. Experiments ran on local setups only. Experiments used fakedb and loopback. Section 4 of the audit lists what the audit could not check. The list covers real 97-item transaction latency, regional quotas, and hot-key behaviour.

When someone cites "S3", the person cites one specific claim. A test can prove the claim wrong.

The audit makes these claims:

- S1 says every segment overlaps every other segment at 288 partitions. The single sequencer can neither pipeline nor merge groups for that reason. The bound is one segment per transaction. The bound is about 430-600k msg per second. The bound is not the imagined group rate.
- S3 says Fetch never waits. One idle consumer made about 20.6k CP Lookups per second on a local setup. The audit predicted about 48k per second for 64 consumers. S3 is still open. See `TODO.md`.
- S6 says the code makes one ranged S3 GET per batch. The rate is about 6.5k GET per second at 1 GiB per second read. That rate sits above the per-prefix starting rate. S6 is still open.
- S7 says a 97-item commit costs about 240 WRU. The cost is about $59 per hour at the prices then used. Section 2 of `docs/commit-path-redesign.md` corrects the price basis.
- S8 says a CP outage over about 0.6 s failed every in-flight produce. The code used 6 retries and 20-320 ms backoff. Decision 47 fixed S8. Decision 47 changed the code to retry until a 120 s deadline.
- S13 says the system never deletes anything. The 3-day bucket lifecycle can delete `seg/` objects that the index still points to. S13 is still open. S13 is roadmap item 2.
- S19 is marked KNOWN. S19 says the system has no consumer groups. S19 says the system has no transactions, SASL, TLS, or retention. S19 lists those absences as known facts.

Status varies by id. The team fixed S2 and S4 within days. The fixes were sequencer wiring and `sys/httppool` at 256 idle connections under decision 47. Other items ended without a direct fix. The team declined the partition affinity that S1 proposed. See `docs/TODO.md`. The log control plane in decisions 48-53 then removed the problem. Run w4 measured that system. Pair an S-id with `docs/TODO.md` and the decisions at all times.

## 6. Design A superseded design L, and the three debate docs keep distinct jobs

This debate shows the layers of the library in one place. Learn to tell the three documents apart.

The three documents have distinct jobs:

- `docs/commit-path-redesign.md` is a proposal dated 2026-10-20. The proposal starts from w2. Run w2 showed transaction latency at about 8 + 0.7 ms per item. Run w2 showed one transaction in flight. The plateau was about 240k msg per second. The proposal rejects C and keeps B as fallback. The proposal recommends design L. Design L used log-slot CAS in a small transaction. Design L used `G#` items that held computed bases, an asynchronous index, and active-only `Lookup`. The team did not build L as proposed.
- `docs/delos-logdrive-ideas.md` holds ideas from the same day. The doc reads Delos from OSDI 2020 against design L. The doc reads LogDrive and Conflux from OSDI 2026 against design L. Idea A logs the commands and not the results. Every replica is a learner. Offsets exist only at apply time. Any replica can serve a fresh `Lookup`. The design needs no permanent `G#` items. Idea B moves the index out of DynamoDB into S3 checkpoints. Idea B is now the unscheduled S3-index item in `docs/TODO.md`. Ideas C and D cover the Loglet seam and striping. Ideas C and D are insurance. The team did not build ideas C and D.
- `docs/design-a-prototype.md` prototyped A in `cp/logsm`. Test X2 ran 100,000 seeded interleavings with 0 failures. Decisions 48-53 adopted A. Adoption carried the modifications in `docs/log-cp-service.md`. The modifications appear under "Findings that change earlier text". The modifications are a deterministic slot-based idempotency window and tombstone trimming. Seeded tests found both points.

Parts of design L survive in the built system. The asynchronous `R#` index survives. The idea of a log slot as the CAS survives. If a paragraph in the redesign doc conflicts with decisions 48-53, decisions 48-53 have more authority. If a paragraph conflicts with `docs/log-cp-service.md`, `docs/log-cp-service.md` has more authority.

## 7. The current design docs describe stages 2-4 as built

`docs/log-cp-service.md` describes the log-backed control plane as built in stages 2-4. The service uses sticky routing to one appender. The service keeps K=4 appends in flight. A slot is one `LOG/<slot>` item. The service writes the item with `attribute_not_exists`. Replicas tail the log. Replicas apply entries through `cp/applier`. Pebble backs `cp/applier`. A replica answers only after the replica has applied its slot. Standbys serve fresh reads. Standbys answer commits with 503, `Retry-After`, and `X-CP-Role`. Checkpoints go to S3 at `ckpt/<slot>`. Trimming overwrites slots with tombstones. The `R#` index flushes every 10 s under decision 50. Nothing in the service reads the `R#` index yet. The index exists for rollback and a future S3 index. The "NOT done" list in that doc is dated. The list records no AWS run at writing time, no S3 index, and no streaming checkpoints. Stage 5 has since run as w4.

`docs/sim-log-engine.md` describes the matching simulator at `sim/engines/logengine`. Run the simulator with `go run ./cmd/manisim -scenario kafka-log`. The simulator models two replicas, holes, and hole-filling. The simulator models takeover, failback, tombstone trim, and checkpoint restore. The simulator scales slot counts down. A 45 s run crosses the scaled counts for that reason. Four mutants exist. The mutants are `BugWriterOffsets`, `BugTrimDelete`, `BugPerReplicaWindow`, and `BugHoleSkip`. Tests catch each mutant within 6 seeds. A run of 2,000 seeds and 50.7 M steps passes. The Limits section lists what the simulator does not model. The list covers DynamoDB throttling, a third replica, and the `R#` index. Tests in `cp/logcp` cover those points.

`docs/applier-bakeoff.md` holds the measurement behind decision 49. The bake-off compares the same applier on bbolt and Pebble. The bake-off uses 150-partition segments over 10-20k preloaded segments. Pebble ran 1,100-1,330 segments per second. Pebble used about 2.8x less memory. Pebble used half the disk. Bbolt ran 300-430 per second. The team removed bbolt. Use this doc as the model for arguing a dependency. The doc states the workload. The doc states a target above 430 per second. The doc reports results. The doc states dependency cost. The cost was 17 modules at edge only. A record sits at `docs/dep-audits/github.com/cockroachdb/pebble@v1.1.5.md`. The doc also gives lockstep evidence and a "Not done" list.

`docs/flow-control.md` is older and still current. The header says "design agreed, interfaces applied". The doc sets two inboxes per node. The control inbox has strict priority. Bounds on the control inbox come from its construction. The data inbox is bounded. The data inbox keeps one ordered stream per connection. Throttling sits inside `Step`. Soft throttling uses `ThrottleTimeMs` and mute under KIP-219. Hard throttling pauses all reads. The design uses hysteresis. The design never drops an admitted request. Production and the simulator share the pure `core.InboxPolicy`. The doc also records a mutation check. The check swaps in one FIFO queue. The check requires seeds to go red.

## 8. Toys3 supplied the evidence, and the later proposals mostly stayed unbuilt

`docs/interface-changes-from-toys3.md` holds the evidence for decisions 7-22.

The evidence covers these decisions:

- Decision 7 covers `Trip`.
- Decision 8 covers `Stop` and `Exit`.
- Decision 9 says `Machine` includes `Hashable`.
- Decision 10 sets typed I/O.
- Decision 11 sets contract rules.
- Decision 12 says facts are hash inputs.
- Decision 13 says invariants see a `View`.
- Decision 14 says the system draws faults at issue time.
- Decision 15 separates history and the oracle.
- Decision 16 sets per-seed results and golden files.
- Decision 17 sets derived RNG streams.
- Decision 18 sets scheduler order `(At, Tie, Seq)`.
- Decisions 19-20 split `sys`.
- Decision 21 sets `veritas/`.
- Decision 22 sets `format/`.

The header says "accepted, not yet applied". That statement was true when the team wrote the doc. The changes landed on 2026-10-16. The commits are `00241cb`-`1d147fa`. Read the doc for provenance and not for status. Section 6 rejects porcupine as checker of record. Porcupine verdicts depend on wall-clock time. Section 6 also bars the oracle from importing veritas.

`docs/ddd-refactoring-proposal.md` proposed a glossary and value objects. The value objects were `SegmentKey`, `OffsetRange`, and a `Verdict`. The `Verdict` would replace `Base == -1`. The proposal also proposed broker aggregate extraction. The proposal also proposed fact-name constants. The proposal also proposed a split of the overloaded `ErrFenced`. Decision 34 adopted only item 7, option A, on typed accessors. Decision 34 states plainly, "The rest of the DDD proposal is deferred." Decisions 41-44 then removed fencing. Even item 7 lost its subject after decisions 41-44.

`docs/refactoring-proposal.md` is in its second edition. The second edition supersedes the open items in the DDD doc. The second edition measures the open items again. The broker had grown to 1,949 lines. Fact-name literals had spread to 18 files. The header says "proposal only, nothing applied".

The deferred work comes from its waves and §7:

- Typed fact and metric names with contract tests sit under Q3 and Q4.
- Extraction of the segment commit pipeline behind a `commitSched` seam sits under S1.
- Shared `cmd` wiring and one effective-config description sit under Q5 and Q6.
- `core.Hasher` unification sits under Q7.
- Broker renames sit under Q2.
- Kafka error translation in one place sits under S2.
- Value objects sit under S4. This item sits furthest out.
- Non-serial commit scheduling sits under S3. This item also sits furthest out.

Section 5, "What NOT to do", still binds. Section 5 bars a broker sub-package split. Section 5 bars a shared idempotence-rules package. Section 5 bars renaming facts or enum order. Section 5 bars Prometheus in `broker/`. The sequencing assumed "range compaction in flight". That work landed as decision 39. If you revive an item, check the premises again first.

## 9. `docs/TODO.md` alone lists work that the team accepted but did not build

`docs/TODO.md` is the only doc whose job is unfinished work.

Current items in `docs/TODO.md` are these items:

- The consumer read path comes before any consume stage. The read path needs long-poll Fetch that honours `MinBytes` and `MaxWait`. The read path needs a Lookup cache. The read path needs an S3 read cache with coalescing. Consumers sit at the tail. This item turns surprises S3, S5, and S6 into a plan. This item is roadmap item 1.
- Metadata lists every live broker. This work needs new heartbeat-lease and `DescribeBrokers` ops. The lease feeds Metadata only. The lease fences nothing under decision 44.
- The index moves to S3 as a follow-up to decisions 48-50. Checkpoints and immutable S3 index files take over. The system trims the log behind the checkpoints and files. DynamoDB `R#` goes.
- Go 1.26 and current franz-go come in as planned upgrades. The upgrade needs a new audit of kmsg under decision J.

Idempotent producers are not planned. The doc says so in plain terms. The team declined per-broker partition ownership. Per-broker partition ownership would raise the sequencer limit. Per-broker partition ownership breaks leaderless metadata under decision 29.

Done items stay marked done. Compression is decision 45. Range compaction is decision 39. Check the markers. Do not rely on the headings alone.

## 10. Results supersede the plan that produced them, and the legal caution blocks the teardown as a source

`docs/stage5-run-plan.md` is dated 2026-10-21. The plan said "NOT executed" when the team wrote the plan. The plan set w4 at 500k, 750k, and 1M msg per second. The plan set random clients, 288 partitions, and produce only. The plan set a log CP plus hot standby. The plan also set a kill-the-appender stage at 500k. The stage had an ack-gap budget of 15 s. The plan records two choices from the product owner. The project dropped 4096 partitions. The project deferred consumers until the TODO read path exists. The run then happened. `docs/w4-log-store-results.md` reports about 998k msg per second for 4 minutes at p99 756 ms. The results doc reports an about 8 s ack gap on the kill. Stages ran 1-4 minutes with one sample each. Those numbers are indicative and not steady-state. Where the plan and the results differ, the results have more authority. The differences cover stage lengths and the missed "<10 s" index-lag bar.

`docs/roadmap-from-warpstream-teardown.md` is dated 2026-10-21. The roadmap ranks five takeaways.

The roadmap ranks the work in this order:

- Consumer read path comes first, at about 20 engineer-days. The work covers long-poll, Lookup cache, coalesced GETs, and chunk cache.
- Retention, orphan GC, and bucket preflight come second, at about 10-12 engineer-days. The system deletes nothing today. The 3-day lifecycle rule is a correctness hazard.
- Broker membership, AZ hints, and drain come third, at about 10-13 engineer-days. The work includes a 1M failover test. This item revisits decision 29.
- Consumer groups, offsets, and minimal admin APIs come fourth, at about 35-45 engineer-days. The work comes from the Kafka protocol spec.
- Compaction of per-partition slices comes fifth, at about 30 engineer-days after the S3 index. One partition-hour is about 40k small slices today.

TLS and SASL need about 10 engineer-days before any serious OSS announcement. Section 3 is an OSS-readiness checklist. The checklist lists BLOCKER items. The items are no root LICENSE and no root README. The checklist lists a secrets scan and a timeline. The timeline puts a quiet `v0.1.0-alpha.1` before any blog post. The timeline says, "If any gate slips, slip the date".

### The legal caution blocks any use of the teardown until a lawyer clears the use

Section 0 of the roadmap is not legal advice. This chapter is not legal advice either. You must not miss section 0. The teardown document sits at `docs/arun-warpstream-agent-reverse-engineering.md`. The teardown document is untracked and not committed. Someone produced the teardown by extracting protobuf descriptors from a vendor binary. Someone extracted symbol tables and strings from the vendor binary and ran the binary. The roadmap says that work is reverse engineering in the ordinary sense. The roadmap says nobody has read the applicable agent licence. Nobody has read the Confluent and IBM terms. The roadmap says the public-sources study in this project treats reverse engineering as presumptively prohibited. Shipping code visibly modelled on the teardown could create legal exposure. Publishing derived material could also create legal exposure. The exposure covers contract, trade secret, and credibility. Get legal advice before the project publishes anything derived from the teardown. Keep the teardown out of the OSS repo. Keep anything copied from the teardown out of the OSS repo. Keep the teardown and copied material out of git history. Implement the roadmap clean-room. Use public docs, the Kafka protocol spec, AWS guidance, and our own measurements. Add `docs/arun-*` to `.git/info/exclude`. Never run `git add -A` in this area.

## 11. Superseded decisions stay in the repo, and you must not follow the superseded text

Keep this list next to you for your first month. Each item was once the design. If you follow a superseded item now, you reproduce removed behaviour.

The superseded list is this list:

- Decisions 28 and 34 covered idempotent producers and broker and producer fencing. Decisions 41-44 superseded decisions 28 and 34. The current code has no `InitProducer`. The current code has no epochs. The current code has no `ErrFenced`.
- Decision 46 set a single `Sequencer` over the transaction store as the production design. Decision 48 refined decision 46 into the log appender. The group-commit measurements from decision 46 remain useful evidence.
- `docs/commit-path-redesign.md` offered design L as a build plan. Decisions 48-53 superseded design L with adopted design A. `docs/log-cp-service.md` also supersedes design L.
- `docs/design-a-prototype.md` says in its own status line that the prototype has "no `-store` kind, not wired". Stages 2-4 overtook that line. `manicp -store dynamolog` exists.
- `docs/interface-changes-from-toys3.md` says decisions 7-22 are "not yet applied". The decisions landed on 2026-10-16.
- `docs/refactoring-proposal.md` superseded the open items in `docs/ddd-refactoring-proposal.md`. Decision 34 deferred almost all of those items.
- `docs/lessons.md` §4 covers incarnation fencing. Decisions 41-44 made §4 moot. Decision 17 superseded the "same stream" wording in §9.
- The old DynamoDB transaction-store numbers are historical. This group covers decisions 32, 35, and 39 item arithmetic. This group covers the w2 and w3 plateaus of about 240-275k msg per second. The "~$43-59k/month" DynamoDB figures use the pre-cut $1.25/M WRU price. `docs/commit-path-redesign.md` §2 corrects this price.
- `docs/surprises-audit.md` is a 2026-10-19 snapshot. The audit is not a description of current code. The project fixed or dissolved several S-ids. See §5 above.
- Older benchmark docs use the `vanity` workload name. Older benchmark docs use "WarpStream parity" phrasing. Section 3 of the roadmap asks for dated, attributed, and nominative comparisons only. Section 3 bars "parity" claims. Section 3 bars "faster than" claims.
- Anything derived from the teardown is not a source to design from. See §10.

You keep the superseded text for the reason in the opening. A superseded decision shows the measurement that forced the turn. A reader who sees only the current decision cannot see that constraint.

## 12. The trust table tells you when to read each doc and how far to trust each doc

| Doc | When to read it | Trust level |
|---|---|---|
| `docs/decisions.md` | Before you change any contract, cap, or dependency | Current. Check each row for Superseded and Refines. |
| `docs/lessons.md` | When a contract looks odd and you want the reason | Current, except §4 and §9 wording. Decision 41-44 made §4 moot. Decision 17 superseded §9 wording. |
| `docs/flow-control.md` | When you touch inboxes, throttling, or backpressure | Current. Interfaces applied. |
| `docs/log-cp-service.md` | When you work on `cp/logcp`, failover, checkpoints, or the index | Current for stages 2-4. Run w4 partly overtook the "NOT done" list. |
| `docs/sim-log-engine.md` | When you add sim coverage for the log CP | Current. Stage 3. |
| `docs/applier-bakeoff.md` | When you question Pebble or argue a dependency choice | Historical measurement. Decision 49 keeps the conclusion current. |
| `docs/TODO.md` | When you pick up new work or check whether an idea has a schedule | Current. Accepted and not scheduled. |
| `docs/stage5-run-plan.md` | When you need to understand what w4 was meant to prove | Historical plan. `docs/w4-log-store-results.md` superseded the plan as fact. |
| `docs/roadmap-from-warpstream-teardown.md` | When you plan what comes next or ask about open-sourcing | Current, dated 2026-10-21. Section 0 legal caution applies first. |
| `docs/surprises-audit.md` | Before an AWS run or consume stage, or when you cite S-ids | Historical snapshot, dated 2026-10-19. Verify status per id. |
| `docs/commit-path-redesign.md` | When you need to understand why the transaction store ended | Superseded proposal. The team did not build design L. Sections 1-2 numbers still count as evidence. |
| `docs/delos-logdrive-ideas.md` | When you trace where design A and the S3-index idea came from | Historical ideas. Decision 48 adopted idea A. `TODO.md` holds idea B. The team did not build ideas C and D. |
| `docs/interface-changes-from-toys3.md` | When you need provenance for decisions 7-22 | Historical. The changes applied on 2026-10-16 and overtook the status line. |
| `docs/ddd-refactoring-proposal.md` | Only through the second edition | `docs/refactoring-proposal.md` superseded this proposal. Decision 34 deferred the rest. |
| `docs/refactoring-proposal.md` | When you plan a refactor or check what not to do | Proposal. The team applied nothing at writing. Premises need a new check. |

## Key takeaways

The key points are these points:

- Authority has layers. A decision has more authority than a topic doc. A topic doc has more authority than a proposal. A proposal has more authority than a dated audit and a dated plan. The verbs "supersede" and "refine" set the order inside the decisions. A results doc has more authority than the plan that produced the results doc.
- Decisions 41-45 and 48-53 set the current architecture. Brokers are stateless. Delivery is at-least-once. A log-backed control plane holds raw commands. Replicas apply those commands through a Pebble applier. Veritas is the independent oracle. Production code must agree with veritas. Production code must never become veritas.
- Most docs that look wrong are not mistakes. Each such doc records a measurement or a design that was true for one stage. `git log` and the `Status:` line tell you which stage you read.
- Open work lives in two places. `docs/TODO.md` holds accepted work. `docs/roadmap-from-warpstream-teardown.md` §2 holds ranked work. Any other document that proposes work is a proposal and not a commitment.
- The teardown is a legal risk and not a design source. Keep the teardown out of git. Keep the teardown out of the OSS repo. Build clean-room from public sources.

## Checkpoint

<details>
<summary>1. A colleague proposes re-adding per-producer duplicate detection "like decision 28 says". What is wrong?</summary>

Decisions 41 and 44 superseded decision 28. The system removed idempotent producers. The system removed the 5-entry window, `Dup`, `ErrOutOfOrder`, and `InitProducer`. Delivery is at-least-once. `docs/TODO.md` lists idempotent producers as not planned. The proposal means reopening decision 41.
</details>

<details>
<summary>2. Design L in <code>docs/commit-path-redesign.md</code> recommends a small transaction holding <code>G#</code> items with computed bases. Is that the production CP?</summary>

No. Design L was a proposal. Decisions 48-53 adopted idea A from `docs/delos-logdrive-ideas.md` instead. The log holds raw commands. Every replica applies the commands. Every replica assigns offsets at apply time. The system has no permanent `G#` items. `docs/log-cp-service.md` describes the system as built.
</details>

<details>
<summary>3. What does "S3" mean in a review comment, and how do you check whether S3 still applies?</summary>

S3 is item 3 of `docs/surprises-audit.md`. The audit is dated 2026-10-19. Fetch never waits. Idle consumers generate tens of thousands of CP Lookups per second for that reason. The audit is a dated snapshot. Check status in `docs/TODO.md` under "Consumer read path" and check the decisions. S3 is still open.
</details>

<details>
<summary>4. Why is veritas not the production state machine, if veritas is the spec?</summary>

Decision 49 gives the reason. Veritas keeps every run forever. One batch costs about 257 B. Target rate costs about 6 GB per hour. Veritas inserts keys in O(n). The production applier keeps the same semantics behind a KV interface. The applier is `cp/applier`. Pebble backs the applier per `docs/applier-bakeoff.md`. Lockstep tests use 100,000 seeds. `cptest` conformance proves the applier against veritas.
</details>

<details>
<summary>5. You find a great idea in the WarpStream teardown doc. What must happen before you use the idea?</summary>

Treat the idea as legally tainted until a lawyer clears the idea. Section 0 of `docs/roadmap-from-warpstream-teardown.md` says someone reverse-engineered the teardown from a vendor binary. Section 0 says the licence terms are unread. Section 0 says reverse engineering is presumptively prohibited. Get legal advice. Keep the idea out of git and the OSS repo until clearance. Derive the idea again clean-room. Use public docs, the Kafka spec, AWS guidance, or our own measurements.
</details>

## Try it

You test your reading in these ways:

- Run `git log --date=short --format='%ad %h %s' -- docs/decisions.md`. Read how rows 41-44 and 48-53 arrived. Search your favourite row for "Superseded" and "Refines" before you quote the row.
- Pick one S-id from `docs/surprises-audit.md`. Classify the S-id as fixed, dissolved, declined, or open. Use `docs/decisions.md`, `docs/TODO.md`, and `docs/w4-log-store-results.md` as your only evidence.
- Run `go run ./cmd/manisim -scenario kafka-log -seeds 300`. Match the properties in `docs/sim-log-engine.md` to the report. Find the four log mutants and the seed that first catches each mutant.
- Open `docs/refactoring-proposal.md` §7. Argue from the proof column in §7. Name which of the three priority items costs least to revive today. State which premise already changed. Range compaction landed as decision 39.
- Check `git status` for untracked files. Confirm that no file matching `docs/arun-*` has a staged state. Read §0 of the roadmap once more before any `git add`.

## Further reading

Further reading covers these docs:

- Read `docs/decisions.md` rows I, 21, 41-45, and 48-53. The rows form the core that sets the architecture.
- Read `docs/design-a-prototype.md` for the prototype and the X2 and X5 measurements behind design A. The doc includes the staged production plan.
- Read `docs/w4-log-store-results.md` for what stage 5 measured. The doc includes the caveats. Pair the doc with `docs/w2-bottleneck-verification.md` and `docs/w3-affinity-and-window-experiments.md` for the picture before stage 5.
- Read `docs/x1-x3-x4-results.md` for the DynamoDB experiments behind decisions 48-52. The experiments cover PutItem and transaction use, hot-key headroom, and contention.
- Read `docs/observability.md` and `docs/benchmarks.md`. The docs cover evidence-grade logging and metrics. The docs cover the local limits behind decisions 36-38.
- Read `docs/dep-audits/` for what an accepted dependency record looks like under decision I. Start with the Pebble record from the bake-off.
