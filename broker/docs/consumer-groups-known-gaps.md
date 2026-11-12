# Consumer groups and Kafka CLI work: known gaps

This file lists what is NOT done, not tested, or only partly done in the work
described in [consumer-groups-design.md](consumer-groups-design.md)
(slices 0 to 5). Read it before you trust the feature. Each item says what is
missing and what to do next.

Status in one line: a stock Kafka CLI 3.9.1 can create topics, produce,
consume with a group, commit offsets and rebalance against 3 brokers
(`make e2e-kafka-cli`). The OMB run (slice 6) was skipped on purpose.

## 1. Not tested

| Gap | Detail | Next step |
|---|---|---|
| Other clients | Only the Kafka CLI tools were used. Java `KafkaConsumer` in your own code, franz-go and librdkafka were not tried. | Run a small Java and a franz-go consumer against the e2e stack. |
| OMB | Slice 6 is skipped (no compute on the laptop). Stock OMB with YAML only is the goal, and it is not proven. | Run a very small OMB workload on a bigger machine. |
| Coordinator failover | `Rebuild` is tested in memory with a fake clock. A real standby taking over a live group was not tested. | Add a two-node test: kill the active CP while a group is stable and while it is rebalancing. |
| Lease set failover | After a CP failover the live broker set starts empty and refills in one lease interval. Only one active node is tested. | Same two-node test. |
| Group traffic in the simulator | The simulator scenarios send no group requests. The fold is covered by the oracle and lockstep tests. The coordinator (timers, parked calls) is covered only by unit tests with a fake clock. | Add a coordinator scenario to the simulator, or a seeded test with fault injection. |
| `--reset-offsets` | Not tried. | Try it with `kafka-consumer-groups.sh`. |

## 2. Not served or not done

| Gap | Detail |
|---|---|
| KIP-848 | The new consumer protocol is not served. A client with `group.protocol=consumer` gets UnsupportedVersion. This is decision Q9. |
| `DeleteGroups` (key 42) and `OffsetDelete` (key 47) | Not served, so `kafka-consumer-groups.sh --delete` fails. |
| `MEMBER_ID_REQUIRED` | The coordinator gives a new member a generated id and lets it join at once. Clients accept this. |
| Static membership | `group.instance.id` is read but the member is treated as a normal member. |
| Transaction coordinator | `FindCoordinator` with key type 1 answers `COORDINATOR_NOT_AVAILABLE`. |
| Rebalance state in `DescribeGroups` | A group that is waiting for members to rejoin shows as `Stable`. The real phase is only in memory in the active CP. |
| Create after a lost reply | `CreateTopics` is one atomic CP call, so two creates cannot both win. One edge is left: if the reply is lost and the broker retries, the retry finds the topic and reports success. If another client created the same name in that short window, it can also report success. |
| Simulator and lost creates | The simulator's list of mutating ops leaves out `CreateTopic`, so a lost create reply never happens there, though the real CP may apply it. Changing this would change the fault draws and the pinned hashes. |
| Closed connection | If a client connection closes while its JoinGroup is parked, the broker drops the answer, but the join stays in the CP until it ends or times out. |
| Empty groups | They stay in memory on the active CP until the next rebuild. |

## 3. Performance and cost

| Gap | Detail | Next step |
|---|---|---|
| Fetch load on the CP | An idle consumer makes one CP `Lookup` per `FetchPollInterval` (200 ms) per partition that is still short. About 29 calls in 5 s was measured. | Make `Lookup` multi-partition (one call per Fetch). Later add an end-offset cache or a long-poll CP call. See decision Q8. |
| Slot budget | `OffsetCommit` shares the CP log slot budget with `CommitSegment`. About 13 slots/s at 64 consumers committing every 5 s. It is not verified that the appender packs several commands in one slot. | Check the appender before beta. |
| Group calls are not retried | A CP failure or timeout on a group call becomes `COORDINATOR_NOT_AVAILABLE` and the client retries. The call can be "maybe applied". | Fine for alpha. Check that every group command is safe to apply twice. |
| Fixed margins | JoinGroup waits the rebalance timeout plus 10 s. SyncGroup waits 70 s. | Make them settings if a client needs more. |
| Rebuild watcher | The CP checks every 100 ms whether it must rebuild the coordinator from the log. | Replace with a signal from the role change. |

## 4. Test and tool notes

- The e2e script needs Java 21, the Kafka CLI (downloaded by
  `ops/scripts/fetch-kafka-cli.sh`, about 120 MB from archive.apache.org) and
  minio (from `mise.toml`) as the shared S3 store for 3 brokers.
- The simulator golden hashes (`internal/verify/simulator/kafka/testdata/golden_hashes.txt`)
  changed in slices 1, 3 and 5. The causes were proven each time: a longer
  ApiVersions list, and the Fetch long poll. `capdiff_test.go` now uses 6
  seeds because the long poll changed the timing.
- The fetch long poll has no simulator assert that a parked Fetch ends by
  `MaxWait` plus one poll. The cost model can delay timers, so it would fail
  by chance.
- `docs/decisions.md` row 29 still says Fetch ignores `MinBytes` and
  `MaxWait`. It is out of date after slice 3.

## 5. Older gaps that this work does not change

No compaction, no orphan segment garbage collection and no retention. See
[read-consumer-gaps-for-kafka-clients.md](read-consumer-gaps-for-kafka-clients.md)
and [warpstream-architecture-comparison.md](warpstream-architecture-comparison.md).
