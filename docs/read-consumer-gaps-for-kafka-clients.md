# Read and consumer support for Kafka clients

Status at 2026-10-31. This page lists what a Kafka client can and cannot do on the read side today. It describes the code, not the roadmap. Work items live in `docs/TODO.md`. Estimates live in `docs/roadmap-from-warpstream-teardown.md`.

## What works today

The broker serves sixteen Kafka APIs. `kwire.Supported` in `internal/wire/kafkawire/supported.go` is the source.

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3 to 9 |
| Fetch | 1 | 4 to 12 |
| ListOffsets | 2 | 1 to 7 |
| Metadata | 3 | 1 to 12 |
| OffsetCommit | 8 | 0 to 8 |
| OffsetFetch | 9 | 0 to 8 |
| FindCoordinator | 10 | 0 to 4, group keys only |
| JoinGroup | 11 | 0 to 7 |
| Heartbeat | 12 | 0 to 4 |
| LeaveGroup | 13 | 0 to 5 |
| SyncGroup | 14 | 0 to 5 |
| DescribeGroups | 15 | 0 to 5 |
| ListGroups | 16 | 0 to 4 |
| ApiVersions | 18 | 0 to 3 |
| CreateTopics | 19 | 0 to 7 |
| DescribeConfigs | 32 | 0 to 4, topics only |

CreateTopics takes any replication factor and ignores it, since object storage is the replication. It ignores configs and logs one warning per request that has them. An existing topic gets TOPIC_ALREADY_EXISTS. The check and the create are one control plane step, so two creates of one new name cannot both succeed. A retry after a lost reply that finds the topic counts as success. `kafka-topics.sh --create`, `--list` and `--describe` work. DescribeConfigs is served because `--describe` calls it. It returns an empty config list for each existing topic. `ops/scripts/e2e-kafka-cli.sh` checks these commands.

A client that assigns partitions itself and reads with Fetch can consume. franz-go with `kgo.ConsumePartitions` takes this path. It needs no coordinator and no group state on the broker.

## Consumer groups (classic protocol)

The broker keeps no group state (decision Q3). Each group call is one control plane call. The control plane keeps the state in its log (see `consumer-groups-design.md`). Any broker answers any group call. Only FindCoordinator steers a client to one broker.

- **FindCoordinator** (group keys only). The coordinator is the live broker picked by an FNV-1a 32 bit hash of the group name, modulo the sorted live broker list. With no other live broker, the answer is the broker itself. A transaction key gets COORDINATOR_NOT_AVAILABLE. An empty group id gets INVALID_GROUP_ID. Version 4 takes several keys.
- **JoinGroup and SyncGroup** wait at the control plane until the rebalance step ends. The broker does not block: a waiting call parks its own connection, and other connections are served. Their control plane deadline is the rebalance timeout plus 10 s (JoinGroup; version 0 uses the session timeout). A SyncGroup deadline is 70 s, because the broker does not know the group's rebalance timeout. A call past its deadline gets COORDINATOR_NOT_AVAILABLE, and the client retries.
- **Heartbeat and LeaveGroup** do not wait for a rebalance. They use the normal control plane deadline (10 s). A heartbeat during a rebalance gets REBALANCE_IN_PROGRESS, so the member rejoins.
- **Errors.** UNKNOWN_MEMBER_ID (25), ILLEGAL_GENERATION (22), REBALANCE_IN_PROGRESS (27), INCONSISTENT_GROUP_PROTOCOL (23) and UNKNOWN_TOPIC_OR_PARTITION (3) come from the control plane as they are. Any other failure, such as a timeout or an unavailable control plane, becomes COORDINATOR_NOT_AVAILABLE (15). A broker never retries a join or a sync, because a retry would make a second member.
- **OffsetCommit and OffsetFetch.** Offsets live in the control plane. A commit with generation -1 and no member is a manual commit. It is allowed only when the group has no members. OffsetFetch with an empty topic list (version 2 and up) returns every committed offset. A partition with no commit gets offset -1 and no error.
- **ListGroups** lists only the groups this broker coordinates, so the admin client, which asks every broker, sees each group once. With no other live broker, this broker lists them all. A states filter (version 4) narrows the list.
- **DescribeGroups** answers any group. A group the control plane does not know is "Dead" with no error, as Kafka does. The control plane state PreparingSync shows as CompletingRebalance.
- **Not served.** ConsumerGroupHeartbeat (key 68) and the other KIP-848 APIs are not advertised. A client with `group.protocol=consumer` gets no answer to it.

Known limits of the group API:

- Static membership (`group.instance.id`) is accepted in the fields and treated as a normal member.
- A rebalance that is still waiting shows as Stable in DescribeGroups and ListGroups. The control plane state changes only when the leader's assignment is logged.
- A broker does not send MEMBER_ID_REQUIRED. It gives the new member its id at once.
- ClientHost is the host of the client's connection. The client's own host name is not used.
- A closed connection does not cancel its join or sync at the control plane. The member stays until its session times out. A broker stop answers the waiting calls with COORDINATOR_NOT_AVAILABLE, and the control plane may still finish them.
- Retention and expiry of committed offsets are not done. The leader epoch of a commit is stored as the client sent it.
- OffsetFetch with RequireStable (version 7 and up) is answered as if it were false.
- DeleteGroups (42) and OffsetDelete (47) are not served, so `kafka-consumer-groups.sh --delete` fails.

## How one Fetch works today

`broker.fetch` in `internal/broker/fetch.go` does the following for each partition:

1. The broker sends one `Lookup` call to the control plane. `Lookup` is a `core.CPOp` in `internal/core/controlplane.go`. The call asks for batches at or after the fetch offset, up to the partition byte limit.
2. The broker reads each returned batch with one ranged S3 GET. Reads within one partition run one at a time, in offset order. `broker.onFetchGet` runs the next GET only after the previous GET returns.
3. The broker checks length, record count, and CRC for each batch. A bad GET is retried, up to `MaxAttempts`, which defaults to 6 in `internal/broker/config.go`.
4. The broker rewrites base offset and leader epoch with `kwire.PatchBatch`. Both fields sit outside the batch CRC, so the CRC stays valid.
5. The broker returns the batches. It sets high watermark and last stable offset to the partition end offset from `LookupResp.End`. It sets log start offset to 0.

Byte limits work. `PartitionMaxBytes` bounds one partition. `MaxBytes` bounds the whole response, applied in `broker.finalize` in `internal/broker/conn.go`. A partition that asks for no limit gets `DefaultFetchBytes`, 1 MiB. A response always holds at least one batch per partition, even if that batch exceeds the limit.

## Long poll: MinBytes and MaxWait

A Fetch can wait for data. The rules are:

- `MaxWait` is in milliseconds. The broker caps it at 30 seconds. A `MaxWait` of 0 answers at once, as before.
- `MinBytes` is the total size the answer must reach. A `MinBytes` of 0 or less answers at once.
- A partition with no data is polled again with `Lookup` every `FetchPollInterval`. The default is 200 ms. The flag on `manibroker` is `-fetch-poll-interval`. Each poll is one `Lookup` for one partition.
- A partition that has data keeps it and is not polled again. It waits for the rest of the Fetch.
- The Fetch answers when its total bytes reach `MinBytes`, or when `MaxWait` ends. The answer holds what the broker has, which can be empty.
- A parked Fetch ends at once when its connection closes or the broker stops. A stop answers it empty.
- Nothing is cached. Each poll asks the control plane again.

The last wait is cut at the end of `MaxWait`. So the answer comes at `MaxWait`, not one poll later.

Cost and latency. An idle Java consumer with `fetch.max.wait.ms` 500 on one partition makes three `Lookup` calls per 500 ms cycle, at 0, 200 and 400 ms. That is about 6 a second. `ops/scripts/e2e-kafka-cli.sh` checks this. It counts 29 Lookups in 5 seconds from one idle consumer, with a limit of 60. A new produce is seen at the next poll, so within one `FetchPollInterval` after the commit. The e2e check sees 5 new lines within 2 seconds after the producer returns.

The wait is a timer, not a wake-up. The broker learns of a commit only at its next poll. See "No wake-up path" below.

## Unsupported requests

A request outside the table in `kwire.Supported` gets no answer. Its key or version is not served (for example ConsumerGroupHeartbeat, key 68). `kwire.DecodeRequest` returns `kwire.UnsupportedError` for it. `broker.request` in `internal/broker/conn.go` closes the connection with no error text. Only an ApiVersions request gets an answer.

## Missing Kafka APIs

The group APIs are served now (see "Consumer groups"). Three groups of Kafka APIs are still missing.

| Missing API | Key | What the client loses |
|---|---|---|
| ConsumerGroupHeartbeat and the KIP-848 APIs | 68 | A client with `group.protocol=consumer` cannot join. Not planned (decision Q9). |
| DeleteGroups | 42 | `kafka-consumer-groups.sh --delete` fails. |
| OffsetDelete | 47 | Offsets of one topic cannot be dropped from a group. |
| InitProducerId | 22 | Idempotent and transactional producers cannot start. Decisions 41 to 44 removed this on purpose. It is not planned. |

Kafka Connect, Kafka Streams, and ksqlDB need transactional producers as well, and both are absent.

## Fetch fields the broker ignores or rejects

| Field or feature | Behavior today | Code |
|---|---|---|
| Fetch sessions, `SessionId` and `SessionEpoch` | A request with `SessionEpoch` above 0 gets `FETCH_SESSION_ID_NOT_FOUND`. The broker grants no session. Each Fetch sends full topic and partition state. | `broker.fetch` in `internal/broker/fetch.go`. |
| Topic IDs | Fetch version 13 and above use topic IDs. The broker stops at version 12, so clients must use topic names. A Metadata request by topic ID returns `UNKNOWN_TOPIC_ID`. | `kwire.Supported`, `broker.metaResp`. |
| Isolation level | Read committed and read uncommitted get the same answer. The broker sets last stable offset equal to the end offset and never returns aborted transaction markers. There are no transactions to abort. | `broker.onLookup`. |
| `CurrentLeaderEpoch` and diverging epochs | The broker writes leader epoch 0 into every batch and into ListOffsets. It returns no diverging epoch. Fencing by leader epoch does not exist. | `kwire.PatchBatch` call in `broker.onFetchGet`. Decision 44. |
| Timestamp search in ListOffsets | A timestamp other than earliest (-2) or latest (-1) gets error 43, `UNSUPPORTED_FOR_MESSAGE_FORMAT`. A client that seeks by time fails. | `broker.listOffsets` in `internal/broker/metadata.go`. |

## Read path limits that come from missing caches

Three pieces are absent. Each one shows up at a measured load, not in theory.

### One Lookup per partition per Fetch, with no cache

The broker keeps no copy of partition ends or recent runs. Every Fetch for every partition costs one control plane call over gRPC, and the control plane runs a consistent DynamoDB read for it.

`docs/surprises-audit.md` item S3 measured one idle franz-go consumer on 8 partitions at 20,600 Lookups per second. It predicts about 48,000 Lookups per second for 64 consumers on AWS. That rate sits above the 30,000 warm throughput the audit cites for the table and near the 40,000 default on-demand quota. An idle consumer should cost almost nothing. Today it costs more control plane work than an active producer.

The long poll (see above) cuts the idle cost, but it still polls. An idle partition costs one `Lookup` per poll interval. The missing piece is a broker cache of end offsets and recent `Lookup` results. The cache has one hard case. Broker B must not serve a stale end after broker A acked a produce. A bounded staleness window with a read-your-writes rule for the acking broker is the design question, not the cache itself.

### One ranged GET per batch, in sequence, with no cache

The broker reads one batch per GET and waits for each GET before it starts the next. It keeps no segment data between requests. Six brokers reading the same hot batch run six GETs.

Item S6 of the audit computes about 6,500 GETs per second for a 1 GiB/s read with the measured Java batch size of 79.5 KB. A fresh `seg/` prefix in S3 starts near 5,500 GETs per second. Read throughput for one partition tops out near batch size divided by GET latency, because the GETs run in sequence.

The missing pieces are coalesced reads, where adjacent batches from one segment come back in one ranged GET, parallel GETs within one partition, and a per-broker cache with read-ahead. Decision 5 deferred caches to a later version. That deferral is the thing to revisit.

### No wake-up path from commit to Fetch

The commit path knows the new end offset at ack time. No signal carries that fact to the brokers that serve reads. The long poll does not need one. It asks the control plane again every `FetchPollInterval`. A push signal would cut that poll cost. It is the same cache problem seen from the other side.

## Metadata limits that affect consumers

`broker.metadata` lists every live broker in `brokers[]`, sorted by node id. Each broker beats to the control plane every second, and the control plane drops a broker 5 seconds after its last beat. Each broker still names itself as leader, replica, and in-sync replica for every partition, and as controller. If the control plane is down, the broker answers with itself only.

A client that loses a broker can connect to another one from the live list, so a one-broker seed list is no longer a single point of failure. The live set is in memory on the active control plane. It is not logged, so after a failover it fills again within one heartbeat. The roadmap adds rack and zone data to the same item.

## Offset and retention limits

Earliest is always offset 0. Nothing deletes segment objects today, and no retention setting exists. `broker.listOffsets` returns 0 for earliest with no control plane call.

Two results follow. Storage grows without bound, and the read path can always reach offset 0. The second result ends the day retention starts. Retention also needs a garbage collector for segment objects that no run points at. `broker.onFetchGet` treats a missing committed segment as an unreachable state and fails the request. A lifecycle rule on the bucket can create that state today. Item S13 of the audit covers it. Retention, collection, and a log start offset that moves belong in one change, not three.

There is no log compaction. A client that expects a compacted topic reads the full history.

## Client matrix

| Client use | Works today | First blocker |
|---|---|---|
| franz-go, manual assign, offsets kept by the app | Yes | None on the read path. Lookup and GET rates still apply at scale. |
| franz-go, consumer group | Not tested | Group calls are served. Not run in this slice. |
| Java `KafkaConsumer.assign` | Yes | None on the read path. |
| Java `KafkaConsumer.subscribe` (classic protocol) | Yes, through the Kafka 3.9.1 CLI | Group calls are served. The CLI uses this client code, and `ops/scripts/e2e-kafka-cli.sh` checks it. Not run with a Java test of its own. |
| librdkafka, assign | Yes | None on the read path. |
| librdkafka, subscribe | Not tested | Group calls are served. |
| Kafka Connect | No | Transactions for exactly-once sources and sinks. |
| Kafka Streams, ksqlDB | No | Transactional producers, internal topics through CreateTopics. |
| Spark or Flink, group mode | Not tested | Group calls are served. |
| Spark or Flink, manual assign, offsets kept in the job | Yes, with the read rates above | Lookup rate and GET rate at high partition counts. |
| `kafka-console-consumer` with `--group` | Yes | Checked by `ops/scripts/e2e-kafka-cli.sh`. |
| `kafka-consumer-groups` `--list`, `--describe`, `--describe --members --verbose` | Yes | Checked by `ops/scripts/e2e-kafka-cli.sh`. |
| `kafka-consumer-groups` `--reset-offsets`, `--delete` | Not tested, `--delete` fails | `--delete` needs DeleteGroups (42). |
| `kafka-console-consumer` with `--partition` and `--offset` | Yes | None. This is the fastest manual check of the read path. |

## Order of work already accepted

`docs/TODO.md` puts the read path before any consumer stage. `docs/roadmap-from-warpstream-teardown.md` prices it near 20 engineer-days and groups near 35 to 45 engineer-days after that.

1. Long-poll Fetch. Honor `MinBytes` and `MaxWait`. Done (slice 3 of `consumer-groups-design.md`, see the long poll section above).
2. Broker cache for end offsets and recent Lookup results, with a stated staleness rule.
3. Coalesced ranged GETs, parallel GETs in one partition, and a per-broker read cache.
4. Broker membership. Metadata returns every live broker. Done (heartbeat leases in the control plane, see `consumer-groups-design.md`, slice 2).
5. Consumer groups and committed offsets. Done in slices 4a to 5 of `consumer-groups-design.md`: the control plane keeps the group state in its log, and the broker serves the classic group APIs. The simulator has the trace tags and the speccheck oracle covers the group commands.
6. Retention and garbage collection, with a moving log start offset. Separate from groups, and it gates any long-retention claim.

Each step has a number to move. Lookup rate per idle consumer for step 1 and 2. GETs per second and cost per GiB read for step 3. Failover time without a seed list for step 4. A 64-consumer stage at the produce rate, with end-to-end p99, for step 5.

## Sources

- `internal/wire/kafkawire/supported.go` for the API list and versions.
- `internal/broker/create.go` and `internal/broker/configs.go` for CreateTopics and DescribeConfigs.
- `internal/broker/fetch.go`, `internal/broker/ops.go`, `internal/broker/metadata.go`, and `internal/broker/conn.go` for the read path as built.
- `internal/core/controlplane.go` for the three control plane operations.
- `docs/surprises-audit.md` items S3, S5, S6, and S13 for measured rates.
- `internal/broker/fetch_test.go` and `ops/scripts/e2e-kafka-cli.sh` for the long poll checks.
- `internal/broker/coordinator.go`, `internal/broker/groups.go`, `internal/broker/groups_offsets.go` and `internal/broker/groups_admin.go` for the group APIs, with their tests in `internal/broker/groups*_test.go`.
- `internal/controlplane/controlplaneclient/groups.go` for the group calls to the control plane, and `internal/system/production/executor.go` for the per-call deadline.
- `docs/TODO.md` for accepted work and `docs/roadmap-from-warpstream-teardown.md` sections 1 and 2 for estimates.
