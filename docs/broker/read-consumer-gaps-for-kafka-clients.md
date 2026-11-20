# Read and consumer support for Kafka clients

What a Kafka client can do on the read and classic group paths in 0.1.0.
Unfinished work lives in Jira workstream [MDS-11](https://manifester-e50593c9.atlassian.net/browse/MDS-11) (post GA 0.1.0 clean up).

## What works today

The broker serves sixteen Kafka APIs. `kafkawire.Supported` in `internal/wire/kafkawire/supported.go` is the source.

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
- **KIP-848** is not advertised (decision Q9). A client with `group.protocol=consumer` gets no answer to ConsumerGroupHeartbeat.
- **MEMBER_ID_REQUIRED** is not used. The coordinator gives a new member a generated id and lets it join at once.
- **ClientHost** is the host of the client's connection.

Checked by `ops/scripts/e2e-kafka-cli.sh`: create/list/describe topics, produce, consume with a group, commit offsets, rebalance, and `kafka-consumer-groups.sh --list` / `--describe`.

## How one Fetch works today

`broker.fetch` in `internal/broker/fetch.go` does the following for each partition:

1. The broker sends one `Lookup` call to the control plane. `Lookup` is a `core.CPOp` in `internal/core/controlplane.go`. The call asks for batches at or after the fetch offset, up to the partition byte limit.
2. The broker reads each returned batch with one ranged S3 GET. Reads within one partition run one at a time, in offset order. `broker.onFetchGet` runs the next GET only after the previous GET returns.
3. The broker checks length, record count, and CRC for each batch. A bad GET is retried, up to `MaxAttempts`, which defaults to 6 in `internal/broker/config.go`.
4. The broker rewrites base offset and leader epoch with `kafkawire.ChangeBatchOffset`. Both fields sit outside the batch CRC, so the CRC stays valid.
5. The broker returns the batches. It sets high watermark and last stable offset to the partition end offset from `LookupResp.End`. It sets log start offset to 0.

Byte limits work. `PartitionMaxBytes` bounds one partition. `MaxBytes` bounds the whole response, applied in `broker.finalize` in `internal/broker/conn.go`. A partition that asks for no limit gets `DefaultFetchBytes`, 1 MiB. A response always holds at least one batch per partition, even if that batch exceeds the limit.

## Long poll: MinBytes and MaxWait

A Fetch can wait for data. The rules are:

- `MaxWait` is in milliseconds. The broker caps it at 30 seconds. A `MaxWait` of 0 answers at once, as before.
- `MinBytes` is the total size the answer must reach. A `MinBytes` of 0 or less answers at once.
- A partition with no data is polled again with `Lookup` every `FetchPollInterval`. The default is 200 ms. The flag on `mdsbroker` is `-fetch-poll-interval`. Each poll is one `Lookup` for one partition.
- A partition that has data keeps it and is not polled again. It waits for the rest of the Fetch.
- The Fetch answers when its total bytes reach `MinBytes`, or when `MaxWait` ends. The answer holds what the broker has, which can be empty.
- A parked Fetch ends at once when its connection closes or the broker stops. A stop answers it empty.
- Nothing is cached. Each poll asks the control plane again.

The last wait is cut at the end of `MaxWait`. So the answer comes at `MaxWait`, not one poll later.

An idle Java consumer with `fetch.max.wait.ms` 500 on one partition makes three `Lookup` calls per 500 ms cycle. `ops/scripts/e2e-kafka-cli.sh` counts 29 Lookups in 5 seconds from one idle consumer, with a limit of 60. A new produce is seen at the next poll, so within one `FetchPollInterval` after the commit.

## Unsupported requests

A request outside the table in `kafkawire.Supported` gets no answer. Its key or version is not served. `Codec.DecodeRequest` returns `kafkawire.UnsupportedError` for it. `broker.request` in `internal/broker/conn.go` closes the connection with no error text. Only an ApiVersions request gets an answer.

Idempotent and transactional producers are out of scope (decisions 41 to 44): InitProducerId is not served.

## Fetch fields the broker ignores or rejects

| Field or feature | Behavior today | Code |
|---|---|---|
| Fetch sessions, `SessionId` and `SessionEpoch` | A request with `SessionEpoch` above 0 gets `FETCH_SESSION_ID_NOT_FOUND`. The broker grants no session. Each Fetch sends full topic and partition state. | `broker.fetch` in `internal/broker/fetch.go`. |
| Topic IDs | Fetch version 13 and above use topic IDs. The broker stops at version 12, so clients must use topic names. A Metadata request by topic ID returns `UNKNOWN_TOPIC_ID`. | `kafkawire.Supported`, `broker.metaResp`. |
| Isolation level | Read committed and read uncommitted get the same answer. The broker sets last stable offset equal to the end offset and never returns aborted transaction markers. There are no transactions to abort. | `broker.onLookup`. |
| `CurrentLeaderEpoch` and diverging epochs | The broker writes leader epoch 0 into every batch and into ListOffsets. It returns no diverging epoch. Fencing by leader epoch does not exist. | `kafkawire.ChangeBatchOffset` call in `broker.acceptFetch`. Decision 44. |
| Timestamp search in ListOffsets | A timestamp other than earliest (-2) or latest (-1) gets error 43, `UNSUPPORTED_FOR_MESSAGE_FORMAT`. | `broker.listOffsets` in `internal/broker/metadata.go`. |

## Metadata

`broker.metadata` lists every live broker in `brokers[]`, sorted by node id. Each broker beats to the control plane every second, and the control plane drops a broker 5 seconds after its last beat. Each broker still names itself as leader, replica, and in-sync replica for every partition, and as controller. If the control plane is down, the broker answers with itself only.

A client that loses a broker can connect to another one from the live list. The live set is in memory on the active control plane. It is not logged, so after a failover it fills again within one heartbeat.

## Offsets and storage

Earliest is always offset 0. Nothing deletes segment objects today, and no retention setting exists. `broker.listOffsets` returns 0 for earliest with no control plane call. There is no log compaction.

## Sources

- `internal/wire/kafkawire/supported.go` for the API list and versions.
- `internal/broker/create.go` and `internal/broker/configs.go` for CreateTopics and DescribeConfigs.
- `internal/broker/fetch.go`, `internal/broker/ops.go`, `internal/broker/metadata.go`, and `internal/broker/conn.go` for the read path.
- `internal/broker/fetch_test.go` and `ops/scripts/e2e-kafka-cli.sh` for the long poll checks.
- `internal/broker/coordinator.go`, `internal/broker/groups.go`, `internal/broker/groups_offsets.go` and `internal/broker/groups_admin.go` for the group APIs.
- `consumer-groups-design.md` for the design decisions behind the group path.
