# Read and consumer support for Kafka clients

Status at 2026-10-22. This page lists what a Kafka client can and cannot do on the read side today. It describes the code, not the roadmap. Work items live in `docs/TODO.md`. Estimates live in `docs/roadmap-from-warpstream-teardown.md`.

## What works today

The broker serves five Kafka APIs. `kwire.Supported` in `kwire/supported.go` is the source.

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3 to 9 |
| Fetch | 1 | 4 to 12 |
| ListOffsets | 2 | 1 to 7 |
| Metadata | 3 | 1 to 12 |
| ApiVersions | 18 | 0 to 3 |

A client that assigns partitions itself and reads with Fetch can consume. franz-go with `kgo.ConsumePartitions` takes this path. It needs no coordinator and no group state on the broker.

## How one Fetch works today

`broker.fetch` in `broker/fetch.go` does the following for each partition:

1. The broker sends one `Lookup` call to the control plane. `Lookup` is a `core.CPOp` in `core/controlplane.go`. The call asks for batches at or after the fetch offset, up to the partition byte limit.
2. The broker reads each returned batch with one ranged S3 GET. Reads within one partition run one at a time, in offset order. `broker.onFetchGet` runs the next GET only after the previous GET returns.
3. The broker checks length, record count, and CRC for each batch. A bad GET is retried, up to `MaxAttempts`, which defaults to 6 in `broker/config.go`.
4. The broker rewrites base offset and leader epoch with `kwire.PatchBatch`. Both fields sit outside the batch CRC, so the CRC stays valid.
5. The broker returns the batches. It sets high watermark and last stable offset to the partition end offset from `LookupResp.End`. It sets log start offset to 0.

Byte limits work. `PartitionMaxBytes` bounds one partition. `MaxBytes` bounds the whole response, applied in `broker.finalize` in `broker/conn.go`. A partition that asks for no limit gets `DefaultFetchBytes`, 1 MiB. A response always holds at least one batch per partition, even if that batch exceeds the limit.

## What a group consumer hits first

A client that calls subscribe instead of assign sends FindCoordinator. The broker does not serve FindCoordinator.

`kwire.DecodeRequest` returns `kwire.UnsupportedError` for any key outside the five above, and for a version outside the ranges above. `broker.request` in `broker/conn.go` answers an ApiVersions request with the supported ranges. For any other unsupported request it closes the connection and sends no error response.

The client sees a dropped connection. It reconnects, asks ApiVersions, sees no group APIs, and fails or retries, depending on the client. No error text on the wire tells the user that groups are missing.

## Missing Kafka APIs

The control plane has three operations: `CommitSegment`, `Lookup`, and `DescribeTopics`. There is no operation for group state and no operation for committed consumer offsets. The missing APIs fall out of that fact.

| Missing API | Key | What the client loses |
|---|---|---|
| OffsetCommit | 8 | The broker stores no committed offset for a group or a client. |
| OffsetFetch | 9 | A restarted client cannot ask the broker where it stopped. |
| FindCoordinator | 10 | Group clients cannot find a coordinator. This is the first failure for most consumers. |
| JoinGroup | 11 | No group membership and no partition assignment from the broker. |
| Heartbeat | 12 | No liveness check and no rebalance signal. |
| LeaveGroup | 13 | No clean exit from a group. |
| SyncGroup | 14 | No assignment distribution inside a group. |
| DescribeGroups | 15 | Admin and monitoring tools cannot inspect groups. |
| ListGroups | 16 | Admin tools cannot list groups. |
| CreateTopics | 19 | Topics are created through the control plane HTTP API, not through Kafka. |
| InitProducerId | 22 | Idempotent and transactional producers cannot start. Decisions 41 to 44 removed this on purpose. It is not planned. |

Kafka Connect, Kafka Streams, and ksqlDB need group APIs and, in most setups, transactional producers. Both are absent. Spark and Flink Kafka sources run in group mode by default, so they fail at FindCoordinator unless the job assigns partitions itself and keeps offsets outside Manifester.

## Fetch fields the broker ignores or rejects

| Field or feature | Behavior today | Code |
|---|---|---|
| `MinBytes` | Ignored. A Fetch with no new data returns at once, empty. | `broker/fetch.go` states this in its package comment. Decision 29. |
| `MaxWait` | Ignored. The broker never holds a Fetch open. | Same. |
| Fetch sessions, `SessionId` and `SessionEpoch` | A request with `SessionEpoch` above 0 gets `FETCH_SESSION_ID_NOT_FOUND`. The broker grants no session. Each Fetch sends full topic and partition state. | `broker.fetch` in `broker/fetch.go`. |
| Topic IDs | Fetch version 13 and above use topic IDs. The broker stops at version 12, so clients must use topic names. A Metadata request by topic ID returns `UNKNOWN_TOPIC_ID`. | `kwire.Supported`, `broker.metaResp`. |
| Isolation level | Read committed and read uncommitted get the same answer. The broker sets last stable offset equal to the end offset and never returns aborted transaction markers. There are no transactions to abort. | `broker.onLookup`. |
| `CurrentLeaderEpoch` and diverging epochs | The broker writes leader epoch 0 into every batch and into ListOffsets. It returns no diverging epoch. Fencing by leader epoch does not exist. | `kwire.PatchBatch` call in `broker.onFetchGet`. Decision 44. |
| Timestamp search in ListOffsets | A timestamp other than earliest (-2) or latest (-1) gets error 43, `UNSUPPORTED_FOR_MESSAGE_FORMAT`. A client that seeks by time fails. | `broker.listOffsets` in `broker/metadata.go`. |

## Read path limits that come from missing caches

Three pieces are absent. Each one shows up at a measured load, not in theory.

### One Lookup per partition per Fetch, with no cache

The broker keeps no copy of partition ends or recent runs. Every Fetch for every partition costs one control plane call over HTTP, and the control plane runs a consistent DynamoDB read for it.

`docs/surprises-audit.md` item S3 measured one idle franz-go consumer on 8 partitions at 20,600 Lookups per second. It predicts about 48,000 Lookups per second for 64 consumers on AWS. That rate sits above the 30,000 warm throughput the audit cites for the table and near the 40,000 default on-demand quota. An idle consumer should cost almost nothing. Today it costs more control plane work than an active producer.

The missing pieces are a long-poll Fetch that honors `MinBytes` and `MaxWait`, and a broker cache of end offsets and recent `Lookup` results. The cache has one hard case. Broker B must not serve a stale end after broker A acked a produce. A bounded staleness window with a read-your-writes rule for the acking broker is the design question, not the cache itself.

### One ranged GET per batch, in sequence, with no cache

The broker reads one batch per GET and waits for each GET before it starts the next. It keeps no segment data between requests. Six brokers reading the same hot batch run six GETs.

Item S6 of the audit computes about 6,500 GETs per second for a 1 GiB/s read with the measured Java batch size of 79.5 KB. A fresh `seg/` prefix in S3 starts near 5,500 GETs per second. Read throughput for one partition tops out near batch size divided by GET latency, because the GETs run in sequence.

The missing pieces are coalesced reads, where adjacent batches from one segment come back in one ranged GET, parallel GETs within one partition, and a per-broker cache with read-ahead. Decision 5 deferred caches to a later version. That deferral is the thing to revisit.

### No wake-up path from commit to Fetch

A Fetch cannot wait, so the broker has no reason to learn that a partition grew. The commit path knows the new end offset at ack time. No signal carries that fact to brokers that serve reads. Long-poll needs that signal, or a short poll of end offsets behind it. This is the same cache problem seen from the other side.

## Metadata limits that affect consumers

`broker.metadata` returns one broker. It names the answering broker as leader, replica, and in-sync replica for every partition, and as controller.

A client that loses that broker must find another one from its own seed list. The control plane keeps no list of live brokers and has no heartbeat lease operation. `docs/TODO.md` covers this under "Metadata lists every live broker". The roadmap adds rack and zone data to the same item. Until then, failover speed is a client setting, and a one-broker seed list is a single point of failure at the client.

## Offset and retention limits

Earliest is always offset 0. Nothing deletes segment objects today, and no retention setting exists. `broker.listOffsets` returns 0 for earliest with no control plane call.

Two results follow. Storage grows without bound, and the read path can always reach offset 0. The second result ends the day retention starts. Retention also needs a garbage collector for segment objects that no run points at. `broker.onFetchGet` treats a missing committed segment as an unreachable state and fails the request. A lifecycle rule on the bucket can create that state today. Item S13 of the audit covers it. Retention, collection, and a log start offset that moves belong in one change, not three.

There is no log compaction. A client that expects a compacted topic reads the full history.

## Client matrix

| Client use | Works today | First blocker |
|---|---|---|
| franz-go, manual assign, offsets kept by the app | Yes | None on the read path. Lookup and GET rates still apply at scale. |
| franz-go, consumer group | No | FindCoordinator gets a closed connection. |
| Java `KafkaConsumer.assign` | Yes | None on the read path. |
| Java `KafkaConsumer.subscribe` | No | FindCoordinator. |
| librdkafka, assign | Yes | None on the read path. |
| librdkafka, subscribe | No | FindCoordinator. |
| Kafka Connect | No | Groups, then transactions for exactly-once sources and sinks. |
| Kafka Streams, ksqlDB | No | Groups, transactional producers, internal topics through CreateTopics. |
| Spark or Flink, group mode | No | FindCoordinator. |
| Spark or Flink, manual assign, offsets kept in the job | Yes, with the read rates above | Lookup rate and GET rate at high partition counts. |
| `kafka-console-consumer` with `--group` | No | FindCoordinator. |
| `kafka-console-consumer` with `--partition` and `--offset` | Yes | None. This is the fastest manual check of the read path. |

## Order of work already accepted

`docs/TODO.md` puts the read path before any consumer stage. `docs/roadmap-from-warpstream-teardown.md` prices it near 20 engineer-days and groups near 35 to 45 engineer-days after that.

1. Long-poll Fetch. Honor `MinBytes` and `MaxWait`.
2. Broker cache for end offsets and recent Lookup results, with a stated staleness rule.
3. Coalesced ranged GETs, parallel GETs in one partition, and a per-broker read cache.
4. Broker membership. Metadata returns every live broker.
5. Consumer groups and committed offsets. This step needs new control plane operations, a `veritas` spec for group state, and simulator coverage. Those three parts are the bulk of the estimate.
6. Retention and garbage collection, with a moving log start offset. Separate from groups, and it gates any long-retention claim.

Each step has a number to move. Lookup rate per idle consumer for step 1 and 2. GETs per second and cost per GiB read for step 3. Failover time without a seed list for step 4. A 64-consumer stage at the produce rate, with end-to-end p99, for step 5.

## Sources

- `kwire/supported.go` for the API list and versions.
- `broker/fetch.go`, `broker/ops.go`, `broker/metadata.go`, and `broker/conn.go` for the read path as built.
- `core/controlplane.go` for the three control plane operations.
- `docs/surprises-audit.md` items S3, S5, S6, and S13 for measured rates.
- `docs/TODO.md` for accepted work and `docs/roadmap-from-warpstream-teardown.md` sections 1 and 2 for estimates.
