# What works in 0.1.0

MDS 0.1.0 is a Kafka-protocol broker plus a control plane. You can create
topics, produce, fetch, and use classic consumer groups. The surface is the
sixteen APIs below - nothing else is advertised as shipped.

If this table disagrees with
`broker/internal/wire/kafkawire/supported.go`, **the code wins**.

## Supported Kafka APIs

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3 to 9 |
| Fetch | 1 | 4 to 12 |
| ListOffsets | 2 | 1 to 7 |
| Metadata | 3 | 1 to 12 |
| OffsetCommit | 8 | 0 to 8 |
| OffsetFetch | 9 | 0 to 8 |
| FindCoordinator | 10 | 0 to 4 |
| JoinGroup | 11 | 0 to 7 |
| Heartbeat | 12 | 0 to 4 |
| LeaveGroup | 13 | 0 to 5 |
| SyncGroup | 14 | 0 to 5 |
| DescribeGroups | 15 | 0 to 5 |
| ListGroups | 16 | 0 to 4 |
| ApiVersions | 18 | 0 to 3 |
| CreateTopics | 19 | 0 to 7 |
| DescribeConfigs | 32 | 0 to 4 |

FindCoordinator accepts group keys only. DescribeConfigs answers for topics
only and returns an empty config list for each existing topic.

A client that assigns partitions itself and reads with Fetch can consume without
a group. franz-go with `kgo.ConsumePartitions` takes that path.

## Classic consumer groups

Classic protocol only. **KIP-848 is not advertised.** A client with
`group.protocol=consumer` gets no answer to ConsumerGroupHeartbeat.

- Group state lives in the control plane. Any broker can answer any group call.
  FindCoordinator steers the client: FNV-1a 32-bit hash of the group name,
  modulo the sorted live broker list.
- A transaction coordinator key gets `COORDINATOR_NOT_AVAILABLE`. An empty group
  id gets `INVALID_GROUP_ID`.
- OffsetCommit and OffsetFetch store offsets in the control plane. A commit with
  generation `-1` and no member is a manual commit, allowed only when the group
  has no members.
- ListGroups lists only groups this broker coordinates (so an admin client that
  asks every broker sees each group once).
- DescribeGroups answers any group. An unknown group is `Dead` with no error.
- `MEMBER_ID_REQUIRED` is not used: a new member gets a generated id and joins
  at once.

More intentional diffs vs Apache Kafka: [kafka-compatibility.md](kafka-compatibility.md).

## CreateTopics quirks

- Replication factor is accepted and ignored (object storage is the replication).
- Topic configs are ignored; one warning is logged per request that has them.
- An existing topic gets `TOPIC_ALREADY_EXISTS`. Check and create are one control
  plane step, so two creates of one new name cannot both succeed. A retry after a
  lost reply that finds the topic counts as success.

`kafka-topics.sh --create`, `--list`, and `--describe` work. DescribeConfigs is
served because `--describe` calls it.

## Fetch (user-visible)

- Long poll: `MaxWait` (capped at 30 s) and `MinBytes` are honored. Default poll
  interval is 200 ms (`-fetch-poll-interval`).
- Fetch sessions are not granted (`SessionEpoch` above 0 gets
  `FETCH_SESSION_ID_NOT_FOUND`).
- Fetch stops at version 12 (topic names, not topic IDs).
- ListOffsets supports earliest (`-2`) and latest (`-1`) only. Other timestamps
  get `UNSUPPORTED_FOR_MESSAGE_FORMAT`.
- Earliest offset is always 0. No retention delete and no log compaction in 0.1.0.

## Unsupported requests

A request whose key or version is outside the table gets **no error payload**:
the broker closes the connection. Only an out-of-range ApiVersions request still
gets a response (`UNSUPPORTED_VERSION`).

Not in the table (and therefore not served): InitProducerId, transactions,
KIP-848 group APIs, and every other Kafka API key.

## Checking it

`ops/broker/scripts/e2e-kafka-cli.sh` exercises create/list/describe topics,
produce, consume with a group, commit offsets, rebalance, and
`kafka-consumer-groups.sh --list` / `--describe`.

## After 0.1.0

Post-release clean-up and follow-ups are tracked in
[MDS-11](https://manifester-e50593c9.atlassian.net/browse/MDS-11). That ticket is
not required to use the OSS build.
