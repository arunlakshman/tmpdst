# Kafka compatibility

MDS speaks a subset of the Kafka protocol. Classic consumer groups only. This
page is the user-facing summary of version ranges and intentional differences
from Apache Kafka.

## Supported API version ranges

The table lives in [what-works-in-0.1.0.md](what-works-in-0.1.0.md). Source of
truth: `broker/internal/wire/kafkawire/supported.go`. If docs and code disagree,
the code wins.

Sixteen APIs are served (Produce through DescribeConfigs, including ApiVersions).
Anything else closes the connection with no error body (ApiVersions is the
exception: unsupported version still gets `UNSUPPORTED_VERSION`).

KIP-848 (`group.protocol=consumer` / ConsumerGroupHeartbeat) is **not**
advertised and is not shipped in 0.1.0.

## Compression

Record batches are stored and fetched as **opaque bytes**. The broker accepts
gzip, snappy, lz4, and zstd (attribute bits) and never decompresses. CRC and
length checks use the batch header; clients decompress on read. No codec library
runs on the broker path.

## Classic groups: intentional diffs

MDS aims to match open-source Apache Kafka for the classic group protocol. These
behaviors differ on purpose (or are not implemented) and are visible to clients:

| Topic | Apache Kafka (classic) | MDS 0.1.0 |
|---|---|---|
| Unchanged follower rejoin in Stable / CompletingRebalance | Often answered at once with the current generation (no new round); leader rejoin in Stable starts a round | Any JoinGroup on Stable or CompletingRebalance opens a new rebalance immediately |
| Empty round | Generation still advances when the group becomes Empty | Generation kept; group goes Empty |
| Protocol choice | Members vote among protocols everyone supports | First protocol in the leader's list that every member has |
| Protocol mismatch on join | Refused immediately with `INCONSISTENT_GROUP_PROTOCOL` when no common protocol | Protocol *type* checked at once; a joiner with no common protocol is parked, then the round ends with `INCONSISTENT_GROUP_PROTOCOL` |
| SyncGroup assignment | Missing members get an empty assignment; no strict check | Leader sync validated: unknown assigned member or duplicate assignment is an error |
| Manual / simple groups | Generation < 0 on Empty can create a "simple group"; List/Describe show it | Manual OffsetCommit does not create a group; Describe says Dead; List omits it |
| Describe outside Stable | Often omits protocol / member metadata outside Stable | Describe returns logged protocol, metadata, and assignment with the current memory state name |
| `MEMBER_ID_REQUIRED` (KIP-394) | Empty member id (v4+) gets a required-id challenge, then a second join | Empty id creates the member and joins in one step |
| Static membership (`group.instance.id`) | Supported on classic paths | Instance id stored, not used; dynamic members only |

FindCoordinator for groups hashes the group name (FNV-1a 32-bit) over the sorted
live broker list. Transaction keys are not coordinated
(`COORDINATOR_NOT_AVAILABLE`).

CreateTopics and DescribeConfigs quirks (ignored replication factor and configs,
empty config lists) are listed under
[what-works-in-0.1.0.md](what-works-in-0.1.0.md).

## Related

- [What works in 0.1.0](what-works-in-0.1.0.md)
- [Architecture](architecture.md)
- [Getting started](getting-started.md)
