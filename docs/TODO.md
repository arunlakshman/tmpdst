# TODO

Accepted work that is not scheduled yet. Each item says why it is needed and
what done means. Decisions it depends on are in `decisions.md`.

## Compression

**Why:** the broker rejects compressed batches (UNSUPPORTED_COMPRESSION_TYPE),
and franz-go compresses by default. Clients have to set
`kgo.ProducerBatchCompression(kgo.NoCompression())` (decision 33).

**Plan:**
- Accept gzip, snappy, lz4 and zstd.
- Decompress only to validate the batch: CRC, record count, last offset delta.
- Store and serve the batch bytes exactly as the producer sent them. Fetch keeps patching only the base offset and leader epoch (decision 30).

**Dependencies:** each codec library needs an audit record under `docs/dep-audits/` and an allow-list entry (decision I). Pure-Go codecs only.

**Done when:**
- `kwire` handles compressed batches.
- The broker admits them.
- The sim workload produces a mix of codecs.
- The `it/` tests run with kgo's default compression.

## Metadata lists every live broker

**Why:** Metadata lists only the broker that answered (decision 29). Client failover therefore depends on the client's seed list, and with franz-go defaults it takes about 5 s.

**Plan:**
- The CP keeps each registered broker's advertised host, port and broker id.
  - Register already carries the node, so the address needs to be added to it.
  - A new `DescribeBrokers` op lists live brokers.
- Metadata returns all live brokers. Partition leaders are spread over them deterministically, for example by hashing (topic, partition) over the sorted broker ids.
- Liveness comes from a lease or heartbeat with an expiry, decided in the CP.

**Contract changes:**
- `core` CP ops.
- `veritas` spec.
- `cpengine`.
- `cp/` service and DynamoDB items.
- Lockstep conformance (C3).

**Done when:**
- Kill-broker tests in `it/` fail over without the client's seed list and without `MetadataMinAge`.
- The sim covers broker membership changes.

## Go 1.26 and current franz-go

**Why:** `it/` pins franz-go v1.20.7 and kadm v1.17.1. Current franz-go needs Go 1.26 and kmsg v1.15, and the repo is on Go 1.24 with an audit record for kmsg v1.14.0 (decision J).

**Plan:**
- Move go.mod to Go 1.26.
- Bump kmsg and re-run its dependency audit (`scripts/audit-dep.sh`) with a new record.
- Bump franz-go and kadm.
- Re-check `kwire`'s Supported version table against the new kmsg.

**Done when:** `scripts/ci-determinism.sh`, `go test ./...` (including `it/`) and a 300-seed campaign pass. Trace hashes may change if kmsg encoding changes; review any re-baseline by hand.

## CP range compaction

**Why:** the DynamoDB commit stores one range item per batch and is limited to 100 items, so a segment is capped at 32 batches (decision 35) however large it could be. A 16 MiB segment of small batches is cut short by the batch cap, not by size.

**Plan:** store one range item per contiguous run of same-partition batches in a segment, so large segments with many small batches are not capped by the 100-item transaction limit. This is a contract change in core/veritas/cp. Idempotence windows must stay per batch.

**Done when:**
- `MaxSegmentBatches` can be raised well above 32 with the DynamoDB CP.
- The spec, the sim CP engine (`MaxCommitItems` arithmetic) and `cp/dynamo` agree.
- `cp/cptest` covers segments with many small batches.
