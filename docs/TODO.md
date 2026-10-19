# TODO

Accepted work that is not scheduled yet. Each item says why it is needed and
what done means. Decisions it depends on are in `decisions.md`.

## Compression

**Done (decision 45).** The broker accepts gzip, snappy, lz4 and zstd and stores and serves the batches as opaque bytes. It validates the header only (magic, CRC, codec id, count against `lastOffsetDelta`) and never decompresses, so there is no codec dependency to audit. Fetch still patches only the base offset and leader epoch (decision 30). `MaxFrame` is 100 MiB so `max.request.size=64000000` fits. The sim workload mixes codecs, the `it/` tests run every codec and kgo's default, and the OMB driver defaults to lz4.

**Open:** an lz4 run of the real OMB build, and of the AWS benchmark, has not happened.

## Metadata lists every live broker

**Why:** Metadata lists only the broker that answered (decision 29). Client failover therefore depends on the client's seed list, and with franz-go defaults it takes about 5 s.

**Plan:**
- The CP keeps each live broker's advertised host, port and broker id. There is no Register and no broker epoch any more (decisions 41-44), so this needs a new heartbeat lease op that carries the node and its address.
  - A new `DescribeBrokers` op lists brokers whose lease has not expired.
- Metadata returns all live brokers. Partition leaders are spread over them deterministically, for example by hashing (topic, partition) over the sorted broker ids.
- Liveness comes from the heartbeat lease with an expiry, decided in the CP. The lease only feeds Metadata; it does not fence anything (commits stay unfenced, decision 44).

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

**DONE** (decision 39). The DynamoDB commit stores one range run item per (partition, segment) instead of one per batch, so the commit costs 2 + 2 x partitions + producer states + grown topics items whatever the number of batches. `MaxSegmentBatches` default is 256; the new `MaxSegmentItems` (96) bounds the producer states, the one remaining per-batch growth. The abstract contract (core, veritas) did not change; `cp/dynamo` and the sim CP engine's `MaxCommitItems` arithmetic agree, and `cp/cptest` covers segments with many small batches.

The producer-state follow-up is gone: the S# items no longer exist (decisions 41-44), so a commit costs 1 + partitions + grown topics and `MaxSegmentPartitions` (96) is the only item bound.

## Idempotent producers: explicitly not planned

Bringing back idempotent producers (InitProducerId, producer epochs and sequence numbers, duplicate detection in the CP) is **not planned**. The contract is stateless at-least-once (decisions 41-44): clients must disable idempotence (franz-go `kgo.DisableIdempotentWrite()`, Apache Kafka `enable.idempotence=false`), and the broker rejects idempotent batches with INVALID_REQUEST.
