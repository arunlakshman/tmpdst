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

## Consumer read path (needed before any consume stage; not scheduled)

**Why:** the audit (docs/surprises-audit.md, items 3, 5, 6, 7) found that consumer stages would fail before measuring anything: Fetch never waits (MinBytes/MaxWait ignored), so every partition of every Fetch is a control-plane Lookup (one idle consumer made 20k Lookups/s locally; about 48k consistent reads/s predicted on AWS), the read path does one S3 GET per batch with no cache (about 6.5k GET/s at 1 GiB/s, above S3's 5,500/s starting rate per prefix), and benchmark consumers start at offset 0 of a topic that earlier stages filled.

**Plan (the product owner agreed a cache is needed, "not now"):**
- Fetch long-polling that honours MinBytes/MaxWait, so idle consumers stop busy-polling.
- A Lookup result cache in the broker (end offsets and recent runs, invalidated by commits it knows about, bounded staleness).
- A broker-side S3 read cache with read-ahead and coalescing of adjacent batches in one segment into a single ranged GET.
- Control-plane read capacity: check the DynamoDB table quotas in ap-south-2 (30k warm throughput, 40k default) before a consume stage.
- Benchmark workloads: consumers start at the tail (or use a fresh topic per stage).

**Done when:** a 64-consumer stage at the produce rate runs without Lookup or GET storms, with measured GET/s, Lookup/s and cost per GiB reported in the summary.

## Per-broker partition ownership: declined

The audit (item 1) showed that at 288 partitions every segment overlaps every other, so the single sequencer cannot pipeline and its bound is about transactions/s x ~3 segments per transaction. Giving each broker its own partition group would raise the modelled ceiling to 140-191 commits/s, but it breaks the leaderless Metadata design (decision 29). **Declined by the product owner**: brokers stay interchangeable. Revisit only if measured commit rate on real DynamoDB is the limiting factor.

## Move the index to S3 (accepted, not scheduled)

**Why:** with design A (decisions 48-50) the control plane's materialised state (partition ends, runs for Lookup, the key window) is rebuilt from the DynamoDB log plus a checkpoint, and the `R#` index rows are written to DynamoDB every 10 s. That is fine for hours of retention. Memory and DynamoDB item cost grow with retention (about 6 GB per hour of index-able batches at 6,450 batches/s, 5.6 GB per day with a compact index), so long retention needs the index out of the CP's memory and out of DynamoDB. The Conflux model (idea B of `docs/delos-logdrive-ideas.md`) does this: the log is short, state lives in object storage.

**Plan:**
- The applier checkpoints its state to S3 (a checkpoint object per slot, written by the appender or the standby, never required for safety).
- The index becomes immutable index files in S3 (runs per partition range, packed as in `cp/dynamo/runs.go`), written by the indexer, listed by a small manifest in the checkpoint.
- The log is trimmed behind the newest checkpoint, but never closer than the key window (decision 48: window = log retention, at least 2 x `CommitDeadline` plus the checkpoint interval).
- A Lookup below the in-memory tail reads the index file for that partition range; the newest runs stay in memory.
- A restart loads the latest checkpoint and replays the log tail; `R#` rows in DynamoDB are removed.

**Contract changes:** none for brokers (Lookup keeps its shape). `cp/` service, indexer, checkpoint format, the sim CP engine for the log.

**Done when:** a CP restarts from an S3 checkpoint plus a short log tail with the index larger than memory; Lookup tail latency is reported for in-memory and S3-resident ranges; the log holds minutes, not hours; the sim covers a restart from a checkpoint.
