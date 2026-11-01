# Reverse engineering `arun:latest` — the WarpStream Agent: high-level architecture, control-plane interaction, and object-store file history

> **Target image:** `arun:latest`
> **Image ID:** `sha256:65a6e2969262e00b0d37db8d5c9f33ad64f302701e041f2b25d454b73fc3ba98`
> **Also tagged locally as:** `public.ecr.aws/warpstream-labs/warpstream_agent:latest-stable`
> **Reported version (`warpstream version`):** `WarpStream Agent Version: v842`
> **Binary build:** `github.com/warpstreamlabs/warpstream` @ `v0.0.0-20260917125755-52745a6d3d70`, git commit `52745a6d3d70da714eb2aec0e164b0c828df4b3d`, git time `2026-09-17T12:57:55Z`, `go1.27.1-X:simd`, `linux/arm64`
> **Date of this study:** 2026-10-21
> **Workspace note:** there is no image named `arin:latest` on this machine (`docker inspect arin:latest` → *No such image*). `arun:latest` is the only 689 MB / 205 MB image matching the description the user pasted, and it is byte-identical (same image ID) to the public WarpStream agent image. **This entire document is therefore about `arun:latest`. If you meant a different, private `arin` image, none of what follows applies to it — point me at that image / tarball and I will redo it.**

---

## 0. How to read this document, method, and confidence tags

This is a **binary-only reverse-engineering study**. No WarpStream account, API key, bucket, or hosted control-plane call was used. Nothing was sent to `warpstream.com`. The work was done entirely offline on this Mac, in this order:

1. **Docker metadata** — `docker inspect`, `docker history --no-trunc` (image config, entrypoint, layers, build provenance).
2. **Filesystem extraction** — `docker create` + `docker export`, then listing the rootfs. The image is remarkably small in *content*: one 425 MB static Go binary plus a Wolfi base.
3. **Running the binary inside Docker** — every subcommand's `-help` output was captured (`agent` alone exposes **157 flags**), plus `version`.
4. **Go build info** — `go version -m /warpstream` (module path, exact pseudo-version, ~490 dependency lines).
5. **Symbol table** — the binary is **not stripped and carries `debug_info`**, so `go tool nm` yields **54,858 functions in WarpStream's own packages** alone. Package, type, receiver and method names in this document come from there. This is the single richest source: e.g. the control-plane client is one type, `pkg/streamclient.(*httpStreamClient)`, with **161 methods**, and every one of them is listed in Appendix B.
6. **Strings** — `strings -a` (≈2.5 M lines, 90 MB) mined for URLs, domains, env-var names (`WARPSTREAM_*`), protobuf file names, HTTP paths (`/v1/agent/...`), and error/log phrases.
7. **A live smoke test** — `warpstream local` (the fully in-memory mode) was booted, a topic created, records produced and fetched back with the image's own `kcmd` tool, all inside a container network. The observed output is quoted verbatim in §3.3. This converts several "the symbols suggest…" claims into "we watched it do this."

Every factual claim is tagged:

| Tag | Meaning |
|---|---|
| **[IMG]** | Observed in Docker image metadata / layer history / rootfs listing |
| **[BIN]** | Observed in the binary: symbols (`go tool nm`), `go version -m`, or strings |
| **[RUN]** | Observed by actually running the image (`-help`, `version`, `local` smoke test) |
| **[INF]** | My inference from the above. Plausible, architecturally consistent, **not directly observed** |
| **[PUB]** | Cross-check against WarpStream's public documentation / this repo's earlier public-sources-only study (`docs/warpstream-architecture-comparison.md`). Used only to sanity-check, never as the primary source |

A deliberate consequence: where the binary and public docs disagree, **the binary wins and the disagreement is called out** (see the batch-size defaults in §8.2 — the flag help in v842 says 1 MiB compressed, older public docs say 4–8 MiB).

**Legal/scope note.** This study uses only the image's public surface as shipped on this machine (manifest, help text, symbol/string tables, and running it locally with no credentials). No disassembly/decompilation of logic, no hosted-service probing, no credential use. Method names and flag names are facts about the artifact; the *server-side* implementation of the hosted control plane is not in this image in executable form (but see §10.3 — a surprising amount of shared state-machine code *is* linked in, because the same codebase powers `local`/`playground` mode and, evidently, the hosted backend).

---

## 1. Executive summary (the whole architecture on one page)

WarpStream replaces Kafka's broker-and-disks design with three planes, and this image contains exactly one of them — the **agent** — plus embedded, in-memory stand-ins for the other two so it can run standalone in `local`/`playground` mode.

```
                        ┌─────────────────────────────────────────────────────────┐
                        │              HOSTED CONTROL PLANE (WarpStream cloud)     │
                        │  "Virtual Cluster" = replicated state machine (RSM)      │
                        │  per customer cluster: owns ALL metadata, assigns ALL    │
                        │  offsets, tracks every file, groups, producers, schemas, │
                        │  jobs. Journaled, snapshotted. Agents reach it over      │
                        │  HTTPS protobuf RPC (protoOverHTTP) at                   │
                        │  metadata.<region>.warpstream.com (or -metadataURL).     │
                        └───────▲───────────────────────────▲──────────────────────┘
                   metadata RPC │ (agentKey auth,         │ jobs: PollJobs/AckJobs,
                   (PutFile,    │  heartbeat, discovery)  │ compaction, dead-scan,
                    GetFiles,   │                         │ schema, query, datalake…
                    GetRecords…)│                         │
                        ┌───────┴───────────────────────────┴──────────────────────┐
   Kafka clients ──────▶│                    AGENT  (this image)                   │
   (producers,          │  stateless Go binary, roles = "proxy, jobs"              │
    consumers,          │  • Kafka protocol server :9092 (Produce/Fetch/Metadata/  │
    admin, SR clients)  │    JoinGroup/SyncGroup/SASL…)                            │
                        │  • batcher: many partitions → ONE file, 250 ms / ~1 MiB  │
                        │  • distributed in-memory file cache (peer-to-peer,       │
                        │    chunked, AZ-aware), HTTP :8080                        │
                        │  • job runners: compaction, dead-file scanning, Table-   │
                        │    flow/datalake, pipelines, query engine, ripcord…      │
                        │  • NO local disk state for data (file cache is RAM)      │
                        └───────┬──────────────────────────────────────────────────┘
                                │ PUT file → then commit metadata → then ack
                                │ GET range reads (footer + chunks), signed URLs
                        ┌───────▼──────────────────────────────────────────────────┐
                        │        CUSTOMER OBJECT STORE  (BYOC: customer's VPC)      │
                        │  s3:// / gs:// / Azure Blob / mem:// — the ONLY place     │
                        │  record bytes ever live. Files are immutable, multi-      │
                        │  partition, self-describing (header/pages/footer+index).  │
                        │  Lifecycle: written → indexed by CP → compacted into      │
                        │  new files → old files obsolete → dead → deleted by       │
                        │  deadscanner agents. The CP's file index IS the history.  │
                        └───────────────────────────────────────────────────────────┘
```

The five ideas that explain everything else in this document:

1. **The agent is stateless; the control plane is the system.** An agent holds no durable state. Everything Kafka normally keeps on broker disks — partition logs, offsets, consumer-group state, producer (idempotence/transaction) state, topic config, ACLs, schemas — is a record in a hosted replicated state machine the code calls the **Virtual Cluster**. The agent binary even embeds that state machine (`pkg/stream/statemachine`, ~9,400 symbols, §10) because `local` mode must *be* the control plane, in memory.

2. **Object storage is the log; the control plane is the log's index and history.** Record bytes are batched across *many topic-partitions into one immutable file*, PUT to the customer's bucket, and **only then** committed to the control plane via a `PutFile` RPC carrying the file's per-partition batch ranges. The control plane assigns offsets *at commit time* and appends the file to its indexed history. A produce is acked only after both steps. Reads are the mirror image: ask the control plane "which files hold offsets ≥ X?" (`GetFiles`), then range-GET those files (usually from a peer agent's RAM cache, not from S3).

3. **Agents find each other and get work through the control plane.** `Heartbeat`, `Discover`/`DiscoverBatch` (service discovery with a node-pool tracker, blob-backed fallback), and a generic **jobs** system (`PollJobs`/`AckJobs`) turn the agent fleet into a work pool: the control plane decides *which* agent compacts *which* files, which agent scans a bucket for dead files, which agent runs a Tableflow/Iceberg job, a data pipeline, a query, etc. The default `-roles "proxy, jobs"` and the flag docs' talk of "splitting agent roles" confirm agents are meant to be specialised by role in larger deployments [RUN][PUB].

4. **The file lifecycle *is* the retention/compaction/GC story.** Small files are merged by compaction jobs into larger ones (levelled, planner-driven, §15); superseded inputs become *obsolete*; retention/`DeleteRecords` *expire* ranges; the control plane's `DeadFiles` view plus a bucket-scanning **deadscanner** job reconcile "what the index says is dead" with "what is actually in the bucket" and delete the bytes. Nothing is ever mutated in place.

5. **BYOC means the data plane never leaves the customer.** The only WarpStream-cloud traffic is metadata RPC, heartbeats, metrics/logs/diagnostics upload (all disablable via flags [RUN]), and job control. Record bytes flow client → agent → customer bucket, and agent ↔ agent for cache hits. The binary's cloud SDK surface (AWS S3, GCS, Azure Blob, §11) and `mem://` buckets for local mode are all customer-side.

---

## 2. Image anatomy: what `arun:latest` physically is

### 2.1 Identity and provenance [IMG]

| Property | Value |
|---|---|
| Image ID | `sha256:65a6e2969262…` |
| Tags | `arun:latest`, `public.ecr.aws/warpstream-labs/warpstream_agent:latest-stable` (same ID — `arun` is a local re-tag) |
| Created | `2026-09-17T13:19:13Z` (image); binary git time `2026-09-17T12:57:55Z` |
| Architecture / OS | `arm64` / `linux` (an `amd64` sibling presumably exists upstream; not present locally) |
| Size | 689 MB virtual / 205 MB content (the binary is 445,856,696 bytes ≈ 425 MiB uncompressed; it compresses well) |
| Entrypoint | `["/warpstream"]`, no default `Cmd`, `WorkingDir=/app`, `User=0` (root) |
| Base image | Chainguard **Wolfi** (`dev.chainguard.image.title: wolfi-base`, apko-built), `DockerVersion 27.3.1`, buildkit Dockerfile |
| Extra packages | `apk add --no-cache curl netcat-openbsd` on top of Wolfi base [IMG history] |
| Files of note in rootfs | `/warpstream` (the binary), `/notices.txt` + `/non_ibm_license.txt` (IBM WarpStream third-party notices — WarpStream is now an IBM product), CA bundle, busybox userland. **No config files, no data directories, no shell scripts, no second binary.** `/app` is an empty working dir |

Layer history (top → bottom, condensed) [IMG]:

```
ENTRYPOINT ["/warpstream"]
COPY services/agent/non_ibm_license.txt /non_ibm_license.txt     57 kB
COPY notices.txt /notices.txt                                    57 kB
RUN  stat /warpstream ; chmod +x /warpstream
COPY ./warpstream_agent_linux_arm64 /warpstream                  446 MB   ← the entire product
WORKDIR /app
ARG  BINARY_PATH=./warpstream_agent_linux_arm64
RUN  apk add --no-cache curl netcat-openbsd                      22.6 MB
apko … (11 Wolfi base layers)                                    ~17 MB
```

**Reading:** this is the classic "one static binary in a minimal rootfs" distribution. There is nothing to configure by file editing inside the image; all configuration is flags/env (§5). The presence of `curl`/`nc` is for in-container health-checks/debugging by operators, not for the data path [INF].

### 2.2 The binary [BIN]

```
$ file warpstream
ELF 64-bit LSB executable, ARM aarch64, statically linked, for GNU/Linux 3.7.0,
Go BuildID=xREcAZR7jauPuvybPla2/…, with debug_info, not stripped

$ go version -m warpstream
path    github.com/warpstreamlabs/warpstream/services/agent/app
mod     github.com/warpstreamlabs/warpstream  v0.0.0-20260917125755-52745a6d3d70
go      go1.27.1-X:simd
```

Because it is not stripped, `go tool nm` exposes the full internal package architecture. Aggregating WarpStream's own symbols by package (generics-inflated, but the ranking is meaningful):

| Package (under `github.com/warpstreamlabs/warpstream/`) | Symbols | What it is (detail in §6) |
|---|---:|---|
| `pkg/stream/pb/streampb` | 10,789 | Protobuf types for the agent↔control-plane stream API (requests/responses, file metadata, datalake `DL*`) |
| `pkg/stream/statemachine` (+`/datalake`) | 9,397 (+1,528) | The Virtual Cluster state machine itself: files, streams, groups, producers, transactions, schemas |
| `pkg/stream/pb/agentpoolpb` | 4,633 | Protobuf for the agent-pool/jobs protocol (job inputs/outputs, heartbeats, agent stats) |
| `pkg/diagnostics/…` | ~2,800 | Metrics/diagnostics collection, scrapers, forwarders to the backend |
| `pkg/kafka/types` | 2,609 | Kafka wire protocol types |
| `pkg/cache`, `pkg/futures` | 2,229 / 2,297 | Generic async/cache plumbing used everywhere |
| `pkg/kafka/agent` | 1,070 | The Kafka protocol handler in the agent (`AgentHandler`, batching client) |
| `pkg/compaction` | 661 | Compaction planners/executors |
| `pkg/blob` | 659 | Object-store abstraction (S3/GCS/Azure/DynamoDB/SQL/mem/multi-bucket) |
| `pkg/warpcluster` | 650 | Cluster routing, RSM actor RPC, service-discovery routing (backend-side, shared) |
| `pkg/rsm`, `pkg/rsmy` | 677 / 341 | Generic replicated-state-machine framework (proposer, log store, snapshots) |
| `pkg/streamclient` | 224 funcs / one huge interface | **The control-plane client** (`httpStreamClient`, 161 methods) |
| `pkg/filecache` | 138 | Distributed in-memory file cache |
| `pkg/servicediscovery` | 151 | Agent discovery (node pool tracker, heartbeater, blob backend) |
| `pkg/deadscanner` | 87 | Bucket scanner that deletes dead files |
| `pkg/jobs` | 284 | Job runner framework (poll/ack, balanced queues) |
| `pkg/schemaregistry`, `pkg/datalake/*`, `pkg/query/*`, `pkg/pipelines`, `pkg/orbit`, `pkg/ripcord/*`, `pkg/acls`, `pkg/transaction` … | hundreds each | Secondary subsystems, §17 |

Notable third-party dependencies from `go version -m` [BIN] (full list ~490 lines): `aws-sdk-go-v2` (S3, DynamoDB, STS…), `cloud.google.com/go/storage`, `Azure …/azblob`, `gocloud.dev`, `apache/iceberg-go` + `parquet-go` (Tableflow), `IBM/sarama` (Kafka client, for `kcmd`/`cli`), `apache/pulsar-client-go`, `ClickHouse/clickhouse-go`, `elastic/go-elasticsearch`, `DataDog/dd-trace-go`, OpenTelemetry, Prometheus. **Dependency presence ≠ agent data-path use** — this one binary also ships the CLI tools, benchmarks, pipeline connectors (Bento-style, hence the enormous connector surface), and Tableflow, which drag in most of that list. I flag below every place where I rely on a dependency as evidence.

---

## 3. Process model: one binary, eleven subcommands

### 3.1 Subcommand surface [RUN]

Running with no args (or an unknown one) prints the dispatch table verbatim:

```
no sub command provided, try one of:
    cli            cli-beta (deprecated alias)   cli-old (deprecated)
    kcmd           srcmd
    demo           playground                  local
    agent          export                      version     metrics
```

| Subcommand | Purpose (observed) | Needs hosted CP? |
|---|---|---|
| **`agent`** | The production agent: Kafka proxy + job runner against the hosted control plane and a customer bucket. 157 flags. | **Yes** — `-agentKey`, `-bucketURL`, `-region`/`-metadataURL` |
| `playground` | Hosted trial mode: signs up via `https://api.warpstream.com`, bucket defaults to `mem://warpstream_playground`, but the control plane is WarpStream's. 4 h default timeout. | Yes (hosted, in-mem bucket) |
| `demo` | Like playground with a real bucket required, 1 h timeout. | Yes |
| **`local`** | **Fully offline**: "a fake, fully in-memory control plane… all data and state is ephemeral". Bucket `mem://warpstream_local`. Boots Kafka + Schema Registry + Tableflow personalities in one process. | **No** |
| `cli` | Operator Kafka CLI with 25 sub-verbs: `create-topic`, `console-producer/consumer`, `benchmark-producer/consumer`, `file-reader`, `file-scrubber`, `diagnose-connection`, `diagnose-record`, `create/delete/describe-acls`, `consumer-group-lag`, `validate-pipeline-config`, … | Talks Kafka to any cluster |
| `kcmd` | Older/smaller Kafka tool: `produce, fetch, create-topic, list-topics, delete-topic, commit-offsets, describe-groups, list-offsets, diagnose-connection, …` (17 types) | Kafka only |
| `srcmd` | Schema-registry tool (`diagnose-connection`). | — |
| `export` (`export-file`) | Dump one object-store file (`-file-path s3 path`) to JSON — a forensic/inspection tool for the file format. | Bucket only |
| `metrics` | Standalone scraper: polls the WarpStream backend / CP Prometheus endpoint every minute (`-loopInterval`) and re-publishes metrics locally on :8080. | CP/console API |
| `version` | Prints `WarpStream Agent Version: v842`. | No |

### 3.2 Listening ports (defaults) [RUN]

| Port | Owner | Notes |
|---:|---|---|
| **9092** | Kafka protocol (`-kafkaPort`) | TLS optional (`-kafkaTLS`, mTLS, PROXY-protocol v2 support) |
| **8080** | Agent internal HTTP (`-httpPort`) | Distributed file cache peer traffic, Prometheus metrics, `/v1/status`-style health, HTTP produce/fetch handlers (`HTTPProduceHandler`, `HTTPFetchHandler` in `pkg/kafka/agent`) |
| 8443 | Internal HTTPS (`-httpsPort`, only with `-internalTLS`) | Same internal server over TLS/SPIFFE |
| 9094 / 8070 | Schema Registry protocol / its internal HTTP (`playground`/`local`) | Confluent-compatible SR personality |
| 8081 | Tableflow internal HTTP (`playground`/`local`) | Datalake/Iceberg personality |
| 9999 | "Cluster-wide environment" (`-clusterWideEnvironmentPort`, off by default) | Shared config/env service among agents [INF from flag text] |

### 3.3 Live smoke test: `local` mode actually works as advertised [RUN]

This is the strongest single piece of evidence in the study, because it exercises the *same* code paths as `agent` mode with the control plane swapped for the in-memory one:

```
$ docker run -d --name ws-local arun:latest local -timeout 0
WARNING, RUNNING IN LOCAL MODE. This mode uses a fake, fully in-memory
control plane and requires no external connectivity; all data and state
is ephemeral and will be lost on restart.
Starting in-memory control plane...
Starting local agents...
You now have WarpStream Kafka, Schema Registry, and Tableflow clusters
running locally (no hosted control plane).
Ports:  Kafka 9092 / HTTP 8080 · SR 9094 / 8070 · Tableflow 8081

$ docker run --rm --network container:ws-local arun:latest \
    kcmd -type create-topic -topic demo-re -partition-count 3
created topic "demo-re" successfully, topic ID: MQAAAAAAAAAAAAAAAAAAAA==

$ … kcmd -type produce -topic demo-re -records "hello-re,,world-re"
result: partition:0 offset:0 value:"hello-re"
result: partition:0 offset:1 value:"world-re"

$ … kcmd -type fetch -topic demo-re -partition 0 -consume-from-beginning
result: partition:0 offset:0 key:"hello" value:"hello-re"
result: partition:0 offset:1 key:"hello" value:"world-re"
```

What this proves [RUN]: (a) the in-memory control plane is a real, working Virtual Cluster (it created the topic, assigned offsets 0,1 at commit, and served the fetch); (b) the Kafka surface is genuinely implemented, not a stub; (c) the architecture is *control-plane-pluggable* — `httpStreamClient` against `metadata.…` in production, an embedded RSM in `local` — which is exactly the seam §10 describes. It also shows produce→fetch round-trips work with a `mem://` bucket, i.e. **the bucket is just `pkg/blob.Store`** (§11).

---

## 4. High-level architecture

### 4.1 Component map

```
┌─────────────────────────────── CUSTOMER VPC / CLOUD ACCOUNT ───────────────────────────────┐
│                                                                                            │
│  Kafka clients ──TCP 9092──▶ ┌──────────────────── AGENT (×N, this image) ───────────────┐   │
│  (producers, consumers,      │ Kafka server (pkg/kafka/server + pkg/kafka/agent)         │   │
│   admin, ksqlDB, SR clients) │   Produce / Fetch / Metadata / JoinGroup / SyncGroup /    │   │
│                              │   Heartbeat(Group) / SASL / ACL checks / transactions   │   │
│                              │        │                    ▲                             │   │
│                              │        ▼                    │ ranges                    │   │
│                              │ Batcher (pkg/processor, pkg/stream/batcher)             │   │
│                              │   cross-partition batching, 250 ms / size flush,        │   │
│                              │   backpressure per vCPU (bytes & files in flight)       │   │
│                              │        │ PUT                │ GET range                 │   │
│                              │        ▼                    │                             │   │
│                              │ Object-store client (pkg/blob → S3/GCS/Azure/mem/…)     │   │
│                              │        │                    ▲                             │   │
│                              │ Distributed file cache (pkg/filecache) ◀──HTTP 8080──▶  │◀── peer agents
│                              │   chunked RAM cache, footer cache, consistent routing,  │   │   (same AZ
│                              │   extra replicas; miss → range-GET bucket               │   │    preferred)
│                              │                                                           │   │
│                              │ Job runners (pkg/jobs): compaction · deadscanner ·      │   │
│                              │   Tableflow/datalake · pipelines (Bento) · query engine │   │
│                              │   · ripcord replay · schema migrator · orbit (proxy)    │   │
│                              │ Service discovery client (pkg/servicediscovery):        │   │
│                              │   heartbeat self, Discover peers (power-of-2 choices)   │   │
│                              └──────┬───────────────────────────────────────▲───────────┘   │
│                                     │ record bytes ONLY                       │ metadata    │
│                              ┌──────▼────────────────┐                      │ only        │
│                              │ CUSTOMER OBJECT STORE │                      │             │
│                              │ bucketURL (s3/gs/az)  │                      │             │
│                              │ immutable files,      │                      │             │
│                              │ compacted files,      │                      │             │
│                              │ SR bucket, snapshots… │                      │             │
│                              └───────────────────────┘                      │             │
└─────────────────────────────────────────────────────────────────────────────┼─────────────┘
                                                                               │ HTTPS
                                          protobuf-over-HTTP, agentKey auth,   │ (TLS)
                                          retries, per-region endpoint         │
┌──────────────────────────────────────────────────────────────────────────────▼─────────────┐
│                        WARPSTREAM HOSTED CONTROL PLANE (not in this image)                  │
│  Per-cluster "Virtual Cluster" replicated state machine (pkg/stream/statemachine, pkg/rsm): │
│  streams(=topics/partitions) · file index & file metadata · offset assignment · producers   │
│  (idempotence, txns) · consumer groups · ACLs · schemas · KV · jobs queue · feature flags   │
│  Agent pool: heartbeats, discovery registry, job scheduling, compaction planning authority  │
│  Fronted by metadata.<region>.warpstream.com / api.warpstream.com / console.warpstream.com  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 The three planes, in one table

| Plane | Lives where | Carries | Durability / consistency owner |
|---|---|---|---|
| **Data plane** | Agents + customer bucket | Record bytes (batched files), cache chunks peer-to-peer | Bucket PUT completes **before** commit; file immutable thereafter |
| **Metadata (control) plane** | WarpStream cloud, per-cluster RSM | File index, offsets, groups, producers, schemas, config, jobs | RSM journal; offset assignment is the single serialisation point |
| **Coordination plane** (a facet of the CP) | WarpStream cloud + agent gossip via CP/blob discovery | Agent registry, heartbeats, job offers, feature flags, diagnostics/metrics ingest | Best-effort, heartbeat-refreshed; never on the ack path except discovery |

### 4.3 Why it is shaped this way [INF, anchored in BIN/RUN]

Kafka's cost drivers are broker disks, cross-AZ replication traffic, and per-partition leader state. WarpStream's bet, visible in the code layout, is: *object storage is cheap, durable and AZ-redundant already; what is actually hard is metadata — so centralise exactly that, make it a proper replicated state machine, and make everything else a stateless cache/compute fleet.* Hence: no raft among agents, no partition leaders, no local log segments; instead a batcher that amortises S3 PUTs across partitions, a RAM cache that amortises S3 GETs across consumers in an AZ, and a CP whose load scales with **batches/files, not bytes** (the flag help repeats "lower values reduce latency, but cost more money" on every batching knob [RUN]).

---

## 5. Configuration surface: 157 flags and the `WARPSTREAM_*` environment

`agent -help` [RUN] is the operator contract. The binary also honours `WARPSTREAM_<FLAG_NAME>` env vars (43 distinct `WARPSTREAM_*` strings in the binary [BIN], e.g. `WARPSTREAM_BUCKET_URL`, `WARPSTREAM_REGION`, `WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID`, `WARPSTREAM_DISABLE_S3_CHECKSUMS`); `services/agent/agent.GetUnregisteredWarpStreamEnvVars` [BIN] suggests it even audits env vars that look WarpStream-ish but map to no flag. Grouped summary (full list in Appendix A):

| Group | Representative flags | Architectural signal |
|---|---|---|
| **Identity / CP connection** | `-agentKey` / `-agentKeyPath` / `-apiKey` (alias), `-region`, `-metadataURL`, `-multiregion`, `-multiregionMetadataURLs`, `-defaultVirtualClusterID`, `-agentGroup` ("default"), `-roles` ("proxy, jobs"), `-heartbeatEvery` | CP is regional, key-authenticated, multi-region capable; agents are grouped & roled |
| **Buckets** | `-bucketURL` (required), `-ingestionBucketURL`, `-compactionBucketURL`, `-schemaRegistryBucketURL`, `-additionalBackgroundTasksBucketURLs`, `-additionalDeadscannerBucketURLs` (deprecated), `-bentoBucketURL/-bentoConfigPath`, `-tlsBlobURL`, `-internalTLSBlobURL` | Bucket-per-function is normal; bucket migration is a first-class, supported operation (background tasks scan *old* buckets too) |
| **Batching / write path** | `-batchTimeout` 250 ms, `-batchMaxSizeBytes`, `-batchMaxCompressedSizeBytes` (default outcome: 1 MiB compressed / 128 MiB uncompressed cap), `-storageCompression` `zstd` (or `lz4`), `-batcherMaxInflight{Bytes,Files}PerCPU`, `-maxProduceRecordSizeBytes` 32 MB | Cross-partition batching with explicit cost/latency knobs and memory backpressure |
| **Read path / cache** | `-fileCacheSizeBytes` (default 0.5 GiB/vCPU), `-fileCacheExtraReplicas`, `-autoTuneFetchLimits` (on), `-fetch-quick-retry{,-max-duration 50ms}`, `-kafkaFetchCompression lz4`, `-kafkaMaxFetch{Request,Partition}BytesUncompressedOverride` 256 MiB | RAM cache sized per vCPU; fetch is aggressively tuned, auto-tuning on by default |
| **Kafka protocol** | `-kafkaPort` 9092, `-httpPort` 8080, `-httpsPort` 8443, `-enableKafka` (on), `-kafkaMetadataRefreshInterval` 1 m, `-enableMetadataTopicsCache` (on), `-kafka{CloseIdleConnAfter 1h, LoadBalancingInterval 8760h, InterzoneLoadBalancingInterval 1m}`, `-maxConcurrent{Request,MetadataRequests}PerCPU`, `-highCPUBackpressureThresholdPercent` 98 | Any agent serves any partition; periodic connection churn for load balancing; AZ-aware client steering (clients embed `warpstream_az=` / `warpstream_interzone_lb=true` in client.id [BIN strings]) |
| **AuthN/Z** | `-requireSASLAuthentication`, `-enabledSASLMechanisms` (OAUTHBEARER, PLAIN, SCRAM-SHA-512), `-requireMTLSAuthentication`, `-tls*`, `-internalTLS*`, SPIFFE (`-internalSpiffe*`, `-spiffeMTLSAuthentication`), `-enableACLLogs` (on), impersonation header flags, `-strictClientIDChecks` | Full Kafka security model plus workload identity for agent↔agent and agent↔CP (`-workloadIdentityTokenSource` aws/gcp in `metrics` mode) |
| **Jobs / background** | `-jobSelector` (e.g. `query`, `-orbit`, `orbit,query`), `-jobsRunnerSlotsAmplification`, `-enableBackgroundVerifiers`, `-disableDeadScannerOptimisticQueue`, `-enableRipcord`, `-enableManagedPipelines`, `-enableQueryEngine` (on) / `-disableQueryEngine`, `-enableEvents` (on) | The agent fleet is a general work pool; individual fleets opt in/out per job family |
| **Schema Registry** | `-schemaRegistry{Port,URL,BucketURL,BasicAuth,TLS…}`, `-externalSchemaRegistry*`, `-schemaValidation{URL,AgentKey,VirtualClusterID}` | Embedded Confluent SR personality + proxying/validation against external SR |
| **Observability** | `-enablePrometheusMetrics` (on), `-enableDatadog{Metrics,Profiling,Tracing}`, `-datadogTracingRate`, `-disable{LogsCollection,PublishMetricsJob,ProfileForwarding}`, `-logInterval` 15 s, Go pprof knobs (`-blockProfileRate`, `-mutexProfileFraction`) | Metrics/logs/profiles are *pushed to the backend* by default, and each stream can be turned off — the BYOC privacy knobs |
| **Lifecycle** | `-gracefulShutdownDuration` 5 m, `-enableShutdownLock`, `-availabilityZoneRequired`, `-advertiseHostnameStrategy` (auto-ip4/ip6/local/custom), `-clusterWideEnvironmentPort` 9999 | Rolling-upgrade-aware: long drain so clients re-resolve via discovery before exit |

The startup log line [RUN] is itself a fingerprint of the telemetry defaults: JSON log with `git_commit`, `process_generation` (a fresh UUID per boot), `hostname_fqdn`, `private_ips`, `num_vcpus`, and even a DataDog cost estimate field — agents phone home structured logs unless `-disableLogsCollection`.

---

## 6. Internal package architecture (what the symbol table says the binary *is*)

Reading the package map (§2.2) as a layered design:

```
services/agent/app            main(): subcommand dispatch (agent/cli/kcmd/local/…)
services/agent/agent          agent bootstrap: Run(), NewStreamClient(), AzLoader,
                              bucket lifecycle checker, stats pusher, blob proxy server
  ├─ pkg/kafka/server         TCP Kafka server, connection handling
  ├─ pkg/kafka/agent          AgentHandler: HandleProduce/Fetch/Metadata/JoinGroup/…
  │    └─ pkg/processor + pkg/stream/batcher     cross-partition batcher & flush
  ├─ pkg/streamclient         httpStreamClient → hosted CP (protobuf over HTTPS)
  ├─ pkg/blob                 Store abstraction over S3 / GCS / Azure / DynamoDB / SQL / mem
  ├─ pkg/filecache            distributed chunk cache (actor model, footer cache)
  ├─ pkg/servicediscovery     NodePoolTracker + NodeHeartbeater (+ blob fallback backend)
  ├─ pkg/jobs                 PollJobs/AckJobs runners, balanced queues
  │    ├─ pkg/compaction      planners (Composite/FanIn/Eager…), merge executors
  │    ├─ pkg/deadscanner     bucket scan → delete dead files
  │    ├─ pkg/datalake/*      Tableflow: ingestion, compaction, catalog sync, deletions
  │    ├─ pkg/pipelines       managed pipelines (Bento config from bucket)
  │    ├─ pkg/query/*         query engine (parser/interpreter/exec, parquet)
  │    ├─ pkg/orbit           Orbit proxying to external Kafka ("orbit" topics)
  │    └─ pkg/ripcord/*       Lightning/ripcord ingest & replay
  ├─ pkg/schemaregistry (+ byocschemaregistry)   SR server & validation
  ├─ pkg/acls, pkg/transaction, pkg/ratelimit, pkg/featureflags …
  └─ (shared with the backend / local mode)
       pkg/stream/statemachine   the Virtual Cluster state machine
       pkg/rsm, pkg/rsmy         replicated-state-machine framework
       pkg/warpcluster           RSM/agent-pool actor routing
       nola/virtual              virtual (in-memory) environment for local/playground
```

Three structural observations:

1. **The agent is a client of an interface, not of a network address.** `pkg/streamclient` defines the CP contract; `httpStreamClient` implements it over HTTPS for production, and the batching layer depends on an abstraction (`streamclient.FastListStreamClient`, visible in `pkg/processor` generics [BIN]). `local` mode substitutes the embedded RSM. This is why the smoke test in §3.3 works unchanged.

2. **The state machine is a library, linked — not a separate process the agent shells to.** `pkg/stream/statemachine.StreamReplica` (374 methods [BIN]) plus `pkg/rsm.Proposer/LogStore/SnapshotStore` is a complete consensus-replicated store. Its presence in an "agent" binary is explained by `local`/`playground`/tests and by code sharing with the hosted backend (`pkg/warpcluster.RouteRSM`, `rsmActor.handleProposeV2` [BIN] are unmistakably server-side handlers). Do not conclude production agents run consensus — in `agent` mode the CP is remote and reached via `httpStreamClient` (flags `-metadataURL`/`-region`/`-agentKey` would be meaningless otherwise) [INF, strongly anchored].

3. **Everything background is a "job".** `pkg/jobs.Runner` + `agentpoolpb` job types (§14) unify compaction, dead-scanning, datalake maintenance, pipelines, queries, schema migration, metrics publishing and more behind one poll/ack protocol. The CP is therefore not just a metadata store but a **scheduler**, and the agent fleet is its worker pool.

---

## 7. The Kafka protocol surface

### 7.1 What the agent speaks [BIN]

`pkg/kafka/agent.(*AgentHandler)` exposes (symbol names verbatim): `HandleProduce`, `HandleFetch`, `HandleMetadata`, `HandleJoinGroup`, `HandleSyncGroup`, `HandleSASLHandshake`, `HandleSASLAuthenticate`, `HandleRequest`/`HandleDecodedRequest`, plus HTTP variants `HTTPProduceHandler`, `HTTPFetchHandler`, `HTTPFetchSingleRecordHandler`, `HTTPFetchRecordByPathHandler`, and telemetry handlers. The matching control-plane methods (§9) show the rest of the Kafka surface is implemented *by delegation*: consumer groups (`JoinGroup/SyncGroup/HeartbeatGroup/GetGroupsOffsets/PutGroupsOffsets/DeleteGroups…`), idempotent/transactional producers (`InitProducerID`, `EndTransaction`, `DescribeProducers`, `DescribeTransactions`, aborted-transaction lookups), admin (`CreateStreams/DeleteStreams/UpdateConfigurations/DescribeCluster…` — a "stream" is the internal name for a topic-partition backing store [INF from naming]), ACLs (`ListACLs` + `pkg/acls`), and `DeleteRecords`.

In other words the agent implements the Kafka API the way a *proxy* would: terminate the wire protocol, translate each request into one or more CP RPCs and/or bucket reads, translate back. [PUB] agrees: "any agent can lead any partition, commit group offsets, or coordinate" — there are no leaders because there is nothing to lead; the CP serialises.

### 7.2 Clients, load balancing, and AZ steering [RUN][BIN]

- Bootstrap is any agent (or a load balancer / "magic URL" in front of them [PUB]); `HandleMetadata` returns agents as brokers. `-enableMetadataTopicsCache` (default on) trades up-to-1 s topic-visibility staleness for metadata latency [RUN flag text].
- Connections are periodically closed (`-kafkaLoadBalancingInterval`, default **8760 h ≈ never**, plus a 1-minute inter-zone check) to re-spread clients across agents; the flag text and strings (`warpstream_az=`, `warpstream_interzone_lb=true` embedded in `client.id`) show the AZ variant: if a client is cross-AZ and a same-AZ agent exists, the connection is dropped so the client re-resolves locally. This is the cross-AZ cost story made mechanical.
- Backpressure is explicit and CPU-normalised: max connections/requests/metadata-requests per vCPU, inflight produce bytes/files per vCPU in the batcher, inflight fetch compressed bytes per vCPU, and a 98 % CPU high-water mark that starts shedding. A stateless fleet survives by *refusing* work early rather than queueing it on disk (there is no disk) [INF from flag semantics].

---

## 8. The write path (produce), step by step

### 8.1 Sequence [BIN method names; RUN defaults; PUB order-of-operations]

```
Producer ──Produce(partition, records)──▶ Agent (any, same AZ preferred)
                                            │  validate ACL/SASL, quotas (pkg/ratelimit),
                                            │  schema validation if configured
                                            ▼
                                   Batcher (pkg/processor.Batcher,
                                   pkg/kafka/agent.batchingStreamClient)
                                   • key = (virtual cluster, …), batches from MANY
                                     topic-partitions accumulate in ONE in-memory file
                                   • flush on: 250 ms timer  OR  size cap
                                     (compressed ~1 MiB default outcome) [RUN]
                                   • sort/buffer records (RecordBufferer.SortRecords),
                                     build file (pkg/stream/segment.Builder)
                                            │ 1) PUT file (zstd default)
                                            ▼
                                   Customer bucket (pkg/blob)  ── durable ──┐
                                            │ 2) PutFile(file metadata: per-partition
                                            │    batch ranges, counts, producer seqs)
                                            ▼                               │
                                   CONTROL PLANE (Virtual Cluster RSM)      │
                                   • assigns base offsets per partition     │
                                   • appends FileMetadata + FileIndexEntry  │
                                   • dedups idempotent batches, fences      │
                                     stale producers, records txns          │
                                            │ 3) offsets returned           │
                                            ◀───────────────────────────────┘
                                   Agent acks each waiting Produce with its
                                   assigned offsets.  (Ack AFTER commit —
                                   never before.) [PUB, consistent with
                                   PutFile/ack symbol layout]
```

### 8.2 Details that matter

- **Cross-partition batching is the economic core.** One file holds batches for many partitions (the CP-side `FileMetadata`/`FileIndexEntry` and the packed per-batch ranges prove the file is multi-partition [BIN]). Object-store PUT prices and CP load both scale with *files/batches*, so the batcher amortises both. Flag text is blunt about the trade: "Lower values may reduce latency, but will increase costs" on `-batchTimeout`, `-batchMaxSizeBytes`, `-batchMaxCompressedSizeBytes` [RUN].
- **Defaults discrepancy (binary wins):** v842's help says if both size knobs are unset "the max file size is 1 MiB compressed and 128 MiB uncompressed"; [PUB] public docs quoted in the earlier study said 250 ms / 4–8 MiB. Either the defaults changed, or the compressed-cap is the binding one in practice. Treat "250 ms timer, ~1 MiB compressed file" as the v842-observed behaviour and the 4 MiB figure as stale [RUN vs PUB].
- **Compression:** file bodies are `zstd` by default (`-storageCompression`, or `lz4`); Fetch responses default to `lz4` (`-kafkaFetchCompression`). Records arrive already client-compressed; `Builder`/`BatchWriter`/`pageWriter` symbols in `pkg/stream/segment` (dense/sparse pages, bitsets, Roaring bitmap dep [BIN]) describe a paged file layout that can skip/rewrite batches without decompressing everything [INF].
- **Ordering is decided at commit, not at upload.** Many agents PUT files concurrently; whichever `PutFile` the RSM applies first gets the lower offsets. Per-partition order = CP apply order. This is only possible because offset assignment lives in exactly one serial place (§10).
- **Idempotence & transactions are CP state.** `InitProducerID`, producer sequence tracking, `GetAbortedTransactions(InRange)`, `EndTransaction`, `CompleteAbortedTransactionsOffload` [BIN] — the state machine keeps per-producer sequences (public material says the last 5 batches per (producer, partition), with retroactive tombstoning of duplicates at compaction [PUB]). The file may physically contain a duplicate batch; the *index* excludes it. Data plane dumb, control plane smart, again.
- **Ripcord / "lightning" escape hatch:** `-enableRipcord` — "continue ingesting data for lightning topics even when the control plane is unreachable", with `pkg/ripcord/ingest`, `RipcordSequenceStatus/RipcordCloseSequence` CP calls and `RipcordReplayJob*` job types [BIN]. This is the deliberate availability-over-consistency mode: journal now, sequence/replay into the CP later. It is off by default and per-topic in spirit [INF].

---

## 9. Control-plane interaction (the heart of the request)

### 9.1 Endpoint resolution, transport, auth [RUN][BIN]

| Aspect | Observed behaviour |
|---|---|
| Endpoint selection | `-region <r>` → regional metadata endpoint; strings show `metadata.default.us-east-1.warpstream.com` as the pattern's exemplar; `-metadataURL` overrides directly; `-multiregion` / `-multiregionMetadataURLs` take comma-separated lists for multi-region clusters. `-region` and `-metadataURL` are mutually exclusive [RUN]. Other WarpStream hosts in strings: `api.warpstream.com` (signup/console API), `console.warpstream.com`, `api-secure.warpstream.com` [BIN] |
| Transport | **Protobuf over HTTPS.** The client's core methods are literally `protoOverHTTP`, `retryProtoOverHTTP`, `retryProtoOverHTTPCustomTimeouts`, `retryProtoOverHTTPSchemaRegistry` [BIN]. Service paths seen in strings: `/v1/agent/stream`, `/v1/agent/agentpool`, `/v1/agent/schemaregistry`, `/v1/agent/health`, `/v1/agent/dl-metadata-proxy` [BIN]. This is RPC-by-POST of `streampb`/`agentpoolpb` messages, not gRPC [INF from method naming — no `grpc` service stub dominates the client, and "protoOverHTTP" is unambiguous] |
| Auth | `-agentKey` (or file `-agentKeyPath`; `-apiKey` is the legacy alias). Sent per-request [INF]; `GetCredentials`/`SaslAuth` CP calls handle *Kafka-client* credential checks delegated upward. Workload-identity token exchange (`tokenexchangepb`, `-workloadIdentityTokenSource aws|gcp` in `metrics`) exists as an alternative to static keys [BIN] |
| Retries | Layered: per-call retry wrappers with custom timeouts, jittered [INF]; CP errors classified in the client (`maybeHandleClusterMigratedError` — clusters can migrate between CPs/regions and the client follows [BIN]) |
| Liveness | `-heartbeatEvery` → `Heartbeat` CP call ("how often the agent should heartbeat the WarpStream backend") plus `SendAgentStats`, `Statistics`, `TimeSeries`, `PushDiagnostics`, `SendLogs`, `SendAuditLogs` [BIN]. Heartbeats double as the service-discovery registry write (§13) |
| Routing inside the CP | `RouteStream`, `Discover`/`DiscoverBatch`, `GetClusterLeaderRegion`, `IsClusterActive` [BIN] — the agent asks *which* CP shard/region leads its virtual cluster rather than assuming [INF] |

### 9.2 The CP API catalogue (161 client methods, grouped) [BIN]

`pkg/streamclient.(*httpStreamClient)` — full list in Appendix B. Grouped by subsystem, this *is* the control-plane contract:

**(a) Stream & file lifecycle — the metadata core.** `CreateStreams`, `DescribeStream(s)`, `ListStreams`, `FastListStreams`, `ListAllStreams`, `UpdateStreams`, `DeleteStreams`, `UndeleteStreams`, `CreateStream…`-adjacent `RouteStream`, `FileIDAllocator`, **`NewFile`**, **`PutFile`**, **`PutFileBatch`**, `GetFiles`, `ScanFiles`, `ScanFilesByLane`, `MergeShards`, `SplitShard`, `SeekShard`. → Everything about topics-as-streams and the file index.

**(b) Records & offsets.** `GetRecords`, `GetRecordsBatch`, `ListOffsetsStream(s)`, `DeleteRecords`, `GetSizeHint`. → Fetch planning and retention-by-offset.

**(c) Consumer groups (both classic and the new "group-type consumer").** `JoinGroup(Batch)`, `SyncGroup(Batch)`, `HeartbeatGroup(Batch)`, `LeaveGroup`, `TimeoutGroupHeartbeats`, `ListGroups`, `DeleteGroups`, `GetGroupsOffsets(Batch)`, `PutGroupsOffsets(Batch)`, `DeleteGroupsOffsets`, `GroupTypeConsumerHeartbeat/Describe`, `GetGroupTypeConsumerTargetAssignmentInputs`, `InstallGroupTypeConsumerTargetAssignment`, `DescribeGroupTypeConsumerConfigurations`. → The group coordinator lives in the CP; agents are stateless front-ends. (Hence the [PUB] flag `KAFKA_HANDLE_CONSUMER_GROUPS_IN_BACKEND`.)

**(d) Producers & transactions.** `InitProducerID`, `DescribeProducers`, `DescribeTransactions`, `ListTransactions`, `TransactionsInfo`, `EndTransaction`, `GetAbortedTransactions(InRange)`, `Initiate/CompleteAbortedTransactionsOffload`, `ExpireOffloadedTransactionFiles`, Orbit variants (`OrbitResolveSrcProducerIDs`, `OrbitSeedProducerSequences`).

**(e) Compaction & file expiry.** `Compact`, `ExpireFiles`, `ExpireFilesV2`, `DeadFiles`. → Agents *execute* compaction but the CP *authorises* the index swap (§15).

**(f) Jobs & agent pool.** `PollJobs`, `AckJobs`, `Heartbeat`, `Unregister`, `SendAgentStats`, `PollFeatureFlags`, `LaneConfig`. → §14.

**(g) Schema registry.** `CreateSchemaMetadata`, `GetSchemaMetadata(ByHash)`, `GetSchemaMetadatas…` (five variants), `GetSchemaRegistrySubjects`, `GetSubjectVersionsBatch`, `SchemaIDExists`, `DescribeSchemaContexts`, `UpdateSchemaContexts`, `SchemaRegistry`, `SchemaRegistryAuth`. → Schemas are CP metadata even though SR *traffic* terminates on agents.

**(h) KV & cluster admin.** `KVGet`, `KVGetMany`, `KVSet`, `KVSetMany`, `KVCompareAndSwap`, `KVOperations`, `DescribeCluster`, `UpdateConfigurations`, `ListACLs`, `ListClusterSuperusers`, `GetCredentials`, `SaslAuth`, `MigrateCluster`, `UnmigrateCluster`, `GetClusterLeaderRegion`, `IsClusterActive`, `RSMVersion`. → A general KV inside the state machine explains how config/ACL/feature data all ride the same replicated log.

**(i) Datalake / Tableflow (`DL*`, ~35 methods).** `DLCreateTable`, `DLPutFile`, `DLGetFiles`, `DLTakeSnapshot`, `DLCommitSnapshot`, `DLRegisterSnapshot`, `DLGetLiveSnapshots`, `DLCompact`, `DLMarkFilesObsolete`, `DLDeadFiles`, `DLCreateDeletion/DLPreviewDeletion/DLApplyDeletionBatch…`, `DLScanFiles`, `DLUpsertIngestion`, `DLUpdateCatalogSnapshotVersion`… → Tableflow reuses the *same pattern* (files in a bucket + CP-held index/snapshots) for Iceberg tables. The architecture is fractal.

**(j) Telemetry & misc.** `PushDiagnostics`, `SendLogs`, `SendAuditLogs`, `Statistics`, `TimeSeries`, `DumpAndResetClientStatistics`, `QueryEngine`, `CreateHyperlakeSnapshot`, `RipcordSequenceStatus`, `RipcordCloseSequence`.

**What the catalogue tells you about load:** the hot path is tiny — `PutFile` (writes), `GetFiles`/`GetRecords` (reads), group offset calls (consumers), `Heartbeat` (liveness). Everything else is rare/admin/job traffic. CP cost therefore scales with *commit rate × batches per commit*, exactly as [PUB] warns (400 k batches/s alarm; `partitions × flushes/s × AZs` formula). The agent-side caches (`ListStreamsCacheKey`, metadata topics cache, `pkg/stream/cache`) exist to keep the long tail off the CP [BIN].

### 9.3 Request flow for the four hot operations

| Kafka op | Agent-local work | Bucket work | CP calls |
|---|---|---|---|
| Produce | batch, build file | **PUT file** | `PutFile` → offsets → ack |
| Fetch | cache lookup, decompress, assemble | range-GET on cache miss (or peer agent) | `GetFiles` (which files/offsets), sometimes `GetRecords` planning |
| Commit offset | translate | — | `PutGroupsOffsets(Batch)` |
| Heartbeat (group) | translate | — | `HeartbeatGroup(Batch)` / `GroupTypeConsumerHeartbeat` |
| Create topic | translate | — | `CreateStreams` + `UpdateConfigurations` |
| Compact (job) | merge files (§15) | GET inputs, **PUT output** | `PollJobs` → `ScanFiles` → `Compact` (index swap) → `AckJobs` |

---

## 10. Inside the control plane, as far as the binary reveals it

The hosted CP is not shipped in this image — but its *state machine* is, because the same code runs in-memory for `local` mode and is shared with the backend (`pkg/warpcluster` routing, RSM actors). This section is therefore **[BIN] about the code, [INF] about the hosted deployment**.

### 10.1 A replicated state machine per virtual cluster

`pkg/rsm` provides the generic machinery: a `Proposer` (batches proposals, `compressAndWrapBatch`, tracks replica lag), a `LogStore` (`AppendEntry(WithAutoIndex)`, `ReadEntries`, `peerAwareLogStore` with peer-first reads), a `SnapshotStore` (snapshot reader/writer), and replica management with snapshots and catch-up (`cloneStateMachineViaSnapshot`, `loadSnapshotIntoAllReplicas`). `pkg/stream/statemachine` is the *application*: `StreamReplica` applies commands against state whose snapshot surface (`SnapshotReader/Writer.Load*/Write*` [BIN]) enumerates the entire domain model:

`Stream`, `DeletedStream`, `StreamNameIndex`, **`FileMetadata`, `FileIndex`**, `FileIDs`, `Pointer` (compaction pointers), `GroupState`, `GroupOffset`, group-type-consumer members/heartbeats/target-assignments, `Producer`, `Sequence`, `Shards`, `KVStoreItem/KVStoreMetadata`, `TimestampIndex`, `MetricSample`, `ClusterConfiguration`, `HyperlakeSnapshot`, datalake tables/snapshots/files.

So a Virtual Cluster is: **one RSM whose state is (streams × file index × groups × producers × schemas × KV), journaled through a log store, periodically snapshotted** (snapshots themselves go to object storage — `SnapshotReader.LoadBucket`, presigned snapshot upload URLs in `pkg/warpcluster.cluster.GetPresignedSnapshotUploadURL` [BIN]). Offset assignment, idempotent-producer sequencing, group membership and compaction-pointer moves are all just *state transitions applied in log order* — which is why they're atomic and why any agent can front any of them.

### 10.2 Sharding, regions, migration

`MergeShards`/`SplitShard`/`SeekShard`, `GetClusterLeaderRegion`, `MigrateCluster/UnmigrateCluster`, `ClusterMigratedError`, `-multiregion*` [BIN][RUN] together say: virtual clusters are sharded, have a leader region, and can be migrated live, with clients redirected via a typed error. Point-in-time recovery also exists in the model (`AbortCausePITR`, `FindClosestSnapshots`, `/v1/…get_virtual_cluster_closest_snapshots`, `…start_virtual_cluster_pitr_restore` strings [BIN]) — the CP keeps snapshot history, not just current state.

### 10.3 History, in the CP sense

"History" for WarpStream = **the file index over time**: every file ever committed is a `FileMetadata` + `FileIndexEntry` (per-partition offset ranges within the file); compaction moves *pointers* (`CompactReplacePointerRange`, pointer load/write in snapshots [BIN]); expiry and deletion are index operations first and byte operations later (§16). The RSM log is the history of *decisions*; the snapshot series is the history of *states*; the bucket is the history of *bytes*, pruned by the deadscanner. A fetch for an old offset is answered from the index (which files, which ranges), never by scanning the bucket.

---

## 11. Object-store interaction

### 11.1 The `pkg/blob` abstraction [BIN]

One interface (`blob.Store`, `NewReader/NewWriter/NewRangeReader`, `PutBytes(Pooled)`, `List`, `Delete`, `Attributes`, `SignedURL`, capabilities) over bucket types found in symbols: **`s3Bucket`-family via `singleBucket`, `gcsBucket`, `multiBucket`, `dynamodbBucket`, `sqlBucket`** (plus `mem://` in local/playground and file-backed test buckets [RUN][BIN]). SDK evidence [BIN deps]: AWS SDK v2 S3 (+presigned URLs, `feature/s3/manager`), `cloud.google.com/go/storage`, Azure `azblob`, and `gocloud.dev` — the last likely providing the uniform URL scheme (`s3://`, `gs://`, …) that `-bucketURL` parses. Extras visible in symbols: per-bucket circuit breakers, credential lazy-fetch/assume-role (`WARPSTREAM_BUCKET_ASSUME_ROLE_ARN_*`), S3 checksum disable (`WARPSTREAM_DISABLE_S3_CHECKSUMS`), GCS direct-connectivity hard-fail (`-gcsDirectConnectivityRequired`), and a *DynamoDB-as-blob-store* chunked file table (`dynamoFilesTable/dynamoChunksTable` — files split into chunk items, used for service-discovery/snapshot-ish small blobs [INF]).

Bucket URLs by role [RUN]: ingestion (`-bucketURL`, overridable by `-ingestionBucketURL`), compaction output (`-compactionBucketURL`), schema registry (`-schemaRegistryBucketURL`), plus *additional* buckets scanned by background tasks during migrations. Startup optionally verifies permissions (`-checkBucketAccessRetryCount`, `-skipBucketsPermissionCheck`); a `BucketLifecyclePolicyChecker` in `services/agent/agent` [BIN] suggests the agent validates/warns on bucket lifecycle config.

### 11.2 What is written, and when

| Object kind | Written by | Trigger | CP record |
|---|---|---|---|
| Data file (multi-partition batches) | proxy-role agent batcher | flush (250 ms / size) | `PutFile` → `FileMetadata`+index |
| Compacted file | jobs-role agent | compaction job (§15) | `Compact` index swap |
| Datalake/Iceberg files (Parquet) | Tableflow jobs | ingestion/compaction jobs | `DLPutFile`, snapshots |
| Schema-registry data | SR personality | schema create (with bucket-backed SR mode) | `CreateSchemaMetadata` |
| RSM snapshots (local/backend) | CP replicas | snapshot interval | snapshot store (`LoadBucket`) |
| Service-discovery blobs | agents (blob backend mode) | heartbeat/discovery fallback | blob keys (`BlobServiceDiscoveryBackend`) |
| Pipeline (Bento) config | operator → read by agents | `-bentoBucketURL/-bentoConfigPath` | managed-pipelines jobs |
| TLS/internal certs | operator → agents | `-tlsBlobURL`, `-internalTLSBlobURL` | — (read-only) |

### 11.3 Read economics

Object-store GETs are the variable cost the whole read path is designed to avoid: footer-first reads (`ReadFooters`, `FooterCache` [BIN]) so a fetch touches metadata pages before record pages; range reads (`NewRangeReader`, chunk-index → fetch-range mapping in `fileCacheActor` [BIN]); request coalescing/batching (`BatchableStreamRequests`, `loadRemoteBatched`); and presigned/signed URLs so *peer agents or the CP* can hand a reader a direct bucket URL (`DLCopyFilesUnsignedURLs` shows the unsigned variant in datalake land [BIN]). [PUB] quantifies the result (GET bill ≈ $508/mo at 1 GiB/s×3 read fan-out) — the mechanism for "loaded once per AZ" is §12.

---

## 12. The read path and the distributed file cache

```
Consumer ──Fetch(topic, partition, offset X)──▶ Agent
                                                  │  CP: GetFiles(from X) → [(file, range)…]
                                                  ▼
                                        pkg/filecache (per agent, RAM)
                                        • key = file/chunk; FooterCache for indexes
                                        • hit  → serve chunk from RAM (maybe a peer's:
                                          discovery routes to the agent holding it —
                                          loadRemote vs loadFromCache vs loadDirect [BIN])
                                        • miss → range-GET bucket, populate chunk cache
                                          (chunked, ~large chunks [INF]), serve
                                                  │
                                        assemble records (segment.Reader: pages,
                                        index, skip aborted-txn batches), compress
                                        (lz4 fetch default), respect max.bytes /
                                        partition caps & auto-tuned limits [RUN]
                                                  ▼
                                               Consumer
```

Key facts: cache size defaults to **0.5 GiB per vCPU** [RUN] and is *memory*, not disk (the stateless/no-disk claim is literal [PUB]; `fileCacheSizeBytes` + `memChunkCache` + actor `MemoryUsageBytes` [BIN]). `-fileCacheExtraReplicas` replicates chunks to extra agents so an agent shutdown doesn't force bucket re-reads. Routing uses the same service-discovery pool (§13) and is AZ-aware — the point is one bucket GET per AZ per hot chunk, with agents serving each other over HTTP :8080 (`blobStoreToFetcher.fetchRange` behind the cache's HTTP handlers [BIN]). Fetch also has latency trims everywhere: `-fetch-quick-retry` (≤50 ms poll when no new data), fetch auto-tuning of byte limits, and a metadata-topics cache. Orbit topics invert the flow: fetch is proxied to an *external* Kafka (`OrbitFetchJob*`, `pkg/orbit` [BIN]) — WarpStream can front a conventional cluster.

---

## 13. Service discovery and the agent pool

Agents are a fleet, so they need a registry — but they're stateless, so the registry is soft state rebuilt by heartbeats:

- **Write side:** `pkg/servicediscovery.NodeHeartbeater` (`HeartbeatOnce`, stale-heartbeat detection, `Unregister` on shutdown; `-gracefulShutdownDuration` 5 m gives discovery time to drain the node before exit [RUN]).
- **Read side:** `NodePoolTracker` (`Track/TrackBatch/Fetch/FetchFilter`, GC loop, stale checks) with three backends [BIN]: **remote** (via CP: `RemoteServiceDiscoveryBackend`, `Discover/DiscoverBatch` CP calls), **blob** (`BlobServiceDiscoveryBackend` — discovery through the customer bucket, the BYOC/offline path), and **embedded** (`EmbeddedNodePoolTrackerReplica` — an RSM-replicated tracker, again the local/backend sharing).
- **Selection:** `sortPowerOf2RandomChoices…` and `sortRoundRobin…` [BIN] — power-of-two-choices load balancing for cache/job routing. `-agentGroup` partitions the fleet into isolated pools (separate VPCs/regions/accounts shouldn't cross-talk [RUN]); advertise strategies (auto-ip4/ip6/local/custom hostname) handle the usual container/NAT addressing mess.
- **CP-mediated view:** `agentpoolpb` (`OrbitClusterState`, agent `Tags`, `Stats`, `NodePressureStats`, load-balancing strategy enum [BIN]) shows the CP keeps its own pool view from heartbeats, used for job placement and for telling clients which agents exist.

Failure semantics [INF]: a dead agent simply goes stale and is GC'd; clients re-resolve (helped by periodic connection churn, §7.2); its cache chunks are re-fetched from the bucket (or extra replicas); its in-flight batcher files were either committed (durable) or never PUT/acked ( producer retries elsewhere). Nothing to recover *from* the agent, because the agent *has* nothing.

---

## 14. The jobs system: the control plane as scheduler

`PollJobs`/`AckJobs` [BIN] plus `pkg/jobs` (balanced/backoff queues, per-type concurrency limits, slot amplification flag) turn every background duty into CP-assigned work. Job types from `agentpoolpb` (input/output message pairs [BIN]), abridged:

| Family | Job types (evidence) | § |
|---|---|---|
| Kafka core | `CompactionJob*`, `DeadscannerJob*`, profile upload, metrics publishing (`PublishVirtualClusterMetricsJob*`), feature-flag polling | §15–16 |
| Datalake/Tableflow | `DLCompactionJob*`, `DLIngestionJob*`, `DLMetadataSyncJob*`, `DLOffsetScraperJob*`, `DLSchemaScraperJob*`, `DLTopicScraperJob*`, `DLSnapshotCleanupJob*`, `DLDataCleanupJob*`, `DLDeletionJob*`, `DLPreviewDeletionJob*`, Glue/BigLake/BigQuery/Hive table-management jobs | §17 |
| Pipelines | managed-pipeline execution (Bento config from bucket), shared rate limit per vCPU | §17 |
| Query | `QueryEngineJob*` (agent runs queries over the files; `-disableQueryEngine`) | §17 |
| Orbit | `OrbitFetchJob*`, `OrbitJob*`, `OrbitTopicMetadataJob*`, `OrbitGroupOffsetsJob*`, `OrbitACLJob*`… | §12 |
| Ripcord | `RipcordReplayJob*`, `RipcordScanReplaysJob*` | §8.2 |
| Schema | `SchemaMigrator*Job*` (copy/fetch subjects/versions between registries) | §17 |
| Events/misc | `EmitEventsJob*`, audit-log shipping | §18 |

Placement controls: `-roles` (default `"proxy, jobs"` — split fleets into proxy-only vs jobs-only [RUN][PUB "splitting agent roles"]), `-jobSelector` (include/exclude job families), agent tags/groups, and CP-side stats (`JobRunnerStats`, priority stats in discovery messages [BIN]). **Architectural read:** the CP never pushes work over a bespoke channel per feature; features are *job types*, agents are *capacity*. New background features ship as new protobuf job payloads + a handler (`pkg/jobs.handler.HandleStreamJob`, `pkg/compaction.Handler.HandleStreamJob`, `pkg/deadscanner` handler — note how many packages expose the same `HandleStreamJob` shape [BIN]).

---

## 15. Compaction: how small files become big files

Symbols tell a fairly complete story [BIN] (`pkg/compaction`, 661):

- **Planners:** `CompositePlanner` (small + big planners, cluster-size monitoring, per-lane scan limits), `FanInPlanner/FanInMergePlanner` (bucket files into windows), `EagerPlanner(s)` — multiple strategies composed, run on intervals (`RunEvery`, `ScanLevels`, `ScanLaneTypes`, `TracksInflightFiles`). Files live in **levels and lanes** (`GetMaxLevelWithSomeFiles`, `GetPointerCountForLevel`, `CompactionLane/Level` in the state machine) — a levelled-compaction design in the LSM sense, keyed by stream/partition ranges.
- **Authority:** planners propose; the **CP disposes**. Execution reads inputs (via filecache/bucket), merges with `createMergeOptions`, drops aborted-transaction batches and tombstoned duplicates ([PUB] retroactive tombstoning lands here), writes one output file to `-compactionBucketURL` (possibly a *different* bucket — compaction as bucket migration!), then calls **`Compact`** with `CompactionRequest/Response` (`CompactionOutput` [BIN]) so the RSM atomically swaps index pointers (`CompactReplacePointerRange`) from inputs to output. A crash mid-job leaves inputs live and the output orphaned — cleaned by… the deadscanner (§16). No partial visibility is possible because visibility *is* the index.
- **Guardrails in the state machine:** `CompactionRejectsFileIndexOverlap`, `CompactionRejectsWrongLane`, `AcceptHigherCompactionLevels`, `FastCompactionGetValidation` [BIN] — the CP validates every swap. Agents are untrusted executors; the index transition is checked.

## 16. Object-store history, retention, and garbage collection (file lifecycle)

This is the lifecycle the user asked about — how files are born, indexed, superseded, and finally deleted, i.e. **the history of the object store as the CP sees it**:

```
                 PUT (agent batcher)                PUT (compaction job)
                      │                                   │
                      ▼                                   ▼
              ┌───────────────┐  PutFile   ┌────────────────────────┐
              │  ORPHAN       │───────────▶│  LIVE (indexed)        │
              │  in bucket,   │  CP assigns│  FileMetadata +        │
              │  unknown to CP│  offsets   │  FileIndexEntry ranges │
              └──────┬────────┘            └──────┬─────────────────┘
                     │                            │  retention passes /
                     │ (never committed:          │  DeleteRecords /
                     │  crashed agent,             │  compaction inputs
                     │  un-acked output)           │  superseded
                     │                            ▼
                     │                   ┌────────────────────────┐   ExpireFiles(V2)
                     │                   │  OBSOLETE / EXPIRED    │──────────────┐
                     │                   │  (index-marked; bytes  │              │
                     │                   │   still in bucket)     │              ▼
                     │                   └──────────┬─────────────┘   ┌───────────────────┐
                     │                              │ DeadFiles     │  DEAD (CP-approved │
                     └──────────────┬───────────────┘ (CP view)      │  for deletion)     │
                                    ▼                                └─────────┬─────────┘
                        ┌───────────────────────┐                               │
                        │  DEADSCANNER (job)    │◀──────────────────────────────┘
                        │  lists bucket, diffs  │        also sweeps ORPHANs
                        │  against CP dead set, │        (additional buckets too)
                        │  deletes bytes        │
                        └──────────┬────────────┘
                                   ▼
                              (bytes gone;
                               index history remains in CP snapshots/log)
```

Stage by stage:

1. **Birth (PUT before commit).** Every file lands in the bucket *before* the CP knows it (§8). The window between PUT and `PutFile` — or a crash inside it — creates orphans by design. Correctness never depends on bucket listing; the index is truth.
2. **Live.** `PutFile`/`DLPutFile` inserts file + per-partition ranges into the RSM state. `GetFiles`/`ScanFiles(ByLane)` answer "what's live where" for fetch and compaction; `ListOffsetsStreams` derives log-start/log-end from the index.
3. **Superseded.** Compaction (§15) swaps pointers; inputs become obsolete *in the index* instantly (fetches stop seeing them) while bytes linger. `DLMarkFilesObsolete` is the datalake twin.
4. **Expired.** Retention and `DeleteRecords` advance the log-start; `ExpireFiles`/`ExpireFilesV2` (and `ExpireTimestampIndex`, `ExpireOffloadedTransactionFiles`) mark whole files/ranges dead in the CP. V2's existence beside V1 [BIN] says this protocol was revised — batching/deletion granularity improved over versions [INF].
5. **Dead.** `DeadFiles`/`DLDeadFiles` CP calls hand agents the CP-approved deletion set.
6. **Deleted (bytes).** The **deadscanner** (`pkg/deadscanner`: `Scheduler`, `OptimisticQueue`, `processBatchDelete`, `shouldBeRewritten`, `NewDeadscanner` [BIN]) is a scheduled job that *also lists the bucket* — reconciling in the other direction to catch orphans the CP never knew (uncommitted PUTs, compaction outputs never registered, files from bucket migrations; hence `-additionalBackgroundTasksBucketURLs` scans old buckets "for dead files and for ripcord files that need to be ingested" [RUN]). Deletion is therefore *eventually consistent and scan-verified*, never index-only. The optimistic queue (disablable) presumably fast-path deletes files the CP just declared dead without a full scan [INF].
7. **History after deletion.** Bytes are gone, but the *history* — that file F held offsets [a,b) of partition P, was compacted into G at time T, expired at T′ — persists as RSM log entries and snapshot generations (§10.3), which is what makes PITR snapshot restore and audit/diagnostics coherent. The bucket holds no journal and no manifest files for the Kafka path [INF — no such writer symbols; contrast datalake, where Iceberg catalog snapshots are explicit `DL*` state].

**Why two-phase (index-then-scan) deletion matters:** it is safe under every crash interleaving. Delete bytes only for files the CP calls dead, *and* sweep for bytes the CP never heard of. Either mechanism alone leaks or loses data; together they converge the bucket to the index.

---

## 17. Secondary subsystems (same pattern, other domains)

- **Schema Registry** (`pkg/schemaregistry`, `pkg/byocschemaregistry`): Confluent-compatible API on :9094, schemas as CP metadata (§9.2g), optional bucket-backed mode (`-schemaRegistryBucketURL` — BYOC SR where the registry payload lives in the customer bucket), external-SR proxying and *schema validation* on produce (`-schemaValidationURL`, separate key/cluster). Validators for Avro/JSON/Protobuf in symbols.
- **Tableflow / datalake** (`pkg/datalake/*`, `pkg/iceberg/*`, `parquet-go`, `iceberg-go`): Kafka topics continuously materialised into Iceberg tables — ingestion jobs write Parquet files, `DL*` CP calls maintain table metadata/snapshots/catalog sync (Glue/BigLake/BigQuery/Hive management jobs), deletion jobs implement record-level deletes with snapshot semantics. Architecturally §9–§16 *again*, with Iceberg as the index format the CP mirrors.
- **Managed pipelines** (`pkg/pipelines`, Bento): streaming ETL configs fetched from a bucket path, executed by pipeline-role agents under a shared bytes/vCPU/s rate limit, state-changeable from the CP (`-enableManagedPipelines`, pipeline group name).
- **Query engine** (`pkg/query/*`): SQL-ish parser/interpreter/executor over the stored files (with Parquet support and its own job type) — possible *because* files are self-describing and the CP knows where everything is.
- **Orbit** (`pkg/orbit`): proxy/mirror mode against external Kafka clusters (fetch/metadata/group-offsets as jobs) for migration and federation.
- **Ripcord** (`pkg/ripcord/*`): the lightning-ingest journal/replay machinery of §8.2.
- **Events** (`pkg/events`, `-enableEvents` default on): WarpStream-level event stream (audit/ops events) distinct from Kafka topics.

Each is a *tenant* of the same three primitives — blob store, CP index/RSM, job pool — which is the deepest architectural statement the binary makes.

## 18. Observability and operability [RUN][BIN]

- **Outbound telemetry by default:** agent stats (`SendAgentStats`), CP/client statistics (`Statistics`, `TimeSeries`), diagnostics (`PushDiagnostics`, `pkg/diagnostics` scrapers/updaters/forwarders), structured logs (`SendLogs`, V2 log buffers in `agentpoolpb`), audit logs (`SendAuditLogs` — SASL/ACL events), and Go profiles (profile forwarding). Every stream has an off-switch (`-disableLogsCollection`, `-disablePublishMetricsJob`, `-disableProfileForwarding`, `-disableConsumerGroupMetrics`) — the BYOC story is "data stays, metadata about health may go, your choice."
- **Local observability:** Prometheus on :8080 (`-enablePrometheusMetrics`, default on), DataDog metrics/tracing/profiling integrations, the standalone `metrics` subcommand that scrapes the CP's *own* Prometheus endpoint (`-enableControlPlanePrometheusEndpoint`) for control-plane-side metrics (their published `warpstream_control_plane_utilization` gauge is the canonical one [PUB]).
- **Ops ergonomics:** rolling-upgrade draining (5 m graceful shutdown, shutdown lock, discovery deregistration), bucket-permission preflight, AZ resolution gate (`-availabilityZoneRequired`), `cli diagnose-*` and `file-reader/file-scrubber` forensics (read/scrub a single file from the bucket offline — the file format is inspectable by design).

## 19. Security model [RUN][BIN]

| Boundary | Mechanisms (observed) |
|---|---|
| Client → agent | Kafka SASL PLAIN / SCRAM-SHA-512 / OAUTHBEARER (`-enabledSASLMechanisms`), Kafka TLS + mTLS (`-requireMTLSAuthentication`), PROXY-protocol principal TLVs, ACL enforcement with denial logging, principal impersonation header (allow-listed), client.id strictness per virtual cluster |
| Agent → CP | `agentKey` secret (flag/file), TLS, OIDC workload-identity token exchange (AWS/GCP) as key alternative, per-virtual-cluster scoping |
| Agent → agent | Internal HTTP(S) :8080/:8443, internal mTLS with certs loaded *from a bucket* (`-internalTLSBlobURL`), SPIFFE workload identity (`-internalSpiffeTrustDomain/WorkloadID`) |
| Agent → bucket | Cloud-native credentials (IAM role assumption per-bucket ARN patterns, GCS/Azure equivalents), presigned URLs for delegated reads, optional S3-checksum disable, bucket permission preflight |
| Data confidentiality | Record bytes exist only in (a) client↔agent TLS (optional), (b) agent RAM (batcher + cache), (c) customer bucket. CP payloads are metadata: file keys, ranges, counts, offsets, group/schema state [INF — by construction of the `PutFile`/`GetFiles` contract; the CP never receives record payloads except in HTTP produce/fetch proxy handlers terminated *on agents*] |

## 20. Failure modes and availability reasoning [INF, anchored]

| Failure | Behaviour (inferred from mechanisms) |
|---|---|
| Agent dies mid-batch | Un-PUT/uncommitted data never acked → producer retries on another agent (any agent, any partition). Committed files survive — they're in the bucket + index. |
| Agent dies mid-compaction | Output orphaned; inputs still live (swap never happened). Deadscanner collects the orphan. Job re-offered by CP. |
| CP unreachable | Produces cannot commit → backpressure/refusal (batcher inflight caps), *except* ripcord/lightning topics which journal locally and replay later (`-enableRipcord`). Fetch of already-indexed files could in principle continue from cache, but offset discovery needs the CP [INF]. |
| Bucket unavailable/slow | PUT path stalls before commit; blob-layer circuit breakers [BIN] fail fast; no acks without durable PUT — the core guarantee is preserved by refusing progress. |
| CP cluster migration | Typed `ClusterMigratedError` + client redirect handling [BIN]; agents follow the virtual cluster to its new home. |
| Agent fleet churn | Heartbeat staleness + GC, power-of-2 re-routing, cache extra-replicas, 5 m drain — designed for rolling upgrades as the normal case [RUN docs link in strings: "rolling-restarts-and-upgrades"]. |
| Clock/offset edge cases | Timestamp index (`LoadTimestampIndex`, `ExpireTimestampIndex`) supports time-based offset lookup (`ListOffsets` by timestamp) and its expiry [BIN]. |

## 21. Side-by-side with this repo (`mds`) — one-paragraph version

The full public-sources comparison already lives in `docs/warpstream-architecture-comparison.md`; the binary confirms its skeleton and corrects its defaults. Both systems: stateless brokers/agents, cross-partition files in S3, offsets assigned by a single control-plane authority at commit, ack-after-commit. Differences the binary makes concrete: WarpStream's CP is a **hosted, sharded RSM with a 161-method API** (vs. mds's single-appender DynamoDB-log CP with one `CommitSegment` hot call); WarpStream *has* the pieces mds lacks — consumer-group coordination in the CP, idempotent/transactional producers, a levelled compaction service, a distributed RAM file cache, a bucket-reconciling deadscanner, schema registry, and a general jobs scheduler — all visible as packages in §6. MDS's deliberate simplifications (no idempotency, no groups, no compaction yet) are exactly WarpStream's most expensive-looking subsystems.

## 22. What the binary cannot tell us (honest unknowns)

1. **Hosted CP implementation choices:** consensus protocol inside `pkg/rsm` deployments (the code is generic; production quorum/journal storage — cloud log service? DynamoDB? disks? — is server-side config, not in this image), its scaling limits, and per-cluster isolation mechanics beyond `VirtualClusterID` scoping.
2. **Wire details:** ~~exact protobuf field layouts~~ — **now extracted, see Appendix E** (descriptor recovery from the embedded vtproto blobs). Remaining wire unknowns are only HTTP framing details (exact per-method URL path suffix / headers), not message schemas.
3. ~~Object key naming~~ — **now decoded from `pkg/stream/segmentpath`, see Appendix F.1** (sharded by CP-issued file ID). Remaining bucket unknowns are only customer-chosen prefixes/suffixes and lifecycle settings in real deployments.
4. **CP pricing/limits enforcement internals** (rate-limit package exists agent-side; CP-side quota mechanics are hosted).
5. **Whether production agents ever run the embedded RSM** (e.g. as CP read replicas) — symbols prove code *presence*, §10 argues agent-mode doesn't execute it, but only server-side deployment could confirm.
6. **Performance numbers** — none are claimed here; flags and [PUB] marketing figures are not measurements. Benchmarking `local` mode measures the in-memory CP, not the hosted one, and was used only as a functional smoke test.

## 23. Reproduction guide (every command used)

```bash
# 0. Identity (note: arin:latest does not exist; arun:latest does)
docker images; docker inspect arun:latest; docker history arun:latest --no-trunc

# 1. Extract
docker create --name arin-re arun:latest
docker export arin-re -o /tmp/arin-re/rootfs.tar
mkdir -p /tmp/arin-re && tar -xf /tmp/arin-re/rootfs.tar -C /tmp/arin-re warpstream notices.txt

# 2. Binary facts
file /tmp/arin-re/warpstream
go version -m /tmp/arin-re/warpstream
go tool nm /tmp/arin-re/warpstream | grep 'pkg/streamclient\.\(\*httpStreamClient\)'   # 161 CP methods
strings -a /tmp/arin-re/warpstream | grep -o 'WARPSTREAM_[A-Z0-9_]*' | sort -u

# 3. Runtime surface
docker run --rm arun:latest                 # subcommand list
docker run --rm arun:latest version         # v842
docker run --rm arun:latest agent -help     # 157 flags
docker run --rm arun:latest cli -help       # operator CLI verbs

# 4. Functional smoke test (offline, in-memory CP)
docker run -d --name ws-local arun:latest local -timeout 0
docker run --rm --network container:ws-local arun:latest kcmd -type create-topic -topic demo-re -partition-count 3
docker run --rm --network container:ws-local arun:latest kcmd -type produce -topic demo-re -records "hello-re,,world-re"
docker run --rm --network container:ws-local arun:latest kcmd -type fetch -topic demo-re -partition 0 -consume-from-beginning
docker rm -f ws-local
```

---

## Appendix A — `agent` flags (157) [RUN]

Verbatim flag names from `agent -help`; see §5 for semantics of the important ones and the help text itself for the rest (the help text is unusually honest and is worth reading in full via the command above).

```
additionalBackgroundTasksBucketURLs  additionalDeadscannerBucketURLs  advertiseHostnameCustom
advertiseHostnameStrategy  agentGroup  agentKey  agentKeyPath  apiKey  autoTuneFetchLimits
availabilityZoneRequired  batchMaxCompressedSizeBytes  batchMaxSizeBytes  batchTimeout
batcherMaxInflightBytesPerCPU  batcherMaxInflightFilesPerCPU  bentoBucketURL  bentoConfigPath
blockProfileRate  bucketURL  checkBucketAccessRetryCount  clusterWideEnvironmentPort
compactionBucketURL  datadogTracingRate  defaultPartitionAssignmentStrategy
defaultVirtualClusterID  disableAzLookupWarnings  disableConsumerGroupMetrics
disableConsumerGroupsMetricsTags  disableDeadScannerOptimisticQueue  disableLogsCollection
disableOrbitFetchV2  disableOutOfRangeOrbit  disableProfileForwarding  disablePublishMetricsJob
disableQueryEngine  enableACLLogs  enableBackgroundVerifiers  enableClusterWideEnvironment
enableConfluentComponents  enableDatadogMetrics  enableDatadogProfiling  enableDatadogTracing
enableEvents  enableKafka  enableManagedPipelines  enableMetadataTopicsCache
enablePrometheusMetrics  enableQueryEngine  enableRipcord  enableSetBlockProfileRate
enableSetMutexProfileFraction  enableShutdownLock  enabledSASLMechanisms
externalSchemaRegistry* (6)  fetch-quick-retry  fetch-quick-retry-max-duration
fileCacheExtraReplicas  fileCacheSizeBytes  gcsDirectConnectivityRequired
gracefulShutdownDuration  heartbeatEvery  highCPUBackpressureThresholdPercent  httpPort
httpsPort  httpsProxyCACertFile  impersonationAllowedPrincipals  impersonationHeaderKey
ingestionBucketURL  injectPrincipalHeader  internalSpiffeTrustDomain  internalSpiffeWorkloadID
internalTLS  internalTLSBlobURL  internalTLS{ClientCA,ServerCert,ServerPrivateKey,…}Files
jobSelector  jobsRunnerSlotsAmplification  kafkaCloseIdleConnAfter  kafkaFetchCompression
kafkaHighCardinality{Distribution,}Metrics  kafkaInterzoneLoadBalancingInterval
kafkaLoadBalancing{DrainingTime,Interval}  kafkaMaxFetch{Partition,Request}Bytes…Override
kafkaMetadataRefreshInterval  kafkaPort  kafkaProxyProtocol{,PrincipalTLVType,ReadHeaderTimeout}
kafkaTLS  logInterval  logMTLSCertificateInfo  managedPipelinesGroupName
maxConcurrentMetadataRequestsPerCPU  maxConcurrentRequestPerCPU  maxFetchBytesAutoTuningThreshold
maxInflightFetchCompressedBytesPerCPU  maxNumberOfConnectionsPerCPU
maxPartitionFetchBytesAutoTuningThreshold  maxProduceRecordSizeBytes  maxProfileSize
memoryBallast{Enabled,SizeBytes}  metadataURL  multiregion  multiregionMetadataURLs
mutexProfileFraction  overridePipelinesSharedRateLimitPerVCPU  region  requireAuthentication
requireInternalMTLSAuthentication  requireMTLSAuthentication  requireSASLAuthentication  roles
saslOauth{Audience,IssuerURL}  schemaRegistry{BasicAuth,BucketURL,EnableLogRequest,Port,TLS,URL}
schemaValidation{AgentKey,URL,VirtualClusterID}  skipBucketsPermissionCheck
spiffeMTLSAuthentication  storageCompression  strictClientIDChecks
tableflow{ClusterMetricsTagDenyList,HighCardinalityDistributionMetrics,MetadataSyncMode}
testMode  tls{BlobURL,ClientCACertFile,JavaKeystore*,JavaTruststore*,PrincipalMappingRule,
Profile,ServerCertFile,ServerPrivateKeyFile,ServerPrivateKeyPasswordFile,VerifyClientCert}
workloadIdentityTokenSource  zonedCIDRBlocks
```

Environment-variable mirrors observed [BIN]: `WARPSTREAM_BUCKET_URL`, `WARPSTREAM_REGION`, `WARPSTREAM_BATCH_MAX_SIZE_BYTES`, `WARPSTREAM_COMPACTION_BUCKET_URL`, `WARPSTREAM_INGESTION_BUCKET_URL`, `WARPSTREAM_SCHEMA_REGISTRY_{BUCKET_URL,URL,TLS_ENABLED}`, `WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID`, `WARPSTREAM_HTTP_PORT`, `WARPSTREAM_LOG_LEVEL`, `WARPSTREAM_DISABLE_S3_CHECKSUMS`, `WARPSTREAM_MAX_PRODUCE_RECORD_SIZE_BYTES`, `WARPSTREAM_MAX_COMPACTION_BUFFER_SIZE`, `WARPSTREAM_{ADVERTISE,ADVERTISED}_HOSTNAME_*`, `WARPSTREAM_DISCOVERY_*_OVERRIDE`, `WARPSTREAM_PRIVATE_IP_OVERRIDE`, `WARPSTREAM_ZONED_CIDR_BLOCKS`, `WARPSTREAM_LOAD_CONFIGURATION_FILES`, `WARPSTREAM_CHAOS_FAULT` (fault injection — they chaos-test the agent against itself), `WARPSTREAM_CI`, and more in the strings dump.

## Appendix B — Control-plane client methods (161) [BIN]

`go tool nm | grep 'pkg/streamclient.(*httpStreamClient)'`, method names verbatim, sorted (infrastructure helpers `protoOverHTTP`, `retryProtoOverHTTP*`, `getURLs/setURLs`, `Init`, `VirtualClusterID`, `DumpAndResetClientStatistics` omitted from prose but included here):

> AckJobs · Compact · CompleteAbortedTransactionsOffload · CreateHyperlakeSnapshot · CreateSchemaMetadata · CreateStreams · DeadFiles · DeleteGroups · DeleteGroupsOffsets · DeleteRecords · DeleteStreams · DescribeCluster · DescribeGroupTypeConsumerConfigurations · DescribeProducers · DescribeSchemaContexts · DescribeStream · DescribeStreams · DescribeTransactions · Discover · DiscoverBatch · DLAdvanceDeletion · DLApplyDeletionBatch · DLCancelDeletion · DLCommitSnapshot · DLCompact · DLCreateDeletion · DLCreateTable · DLDeadFiles · DLDeleteTable · DLEnforceSnapshotCount · DLGetConsumedRanges · DLGetDeletionConfig · DLGetFiles · DLGetLiveSnapshots · DLGetSnapshotMetadata · DLGetTable · DLIncrementalMaintenance · DLListDeletion · DLListTables · DLMarkFilesObsolete · DLPreviewDeletion · DLPutFile · DLRegisterSnapshot · DLScanFiles · DLScanSnapshotFiles · DLTakeSnapshot · DLUpdateCatalogSnapshotVersion · DLUpdateDeletionConfig · DLUpdateTable · DLUpsertIngestion · EndTransaction · ExpireFiles · ExpireFilesV2 · ExpireOffloadedTransactionFiles · FastListStreams · FileIDAllocator · GetAbortedTransactions · GetAbortedTransactionsInRange · GetClusterLeaderRegion · GetCredentials · GetFiles · GetGroupTypeConsumerTargetAssignmentInputs · GetGroupsOffsets · GetGroupsOffsetsBatch · GetRecords · GetRecordsBatch · GetSchemaMetadata · GetSchemaMetadataByHash · GetSchemaMetadatasBySubjectVersions · GetSchemaMetadatasForCompatibility · GetSchemaMetadatasForSubject · GetSchemaRegistrySubjects · GetSizeHint · GetSubjectVersionsBatch · GroupTypeConsumerDescribe · GroupTypeConsumerHeartbeat · Heartbeat · HeartbeatGroup · HeartbeatGroupBatch · InitiateAbortedTransactionsOffload · InitProducerID · InstallGroupTypeConsumerTargetAssignment · IsClusterActive · JoinGroup · JoinGroupBatch · Kafka · KafkaBatch · KVCompareAndSwap · KVGet · KVGetMany · KVOperations · KVSet · KVSetMany · LaneConfig · LeaveGroup · ListACLs · ListAllStreams · ListClusterSuperusers · ListGroups · ListOffsetsStream · ListOffsetsStreams · ListStreams · ListTransactions · MergeShards · MigrateCluster · NewFile · OrbitResolveSrcProducerIDs · OrbitSeedProducerSequences · PollFeatureFlags · PollJobs · PushDiagnostics · PutFile · PutFileBatch · PutGroupsOffsets · PutGroupsOffsetsBatch · QueryEngine · RipcordCloseSequence · RipcordSequenceStatus · RouteStream · RSMVersion · SaslAuth · ScanFiles · ScanFilesByLane · SchemaIDExists · SchemaRegistry · SchemaRegistryAuth · SeekShard · SendAgentStats · SendAuditLogs · SendLogs · SplitShard · Statistics · SyncGroup · SyncGroupBatch · TimeoutGroupHeartbeats · TimeSeries · TransactionsInfo · UndeleteStreams · UnmigrateCluster · Unregister · UpdateConfigurations · UpdateSchemaContexts · UpdateStreams

(Also present: a generic `Kafka`/`KafkaBatch` RPC — the CP can evidently proxy arbitrary Kafka-API operations agent→backend, which is how the "consumer groups in backend" mode and Orbit are implemented [INF].)

## Appendix C — Package map (top packages by symbol count) [BIN]

See §2.2/§6. Method: `go tool nm` on the unstripped binary, aggregating `github.com/warpstreamlabs/warpstream/…` symbols. Counts are generics-inflated; use them for relative weight, not absolutes. The full per-package function lists, the 90 MB strings dump, all `-help` captures, `docker inspect` JSON and `go version -m` output were generated under `/tmp/arin-re/` during this study (ephemeral; regenerate with §23).

## Appendix D — Glossary

| Term | Meaning in this codebase |
|---|---|
| **Agent** | This binary in `agent` mode: Kafka proxy + job worker. Stateless. |
| **Virtual Cluster** | One customer cluster as a replicated state machine in the hosted CP; the unit of metadata, isolation and (presumably) billing. ID: `vci_…` [BIN `strictClientIDChecks` text] |
| **Stream** | Internal backing entity for topic-partitions (`CreateStreams`, `StreamReplica`); "stream" ≈ the partitioned log abstraction topics map onto [INF] |
| **File** | Immutable multi-partition batch container in the bucket; unit of PUT, indexing, compaction and deletion |
| **File index / pointer** | CP state mapping (stream, offset range) → (file, byte range); compaction moves pointers |
| **Lane / level** | Compaction organisation: files progress through levels within lanes (partition-range shards) |
| **Job** | CP-assigned unit of background work (`PollJobs/AckJobs`), typed by `agentpoolpb.JobType` |
| **Dead file** | Bucket object the CP has approved for deletion (obsolete/expired) or never knew (orphan); removed by the deadscanner |
| **Ripcord / lightning** | Optional path that keeps ingesting with CP unreachable, replaying into the CP later |
| **Orbit** | Proxying/federation against external (non-WarpStream) Kafka clusters |
| **Tableflow** | The datalake personality: topics → Iceberg tables (`DL*` APIs) |
| **RSM** | Replicated state machine (`pkg/rsm` framework; `pkg/stream/statemachine` application) — the Virtual Cluster's implementation shape |


## Appendix E — Control-plane RPC signatures (extracted protobuf descriptors) [BIN]

### E.0 How these were obtained (reproducible, offline)

The binary embeds its protobuf `FileDescriptorProto`s in clear (vtproto registration). Scanning `/warpstream` for the `0x0A <len> <name>.proto` header, then parsing to the next invalid top-level field boundary and decoding with `google.protobuf.descriptor_pb2`, recovers the full schemas. Core files recovered this way:

| File | Package | Messages | Enums | Services | Descriptor size |
|---|---|---:|---:|---:|---:|
| `stream.proto` | `streampb` | **379** | 38 | 1 (`Stream`, **125 RPCs**) | 161,861 B |
| `agentpool.proto` | `agentpoolpb` | 179 | 14 | 1 (`Agents`, 12 RPCs) | 61,396 B |
| `miny.proto` | `agentpoolpb` | 42 | 4 | 1 (`Miny`, 7 RPCs) | 21,245 B |
| `service_discovery.proto` | `servicediscoverypb` | 40 | 2 | (types used by `Agents`) | 17,642 B |
| `cluster.proto` | `clusterpb` | 12 | 2 | 1 (`Cluster`, 3 RPCs) | 4,057 B |
| `diagnostics.proto` | `diagnosticspb` | 80–203* | 3–10* | 2* | ~81 KB |

*diagnostics has multiple descriptor copies at different offsets (versioned embeds). **1,065 WarpStream message types were indexed in total** across the recovered files. This replaces "unknown #2" in §22: the wire format is now known at field level. What remains unknown is only server-side behaviour behind these signatures.

### E.1 Two layers of "RPC" — read this first

There are **two distinct envelopes**, and confusing them misreads the whole API:

1. **Agent ↔ control plane (the signatures in E.2/E.3).** `service Stream` (125 unary methods) + `service Agents` (12) are the logical CP API. The agent's `httpStreamClient.protoOverHTTP` POSTs the serialised `*Request` protobuf to the regional endpoint (`/v1/agent/stream`, `/v1/agent/agentpool` paths observed in strings) with the agent key; the method invoked determines which request/response pair below applies. All 125 Stream methods are **unary** (no streaming flags set on any method) — even `GetRecords` is one request / one (pointer-list) response; record bytes never flow through the CP, only pointers (`GetRecordsPointer`: file_id + index offset/length + batch_index + bucket_id).

2. **Inside the control plane (the RSM command envelope, E.4).** `streampb.Request` / `streampb.Response` are a tagged union (discriminator `request_type`, one sub-message field per operation, field numbers 3–135, plus `Request system = 12345`) — this is what gets *proposed to the replicated state machine* (`warpclusterpb.ProposeV2Request` wraps it). I.e. the CP front-end translates each `Stream` RPC into one `Request` command, the RSM applies it in log order, and the resulting `Response` union is mapped back. This is the concrete mechanism behind §10: **offset assignment in `PutFile` is a state-machine transition**, and `PutFileResponse.offsets` is computed at apply time (note `offsets` exists in stored `FileMetadata` but is *absent* from `PutFileMetadata`, which an agent submits — the agent provably does not choose offsets).

### E.2 Service definitions (complete method tables)

### service Agents — package `agentpoolpb` (file `agentpool.proto`), 12 RPCs

| # | RPC | Request | Response | Streaming |
|---:|---|---|---|---|
| 1 | `Init` | `.agentpoolpb.InitRequest` | `.agentpoolpb.InitResponse` | unary |
| 2 | `Heartbeat` | `.servicediscoverypb.HeartbeatRequest` | `.servicediscoverypb.HeartbeatResponse` | unary |
| 3 | `Unregister` | `.servicediscoverypb.UnregisterRequest` | `.servicediscoverypb.UnregisterResponse` | unary |
| 4 | `Discover` | `.servicediscoverypb.DiscoverRequest` | `.servicediscoverypb.DiscoverResponse` | unary |
| 5 | `DiscoverBatch` | `.servicediscoverypb.DiscoverBatchRequest` | `.servicediscoverypb.DiscoverBatchResponse` | unary |
| 6 | `PollJobs` | `.agentpoolpb.PollJobsRequest` | `.agentpoolpb.PollJobsResponse` | unary |
| 7 | `AckJobs` | `.agentpoolpb.AckJobsRequest` | `.agentpoolpb.AckJobsResponse` | unary |
| 8 | `SendLogs` | `.agentpoolpb.SendLogsRequest` | `.agentpoolpb.SendLogsResponse` | unary |
| 9 | `PushDiagnostics` | `.agentpoolpb.PushDiagnosticsRequest` | `.agentpoolpb.PushDiagnosticsResponse` | unary |
| 10 | `PollFeatureFlags` | `.agentpoolpb.PollFeatureFlagsRequest` | `.agentpoolpb.PollFeatureFlagsResponse` | unary |
| 11 | `SendAuditLogs` | `.agentpoolpb.SendAuditLogsRequest` | `.agentpoolpb.SendAuditLogsResponse` | unary |
| 12 | `SendAgentStats` | `.agentpoolpb.SendAgentStatsRequest` | `.agentpoolpb.SendAgentStatsResponse` | unary |


### service Miny — package `agentpoolpb` (file `miny.proto`), 7 RPCs

| # | RPC | Request | Response | Streaming |
|---:|---|---|---|---|
| 1 | `APIVersions` | `.agentpoolpb.APIVersionsRequest` | `.agentpoolpb.APIVersionsResponse` | unary |
| 2 | `SASLHandshake` | `.agentpoolpb.SASLHandshakeRequest` | `.agentpoolpb.SASLHandshakeResponse` | unary |
| 3 | `SASLAuthenticate` | `.agentpoolpb.SASLAuthenticateRequest` | `.agentpoolpb.SASLAuthenticateResponse` | unary |
| 4 | `Heartbeat` | `.agentpoolpb.MinyHeartbeatRequest` | `.agentpoolpb.MinyHeartbeatResponse` | unary |
| 5 | `Unregister` | `.agentpoolpb.MinyUnregisterRequest` | `.agentpoolpb.MinyUnregisterResponse` | unary |
| 6 | `Discover` | `.agentpoolpb.MinyDiscoverRequest` | `.agentpoolpb.MinyDiscoverResponse` | unary |
| 7 | `ReportBillingEvents` | `.agentpoolpb.MinyReportBillingEventsRequest` | `.agentpoolpb.MinyReportBillingEventsResponse` | unary |


### service Cluster — package `clusterpb` (file `cluster.proto`), 3 RPCs

| # | RPC | Request | Response | Streaming |
|---:|---|---|---|---|
| 1 | `CleanupRSMStores` | `.clusterpb.CleanupRSMStoresRequest` | `.clusterpb.CleanupRSMStoresResponse` | unary |
| 2 | `AnalyzeConsumerGroups` | `.clusterpb.AnalyzeConsumerGroupsRequest` | `.clusterpb.AnalyzeConsumerGroupsResponse` | unary |
| 3 | `DescribeStreamAllPartitions` | `.clusterpb.DescribeStreamAllPartitionsRequest` | `.clusterpb.DescribeStreamAllPartitionsResponse` | unary |


### service S2AService — package `servicediscoverypb` (file `service_discovery.proto`), 1 RPCs

| # | RPC | Request | Response | Streaming |
|---:|---|---|---|---|
| 1 | `SetUpSession` | `.s2a.proto.v2.SessionReq` | `.s2a.proto.v2.SessionResp` | client server |


### service Stream — package `streampb` (file `stream.proto`), 125 RPCs

| # | RPC | Request | Response | Streaming |
|---:|---|---|---|---|
| 1 | `ListStreams` | `.streampb.ListStreamsRequest` | `.streampb.ListStreamsResponse` | unary |
| 2 | `ListAllStreams` | `.streampb.ListAllStreamsRequest` | `.streampb.ListAllStreamsResponse` | unary |
| 3 | `FastListStreams` | `.streampb.FastListStreamsRequest` | `.streampb.FastListStreamsResponse` | unary |
| 4 | `CreateStreams` | `.streampb.CreateStreamRequest` | `.streampb.CreateStreamResponse` | unary |
| 5 | `DescribeStream` | `.streampb.DescribeStreamRequest` | `.streampb.DescribeStreamResponse` | unary |
| 6 | `DescribeStreams` | `.streampb.DescribeStreamsRequest` | `.streampb.DescribeStreamsResponse` | unary |
| 7 | `ListOffsetsStream` | `.streampb.ListOffsetsStreamRequest` | `.streampb.ListOffsetsStreamResponse` | unary |
| 8 | `ListOffsetsStreams` | `.streampb.ListOffsetsStreamsRequest` | `.streampb.ListOffsetsStreamsResponse` | unary |
| 9 | `UpdateStreams` | `.streampb.UpdateStreamsRequest` | `.streampb.UpdateStreamsResponse` | unary |
| 10 | `UpdateConfigurations` | `.streampb.UpdateConfigurationsRequest` | `.streampb.UpdateConfigurationsResponse` | unary |
| 11 | `OrbitSeedProducerSequences` | `.streampb.OrbitSeedProducerSequencesRequest` | `.streampb.OrbitSeedProducerSequencesResponse` | unary |
| 12 | `DescribeCluster` | `.streampb.DescribeClusterRequest` | `.streampb.DescribeClusterResponse` | unary |
| 13 | `DescribeGroupTypeConsumerConfigurations` | `.streampb.DescribeGroupTypeConsumerConfigurationsRequest` | `.streampb.DescribeGroupTypeConsumerConfigurationsResponse` | unary |
| 14 | `GetSizeHint` | `.streampb.GetSizeHintRequest` | `.streampb.GetSizeHintResponse` | unary |
| 15 | `NewFile` | `.streampb.NewFileRequest` | `.streampb.NewFileResponse` | unary |
| 16 | `PutFile` | `.streampb.PutFileRequest` | `.streampb.PutFileResponse` | unary |
| 17 | `PutFileBatch` | `.streampb.PutFileBatchRequest` | `.streampb.PutFileBatchResponse` | unary |
| 18 | `Compact` | `.streampb.CompactionRequest` | `.streampb.CompactionResponse` | unary |
| 19 | `GetRecords` | `.streampb.GetRecordsRequest` | `.streampb.GetRecordsResponse` | unary |
| 20 | `GetRecordsBatch` | `.streampb.GetRecordsBatchRequest` | `.streampb.GetRecordsBatchResponse` | unary |
| 21 | `SeekShard` | `.streampb.SeekShardRequest` | `.streampb.SeekShardResponse` | unary |
| 22 | `SplitShard` | `.streampb.SplitShardRequest` | `.streampb.SplitShardResponse` | unary |
| 23 | `MergeShards` | `.streampb.MergeShardsRequest` | `.streampb.MergeShardsResponse` | unary |
| 24 | `GetFiles` | `.streampb.GetFilesRequest` | `.streampb.GetFilesResponse` | unary |
| 25 | `ScanFiles` | `.streampb.ScanFilesRequest` | `.streampb.ScanFilesResponse` | unary |
| 26 | `ScanFilesByLane` | `.streampb.ScanFilesByLaneRequest` | `.streampb.ScanFilesByLaneResponse` | unary |
| 27 | `ExpireFiles` | `.streampb.ExpireFilesRequest` | `.streampb.ExpireFilesResponse` | unary |
| 28 | `ExpireFilesV2` | `.streampb.ExpireFilesV2Request` | `.streampb.ExpireFilesV2Response` | unary |
| 29 | `DeadFiles` | `.streampb.DeadFilesRequest` | `.streampb.DeadFilesResponse` | unary |
| 30 | `TimeSeries` | `.streampb.TimeSeriesRequest` | `.streampb.TimeSeriesResponse` | unary |
| 31 | `Statistics` | `.streampb.StatisticsRequest` | `.streampb.StatisticsResponse` | unary |
| 32 | `RSMVersion` | `.streampb.RSMVersionRequest` | `.streampb.RSMVersionResponse` | unary |
| 33 | `GetGroupsOffsets` | `.streampb.GetGroupsOffsetsRequest` | `.streampb.GetGroupsOffsetsResponse` | unary |
| 34 | `GetGroupsOffsetsBatch` | `.streampb.GetGroupsOffsetsBatchRequest` | `.streampb.GetGroupsOffsetsBatchResponse` | unary |
| 35 | `PutGroupsOffsets` | `.streampb.PutGroupsOffsetsRequest` | `.streampb.PutGroupsOffsetsResponse` | unary |
| 36 | `PutGroupsOffsetsBatch` | `.streampb.PutGroupsOffsetsBatchRequest` | `.streampb.PutGroupsOffsetsBatchResponse` | unary |
| 37 | `DeleteGroupsOffsets` | `.streampb.DeleteGroupsOffsetsRequest` | `.streampb.DeleteGroupsOffsetsResponse` | unary |
| 38 | `JoinGroup` | `.streampb.JoinGroupRequest` | `.streampb.JoinGroupResponse` | unary |
| 39 | `JoinGroupBatch` | `.streampb.JoinGroupBatchRequest` | `.streampb.JoinGroupBatchResponse` | unary |
| 40 | `SyncGroup` | `.streampb.SyncGroupRequest` | `.streampb.SyncGroupResponse` | unary |
| 41 | `SyncGroupBatch` | `.streampb.SyncGroupBatchRequest` | `.streampb.SyncGroupBatchResponse` | unary |
| 42 | `HeartbeatGroup` | `.streampb.HeartbeatGroupRequest` | `.streampb.HeartbeatGroupResponse` | unary |
| 43 | `HeartbeatGroupBatch` | `.streampb.HeartbeatBatchRequest` | `.streampb.HeartbeatBatchResponse` | unary |
| 44 | `LeaveGroup` | `.streampb.LeaveGroupRequest` | `.streampb.LeaveGroupResponse` | unary |
| 45 | `TimeoutGroupHeartbeats` | `.streampb.TimeoutGroupHeartbeatsRequest` | `.streampb.TimeoutGroupHeartbeatsResponse` | unary |
| 46 | `ListGroups` | `.streampb.ListGroupsRequest` | `.streampb.ListGroupsResponse` | unary |
| 47 | `DeleteGroups` | `.streampb.DeleteGroupsRequest` | `.streampb.DeleteGroupsResponse` | unary |
| 48 | `InitProducerID` | `.streampb.InitProducerIDRequest` | `.streampb.InitProducerIDResponse` | unary |
| 49 | `OrbitResolveSrcProducerIDs` | `.streampb.OrbitResolveSrcProducerIDsRequest` | `.streampb.OrbitResolveSrcProducerIDsResponse` | unary |
| 50 | `DeleteStreams` | `.streampb.DeleteStreamsRequest` | `.streampb.DeleteStreamsResponse` | unary |
| 51 | `UndeleteStreams` | `.streampb.UndeleteStreamsRequest` | `.streampb.UndeleteStreamsResponse` | unary |
| 52 | `DeleteRecords` | `.streampb.DeleteRecordsRequest` | `.streampb.DeleteRecordsResponse` | unary |
| 53 | `DescribeTransactions` | `.streampb.DescribeTransactionsRequest` | `.streampb.DescribeTransactionsResponse` | unary |
| 54 | `DescribeProducers` | `.streampb.DescribeProducersRequest` | `.streampb.DescribeProducersResponse` | unary |
| 55 | `ListTransactions` | `.streampb.ListTransactionsRequest` | `.streampb.ListTransactionsResponse` | unary |
| 56 | `Kafka` | `.streampb.KafkaRequest` | `.streampb.KafkaResponse` | unary |
| 57 | `KafkaBatch` | `.streampb.KafkaBatchRequest` | `.streampb.KafkaBatchResponse` | unary |
| 58 | `SaslAuth` | `.streampb.SaslAuthRequest` | `.streampb.SaslAuthResponse` | unary |
| 59 | `ListACLs` | `.streampb.ListACLsRequest` | `.streampb.ListACLsResponse` | unary |
| 60 | `ListClusterSuperusers` | `.streampb.ListClusterSuperusersRequest` | `.streampb.ListClusterSuperusersResponse` | unary |
| 61 | `GetCredentials` | `.streampb.GetCredentialsRequest` | `.streampb.GetCredentialsResponse` | unary |
| 62 | `KVOperations` | `.streampb.KVOperationsRequest` | `.streampb.KVOperationsResponse` | unary |
| 63 | `LaneConfig` | `.streampb.LaneConfigRequest` | `.streampb.LaneConfigResponse` | unary |
| 64 | `TransactionsInfo` | `.streampb.TransactionsInfoRequest` | `.streampb.TransactionsInfoResponse` | unary |
| 65 | `EndTransaction` | `.streampb.EndTransactionRequest` | `.streampb.EndTransactionResponse` | unary |
| 66 | `CreateHyperlakeSnapshot` | `.hyperlakepb.CreateHyperlakeSnapshotRequest` | `.hyperlakepb.CreateHyperlakeSnapshotResponse` | unary |
| 67 | `SchemaRegistry` | `.streampb.SchemaRegistryRequest` | `.streampb.SchemaRegistryResponse` | unary |
| 68 | `InitiateAbortedTransactionsOffload` | `.streampb.InitiateAbortedTransactionsOffloadRequest` | `.streampb.InitiateAbortedTransactionsOffloadResponse` | unary |
| 69 | `GetAbortedTransactions` | `.streampb.GetAbortedTransactionsRequest` | `.streampb.GetAbortedTransactionsResponse` | unary |
| 70 | `CompleteAbortedTransactionsOffload` | `.streampb.CompleteAbortedTransactionsOffloadRequest` | `.streampb.CompleteAbortedTransactionsOffloadResponse` | unary |
| 71 | `GetAbortedTransactionsInRange` | `.streampb.GetAbortedTransactionsInRangeRequest` | `.streampb.GetAbortedTransactionsInRangeResponse` | unary |
| 72 | `ExpireOffloadedTransactionFiles` | `.streampb.ExpireOffloadedTransactionFilesRequest` | `.streampb.ExpireOffloadedTransactionFilesResponse` | unary |
| 73 | `SchemaRegistryAuth` | `.streampb.SchemaRegistryAuthRequest` | `.streampb.SchemaRegistryAuthResponse` | unary |
| 74 | `CreateSchemaMetadata` | `.streampb.CreateSchemaMetadataRequest` | `.streampb.CreateSchemaMetadataResponse` | unary |
| 75 | `GetSchemaMetadata` | `.streampb.GetSchemaMetadataRequest` | `.streampb.GetSchemaMetadataResponse` | unary |
| 76 | `GetSchemaMetadataByHash` | `.streampb.GetSchemaMetadataByHashRequest` | `.streampb.GetSchemaMetadataByHashResponse` | unary |
| 77 | `GetSchemaMetadatasBySubjectVersions` | `.streampb.GetSchemaMetadatasBySubjectVersionsRequest` | `.streampb.GetSchemaMetadatasBySubjectVersionsResponse` | unary |
| 78 | `GetSchemaMetadatasForCompatibility` | `.streampb.GetSchemaMetadatasForCompatibilityRequest` | `.streampb.GetSchemaMetadatasForCompatibilityResponse` | unary |
| 79 | `GetSchemaRegistrySubjects` | `.streampb.GetSchemaRegistrySubjectsRequest` | `.streampb.GetSchemaRegistrySubjectsResponse` | unary |
| 80 | `UpdateSchemaContexts` | `.streampb.UpdateSchemaContextsRequest` | `.streampb.UpdateSchemaContextsResponse` | unary |
| 81 | `DescribeSchemaContexts` | `.streampb.DescribeSchemaContextsRequest` | `.streampb.DescribeSchemaContextsResponse` | unary |
| 82 | `GetSchemaMetadatasForSubject` | `.streampb.GetSchemaMetadatasForSubjectRequest` | `.streampb.GetSchemaMetadatasForSubjectResponse` | unary |
| 83 | `GetSubjectVersionsBatch` | `.streampb.GetSubjectVersionsBatchRequest` | `.streampb.GetSubjectVersionsBatchResponse` | unary |
| 84 | `SchemaIDExists` | `.streampb.SchemaIDExistsRequest` | `.streampb.SchemaIDExistsResponse` | unary |
| 85 | `IsClusterActive` | `.streampb.IsClusterActiveRequest` | `.streampb.IsClusterActiveResponse` | unary |
| 86 | `GetClusterLeaderRegion` | `.streampb.GetClusterLeaderRegionRequest` | `.streampb.GetClusterLeaderRegionResponse` | unary |
| 87 | `MigrateCluster` | `.streampb.MigrateClusterRequest` | `.streampb.MigrateClusterResponse` | unary |
| 88 | `UnmigrateCluster` | `.streampb.UnmigrateClusterRequest` | `.streampb.UnmigrateClusterResponse` | unary |
| 89 | `DLCreateTable` | `.streampb.DLCreateTableRequest` | `.streampb.DLCreateTableResponse` | unary |
| 90 | `DLDeleteTable` | `.streampb.DLDeleteTableRequest` | `.streampb.DLDeleteTableResponse` | unary |
| 91 | `DLUpdateTable` | `.streampb.DLUpdateTableRequest` | `.streampb.DLUpdateTableResponse` | unary |
| 92 | `DLPutFile` | `.streampb.DLPutFileRequest` | `.streampb.DLPutFileResponse` | unary |
| 93 | `DLScanSnapshotFiles` | `.streampb.DLScanSnapshotFilesRequest` | `.streampb.DLScanSnapshotFilesResponse` | unary |
| 94 | `DLGetFiles` | `.streampb.DLGetFilesRequest` | `.streampb.DLGetFilesResponse` | unary |
| 95 | `DLGetConsumedRanges` | `.streampb.DLGetConsumedRangesRequest` | `.streampb.DLGetConsumedRangesResponse` | unary |
| 96 | `DLTakeSnapshot` | `.streampb.DLTakeSnapshotRequest` | `.streampb.DLTakeSnapshotResponse` | unary |
| 97 | `DLRegisterSnapshot` | `.streampb.DLRegisterSnapshotRequest` | `.streampb.DLRegisterSnapshotResponse` | unary |
| 98 | `DLCommitSnapshot` | `.streampb.DLCommitSnapshotRequest` | `.streampb.DLCommitSnapshotResponse` | unary |
| 99 | `DLListTables` | `.streampb.DLListTablesRequest` | `.streampb.DLListTablesResponse` | unary |
| 100 | `DLScanFiles` | `.streampb.DLScanFilesRequest` | `.streampb.DLScanFilesResponse` | unary |
| 101 | `DLCompact` | `.streampb.DLCompactionRequest` | `.streampb.DLCompactionResponse` | unary |
| 102 | `DLApplyDeletionBatch` | `.streampb.DLApplyDeletionBatchRequest` | `.streampb.DLApplyDeletionBatchResponse` | unary |
| 103 | `DLCreateDeletion` | `.streampb.DLCreateDeletionRequest` | `.streampb.DLCreateDeletionResponse` | unary |
| 104 | `DLListDeletion` | `.streampb.DLListDeletionRequest` | `.streampb.DLListDeletionResponse` | unary |
| 105 | `DLCancelDeletion` | `.streampb.DLCancelDeletionRequest` | `.streampb.DLCancelDeletionResponse` | unary |
| 106 | `DLAdvanceDeletion` | `.streampb.DLAdvanceDeletionRequest` | `.streampb.DLAdvanceDeletionResponse` | unary |
| 107 | `DLPreviewDeletion` | `.streampb.DLPreviewDeletionRequest` | `.streampb.DLPreviewDeletionResponse` | unary |
| 108 | `DLGetDeletionConfig` | `.streampb.DLGetDeletionConfigRequest` | `.streampb.DLGetDeletionConfigResponse` | unary |
| 109 | `DLUpdateDeletionConfig` | `.streampb.DLUpdateDeletionConfigRequest` | `.streampb.DLUpdateDeletionConfigResponse` | unary |
| 110 | `DLGetTable` | `.streampb.DLGetTableRequest` | `.streampb.DLGetTableResponse` | unary |
| 111 | `DLGetSnapshotMetadata` | `.streampb.DLGetSnapshotMetadataRequest` | `.streampb.DLGetSnapshotMetadataResponse` | unary |
| 112 | `DLGetLiveSnapshots` | `.streampb.DLGetLiveSnapshotsRequest` | `.streampb.DLGetLiveSnapshotsResponse` | unary |
| 113 | `DLEnforceSnapshotCount` | `.streampb.DLEnforceSnapshotCountRequest` | `.streampb.DLEnforceSnapshotCountResponse` | unary |
| 114 | `DLDeadFiles` | `.streampb.DLDeadFilesRequest` | `.streampb.DLDeadFilesResponse` | unary |
| 115 | `DLMarkFilesObsolete` | `.streampb.DLMarkFilesObsoleteRequest` | `.streampb.DLMarkFilesObsoleteResponse` | unary |
| 116 | `DLIncrementalMaintenance` | `.streampb.DLIncrementalMaintenanceRequest` | `.streampb.DLIncrementalMaintenanceResponse` | unary |
| 117 | `DLUpdateCatalogSnapshotVersion` | `.streampb.DLUpdateCatalogSnapshotVersionRequest` | `.streampb.DLUpdateCatalogSnapshotVersionResponse` | unary |
| 118 | `DLUpsertIngestion` | `.streampb.DLUpsertIngestionRequest` | `.streampb.DLUpsertIngestionResponse` | unary |
| 119 | `QueryEngine` | `.streampb.QueryEngineRequest` | `.streampb.QueryEngineResponse` | unary |
| 120 | `RipcordCloseSequence` | `.streampb.RipcordCloseSequenceRequest` | `.streampb.RipcordCloseSequenceResponse` | unary |
| 121 | `RipcordSequenceStatus` | `.streampb.RipcordSequenceStatusRequest` | `.streampb.RipcordSequenceStatusResponse` | unary |
| 122 | `GroupTypeConsumerHeartbeat` | `.streampb.GroupTypeConsumerHeartbeatRequest` | `.streampb.GroupTypeConsumerHeartbeatResponse` | unary |
| 123 | `GroupTypeConsumerDescribe` | `.streampb.GroupTypeConsumerDescribeRequest` | `.streampb.GroupTypeConsumerDescribeResponse` | unary |
| 124 | `GetGroupTypeConsumerTargetAssignmentInputs` | `.streampb.GetGroupTypeConsumerTargetAssignmentInputsRequest` | `.streampb.GetGroupTypeConsumerTargetAssignmentInputsResponse` | unary |
| 125 | `InstallGroupTypeConsumerTargetAssignment` | `.streampb.InstallGroupTypeConsumerTargetAssignmentRequest` | `.streampb.InstallGroupTypeConsumerTargetAssignmentResponse` | unary |

### E.3 Hot-path message signatures (field level)

Field numbers and types below are exact (descriptor-decoded). Parallel-array encoding is pervasive: `PutFileMetadata` carries one entry *per batch* in each of `stream_ids / shard_ids / counts / *_size_hints / kafka_producer_*` — this is the concrete form of "CP load scales with batch count" (§9.2).

```proto
message PutFileRequest {
  PutFileMetadata file_metadata = 1;
  uint64 timestamp_nanos = 2;
  bool extended = 3;
  string bucket_url = 4;
}```

```proto
message PutFileResponse {
  repeated uint64 offsets = 1;
  repeated BatchOutcome outcomes = 3;
  repeated uint64 extended_first_offsets = 2;
  RipcordIngestStatus ripcord = 4;
}```

```proto
message PutFileBatchRequest {
  repeated PutFileRequest requests = 1;
}```

```proto
message NewFileRequest {
  uint64 count = 1;
}```

```proto
message NewFileResponse {
  uint64 min = 1;
}```

```proto
message GetFilesRequest {
  repeated uint64 file_ids = 1;
}```

```proto
message GetFilesResponse {
  repeated bool exists = 1;
  repeated FileMetadata file_metadata = 2;
  repeated Bucket buckets = 3;
  repeated uint32 compacted_topics_stream_ids = 4;
}```

```proto
message GetRecordsRequest {
  string stream_name = 1;
  uint32 stream_id = 7;
  uint32 shard_id = 2;
  uint64 offset = 3;
  uint32 limit = 4;
  uint32 uncompressed_bytes_limit = 5;
  bool ignore_bytes_limit = 14;
  bool allow_exceed_records_limit = 15;
  uint32 pointer_limit = 8;
  bool extended = 6;
  bool include_aborted_transactions = 9;
  OffsetUpperBound upper_bound = 10;
  bool use_timestamp_based_leader_epoch = 11;
  uint64 timestamp_nanos = 12;
}```

```proto
message GetRecordsResponse {
  repeated GetRecordsPointer pointers = 2;
  EndOfShard end_of_shard = 3;
  ShardOffsetInfo extended_shard_info = 4;
  repeated Bucket buckets = 5;
  uint64 nanos_behind_latest = 6;
  CompressionType application_compression_type = 7;
  repeated int64 aborted_transactions_producer_ids = 8;
  repeated uint64 aborted_transactions_first_offset = 9;
  repeated uint64 aborted_transactions_last_offset = 10;
  repeated AbortedTransactionsFile aborted_transactions_offloaded_files = 12;
  uint64 stop_at_offset = 11;
  int32 leader_epoch = 13;
}```

```proto
message CompactionRequest {
  CompactionOutput output = 1;
}```

```proto
message CompactionResponse {
}```

```proto
message FileMetadata {
  uint64 file_id = 1;
  uint32 bucket_id = 16;
  ContentType content_type = 20;
  uint64 index_offset = 2;
  uint64 index_length = 3;
  uint64 uncompressed_size_hint = 13;
  repeated uint32 stream_ids = 4;
  repeated uint32 shard_ids = 5;
  repeated uint64 counts = 6;
  repeated uint64 offsets = 7;
  repeated uint64 uncompressed_size_hints = 12;
  repeated BatchOutcome outcomes = 17;
  repeated uint32 truncated_max_timestamps = 21;
  uint64 file_index_min = 8;
  uint64 file_index_max = 9;
  uint64 min_timestamp_nanos = 14;
  uint64 max_timestamp_nanos = 15;
  uint32 level = 10;
  int32 lane_id = 18;
  LaneType lane_type = 19;
  bool is_eof = 11;
  RipcordFileLocation ripcord = 22;
  uint64 created_at_timestamp_nanos = 23;
  uint64 earliest_record_not_compacted_timestamp_nanos = 24;
}```

```proto
message CreateStreamRequest {
  repeated Stream streams = 1;
  bool partial = 2;
  bool validate_only = 3;
}```

```proto
message CreateStreamResponse {
  repeated DescribeStreamResponse descriptions = 1;
  repeated Error partial_errors = 2;
}```

```proto
message Request {
  RequestType request_type = 1;
  string client_request_id = 2;
  string client_id = 32;
  uint64 proposer_timestamp_nanos = 50;
  bool force_low_latency = 54;
  CreateStreamRequest create_stream = 3;
  DescribeStreamRequest describe_stream = 10;
  DescribeStreamsRequest describe_streams = 44;
  ListOffsetsStreamRequest list_offsets_stream = 37;
  ListOffsetsStreamsRequest list_offsets_streams = 45;
  UpdateStreamsRequest update_streams = 28;
  NewFileRequest new_file = 4;
  PutFileRequest put_file = 5;
  GetRecordsRequest get_records = 6;
  GetRecordsBatchRequest get_records_batch = 33;
  CompactionRequest compaction = 7;
  ScanFilesRequest scan_files = 8;
  GetFilesRequest get_files = 9;
  SplitShardRequest split_shard = 11;
  MergeShardsRequest merge_shards = 12;
  SeekShardRequest seek_shard = 13;
  ExpireFilesRequest expire_files = 14;
  ListStreamsRequest list_streams = 15;
  ListAllStreamsRequest list_all_streams = 81;
  FastListStreamsRequest fast_list_streams = 56;
  DeadFilesRequest dead_files = 16;
  TimeSeriesRequest time_series = 17;
  StatisticsRequest statistics = 18;
  RSMVersionRequest rsm_version = 103;
  GetGroupsOffsetsRequest get_groups_offsets = 20;
  GetGroupsOffsetsBatchRequest get_groups_offsets_batch = 102;
  PutGroupsOffsetsRequest put_groups_offsets = 21;
  DeleteGroupsOffsetsRequest delete_groups_offsets = 22;
  PutFileBatchRequest put_file_batch = 34;
  InitProducerIDRequest init_producer_id = 35;
  KafkaRequest kafka = 19;
  KafkaBatchRequest kafka_batch = 46;
  JoinGroupRequest join_group = 23;
  SyncGroupRequest sync_group = 24;
  HeartbeatGroupRequest heartbeat_group = 25;
  LeaveGroupRequest leave_group = 31;
  TimeoutGroupHeartbeatsRequest timeout_group_heartbeats = 26;
  ListGroupsRequest list_groups = 27;
  DeleteGroupsRequest delete_groups = 30;
  DeleteStreamsRequest delete_streams = 36;
  UpdateConfigurationsRequest update_configurations = 38;
  DescribeClusterRequest describe_cluster = 39;
  ListACLsRequest list_acls = 40;
  ListClusterSuperusersRequest list_cluster_superusers = 41;
  KVOperationsRequest kv_operations = 42;
  LaneConfigRequest lane_config = 43;
  SaslAuthRequest sasl_auth = 47;
  PutGroupsOffsetsBatchRequest put_groups_offsets_batch = 48;
  HeartbeatBatchRequest heartbeat_batch = 49;
  JoinGroupBatchRequest join_group_batch = 123;
  SyncGroupBatchRequest sync_group_batch = 124;
  TransactionsInfoRequest transactions_info = 51;
  CreateHyperlakeSnapshotRequest create_hyperlake_snapshot = 52;
  EndTransactionRequest end_transaction = 53;
  GetSizeHintRequest get_size_hint = 55;
  SchemaRegistryRequest schema_registry = 57;
  InitiateAbortedTransactionsOffloadRequest initiate_transaction_offload = 59;
  GetAbortedTransactionsRequest get_aborted_transactions = 61;
  CompleteAbortedTransactionsOffloadRequest complete_transaction_offload = 60;
  GetAbortedTransactionsInRangeRequest get_aborted_transactions_in_range = 62;
  CreateSchemaMetadataRequest create_schema_metadata = 63;
  GetSchemaMetadataRequest get_schema_metadata = 64;
  GetSchemaMetadataByHashRequest get_schema_metadata_by_hash = 122;
  GetSchemaMetadatasBySubjectVersionsRequest get_schema_metadatas_by_subject_versions = 65;
  GetSchemaMetadatasForCompatibilityRequest get_schema_metadatas_for_compatibility = 66;
  GetSchemaRegistrySubjectsRequest get_schema_registry_subjects = 67;
  DeleteRecordsRequest delete_records = 68;
  DescribeTransactionsRequest describe_transactions = 69;
  ExpireOffloadedTransactionFilesRequest expire_offloaded_transaction_files = 70;
  ExpireFilesV2Request expire_files_v2 = 71;
  ListTransactionsRequest list_transactions = 72;
  IsClusterActiveRequest is_cluster_active = 73;
  SchemaRegistryAuthRequest schema_registry_auth = 74;
  UpdateSchemaContextsRequest update_schema_contexts = 75;
  DescribeProducersRequest describe_producers = 76;
  GetSubjectVersionsBatchRequest get_subject_versions_batch = 77;
  SchemaIDExistsRequest schema_id_exists = 78;
  GetCredentialsRequest get_credentials = 79;
  UndeleteStreamsRequest undelete_streams = 80;
  GetClusterLeaderRegionRequest get_cluster_leader_region = 82;
  DescribeSchemaContextsRequest describe_schema_contexts = 83;
  GetSchemaMetadatasForSubjectRequest get_schema_metadatas_for_subject = 101;
  DLCreateTableRequest dl_create_table = 84;
  DLDeleteTableRequest dl_delete_table = 85;
  DLUpdateTableRequest dl_update_table = 109;
  DLPutFileRequest dl_put_file = 86;
  DLScanSnapshotFilesRequest dl_scan_snapshot_files = 87;
  DLGetFilesRequest dl_get_files = 88;
  DLGetConsumedRangesRequest dl_get_consumed_ranges = 89;
  DLListTablesRequest dl_list_tables = 90;
  DLTakeSnapshotRequest dl_take_snapshot = 91;
  DLRegisterSnapshotRequest dl_register_snapshot = 93;
  DLCommitSnapshotRequest dl_commit_snapshot = 92;
  DLScanFilesRequest dl_scan_files = 95;
  DLCompactionRequest dl_compaction = 96;
  DLApplyDeletionBatchRequest dl_apply_deletion_batch = 126;
  DLCreateDeletionRequest dl_create_deletion = 127;
  DLListDeletionRequest dl_list_deletion = 128;
  DLCancelDeletionRequest dl_cancel_deletion = 129;
  DLAdvanceDeletionRequest dl_advance_deletion = 130;
  DLPreviewDeletionRequest dl_preview_deletion = 131;
  DLGetDeletionConfigRequest dl_get_deletion_config = 134;
  DLUpdateDeletionConfigRequest dl_update_deletion_config = 135;
  DLGetTableRequest dl_get_table = 97;
  DLGetLiveSnapshotsRequest dl_get_live_snapshots = 98;
  DLEnforceSnapshotCountRequest dl_enforce_snapshot_count = 99;
  DLDeadFilesRequest dl_dead_files = 100;
  DLMarkFilesObsoleteRequest dl_mark_files_obsolete = 106;
  DLIncrementalMaintenanceRequest dl_incremental_maintenance = 107;
  DLUpdateCatalogSnapshotVersionRequest dl_update_catalog_snapshot_version = 113;
  DLUpsertIngestionRequest dl_upsert_ingestion = 114;
  QueryEngineRequest query_engine = 94;
  RipcordCloseSequenceRequest ripcord_close_sequence = 104;
  RipcordSequenceStatusRequest ripcord_sequence_status = 105;
  MigrateClusterRequest migrate_cluster = 110;
  UnmigrateClusterRequest unmigrate_cluster = 115;
  PITRTransformRequest pitr_transform = 125;
  GroupTypeConsumerHeartbeatRequest group_type_consumer_heartbeat = 111;
  GroupTypeConsumerDescribeRequest group_type_consumer_describe = 116;
  GetGroupTypeConsumerTargetAssignmentInputsRequest get_group_type_consumer_target_assignment_inputs = 120;
  InstallGroupTypeConsumerTargetAssignmentRequest install_group_type_consumer_target_assignment = 121;
  ScanFilesByLaneRequest scan_files_by_lane = 112;
  DescribeGroupTypeConsumerConfigurationsRequest describe_group_type_consumer_configurations = 117;
  DLGetSnapshotMetadataRequest dl_get_snapshot_metadata = 118;
  OrbitResolveSrcProducerIDsRequest orbit_resolve_src_producer_ids = 132;
  OrbitSeedProducerSequencesRequest orbit_seed_producer_sequences = 133;
  Request system = 12345;
}```

```proto
message Response {
  RequestType request_type = 1;
  string client_request_id = 2;
  Error error = 3;
  CreateStreamResponse create_stream = 4;
  DescribeStreamResponse describe_stream = 11;
  DescribeStreamsResponse describe_streams = 44;
  ListOffsetsStreamResponse list_offsets_stream = 37;
  ListOffsetsStreamsResponse list_offsets_streams = 45;
  UpdateStreamsResponse update_streams = 29;
  NewFileResponse new_file = 5;
  PutFileResponse put_file = 6;
  GetRecordsResponse get_records = 7;
  GetRecordsBatchResponse get_records_batch = 33;
  CompactionResponse compaction = 8;
  ScanFilesResponse scan_files = 9;
  GetFilesResponse get_files = 10;
  SplitShardResponse split_shard = 12;
  MergeShardsResponse merge_shards = 13;
  SeekShardResponse seek_shard = 14;
  ExpireFilesResponse expire_files = 15;
  ListStreamsResponse list_streams = 16;
  ListAllStreamsResponse list_all_streams = 79;
  FastListStreamsResponse fast_list_streams = 55;
  DeadFilesResponse dead_files = 17;
  TimeSeriesResponse time_series = 18;
  StatisticsResponse statistics = 19;
  RSMVersionResponse rsm_version = 103;
  KafkaResponse kafka = 20;
  KafkaBatchResponse kafka_batch = 46;
  PutFileBatchResponse put_file_batch = 34;
  InitProducerIDResponse init_producer_id = 35;
  GetGroupsOffsetsResponse get_groups_offsets = 21;
  GetGroupsOffsetsBatchResponse get_groups_offsets_batch = 102;
  PutGroupsOffsetsResponse put_groups_offsets = 22;
  DeleteGroupsOffsetsResponse delete_groups_offsets = 23;
  JoinGroupResponse join_group = 24;
  SyncGroupResponse sync_group = 25;
  HeartbeatGroupResponse heartbeat_group = 26;
  LeaveGroupResponse leave_group = 32;
  TimeoutGroupHeartbeatsResponse timeout_group_heartbeats = 27;
  ListGroupsResponse list_groups = 28;
  DeleteGroupsResponse delete_groups = 31;
  DeleteStreamsResponse delete_streams = 36;
  UpdateConfigurationsResponse update_configurations = 38;
  DescribeClusterResponse describe_cluster = 39;
  ListACLsResponse list_acls = 40;
  ListClusterSuperusersResponse list_cluster_superusers = 41;
  KVOperationsResponse kv_operations = 42;
  LaneConfigResponse lane_config = 43;
  SaslAuthResponse sasl_auth = 47;
  PutGroupsOffsetsBatchResponse put_groups_offsets_batch = 48;
  HeartbeatBatchResponse heartbeat_batch = 49;
  JoinGroupBatchResponse join_group_batch = 123;
  SyncGroupBatchResponse sync_group_batch = 124;
  TransactionsInfoResponse transactions_info = 51;
  CreateHyperlakeSnapshotResponse create_hyperlake_snapshot = 52;
  EndTransactionResponse end_transaction = 53;
  GetSizeHintResponse get_size_hint = 54;
  SchemaRegistryResponse schema_registry = 56;
  InitiateAbortedTransactionsOffloadResponse initiate_transaction_offload = 57;
  GetAbortedTransactionsResponse get_aborted_transactions = 59;
  CompleteAbortedTransactionsOffloadResponse complete_transaction_offload = 58;
  GetAbortedTransactionsInRangeResponse get_aborted_transactions_in_range = 60;
  CreateSchemaMetadataResponse create_schema_metadata = 61;
  GetSchemaMetadataResponse get_schema_metadata = 62;
  GetSchemaMetadataByHashResponse get_schema_metadata_by_hash = 122;
  GetSchemaMetadatasBySubjectVersionsResponse get_schema_metadatas_by_subject_versions = 63;
  GetSchemaMetadatasForCompatibilityResponse get_schema_metadatas_for_compatibility = 64;
  GetSchemaRegistrySubjectsResponse get_schema_registry_subjects = 65;
  DeleteRecordsResponse delete_records = 66;
  DescribeTransactionsResponse describe_transactions = 67;
  ExpireOffloadedTransactionFilesResponse expire_offloaded_transaction_files = 68;
  ExpireFilesV2Response expire_files_v2 = 69;
  ListTransactionsResponse list_transactions = 70;
  IsClusterActiveResponse is_cluster_active = 71;
  SchemaRegistryAuthResponse schema_registry_auth = 72;
  UpdateSchemaContextsResponse update_schema_contexts = 73;
  DescribeProducersResponse describe_producers = 74;
  GetSubjectVersionsBatchResponse get_subject_versions_batch = 75;
  SchemaIDExistsResponse schema_id_exists = 76;
  GetCredentialsResponse get_credentials = 77;
  UndeleteStreamsResponse undelete_streams = 78;
  GetClusterLeaderRegionResponse get_cluster_leader_region = 80;
  DescribeSchemaContextsResponse describe_schema_contexts = 81;
  GetSchemaMetadatasForSubjectResponse get_schemas_metadatas_for_subject = 101;
  DLCreateTableResponse dl_create_table = 84;
  DLDeleteTableResponse dl_delete_table = 85;
  DLUpdateTableResponse dl_update_table = 109;
  DLPutFileResponse dl_put_file = 86;
  DLScanSnapshotFilesResponse dl_scan_snapshot_files = 87;
  DLGetFilesResponse dl_get_files = 88;
  DLGetConsumedRangesResponse dl_get_consumed_ranges = 89;
  DLListTablesResponse dl_list_tables = 90;
  DLTakeSnapshotResponse dl_take_snapshot = 91;
  DLRegisterSnapshotResponse dl_register_snapshot = 93;
  DLCommitSnapshotResponse dl_commit_snapshot = 92;
  DLScanFilesResponse dl_scan_files = 95;
  DLCompactionResponse dl_compaction = 96;
  DLApplyDeletionBatchResponse dl_apply_deletion_batch = 126;
  DLCreateDeletionResponse dl_create_deletion = 127;
  DLListDeletionResponse dl_list_deletion = 128;
  DLCancelDeletionResponse dl_cancel_deletion = 129;
  DLAdvanceDeletionResponse dl_advance_deletion = 130;
  DLPreviewDeletionResponse dl_preview_deletion = 131;
  DLGetDeletionConfigResponse dl_get_deletion_config = 134;
  DLUpdateDeletionConfigResponse dl_update_deletion_config = 135;
  DLGetTableResponse dl_get_table = 97;
  DLGetLiveSnapshotsResponse dl_get_live_snapshots = 98;
  DLEnforceSnapshotCountResponse dl_enforce_snapshot_count = 99;
  DLDeadFilesResponse dl_dead_files = 100;
  DLMarkFilesObsoleteResponse dl_mark_files_obsolete = 106;
  DLIncrementalMaintenanceResponse dl_incremental_maintenance = 107;
  DLUpdateCatalogSnapshotVersionResponse dl_update_catalog_snapshot_version = 113;
  DLUpsertIngestionResponse dl_upsert_ingestion = 114;
  QueryEngineResponse query_engine = 94;
  RipcordCloseSequenceResponse ripcord_close_sequence = 104;
  RipcordSequenceStatusResponse ripcord_sequence_status = 105;
  MigrateClusterResponse migrate_cluster = 110;
  UnmigrateClusterResponse unmigrate_cluster = 115;
  PITRTransformResponse pitr_transform = 125;
  GroupTypeConsumerHeartbeatResponse group_type_consumer_heartbeat = 111;
  GroupTypeConsumerDescribeResponse group_type_consumer_describe = 116;
  GetGroupTypeConsumerTargetAssignmentInputsResponse get_group_type_consumer_target_assignment_inputs = 120;
  InstallGroupTypeConsumerTargetAssignmentResponse install_group_type_consumer_target_assignment = 121;
  ScanFilesByLaneResponse scan_files_by_lane = 112;
  DescribeGroupTypeConsumerConfigurationsResponse describe_group_type_consumer_configurations = 117;
  DLGetSnapshotMetadataResponse dl_get_snapshot_metadata = 118;
  OrbitResolveSrcProducerIDsResponse orbit_resolve_src_producer_ids = 132;
  OrbitSeedProducerSequencesResponse orbit_seed_producer_sequences = 133;
  Response system = 12345;
  uint64 version = 12346;
}```

```proto
message PollJobsRequest {
  Version agent_version = 1;
  JobRunnerStats job_runner_stats = 2;
  Stats agent_stats = 3;
  JobPriority minimum_priority = 4;
  JobTypeSelector job_type_selector = 5;
}```

```proto
message PollJobsResponse {
  repeated JobInput jobs = 1;
}```

```proto
message AckJobsRequest {
  repeated JobOutput job_results = 1;
}```

```proto
message HeartbeatRequest {
  Heartbeat heartbeat = 1;
}```

```proto
message DiscoverRequest {
  repeated string availability_zones = 1;
  bool disable_error_if_insufficient_heartbeats = 2;
  Tags tags = 3;
  EmptyAvailabilityZonesBehavior empty_az_behavior = 4;
  repeated string expected_availability_zones = 5;
  LoadBalancingStrategy load_balancing_strategy = 6;
  bool bypass_embedded_replica = 7;
}```

```proto
message DiscoverResponse {
  repeated Heartbeat heartbeats = 1;
}```

```proto
message JoinGroupRequest {
  uint64 timestamp_nanos = 1;
  string group_id = 2;
  int32 session_timeout_ms = 3;
  uint32 rebalance_timeout_ms = 4;
  string member_id = 5;
  string client_id = 8;
  string client_host = 9;
  string group_instance_id = 6;
  repeated Protocol protocols = 7;
  bool support_skipping_assignment = 10;
  bool require_known_member_id = 11;
  int32 num_poll_iteration = 12;
  uint64 poll_generation_id = 13;
  string protocol_type = 14;
  bool disable_generation_skipping = 15;
  bool dont_compute_target_assignments = 16;
  // oneof _protocol_type
}```

```proto
message GetGroupsOffsetsRequest {
  string group_name = 1;
  repeated GroupOffsetIndex streams_filter = 2;
  bool include_timestamps = 3;
  bool collect_unknown_streams = 4;
  bool validate_member = 5;
  string member_id = 6;
  int64 member_epoch = 7;
  bool use_interpolate_v2 = 8;
}```

```proto
message PutGroupsOffsetsRequest {
  string group_name = 1;
  repeated GroupOffsetValue streams = 2;
  int64 generation_id = 3;
  string member_id = 4;
  string instance_id = 5;
  uint64 timestamp_nanos = 6;
  string transactional_id = 7;
  int64 producer_id = 8;
  int32 producer_epoch = 9;
  int32 offset_commit_kafka_api_version = 10;
  bool force = 11;
}```

```proto
message InitProducerIDRequest {
  string transactional_id = 1;
  int64 producer_id = 2;
  int32 producer_epoch = 3;
  int64 timestamp_nanos = 4;
  int64 transaction_timeout_nanos = 5;
  int64 orbit_src_producer_id = 6;
}```

```proto
message EndTransactionRequest {
  string transactional_id = 1;
  int64 producer_id = 2;
  int32 producer_epoch = 3;
  EndTransactionType outcome = 4;
  bool require_epoch_bump = 5;
  bool allow_during_rebalance = 6;
}```

### E.4 Nested types that carry the semantics

```proto
message PutFileMetadata {          // nested inside PutFileRequest.file_metadata
  uint64 file_id = 1;
  uint64 index_offset = 2;         // byte offset of the file index inside the object
  uint64 index_length = 3;
  uint64 uncompressed_size_hint = 12;
  repeated uint32 stream_ids = 4;  // parallel arrays, one entry per batch:
  repeated uint32 shard_ids = 5;
  repeated uint64 counts = 6;      //   record count per batch
  repeated uint64 uncompressed_size_hints = 11;
  repeated uint64 compressed_size_hints = 20;
  repeated int64  shard_fetch_offset = 18;
  repeated sint64 kafka_producer_ids = 8;      // idempotence, per batch:
  repeated sint32 kafka_producer_epochs = 9;
  repeated sint32 kafka_first_sequences = 10;
  repeated int32  positions = 13;
  RipcordFileLocation ripcord = 19;
}

message FileMetadata {             // CP-side stored form (GetFilesResponse)
  uint64 file_id = 1;
  uint32 bucket_id = 16;           // -> Bucket{bucket_id, bucket_url}
  ContentType content_type = 20;   // RECORDS | ABORTED_TRANSACTIONS | RIPCORD_RECORDS
  uint64 index_offset = 2;
  uint64 index_length = 3;
  repeated uint32 stream_ids = 4;  // parallel arrays, one entry per batch:
  repeated uint32 shard_ids = 5;
  repeated uint64 counts = 6;
  repeated uint64 offsets = 7;     // CP-assigned base offsets (absent in PutFileMetadata!)
  repeated BatchOutcome outcomes = 17;
  uint64 file_index_min = 8;
  uint64 file_index_max = 9;
  uint32 level = 10;               // compaction level
  int32  lane_id = 18;
  LaneType lane_type = 19;         // STANDARD | COMPACTED
  bool   is_eof = 11;
  uint64 min_timestamp_nanos = 14;
  uint64 max_timestamp_nanos = 15;
  uint64 created_at_timestamp_nanos = 23;
}

message GetRecordsPointer {        // one per file-range a Fetch must read
  uint32 shard_id = 7;
  uint64 file_id = 1;
  uint64 file_index_offset = 2;
  uint64 file_index_length = 3;
  uint64 offset = 4;
  uint32 batch_index = 5;
  uint64 count = 6;
  uint32 bucket_id = 9;
  uint32 file_compaction_level = 12;
  uint64 read_until_offset = 10;
}

message Bucket { uint32 bucket_id = 1; string bucket_url = 2; }
```

### E.5 What the signatures prove (that symbols alone did not)

1. **`PutFile` is the commit.** `PutFileRequest{PutFileMetadata file_metadata, timestamp_nanos, extended, bucket_url}` → `PutFileResponse{repeated uint64 offsets, repeated BatchOutcome outcomes, …}`. Agent supplies file identity + per-batch shape + producer sequences; CP returns assigned offsets and per-batch accept/dedup outcomes.
2. **`NewFile` pre-allocates file IDs:** `NewFileRequest{count}` → `NewFileResponse{min}` (a contiguous ID range) — agents reserve IDs *before* PUT, so the object key can embed the CP-issued file_id.
3. **Fetch is pointer-based:** `GetRecords*` returns `GetRecordsPointer`s + `Bucket` table (bucket_id → bucket_url), never bytes. The agent then range-reads its *own* bucket/cache using `file_index_offset/length`.
4. **Compaction is one atomic command:** `CompactionRequest{CompactionOutput output}` → empty `CompactionResponse` (success = no error in the envelope). The swap validates-or-rejects as a single RSM transition.
5. **Jobs are a typed union too:** `JobInput/JobOutput` carry one sub-message per job family (compaction=4, deadscanner=6, DL*=13–36, query=21, ripcord=22–26…), with `job_slots`, `timeout_ms`, queue/submission IDs for idempotent ack.
6. **Groups/producers are first-class RSM state:** `JoinGroupRequest` (15 fields incl. `poll_generation_id`, `support_skipping_assignment`), `PutGroupsOffsetsRequest` (incl. transactional_id + producer id/epoch — transactional offset commit), `InitProducerIDRequest`, `EndTransactionRequest{…, EndTransactionType outcome}`.
7. **Versioning is by field accretion:** request field numbers reach 135 (`DLUpdateDeletionConfig`), `Response.version = 12346`, `system = 12345` — a long-lived, append-only schema. New features arrive as new fields/RPCs, never renumbering (why a v842 binary still carries `ExpireFiles` *and* `ExpireFilesV2`, `Kafka` *and* `KafkaBatch`).



## Appendix F — Late-breaking evidence (second extraction pass) [BIN][RUN]

### F.1 Object-key scheme for data files — decoded, not guessed

`pkg/stream/segmentpath.PathWithPrefixAndSuffix` (constants recovered from the compiled code) builds every Kafka data-file key as:

```
[<custom-prefix>/]warpstream/%05d/%016d[<suffix>]
                     └───────┘ └─────┘
              shard = fileID mod 65536   zero-padded fileID
              shard = fileID mod 512     if the bucket is an S3 directory bucket
                                         (`segmentpath.IsDirectoryBucket`, S3 Express)
```

So the object key *is* the CP-issued file ID (`NewFile` allocation, Appendix E.5), deterministically sharded into 65,536 (or 512) prefix partitions — the standard trick to avoid S3 prefix hot-spotting at high PUT rates. Sibling path helpers in the same package: `ExtractFileID` (key → ID, used by the deadscanner and `file-reader`), `RipcordPath`, `RipcordFilePathForPointer`, `RipcordFilePathForFileMeta`; compaction's aborted-transaction files go through `compaction.abortedTransactionFilePath` → the same builder. `pkg/blob.warpStreamObjectPrefix` reconstructs the `warpstream/` prefix for bucket-lifecycle checks (`InspectBucketLifecycle`, `findS3BucketLifecycleExpiration`, `findGCSBucketLifecycleExpiration` — the agent *audits the customer's bucket lifecycle rules* against this prefix, cf. `NewBucketLifecyclePolicyChecker`).

Bucket addressing beyond a single bucket [BIN]: `ParseMultiBucketURL` / `multi://` wrapper URLs, `ParseStripedBucketURL` with `stripedBuckets.{readWithFallback, recordFailover}`, and `createBucketWithAssumeAWSRole` (per-bucket IAM role assumption, matching the `WARPSTREAM_BUCKET_ASSUME_ROLE_ARN_*` env pattern). Multi/striped buckets are **sharding/failover across buckets, not storage tiering** — no hot/cold tiering of Kafka files was found anywhere; "tiers" in the binary mean LSM size-tiers in compaction (`TieredPlannerCore`, `LevelSizeTiers`) or CP cluster tiers (`VirtualClusterTier`), never object-storage classes. Internal bucket implementations also exist that are *not* customer data-plane options [INF]: DynamoDB-, Spanner- and SQLite-backed `pkg/blob` buckets (used for metadata-ish blobs, tests, and embedded modes).

### F.2 On-disk (in-bucket) file format at field level

The Kafka-serving file is a **custom protobuf-headed segment**, not Parquet (Parquet/Puffin appear only on the Tableflow/Iceberg path: `FILE_FORMAT_PARQUET`, `FILE_FORMAT_PUFFIN`, `pkg/iceberg/manifest.*`). From `pkg/stream/pb/segmentpb` descriptors and `pkg/stream/segment` symbols (`Writer.{WriteHeader,WritePage,WriteFooter}`, `Reader.{Header,Footer}`, `ReadFooters`, `Scrub`):

```proto
message SegmentHeader {           // first bytes of every file
  // Version, ContentType (RECORDS | ABORTED_TRANSACTIONS | RIPCORD_RECORDS)
}
message SegmentFooter {           // located via FileMetadata.index_offset/index_length
  // Version
  // PageOffsets, PageLengths, PageRecordCounts, PageChecksumCrc32S   (per page)
  // BatchOffsets, BatchLengths                                      (per batch)
  // StreamIds, ShardIds                                             (per batch)
  // KafkaProducerIds, KafkaProducerEpochs, KafkaFirstSequences      (per batch, idempotence)
  // + SegmentFooterLite: a reduced variant for cheap footer-cache reads
}
```

Note the symmetry: the footer's per-batch parallel arrays are the *in-file twin* of `PutFileMetadata`'s per-batch arrays (Appendix E) — the CP index and the file index describe the same batches, which is what lets a fetch go CP-pointer → file footer → one page range without scanning. A dedicated footer cache (`filecache.FooterCache`, expirable LRU of `FooterCacheKey → FooterCacheValue`) exists precisely because footers are read far more often than pages. Buffer management is pooled end-to-end: `pkg/common.{BytesPool, NewTieredBytesPool, PooledBytes}`, `putBytesPooled` on every blob backend, `filecache.pooledDirectLoadBuffer`.

### F.3 Kafka ApiVersions — measured live against `local` mode [RUN]

`warpstream cli api-versions` against the smoke-test cluster returned **44 APIs** (Kafka API keys with supported version ranges). Abridged (full list reproducible via §23):

```
Produce [0:9]        Fetch [4:15]          ListOffsets [0:8]     Metadata [0:12]
OffsetCommit [0:9]   OffsetFetch [0:9]     FindCoordinator [0:4] JoinGroup [0:9]
Heartbeat [0:4]      LeaveGroup [0:5]      SyncGroup [0:5]       DescribeGroups [0:6]
ListGroups [0:5]     SASLHandshake [0:1]   SASLAuthenticate [0:2] ApiVersions [0:3]
CreateTopics [0:7]   DeleteTopics [0:6]    DeleteRecords [0:2]   CreatePartitions [0:3]
InitProducerID [0:4] AddPartitionsToTxn [0:4]  AddOffsetsToTxn [0:3]
EndTxn [0:5]         TxnOffsetCommit [0:3]
DescribeACLs/CreateACLs/DeleteACLs [0:3]   DescribeConfigs [0:4]  AlterConfigs [0:2]
DescribeLogDirs [0:4] DeleteGroups [0:2]
ConsumerGroupHeartbeat [0:1]  ConsumerGroupDescribe [0:1]        ← KIP-848 next-gen groups
GetTelemetrySubscriptions [0:0]  PushTelemetry [0:0]
```

Present-but-zero-range (`[0:0]`, advertised-disabled): `IncrementalAlterConfigs`, `AlterPartitionAssignments`, `OffsetDelete`, `DescribeCluster`, `DescribeProducers`, `DescribeTransactions [0:1]`, `ListTransactions [0:1]`, `ListConfigResources`. **Absent entirely** [RUN]: `ElectLeaders`, `OffsetForLeaderEpoch`, `AlterReplicaLogDirs`, delegation-token and fetch-session APIs — exactly the APIs a leaderless, diskless design cannot meaningfully implement. This live list supersedes the symbol-inferred Kafka surface in §7.1.

### F.4 Agent startup sequence (`agent` mode), observed from failure order [RUN]

Running `agent` with incomplete config fails in this exact order, revealing the bootstrap pipeline:

1. `defaultVirtualClusterID` (or `WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID`) **cannot be empty**
2. `bucketURL` (or `WARPSTREAM_BUCKET_URL`) **cannot be empty**
3. One of `agentKey` / `agentKeyPath` / `workloadIdentityTokenSource` (aws|gcp OIDC) **must be set**
4. Single-region defaulting: *"starting in single region mode, going to use the following metadataURL: `https://metadata.default.us-east-1.warpstream.com`"*
5. **Object-store permission probe:** before serving, the agent writes (and presumably reads/deletes) `warpstream/vci_test/permissions/permissions_file_<uuid>` in the target bucket ("verifying access to object storage" → S3 `PutObject`). A bucket the agent cannot write fails startup here, not at first produce.

Health surface verified live [RUN]: `GET :8080/v1/status` → `200 OK` (gated on AZ resolution when `-availabilityZoneRequired`), `GET :8080/metrics` → ~390 KB of Prometheus text including `warpstream_agent_kafka_request_count`, `warpstream_agent_heartbeat`, `warpstream_agent_segment_batcher_flush_*`, `warpstream_agent_deadscanner_outcomes`. `/healthz`, `/readyz`, `/debug/pprof/` returned 404 in `local` mode (pprof strings in the binary are library residue; profiling is handled via Datadog/profile-forwarding instead). The query engine embeds DuckDB (`pkg/query/*` + a `warpstream-duckdb` runtime artifact in the image's app dir [BIN]).

### F.5 Control-plane endpoint machinery (beyond §9.1) [BIN]

Endpoint resolution is a subsystem of its own, `pkg/agent/metadataconfig`: `NewControlPlaneProvider`, `getMetadataURL`, `GetAllHeartbeatURLs`, `ensureZonalURLProviders`, `resolveRegionPair`, **a leader tracker** (`buildLeaderTracker`/`newLeaderTracker` — the agent tracks *which* CP endpoint currently leads its virtual cluster and follows it), `setControlPlaneState/updateControlPlaneState`, `HandleClusterMigratedError`, `isLoopbackMetadataURL` (the `local`-mode hook). URL templates in strings: `metadata.%s.%s.warpstream.com`, `metadata.default.%s...`, and service-discovery DNS forms `<x>.discovery.…warpstream.com`, `brokers.<x>.<y>.discovery.…warpstream.com`, `…discoveryv2.…warpstream.com`. `pkg/deployment` adds a **dedicated control plane** tenancy variant (`Deployment.DedicatedControlPlane`, `GetDedicatedControlPlaneTenantID`) — single-tenant CPs are a deployment shape the client explicitly handles. Group-commit framing: the agent-side `pkg/kafka/agent.(*batchingStreamClient)` wraps the CP client with the `*Batch` RPCs (`PutFileBatch`, `JoinGroupBatch`, `SyncGroupBatch`, `HeartbeatGroupBatch`, `Put/GetGroupsOffsetsBatch`, `KafkaBatch`…), so "group commit" in this codebase = *request coalescing in the agent* in front of the CP's *envelope batching into the RSM* (`RegisterCommands` batcher keyed by `(actorID, virtualClusterID)` in `pkg/warpcluster`) — two batching layers, one per hop.

### F.6 Transport reconciliation (corrects a simplification in §9/E.1)

The descriptor set defines `service Stream` with 125 typed methods (Appendix E.2), *and* the client's core is `protoOverHTTP` around the `streampb.Request` envelope (Appendix E.4). Both are real; the consistent reading [INF, anchored in both artefacts] is: each typed method is invoked by POSTing a `Request` envelope (discriminator `request_type` + the method's sub-message) to the metadata endpoint, and the same envelope is then what the CP proposes into its RSM. The gRPC-style service definition is the *logical* API (and likely serves CP-internal/agentpool transports); the HTTP wire carrying it is envelope-based, unary, and retry-wrapped (`retryProtoOverHTTP*`). No server-streaming or client-streaming exists on the agent↔CP path.


---

*End of study. Appendix E added in a follow-up pass (same binary, descriptor extraction described in E.0).* Primary evidence: image `65a6e2969262` on this machine, binary symbols/help/strings, and the offline `local`-mode smoke test quoted in §3.3. Cross-check document (public sources only, written before the image was inspected): `docs/warpstream-architecture-comparison.md`.*
