# MDS landing page copy

Checked against the repo on 2026-10-21 and the live page at mds.io.
Claims use `docs/w4-log-store-results.md`, `docs/decisions.md`, `docs/TODO.md`, and `internal/wire/kafkawire/supported.go`.

## Read this first. Claims audit of the current page

| Current claim | Verdict | Why |
|---|---|---|
| Apache 2.0, open source, run it in your own account | Remove today | The repo has no root LICENSE and no root README. `docs/roadmap-from-warpstream-teardown.md` lists both as blockers before any open source announcement. Put this back when the licence file lands. |
| Your existing Kafka clients, connectors and tooling just work | Soften | `internal/wire/kafkawire/supported.go` serves 5 APIs. Produce, Fetch, ListOffsets, Metadata, and ApiVersions. There are no consumer groups, no transactions, and no Kafka admin APIs. Producers work. Most connectors need consumer groups. |
| Clients put their AZ in their client ID, brokers route by zone | Remove today | No zone routing exists in `internal/broker/` or `internal/system/production/`. Metadata lists only the broker that answered. That work sits in `docs/TODO.md` under broker membership. |
| Keep days or weeks of retention | Soften | Nothing deletes data yet, and ListOffsets earliest is always 0. Retention and garbage collection are roadmap items. Say storage is cheap. Do not promise a retention feature. |
| No broker limits | Remove | Every system has limits. The segment caps and the control plane commit rate are documented limits. |
| Cross-AZ $17,300 a month to $0, retention $14,500 to $1,400 | Keep, with a label | These are model numbers at 100 MB/s, not measured bills. Label the section as a worked example and state the inputs under the table. |
| A produce is acked only once durable in object storage and offsets are committed | Keep | This is the strongest true claim on the page. Decision 42 and the broker write path back it. |
| Writes land in hundreds of milliseconds, not built for low latency | Keep | Honest, and it matches the 200 ms batch window plus an S3 PUT. More projects should say this on the home page. |

## Recommended page copy

### Title tag

Kafka on S3. Stateless brokers, no broker disks | MDS

### Meta description

MDS is a Kafka-compatible streaming engine that stores every byte in S3. Stateless brokers, offsets assigned by a control plane, and no replication traffic between brokers. Built for throughput workloads that can take hundreds of milliseconds of latency.

### Hero

Headline:

Kafka, with S3 as the disk.

Subheadline:

MDS is a Kafka-compatible streaming engine with no broker disks. Brokers batch records into segments, write them to S3, and ack only after a control plane commits the offsets. No replication between brokers. No disks to rebalance, fill, or replace.

Button 1: Join the design partner list
Button 2: Read the benchmark results

Small print under the buttons:

Produce path works today with standard Kafka producers. Consumer groups and exactly-once transactions are not built yet. We list what is missing on this page, in the open.

### Proof strip

Use these four numbers, with the source line under them.

- About 998k messages per second, produce only, 288 partitions, 4 minutes on AWS
- p99 produce latency 756 ms in the same run
- 8 second ack gap when we killed the control plane appender at 500k messages per second, with zero errors
- 2,000 simulator seeds and 50.7 million steps replay the full cluster, crashes included

Source line: One AWS run, short stages, one sample per stage. Numbers are indicative, not a capacity promise. Full method and caveats at the benchmark link.

### The problem. The 3-AZ tax

Textbook Kafka at 100 MB/s of ingest writes each byte three times, once per zone. Your bill pays for that three times. Once in cross-zone replication traffic. Once in disk. Once in the brokers you run only to hold history.

MDS stores each byte once, in S3. S3 handles durability across zones. Brokers hold nothing durable, so compute scales with traffic and storage scales with retention. The two stop moving together.

Keep the existing worked example table here. Add this line under it:

Worked example, not a quote. Inputs: 100 MB/s ingest, replication factor 3, one consumer group, 7 days retention, gp3 against S3 Standard. Put your own rates in and the shape holds. The replication line goes to zero. The storage line drops by about the replication factor.

### How it works

Three steps, in order.

1. A broker batches records from many partitions into one segment. It seals the segment at 16 MiB or after 200 ms.
2. The broker writes the segment to S3. Nothing is acked yet.
3. A control plane appends the commit to a DynamoDB log and assigns offsets when it applies the commit. Only then does the broker ack.

That order is the product. Acks mean the bytes are in S3 and the offsets exist. A broker can die at any point in those three steps and no acked record is lost.

### Why teams trust a new broker. The simulator

Every broker is a single-threaded state machine. It takes an event, returns actions, and does no input or output itself. The same code runs in production and in a simulator with one seeded random stream and no wall clock.

So we test the way the system can actually break. Lost S3 responses. Slow tails. A control plane node killed mid-commit. A standby taking over. Each failure is a seed, and each seed replays step for step on a laptop.

We also plant bugs on purpose. If a test does not catch the planted bug, the test does not count.

Button: How we test MDS

### A good fit

- Log and event pipelines where hundreds of milliseconds of latency is fine
- Change data capture into a lake or warehouse
- Analytics and observability ingestion
- ML feature pipelines and training data collection
- Any workload where the storage bill and the rebalancing work hurt more than latency helps

### Not a fit today

We would rather lose the deal here than in a proof of concept.

- Workloads that need single-digit millisecond latency
- Consumer groups. Fetch works, group coordination does not exist yet
- Exactly-once producer transactions. Delivery is at-least-once, and clients must turn idempotent producers off
- Compacted topics and managed retention. Nothing is deleted yet
- Kafka admin APIs, ACLs, and TLS. On the roadmap, not in the build

### From the founder

I am Arun. I am building MDS in public.

Our first control plane used DynamoDB transactions and stopped at about 240k messages per second. We measured it, wrote it down, and replaced it with a log. The replacement held about 998k messages per second in the run above.

This page will keep listing what is not built. A streaming system lives on trust, and trust comes from publishing the bad numbers next to the good ones.

Button: Follow the build

### Questions

**Will my Kafka producer work?**
Very likely. The broker serves Produce, Fetch, ListOffsets, Metadata, and ApiVersions, with record batch v2. Turn idempotent producers off. That setting is `enable.idempotence=false` in Apache Kafka clients.

**Will my consumer work?**
A consumer reading with Fetch and tracking its own offsets works. A consumer group does not. Group coordination is the fourth item on our roadmap, after the read path.

**What latency should I expect?**
Hundreds of milliseconds. Records wait in a batch window of 200 ms, then wait for an S3 write and a control plane commit. Our w4 run showed p99 at 756 ms. If you need 5 ms, use something else.

**Where does my data live?**
In your S3 bucket, as segment objects. Segment keys and layout are documented in the repo. The control plane keeps offsets and the segment index in DynamoDB.

**What happens when a broker dies?**
Clients reconnect to another broker. Brokers keep no durable state, so there is nothing to rebalance and no partition to re-elect. What they were batching and had not acked, producers retry.

**What happens when the control plane leader dies?**
A standby has been replaying the same commit log. It takes over. In our test the ack gap was about 8 seconds and no produce failed.

### Final call to action

Headline: Bring us a workload that Kafka storage makes expensive.

Text: We are taking a small number of design partners. You get direct access to the engineers building the broker. We get a real workload and permission to publish what it costs on both systems.

Button: Apply for design partner access
Second link: Or read the benchmark results first

## Three hero variants for an A/B test

Test one variable at a time. Keep the same buttons on all three.

**Variant A, cost.** Use this as the control. It matches the current page and the search intent.

Headline: Stop paying Kafka to copy your data three times.
Subheadline: MDS stores Kafka records once, in S3. Stateless brokers, no replication traffic between zones, and storage priced like object storage instead of replicated disks.

**Variant B, mechanism.** Use this if traffic comes from engineering blogs and Hacker News. Technical readers click on a clear mechanism.

Headline: Kafka, with S3 as the disk.
Subheadline: MDS is a Kafka-compatible streaming engine with no broker disks. Brokers batch records into segments, write them to S3, and ack only after a control plane commits the offsets.

**Variant C, trust.** Use this if the audience already knows the object storage Kafka category and distrusts vendor benchmarks.

Headline: A Kafka broker that publishes its bad numbers.
Subheadline: MDS stores every byte in S3 and tests the whole cluster in a seeded simulator, crashes included. Benchmarks ship with method and caveats. So does the list of what is not built yet.

My pick: run B against A. B says what the product is in six words, and the category already taught buyers the cost argument. C works better as the founder section headline than as the hero.

## Button copy

- Primary button, best first: Join the design partner list
- If there is a self-serve build later: Run it locally
- Proof button: Read the benchmark results
- Founder button: Follow the build
- Avoid: Get started, Book a demo, Contact sales. There is nothing to start yet, and a demo button on a pre-release product costs trust.

## What to change on the current page, in order

1. Remove the Apache 2.0 open source block until the LICENSE and README land in the repo. Replace it with the founder block above.
2. Remove the zone-aligned routing section. Replace it with the three step write path.
3. Soften the Kafka compatibility claim everywhere. Say producers and tooling that only needs Produce, Fetch, ListOffsets, and Metadata. Name the missing APIs in the open.
4. Label the 3-AZ numbers as a worked example with inputs. Keep the numbers, they are the best sales asset on the page after the write path.
5. Add the proof strip with the w4 numbers and the source line. No other object storage Kafka vendor publishes caveats on the home page. That gap is the cheapest trust available in this category.
6. Move "Built for throughput, not microseconds" up, right under How it works. It qualifies the buyer before the sales sections spend their time.
7. Add the simulator section. It is the only claim on the page that a competitor cannot copy in a sprint, because it describes how the code is built, not a feature flag.

## Notes on tone

The repo writes in plain sentences and publishes caveats. The page should sound like the repo. Short claims, each with a number or a file behind it. No "seamless", no "blazing", no "supercharge". Buyers of infrastructure read those words as a signal that the numbers will not survive a proof of concept.
