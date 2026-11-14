# Benchmark

## Short version (for the release note and the launch post)

> mdsbroker sustained **1,000,000 messages/s (about 1 GB/s, 978 MiB/s of 1 KiB messages) of writes while the
> same cluster served the reads**, on 6 brokers (m6in.4xlarge) in one AWS availability zone, with the
> unmodified public OpenMessaging Benchmark. p99 publish latency was 0.74 s and p99 end-to-end latency
> was 1.19 s. No errors. This is the same workload WarpStream published for its "1 GiB/s" result.

That is a single 5-minute run per rate, so read it as "it works at this rate", not as a final number.
Details, limits and how to repeat it are below.

## What we ran

| | |
|---|---|
| Benchmark | OpenMessaging Benchmark (OMB), **unmodified**, commit `5b1fa70951a323da26bd587174b58bb2c65b0b5c`, stock Kafka driver |
| Workload | 1 topic, 288 partitions, 64 producers, 64 consumers (1 subscription), 1 KiB messages, fixed target rate |
| Payload | `randomBytesRatio: 0.5`, `compression.type=lz4` (as in WarpStream's post) |
| Producer settings | `acks=all`, `linger.ms=25`, `batch.size=100000`, `enable.idempotence=false` |
| Brokers | 6 x `mdsbroker`, m6in.4xlarge (16 vCPU, 64 GiB), stateless, data in S3 |
| Control plane | 1 active + 1 standby `mdscontrolplane`, m6in.xlarge, log kept in DynamoDB (on demand, pre-warmed) |
| Load generators | 2 x m6in.8xlarge (32 vCPU, 128 GiB), 1 producer worker, 1 consumer worker |
| Where | AWS ap-south-2, **one availability zone** |
| Each stage | 1 minute warm-up, then 5 minutes measured |
| Builds | 100k to 500k: commit `8b7e864`. 750k and 1M: commit `4e47a35` (see "What changed on the way") |

## Results

All rows: 0 publish errors, consumers kept up with producers (consume rate equals publish rate).

| Target | Published | Write MiB/s | Publish p50 / p99 | End-to-end p50 / p99 | Max backlog |
|---|---|---|---|---|---|
| 100k msg/s | 100,098 msg/s | 98 | 289 ms / 443 ms | 481 ms / 981 ms | 28K msgs |
| 250k msg/s | 250,381 msg/s | 245 | not recorded / 766 ms | not recorded / 1.28 s | 86K msgs |
| 500k msg/s | 500,781 msg/s | 489 | 395 ms / 1.18 s | 749 ms / 2.09 s | 362K msgs |
| 750k msg/s | 751,369 msg/s | 734 | 401 ms / 702 ms | 591 ms / 1.07 s | 198K msgs |
| **1M msg/s** | **1,001,316 msg/s** | **978** | 400 ms / 743 ms | 620 ms / 1.19 s | 799K msgs |

Notes on reading the table:

- 1,000,000 messages/s of 1 KiB is 976.6 MiB/s, or 1.024 GB/s in decimal units. WarpStream's post calls
  this load "1 GiB/s". We are at the same load, so we say "about 1 GB/s" and give MiB/s next to it.
- Reads are included: every message is also consumed by the 64 consumers. The read traffic is about the
  same size as the write traffic, so the cluster moves about twice these numbers in total.
- WarpStream states targets of publish p99 under 1 s and end-to-end p99 under 2 s. We meet both at 750k
  and 1M. At 500k the end-to-end p99 (2.09 s) is just over 2 s, and that row is from the older build.

## What changed on the way (be honest about this)

The first run of 750k and 1M, on the older build, did **not** pass:

- 750k: writes were fine, but end-to-end p99 was 6.8 s (consumers lagged by seconds).
- 1M: writes held 1M msg/s, but consumers stalled at about 750k msg/s and the backlog grew to 13 million
  messages, so the stage was stopped.

Brokers, control plane, DynamoDB and S3 were all idle at that point. Reading was slow because a Fetch did one
S3 GET per batch, one after another, and each batch of one partition sat far from the next one inside the
segment file. Fix (commit `4e47a35`): when a segment is sealed, batches are sorted by partition, and a Fetch
reads batches that sit next to each other with a single GET. After that, 750k and 1M both passed (rows above).

We did not run the old and the new build side by side, so we say the fix is "consistent with" the result, not
"proven to be the cause". 100k to 500k were not re-run on the new build.

## Limits of this result

- **One run per rate, 5 minutes each.** WarpStream's example workload is much longer. We have not done a
  15-minute run or repeated runs, so there is no spread or confidence interval yet.
- **One availability zone.** No cross-zone traffic and no zone-failure test. WarpStream's cost story is multi-zone.
- **At-least-once delivery.** Idempotent producers are not supported (`enable.idempotence=false` is required),
  so a retried produce can create a duplicate.
- **Not the same control plane.** WarpStream uses its hosted control plane. We use a DynamoDB-backed log,
  with different latency, cost and limits.
- **Latency is object-storage latency.** Writes are batched into S3 segments, so p99 is hundreds of
  milliseconds to about a second, not single-digit milliseconds.
- **No cost number yet.** We have not published S3 and DynamoDB request counts or a cost per GB.
- **Only the OMB Kafka workload above.** No fan-out, no larger partition counts, no failover test in this run.
- The benchmark clients and the machines are our own AWS account; results will move with instance types.

## Repeat it

Needs an AWS account, Terraform, Podman and the AWS CLI. Full steps are in `deploy/aws/RUNBOOK.md`.

```
bash deploy/aws/scripts/build-omb.sh                          # stock OMB, built with Podman
cd deploy/aws/terraform && terraform apply -var-file=my.tfvars  # 6 brokers, 2 load generators, control plane
bash deploy/aws/scripts/build-and-upload.sh <bucket> <region>
deploy/aws/bench/bench-ctl.sh --run-id <id> start-workers
deploy/aws/bench/bench-ctl.sh --run-id <id> run --stages ramp-750k,ramp-1000k --abort-backlog-sec 10
terraform destroy -var-file=my.tfvars                         # always, when you are done
```

The raw results of the runs above (OMB JSON and logs, server metric snapshots every 10 s) are saved by the
author and are not in git. Publish them next to this page if you want others to check the numbers.
