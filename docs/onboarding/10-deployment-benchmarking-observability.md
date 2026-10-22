# Chapter 10: Deployment, benchmarking, and observability on AWS

Previous chapters ran `manibroker` and `manicp` as processes. This chapter answers one question. How do you put the system on AWS hosts that you can delete, run load at a stated rate, and record what the system did? The chapter keeps three concerns apart.

| Concern | Code | Main document |
|---|---|---|
| Local test backends | `deploy/docker-compose.yml`, `scripts/it-up.sh` | `docs/decisions.md` row 24 |
| AWS stack that you can delete | `deploy/aws/terraform/` | `deploy/aws/terraform/README.md` |
| Benchmarks | `deploy/aws/bench/`, `cmd/manibench`, `cmd/ddbprobe` | `docs/aws-benchmark-runbook.md` |
| Observability | `deploy/aws/observability/`, `/metrics` + JSON logs | `docs/observability.md` |

Most numbers in this chapter come from short runs with one sample. This chapter names the source for each number. Read each number as a guide unless the source document says more. `docs/w4-log-store-results.md` opens by calling its 1-4 minute, no-warm-up stages "indicative, not a steady-state characterisation".

## 1. Local deployment runs test backends only

`deploy/docker-compose.yml` does not run manifester. The file runs two backends that use real services. The backends bind to loopback. `scripts/it-up.sh` and `it-down.sh` wrap Compose.

- `extenddb` (`extenddb/extenddb-dev:latest`) provides a DynamoDB-compatible endpoint over in-memory SQLite on `127.0.0.1:18080`. Each run starts empty. The endpoint uses AWS's example credentials as seed data.
- `seaweedfs` (`chrislusf/seaweedfs:latest`) provides an S3 gateway on `127.0.0.1:8333` for the object-store adapter. The CP does not use the gateway.

Local benchmarks in `docs/benchmarks.md` start `manicp` (`:8080`), `manibroker` (`:9092`), and `cmd/manibench` by hand. The benchmarks use setup A (`mem/mem`), setup B (ExtendDB and memory), and setup C (ExtendDB and SeaweedFS). That document warns that ExtendDB and SeaweedFS latencies do not predict real DynamoDB or S3 latencies. Use the ratios and the latency structure from setups A, B, and C. Do not use the absolute values.

## 2. AWS topology in `deploy/aws/terraform/` puts each part on its own hosts

The Terraform stack is a benchmark stack that you can delete. WarpStream published a comparable benchmark. The stack is not production. The reference is `deploy/aws/terraform/README.md`. Read `main.tf`, `ec2.tf`, `nlb.tf`, `storage.tf`, `iam.tf`, `variables.tf`, and `outputs.tf` in that order. `main.tf` covers the VPC, the security groups, and the endpoints.

```
loadgen-0..L-1 ──9092──▶ [optional internal NLB :9092] ──▶ broker-1..broker-N
     (OMB)                                                    │ 8080
                                                              ▼
                                              cp rank 0 (+ standby ranks)
                                                   │            │
                                              S3 bucket    DynamoDB table
                               (gateway endpoints, one public subnet, one AZ)
```

The list below gives the defaults from `variables.tf`:

- Brokers use `aws_instance.broker[0..N-1]`. The default `broker_count=6` accepts 1-12. Brokers use `m6in.4xlarge`. Each broker runs `manibroker` (`:9092`, metrics `:9100`), a 10 s `/metrics` snapshotter, and Alloy. The broker id equals index+1. Brokers have no registration and no fencing. Each broker advertises its own private IP. Each broker waits for CP `/healthz`. User-data limits the wait to 10 min.
- The control plane uses `aws_instance.cp` rank 0 on `m6in.xlarge`. The control plane also uses `aws_instance.cp_standby` with `cp_standby_count=1`. Each CP host runs `manicp` (`:8080`, metrics `:9101`). No broker runs `manicp`. `bench-ctl.sh check-cp` verifies that placement. The default `cp_store=log` makes rank 0 the preferred appender with `-store dynamolog`. A standby tails the log. A standby serves reads. A standby answers commits with 503 until takeover with `-takeover-after 5s`. The `cp_store=dynamo` setting is the rollback. The `cp_store=dynamo` setting uses the old single-sequencer transaction store on one host. The `cp_store=dynamo` setting has no standby. Terraform forces brokers to `-max-segment-partitions 96` under that setting because the transaction limit is 100 items.
- Load generators use `loadgen_count=2` on `m6in.8xlarge`. Load generators run Java 17. Load generators have Kafka 3.8 CLI in `/opt/kafka`. Load generators have OMB in `/opt/omb`. Load generators have the bench code in `/opt/bench`. `/etc/profile.d/bench.sh` exports `BOOTSTRAP`, `BOOTSTRAP_NLB`, `BROKER_IPS`, `LOADGEN_INDEX`, `RUN_ID`, `BUCKET`, `TABLE`, and `REGION`.
- The NLB uses `use_nlb=true`. The NLB is an internal TCP load balancer on `:9092`. The NLB uses instance targets. The NLB has cross-zone load balancing off. The NLB spreads connections. The NLB never spreads requests. A broker Metadata response names that broker alone. Clients stay on the bootstrap broker. Skew with few connections is expected. The `balanced` mode in section 5 reduces that skew. The NLB does not reduce that skew.
- Storage uses one S3 run bucket. The bucket uses `force_destroy`, SSE-S3, and whole-bucket request metrics. Objects expire after `bucket_expire_days=3`. Storage also uses one DynamoDB table named `manifester_<run_id>_cp`. The table uses `pk` and `sk`. The table uses on-demand capacity. The table sets `warm_throughput` to `30k/30k units/s`. Terraform creates the table. `manicp` runs without `-create-tables`. `manicp` lacks that permission. Gateway VPC endpoints reach both stores. Traffic stays off the internet.
- The stack limits access and cost with set rules. The stack uses VPC `10.42.0.0/16`. The stack uses one public subnet in one AZ. Access uses SSM Session Manager. The stack has no SSH. Hosts require IMDSv2. A budget alarm fires at 80% actual spend and at 100% forecast spend. Hosts end themselves `max_runtime_minutes=360` after boot. Security groups admit CP `:8080` from the broker security group alone. Security groups admit broker `:9092` from the loadgen security group and from the subnet CIDR for NLB health checks. Loadgen ports admit traffic between loadgens. No metrics ports are open. Alloy scrapes loopback.

A CP host replacement changes the private IP of that host. Brokers carry that IP in user-data as `-cp-url`. Terraform replaces the brokers too. A replacement ends a run in progress. Self-termination ends EC2 instances alone. The NLB, the table, the bucket, and the budget keep billing until `terraform destroy`. Destroy uses `force_destroy` and deletes `results/`. Collect results first under section 6.

### Bootstrap contract

```bash
cd deploy/aws/terraform
cp example.tfvars my.tfvars   # region, az, run_id, owner, budget_*
terraform init -upgrade       # provider ~> 6.13: warm_throughput needs 6.x
terraform plan  -var-file=my.tfvars
terraform apply -var-file=my.tfvars
$(terraform output -raw artifact_upload_command)   # from repo root
```

`scripts/build-and-upload.sh` builds `linux/amd64` `manibroker` and `manicp`. The script uploads `deploy/aws/observability/` without dashboards. The script uploads `deploy/aws/bench/`. The script uploads `build/omb/openmessaging-benchmark.tar.gz` if that file exists. The script writes the `READY` marker last. The marker holds the git sha. User-data proceeds once the marker exists. Key outputs are `kafka_bootstrap`, `kafka_bootstrap_nlb`, `cp_urls`, `bucket`, `dynamodb_table`, and `ssm_start_session_*`.

## 3. Three load tools run in three places and measure three things

The table below names each tool, the place where the tool runs, and the thing that the tool measures:

| Tool | Runs where | Measures |
|---|---|---|
| `cmd/manibench` | Laptop, local only | franz-go producer load against one local broker |
| `cmd/ddbprobe` | EC2 host in the region of the table | Raw DynamoDB operations (X1, X3, and X4, §7) |
| OMB through `deploy/aws/bench/` | Loadgen hosts | Multi-broker benchmark that readers can compare with the WarpStream benchmark |

`cmd/manibench benchmark-producer` uses N independent `kgo.Client` values. The command uses acks=all. The command disables idempotence per decision 41. The command buffers 5000 records per client. The command reports interval latency and summary latency. The runbook says "Never use `cmd/manibench` for the AWS run". The tool runs on a single machine. That machine shares cores with the broker, the CP, and Docker.

`cmd/ddbprobe` tests DynamoDB operations in isolation. Modes are `put|txn|contend|query|batch`. The tool disables SDK retries. Throttling stays visible. The tool uses open-loop pacing at `-rate`. The tool keeps at most `-k` requests in flight. The tool prints one JSON summary line per run. The tool tests commit-path assumptions. The tool must run in the region of the table.

Two bench flows share the AWS stack. Part B uses `run-benchmark.sh`. That script drives Kafka's `kafka-producer-perf-test.sh` through cells `c1_1k c8_1k c32_1k c1_100b`. Each cell runs 60 s with 3 reps. The `--smoke` flag runs a 20 s test that must pass before a longer run. Part B covers the case with one broker. Part A uses the OMB flow in the next section.

## 4. OMB needs four patches to run this broker

The unmodified OpenMessaging Benchmark cannot send load to this broker. The broker has no CreateTopics. The broker has no consumer groups. `scripts/build-omb.sh` builds a pinned OMB commit. The tarball is about 29 MB. Four patches live in `omb/patches/`:

- `0001-manifester-fixed-topic-assign.patch` maps each logical topic to one pre-created topic named `maniTopic`. Consumers `assign()` a static partition share. The patch has no assignment shuffle.
- `0002-kafka-driver-only.patch` drops non-Kafka drivers from the build.
- `0003-produce-only-skip-consumer-wait.patch` stops produce-only workloads from waiting on consumers that do not exist.
- `0004-affine-partitioner.patch` adds an opt-in mode. Producer k writes partitions p where `p % N == k % N`. Results from this mode are not comparable with WarpStream results. Label results "partition-affine producers".

`omb/driver-kafka-manifester.yaml.tmpl` uses WarpStream's published client settings. Settings are `linger.ms=25`, `batch.size=100000`, `acks=all`, `enable.idempotence=false` per decision 41, and `compression.type=@@COMPRESSION@@`. `omb/render-driver.sh` substitutes bootstrap, compression, topic, and flags. `run-omb.sh` calls the render script with `bash`. The S3 upload drops the exec bit. Runbook w2 notes record a bug from this drop. The bug ended a run after pre-flight without a clear error.

Workloads live in `omb/workloads/*.yaml`. Each workload uses 1 KiB messages, 288 partitions, 64 producers, and `randomBytesRatio 0.5`.

The table below gives the rate and the shape for each workload file:

| File | Rate | Shape |
|---|---|---|
| `smoke-100k-produce-only.yaml` | 100k messages per second = 97.7 MiB per second | 3 min + 1 warm-up, must pass before a longer run |
| `ramp-{100,250,500,750,1000}k-produce-only.yaml` | 100k-1M messages per second | 5 min each + 1 warm-up |
| `vanity-1gibps-produce-only.yaml` | 1M messages per second = 976.6 MiB per second | 15 min + 2 warm-up, main stage |
| `vanity-1gibps.yaml` | 1M messages per second + 64 consumers | Adds the Fetch path. The team runs this workload later. This workload has the least test history |
| `w3-*.yaml`, `w4-*.yaml` | 100k-1M messages per second | Short (1-4 min) experiment stages. Indicative only |
| `later/*.yaml` | varies | Fan-out 3×, 256 producers, 1152 or 4608 partitions. `--include-later` only |

With lz4 and half-random payloads, bytes committed to S3 equal about half the OMB payload rate. Use that ratio when you compare client throughput with broker throughput.

## 5. A benchmark run has seven steps in order

Part A of `docs/aws-benchmark-runbook.md` covers the OMB run. Part B uses the same steps with `run-benchmark.sh` and `broker_count=1`.

The run has seven steps in order:

1. Prepare the workstation. Create Grafana tokens and SSM params under `/manifester-bench/grafana/*` per Part B section 1 of the runbook. Run `scripts/build-omb.sh`. The first build takes about 5 min. You can run `python3 -I -m unittest discover -s deploy/aws/bench -p 'test_*.py'`.
2. Apply Terraform with `broker_count=6` and `loadgen_count=2`. Upload artifacts after apply. Apply early. S3 request metrics take about 15 min to show data.
3. Verify the stack. Run `bench-ctl.sh check-cp`. The check confirms one `manicp` per CP host. The check confirms no `manicp` on brokers. The check confirms that brokers reach the CP. Run `apply-dashboards.sh`. Confirm `up{run_id="<run>"}` in Grafana shows all brokers and `manicp`.
4. Create the topic and start workers. Run `bench-ctl.sh create-topic 288`. The command uses SSM to reach the CP host loopback `manicp` API. OMB cannot create topics. The security groups close loadgen to CP traffic by design. Run `bench-ctl.sh start-workers` next. The command runs `start-omb-workers.sh` on each loadgen. Workers serve HTTP on `:8080` and stats on `:9090`. The first half of workers produce records. The second half of workers consume records.
5. Run smoke first. Run `bench-ctl.sh run --stages smoke`. `run-omb.sh` runs on loadgen 0 in detached mode through `bench-ctl.sh run`. The script runs pre-flight checks before each stage. The checks take a new `metrics/cp/` snapshot from S3. The snapshot proves that the CP lives. Loadgens cannot reach the CP. The checks run ApiVersions probes and topic visibility probes on each broker. The checks run an lz4 probe. The probe uses `none` if brokers reject lz4. That choice prints a loud "not apples-to-apples" warning. Do not continue unless the run prints `SMOKE PASS`. Do not continue unless each item on the runbook checklist passes. The checklist covers per-broker Grafana traffic, `segments_failed` at 0, no throttles, and a CP queue that does not grow.
6. Run stages. Run `bench-ctl.sh run --stages smoke,ramp,vanity-produce` for a run that includes the ramp stages. The default is `smoke,vanity-produce`. The coordinator renders a driver per stage. The coordinator enforces `--max-minutes`. The default is 120. The limit controls cost. The coordinator aborts a stage on any 10 s OMB line that meets a limit. One limit is an error rate above 0.1% of publish rate. One limit is publish p99 above 10 s through `--abort-p99-ms`. One limit is consumer backlog at or above 100k with growth. An abort stops OMB. An abort writes `ABORT` with the reason. An abort uploads results. An abort skips later stages. Each broker host and each CP host pushes `/metrics` to S3 every 10 s. No metrics ports are open. The push copies server data from the hosts to S3.
7. Collect results. Run `collect-results.sh <bucket> <run_id> <region>` next. The script syncs `s3://<bucket>/results/<run_id>/`. The script writes `summary.md`. Run `terraform destroy` after collection. Run the README post-destroy checklist after destroy. The checklist covers the NLB and the target groups. The NLB costs about $0.03 per hour as a leftover.

Two more commands change a run in progress. `bench-ctl.sh kill-appender 60` sends SIGKILL to rank-0 `manicp` in the middle of a stage. The experiment measures failover. Runbook A11 covers the experiment. `bench-ctl.sh broker-args -segment-window 1s` restarts brokers with new flags. `run-omb.sh --affine` enables patch 0004. `--bootstrap-mode random` shows unmodified-client skew.

## 6. The results directory starts with `summary.md`

```
results/<run>/
  loadgen/   cells.tsv + <cell>.r<rep>.c<client>.out     (run-benchmark.sh)
  omb/       stages.tsv, run.json, <stage>.json/.log, driver-<stage>.yaml, ABORT?
  metrics/   <broker_id>/<epoch>-broker.txt              every 10 s, per broker
             cp/<epoch>-manicp.txt                       rank 0 (summariser's CP input)
             cp-<rank>/<epoch>-manicp.txt                standby CP nodes (log store)
  markers/   kill-appender-<epoch>.txt                   (failover runs only)
  *-run_id.json manifests, build-git-sha.txt, terraform-outputs.json, summary.md
```

Start with `summary.md`. `deploy/aws/bench/summarize.py` uses the Python standard library alone. Tests live in `test_summarize.py`. Fixtures live in `deploy/aws/bench/testdata/`. The fixtures include a failover fixture. The script selects snapshots around each cell or stage. The script picks the snapshot at or before start. The script picks the snapshot at or after end. Averages are exact. The window can include up to one 10 s idle interval per side. For OMB the script reports target rate and achieved rate in messages per second and MiB per second. The script reports publish percentiles and end-to-end percentiles. The script reports backlog. The script reports per-broker deltas. The script reports skew. The script reports cost per GiB. Prices come from `SUMMARIZE_ARGS='--broker-hourly ...'`. The script prints an apples-to-apples checklist. Read the checklist before you quote any number. Failover runs use the `markers/` marker. Failover runs use `cp/` and `cp-<rank>/` snapshots. The script reconstructs the active CP node from those inputs.

Two known errors need a check by hand. After failover the active node numbers sit in `metrics/cp-1/`. `summarize.py` reads `metrics/cp/` alone. Reconcile the numbers by hand. In w4 a second `run-omb.sh` call overwrote `omb/stages.tsv` and `run.json`. The w4 `summary.md` covers the kill-test stage alone. The per-stage `<stage>.json` files hold the correct record in all runs.

## 7. Past runs report short stages, and most runs have one sample

The runs used short stages. Most runs have one sample. Do not quote a headline without the caveat column.

The table below gives the setup, the headline, and the caveat for each run:

| Run | Setup (per source) | Headline | Caveat |
|---|---|---|---|
| Local (`docs/benchmarks.md`) | 1 broker, M1 laptop, 30 s cells | The commit stage reaches its limit before the S3 PUT stage. PUT calls overlap | Pre-decisions 41-44. Docker backends |
| r1 (`docs/aws-benchmark-results-r1.md`) | 1 broker AWS, cells ×3 | Use the cell table as history only | The source says "not comparable" with current build |
| w2 (`docs/w2-bottleneck-verification.md`) | 6 brokers, transaction store, 288 partitions | The DynamoDB transaction loop of the sequencer stayed about 100% busy at 500k. Each transaction took about 0.7 ms per item. The 250k stage finished below the limit, and that stage did not find the plateau | 500k lasted about 45 s. A ramp, not steady state |
| w3 (`docs/w3-affinity-and-window-experiments.md`) | Same stack, 2 experiments | Affine producers held 500k and 750k, and disjoint partitions let groups overlap. A 1 s window did not help, and the 96-partition cap seals first | Affine producers are a benchmark-only setting, not comparable. Stages lasted minutes |
| w4 (`docs/w4-log-store-results.md`) | Log store, random clients | The 500k, 750k, and 1M stages passed produce-only with p99 below 1 s. Append p99 stayed about 8 ms. The kill-appender ack gap stayed about 8 s with zero errors | 1-4 min, no warm-up, one sample each |
| X1, X3, and X4 (`docs/x1-x3-x4-results.md`) | `ddbprobe`, 1 EC2 host, throwaway tables | PutItem p50 stayed about 3 ms, and the result does not support the 15 ms pessimistic assumption. A hot key throttles at about 200 entries per second for items above 4 KB. A cold on-demand table throttles at about 4k WRU per second | 45-120 s points, one sample, no p99.9 claimed |

The w2, w3, and w4 sequence changed one thing at a time. The limit sat in one big transaction per commit. The limit then sat in transactions with a cost per partition touched. The limit then sat in one small PutItem per log slot. W4 found no component near saturation at 1M after the last move. The load generator was the closest limit in w4. Each step rests on one run. The source documents state that limit.

## 8. Observability puts `run_id` on each metric series and each log line

`docs/observability.md` is the reference.

The list below gives the metric, log, and manifest rules:

- `manibroker` serves metrics on `:9100`. `manicp` serves metrics on `:9101`. Paths are `/metrics` and `/healthz`. Metric names use the `manibroker_` and `manicp_` prefixes. The broker key metric decomposes seal-to-ack. Parts are open age, PUT, queue wait, and commit. An example name is `manibroker_segment_seal_to_ack_seconds`. The CP exposes `manicp_seq_*` under the transaction store. The CP exposes `manicp_log_*` under the log store. The live family tells you the store. The other family is absent.
- Logs are JSON. Each line goes to stderr. Each line carries `time`, `level`, `msg`, `run_id`, `service`, `git_sha`, and `broker_id`. Filter on the stable `event` field. Values are `segment_sealed`, `segment_committed`, `cp_summary`, and `log_role`. The broker writes about 3 lines per segment. `cp_call` is DEBUG alone. A consume stage reaches tens of thousands of requests per second.
- Manifests come from `-manifest-out`. The flag writes one startup JSON per process. The JSON holds flags, resolved tuning, git sha, and EC2 identity. The target is a file or `s3://…/results/<run_id>/`. The 10 s snapshots and the manifest reconstruct a run after destroy.

### Alloy sends host data to Grafana Cloud from `deploy/aws/observability/`

The stack has no self-hosted Prometheus. Grafana Alloy 1.10.1 runs on each host. `install-alloy.sh` installs Alloy. Credentials come from SSM Parameter Store. The repo holds no credential. The env file is `/etc/alloy/alloy.env` with mode 0600. `alloy/config.alloy` is common to all hosts. The common config covers remote-write, the embedded unix exporter, and journald to Loki. The common config sets the external label `run_id`. One role file covers each host type.

The list below gives the role file for each host type:

- `alloy/broker.alloy` scrapes `manibroker :9100`. The file adds the `broker` label. The file adds the embedded CloudWatch and YACE exporter for S3 request metrics.
- `alloy/cp.alloy` scrapes `manicp :9101`. The file adds the `cp_rank` label. `alloy/cpcloudwatch.alloy` covers DynamoDB CloudWatch. Rank 0 alone runs the CloudWatch file. The split avoids duplicate series.
- `alloy/loadgen.alloy` scrapes the WarpStream bench tool on `:8081` when that tool runs. OMB runs have no such tool. `up{job="warpstream_bench"}=0` is expected in OMB runs.

App scrapes run every 5 s. CloudWatch scrapes run every 60 s. Grafana Cloud free retention is 14 days. Export data that you need before the window ends.

### Dashboards need a check against each hypothesis

Four generated dashboards live in `deploy/aws/observability/dashboards/`. Names are `overview`, `pipeline`, `aws`, and `hosts`. Edit `gen/gen_dashboards.py`. Run the generator. Run `apply-dashboards.sh`. Do not edit the JSON. Overview covers throughput and skew. Overview has one CP row per store. Pipeline covers PUT, queue, commit, and seal-to-ack. AWS covers broker-side SDK metrics and CloudWatch metrics. Hosts covers CPU, memory, NIC, and process data.

`docs/dashboard-vs-hypotheses.md` shows a method for dashboard use. The document decomposes the w4 narrative into ten hypotheses. The hypotheses are H1-H10. The document checks each hypothesis against the panel queries in the dashboards.

The list below groups the ten hypotheses by the part of the system that each group tests:

- H1-H3 concern the old transaction bottleneck. The audit confirms that the bottleneck is gone. Log append stays flat as entry size grows. Lost slots and hole fills are 0. Append p99 is about 8 ms.
- H4-H5 concern segment size and S3 PUT cost. Segments become size-bound above about 500k. S3 PUT latency grows with object size at about 10 ms per MiB. S3 PUT latency is not the throughput limit. The audit confirms H4-H5. Several printed p99 values are histogram bucket edges. Values are 254 ms and 318 ms. The values are not measured tails. Use the mean. Use the fraction above a bucket.
- H6-H7 concern index lag and the failover ack gap. Index lag is about 13 s at 1M. The failover ack gap is about 8 s. The audit confirms both effects. The panels in the dashboard read the wrong series for these effects. One panel reads standby lag. One panel uses 1 m windows. A 1 m window dilutes a 10 s stall. The effect held. The cited panel did not support the effect.
- H8-H9 concern load generator limits. The load generator is nearest saturation at 1M. Per-broker NIC use is about 81 MB per second as a mean. The audit confirms H8-H9.
- H10 concerns CloudWatch S3 series. All CloudWatch `aws_s3_*` series read 0 for the whole w4 run. The w2 run shows the same zeros. The document gives a likely cause. YACE uses a scrape window. S3 request metrics arrive with delay. `nil_to_zero` turns no data into 0. H10 stays open. Broker-side `manibroker_s3_*` series carry the same information.

The document lists 18 dashboard defects. Examples include dead transaction-store panels under the log store. Examples also include a role-change counter that `increase()` cannot see. Examples also include legends that use host IP in place of `cp_rank`. Use that list as the list of dashboard work that remains. A panel that draws data does not prove a claim. Check the query against the hypothesis.

## 9. Cost ends with destroy, and failure depends on the store

The Terraform README prices the default stack at an estimated $12 per hour in fixed costs in ap-south-2. Brokers cost about $7.03 per hour for 6 brokers. Load generators cost about $4.69 per hour for 2 loadgens. The CP costs about $0.23 per hour. The NLB costs about $0.03 per hour. Usage costs add S3 PUTs. DynamoDB write units form the largest usage cost at high load. The README worked example sets a 4-hour run at about $48 in fixed costs. The example sets the worst case at up to about $160 at a sustained 1 GiB per second. The w4 run lasted about 37 min. The w4 document estimates $10-13 in total for that run. The figures are list-price estimates from the documents. The figures are not bills. Set `budget_usd` and `budget_email` with true values. Respect `run-omb.sh --max-minutes`.

Failure behaviour differs by store. The runbook treats the stores in separate sections.

`cp_store=dynamo` is case A10. One CP is a single point of failure for availability. One CP is not a single point of failure for correctness. DynamoDB compare-and-set keeps the store correct. Brokers retry commits under the same key until `-commit-deadline`. The default deadline is 120 s. A CP restart of a few seconds fails no produce request. A restart of a few seconds delays acks. A longer gap fails in-flight produces with `REQUEST_TIMED_OUT`. Producers that retry can duplicate records. Duplicates arise when a lost commit reached the store.

`cp_store=log` is case A11. Loss of the standby is invisible to clients. Loss of rank 0 stalls acks. The standby promotes after `-takeover-after` at 5 s. Promotion needs a tail pass after the wait. W4 measured about 8 s from loss to ack flow. Brokers replay commits by segment key after promotion. Replay creates no duplicates. Replay loses no acked data. Loss of both CP nodes degrades the system to the A10 behaviour. Verify roles with `bench-ctl.sh cp-roles`. The command reads `X-CP-Role: active|standby`. Watch the Overview "Control plane (log store)" row during `kill-appender`.

Keep `terraform.tfstate` and `my.tfvars` until the post-destroy checklist prints empty results for each query. Queries cover tagged resources, instances, ENIs, volumes, the VPC, the bucket, the table, IAM, the budget, and the NLB family. Loss of state orphans billable resources. That failure stays invisible without the checklist.

## Key takeaways

- Compose provides test backends alone. `deploy/aws/terraform/` defines a benchmark topology that you can delete. The topology uses 6 brokers, a ranked log-store CP with a hot standby, 2 OMB loadgens, and an optional NLB. SSM access, a budget alarm, and self-terminating hosts guard the stack.
- Three load tools have three jobs. `manibench` covers local runs. `ddbprobe` covers DynamoDB assumptions. Patched OMB covers AWS runs. The wrong tool invalidates comparisons. Affine producers invalidate comparisons.
- A run follows steps in order. Build OMB. Apply Terraform. Upload artifacts and write `READY` last. Verify the stack. Create the topic through the CP. Run smoke. Run stages under abort rules. Run `collect-results.sh`. Run destroy. Collect results before destroy. The bucket uses force-destroy. Objects expire in 3 days.
- Read `results/<run>/summary.md` first. Read the raw `<stage>.json` file when the summary and the stage file disagree. Read the apples-to-apples checklist before you quote a number.
- Each headline number comes from a short run. Most runs have one sample. Each source labels its numbers indicative. The w2 to w4 arc shows a bottleneck move. The arc is not a capacity promise.
- Observability uses Alloy to Grafana Cloud. Each series and log line carries `run_id`. S3 snapshots support offline reconstruction after destroy. `docs/dashboard-vs-hypotheses.md` sets the standard of proof with H1-H10. Verify each query. Distrust bucket-edge p99 values. Treat CloudWatch S3 series as broken until a fix closes H10.

## Checkpoint

<details>
<summary>1. Why does topic creation go through SSM to the CP host instead of from a loadgen or OMB?</summary>

The broker has no CreateTopics and no consumer groups. OMB cannot create topics. The CP security group admits `:8080` from the broker security group alone. Loadgen to CP traffic is closed. The NLB forwards Kafka traffic alone. Loadgens have no `ssm:SendCommand` permission. `bench-ctl.sh create-topic` sends a POST to the `manicp` loopback API on the CP host through SSM. Patch 0001 maps each OMB logical topic onto that pre-created topic.
</details>

<details>
<summary>2. One broker carries twice another's load. Is the NLB broken?</summary>

No. The internal NLB spreads connections with a flow hash. The NLB never spreads requests. A broker Metadata response names that broker alone. Clients stay on the bootstrap broker. Skew is expected with few connections. The bench code defaults to `--bootstrap-mode balanced`. The mode pins producer k to broker k % N. `--bootstrap-mode random` shows unmodified-client skew.
</details>

<details>
<summary>3. You destroyed the stack, then remembered you never collected results. What survives?</summary>

The stack deletes most data by design. `force_destroy` deletes `results/`. The deleted data covers snapshots, OMB JSON files, and manifests. Objects face expiry after 3 days in any case. Old offsets fail with "committed segment missing" after expiry. Grafana Cloud data survives. Free retention is 14 days. A local `results/<run>/` copy from an earlier `collect-results.sh` run survives. Write `summary.md` to local disk first. Destroy the stack second.
</details>

<details>
<summary>4. In w3, why did affine producers fix 500k but a 1 s segment window did not?</summary>

W2 gave the diagnosis. Each segment touched many of 288 partitions. Each sequencer transaction carried about 86 items. Each transaction took about 86 ms. Groups could not overlap. The single loop saturated. Affine producers cut the count to about 48 partitions and about 49 items per transaction. Each transaction took about 48 ms. Disjoint partition sets let up to 4 groups overlap. Loads at 500k and 750k held. A longer window changed no binding limit. Segments hit the 96-partition cap before the timer. Partition seals numbered 1161. Timer seals numbered 71. Transactions grew in size under the longer window. The stages ran for minutes. Each stage had one sample.
</details>

<details>
<summary>5. A panel shows S3 p99 = 254 ms at every load and CloudWatch S3 latency = 0. S3 problem?</summary>

Treat the panel as a measurement problem first. H5 and H10 in `docs/dashboard-vs-hypotheses.md` cover the two values. The 254 ms value is a histogram bucket top. About 93% of attempts fall in the 128-256 ms bucket. The bucket bounds the true p99. The bucket does not measure the true p99. Use the mean. Use the fraction above a bucket. The zeros come from the H10 defect. The exporter window and delayed S3 request metrics cause the defect. `nil_to_zero` renders missing data as 0. W2 and w4 show the zeros. Broker-side `manibroker_s3_*` series are trustworthy. The series show about 10 ms per MiB of scaling with object size. The scaling is expected. The scaling is not the throughput limit.
</details>

## Try it

The list below gives five exercises that you can run without a full AWS benchmark:

- Run a local smoke test without AWS. Run `scripts/it-up.sh`. Start `manicp` and `manibroker` as in `docs/benchmarks.md`. Run `manibench benchmark-producer -topic bench -create-partitions 6 -num-clients 1 -duration 2s`. Run a 30 s run after the short run. Scrape `:9100` and `:9101` before the runs. Scrape the ports after the runs. Find `manibroker_segment_seal_to_ack_seconds`.
- Run the summariser test suite. Run `python3 -I -m unittest discover -s deploy/aws/bench -p 'test_*.py'`. Run `summarize.py` against fixtures in `deploy/aws/bench/testdata/`. Read the failover section in the output.
- Regenerate a dashboard. Change directory to `deploy/aws/observability/`. Run `python3 gen/gen_dashboards.py`. Check that `git diff` touches the JSON files that you intend to change. Run `apply-dashboards.sh --dry-run`.
- Audit one hypothesis. Pick H6 or H7 in `docs/dashboard-vs-hypotheses.md`. Write down the panel for each row. Write down the PromQL for each row. Write down one reason that a naive reading misleads for each row.
- Plan a stack. Do not apply the plan. Copy `example.tfvars` to `my.tfvars`. Run `terraform init -upgrade && terraform plan -var-file=my.tfvars`. Locate the NLB in the plan. Locate the standby CP in the plan. Locate DynamoDB warm throughput in the plan. Locate the bucket lifecycle in the plan. Note the resources that `destroy` deletes. Stop at the plan unless you own the budget.

## Further reading

The list below gives the document to read for each part of the system:

- `deploy/aws/terraform/README.md`. The README covers variables, outputs, the artifact contract, cost, and the destroy and post-destroy checklist.
- `docs/aws-benchmark-runbook.md`. The runbook covers Part A with OMB, Part B with the single-broker smoke test, CP failure modes A10 and A11, and w2 and w3 notes.
- `docs/aws-benchmark-warpstream-comparison.md`. The document states the claims that numbers can and cannot support against WarpStream's published benchmark.
- `docs/benchmarks.md`. The document covers local `manibench` measurements. The document covers the commit-path analysis from those measurements.
- `docs/w2-bottleneck-verification.md`, `docs/w3-affinity-and-window-experiments.md`, `docs/w4-log-store-results.md`. These documents hold the AWS results behind section 7. Read the caveats before the tables.
- `docs/x1-x3-x4-results.md`. The document covers `ddbprobe` experiments. The experiments cover PutItem cost, the hot-key ceiling, and cold-table throttling.
- `docs/observability.md`. The document is the metric, log, and manifest reference. `deploy/aws/observability/README.md` covers the Alloy layout, SSM secrets, and the YACE rationale.
- `docs/dashboard-vs-hypotheses.md`. The document covers the H1-H10 audit. The document lists the dashboard defect backlog.
