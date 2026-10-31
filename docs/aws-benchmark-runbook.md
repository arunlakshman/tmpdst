# AWS benchmark runbook

Two flows share the same Terraform stack and observability:

- **WarpStream-comparable run (6 brokers, OpenMessaging Benchmark): part A below.** Read
  `docs/aws-benchmark-warpstream-comparison.md` first: it says what the numbers can and cannot claim.
- **Quick smoke (1 broker, Kafka `kafka-producer-perf-test` matrix): part B, sections 1-10**, the original flow, still
  valid with `broker_count=1` and the fallback when OMB misbehaves (`run-benchmark.sh` accepts the comma-list
  `BOOTSTRAP` of N brokers too).

Stack reference: `ops/deploy/aws/terraform/README.md` (variables, outputs, NLB trade-off, artifact contract, cost, destroy,
post-destroy checklist). Observability: `ops/deploy/aws/observability/README.md`, `docs/observability.md`.
You run apply and destroy; nothing here is automatic. Never run `docker prune` or touch other AWS resources.

## Part A. 6-broker OMB flow

### A0. What is built where

| piece | where it runs | file |
|---|---|---|
| OMB artifact (Kafka driver only, patched) | built locally with Docker | `ops/scripts/build-omb.sh` -> `build/omb/openmessaging-benchmark.tar.gz` (about 29 MB, pinned OMB commit) |
| operator control (topic, workers, start run) | your workstation, through SSM send-command | `ops/deploy/aws/bench/bench-ctl.sh` |
| OMB workers | every loadgen host, `:8080` (HTTP) and `:9090` (stats) | `ops/deploy/aws/bench/start-omb-workers.sh` |
| OMB coordinator + stage plan + abort rules | loadgen host 0 (detached) | `ops/deploy/aws/bench/run-omb.sh` |
| mdscontrolplane: the log store (`-store dynamolog`) on an active host (rank 0) plus `cp_standby_count` (default 1) hot standbys; `metrics/cp/` (rank 0) and `metrics/cp-<rank>/` snapshots | dedicated control-plane hosts (`cp_instance_type`, default m6in.xlarge), `:8080` | `files/cp-user-data.sh.tpl` |
| mdsbroker `/metrics` snapshots every 10 s (`metrics/<broker id>/`) | every broker host (`mb-snapshot.service`) | terraform user-data |
| summary | your workstation | `ops/deploy/aws/bench/collect-results.sh` -> `results/<run>/summary.md` |

Why topic creation goes through SSM to the control-plane host: OMB cannot create topics (the broker has no CreateTopics),
the CP security group admits `:8080` from the broker SG only (loadgen to CP stays closed), the NLB forwards only Kafka
traffic, and loadgen hosts have no `ssm:SendCommand`. The 288-partition topic is one `CreateTopic` command in the log, so creation is cheap;
the Metadata response for 288 partitions is a few KB. The patched OMB driver maps every logical topic to the
topic you created (`omb-<run_id>`), so OMB's random topic names never matter.

### A1. Prerequisites (workstation)

AWS CLI v2 with credentials and the Session Manager plugin, Terraform >= 1.6, Go, Docker (running), Python 3,
`jq`, git. Grafana tokens and SSM parameters exactly as in part B section 1 below.
Check the instance types exist in your AZ (terraform README, "Variables"); if you substitute m6i the result is not comparable.

### A2. Build the OMB artifact (before apply, any time)

```
open -a Docker                       # macOS, if the daemon is not running
ops/scripts/build-omb.sh                 # about 5 min the first time (Maven cache lives in docker volume mds-omb-m2)
python3 -I -m unittest discover -s ops/deploy/aws/bench -p 'test_*.py'   # summarize tests, optional
```
The artifact upload command printed by terraform (`artifact_upload_command`) uploads `build/omb/openmessaging-benchmark.tar.gz`
to `artifacts/omb/` if it exists, so build first. If you built later:
`aws s3 cp build/omb/openmessaging-benchmark.tar.gz s3://$(terraform output -raw bucket)/artifacts/omb/` and, on each loadgen,
`aws s3 cp ... /tmp/omb.tgz && mkdir -p /opt/omb && tar -xzf /tmp/omb.tgz -C /opt/omb` (via SSM sessions).

### A3. Apply with six brokers

```
cd ops/deploy/aws/terraform
cp example.tfvars my.tfvars       # run_id, owner, budget_usd, budget_email; broker_count = 6, loadgen_count = 2
terraform init -upgrade && terraform plan -var-file=my.tfvars
terraform apply -var-file=my.tfvars                                # you run this
$(terraform output -raw artifact_upload_command)                   # from the repo root; READY is written last
```
Apply early: the S3 request metrics take about 15 minutes to show data.

### A4. Verify (about 5-10 minutes after READY)

```
export AWS_REGION=ap-south-2 RUN_ID=<run_id>
ops/deploy/aws/bench/bench-ctl.sh worker-status || true          # not started yet: expected "NOT running"
$(terraform output -json ssm_start_session_brokers | jq -r '.[0]')   # on broker 1:
  systemctl is-active mdsbroker mb-snapshot alloy ; curl -s 127.0.0.1:9100/healthz ; pgrep -c mdscontrolplane   # 0: no mdscontrolplane on brokers
$(terraform output -raw ssm_start_session_cp)                         # on the control-plane host:
  systemctl is-active mdscontrolplane mb-snapshot alloy ; curl -s 127.0.0.1:8080/healthz ; pgrep -xc mdscontrolplane   # exactly 1
$(terraform output -json ssm_start_session_loadgens | jq -r '.[0]')  # on loadgen 0:
  source /etc/profile.d/bench.sh; echo $BOOTSTRAP $LOADGEN_INDEX $LOADGEN_COUNT; ls /opt/omb/bin /opt/bench
```
Or in one command from the workstation: `ops/deploy/aws/bench/bench-ctl.sh check-cp` (exactly one mdscontrolplane process on the CP host,
healthz ok, no mdscontrolplane on any broker, every broker reaches `<cp_private_ip>:8080`).
Grafana: apply dashboards (`ops/deploy/aws/observability/apply-dashboards.sh`), check `up{run_id="<run>"}` for all six
`broker` values plus one `mdscontrolplane` series with `role="cp"`, and that the `$broker` variable of the Overview dashboard lists 1..6.
The Overview "Control plane" row (slot rate, append latency, lost slots, apply and standby lag, queue, DynamoDB WRU)
reads the active mdscontrolplane; lost slots should stay near 0 (see A10, A11).

### A5. Topic and workers (workstation)

```
ops/deploy/aws/bench/bench-ctl.sh --run-id $RUN_ID create-topic 288         # topic omb-<run_id>, 288 partitions, via SSM on the control-plane host
ops/deploy/aws/bench/bench-ctl.sh start-workers                              # benchmark-worker on every loadgen
ops/deploy/aws/bench/bench-ctl.sh worker-status
```
(`bench-ctl.sh` reads `cp_instance_id`, `cp_private_ip`, `broker_instance_ids` and `loadgen_instance_ids` from
`terraform output`, or set `CP_INSTANCE_ID` / `CP_PRIVATE_IP` / `BROKER_INSTANCE_IDS` / `LOADGEN_INSTANCE_IDS`.) The OMB worker layout: the first half of the workers run producers,
the second half consumers; with `loadgen_count=2` that is loadgen 0 producing and loadgen 1 consuming, so a
produce-only stage uses one loadgen (an m6in.8xlarge is enough, WarpStream used one).

### A6. Smoke, then the full stage

Default plan = pre-flight, `smoke` (100k msg/s = 97.7 MiB/s, 3 min + 1 min warm-up), then `vanity-produce`
(1,000,000 msg/s = 976.6 MiB/s, 288 partitions, 64 producers, produce-only, 15 min + 2 warm-up). Stop on the first failure.
Run the smoke alone first; it prints the pre-flight, the compression decision and `SMOKE PASS` / `SMOKE FAIL`:
```
ops/deploy/aws/bench/bench-ctl.sh run --stages smoke
ops/deploy/aws/bench/bench-ctl.sh tail 60          # repeat; also: Grafana Overview and Pipeline dashboards
```
Pre-flight (automatic): the control plane is alive (a `metrics/cp/<epoch>-mdscontrolplane.txt` snapshot in S3 newer than 90 s;
loadgens cannot reach the CP over the network by design), Kafka ApiVersions on every broker IP (prints node ids, warns on duplicates), the topic
is visible on every broker with 288 partitions, the lz4 probe (one 1 KiB record with `compression.type=lz4`; the brokers accept it since decision 45, and a broker that
rejects it makes the run fall back to `none` with a LOUD "not apples-to-apples" warning). Go only if:
- [ ] `SMOKE PASS`, achieved >= 80% of 100k msg/s, no errors
- [ ] every broker shows traffic in Grafana (Overview, "Per-broker write throughput"), skew max/min about 1 in balanced mode
- [ ] `segments_failed` 0, S3 and DynamoDB throttles 0, `mdscontrolplane` WRU/s (summary.md "Control plane" table; Overview "Control plane" row) and the mean group size: at 288 partitions with random partition spread a segment holds about 96 partitions (97 items; the cap is `-max-segment-partitions 96` on the old store, see the partition cap note below), so
      every group holds ONE segment (a group is capped at 100 items) and the WRU per commit stays about 240 at any load; expect group size 1 and
      240 x commits/s (comparison doc, section 5; `docs/surprises-audit.md` S1/S7). It would fall only with fewer partitions per segment
- [ ] control plane: queue depth not growing, canceled groups about 0 (one writer), commit latency steady
- [ ] no `level=error` Loki lines

Then the careful path or the default:
```
ops/deploy/aws/bench/bench-ctl.sh run --stages smoke,ramp,vanity-produce     # ramp: 100k, 250k, 500k, 750k, 1M msg/s, 5 min each
ops/deploy/aws/bench/bench-ctl.sh run --stages vanity                        # later: adds 64 consumers (Fetch path, least mature)
ops/deploy/aws/bench/bench-ctl.sh run --stages vanity-produce --bootstrap-mode random   # skew as an unmodified client sees it
ops/deploy/aws/bench/bench-ctl.sh run --stages vanity-fanout3 --include-later            # 3x fan-out etc.: workloads/later
```
Rates (1 KiB messages): 100k = 97.7 MiB/s, 250k = 244.1, 500k = 488.3, 750k = 732.4, 1M = 976.6.
Stage names are workload files in `ops/deploy/aws/bench/omb/workloads` (and `later/` with `--include-later`).
Compression: `--compression auto|lz4|none` (default auto = lz4 if the probe passes, as WarpStream; `none` only as a fallback).

Abort rules, checked on every 10 s OMB line: error rate above 0.1% of the publish rate, publish p99 of the interval above
10 s, or (when consumers exist) backlog of 100 K messages or more growing for 2 consecutive intervals. A trip kills OMB,
writes `ABORT` (reason) next to `omb/` on loadgen 0, uploads everything, and skips the remaining stages. Manual stop:
`bench-ctl.sh abort` (touches `/tmp/omb-stop`, kills the coordinator). `--max-minutes` (default 120) refuses an
over-long plan. Results land in `~/bench-out/<run>/omb/` and `s3://<bucket>/results/<run>/omb/` after every stage.

### A7. Collect, then destroy

```
SUMMARIZE_ARGS='--broker-hourly <usd per m6in.4xlarge hour in your region>' \
  ops/deploy/aws/bench/collect-results.sh $(cd ops/deploy/aws/terraform && terraform output -raw bucket) $RUN_ID ap-south-2
```
`results/<run>/summary.md` has the OMB table (target vs achieved msg/s and MiB/s, producer and end-to-end percentiles,
backlog), per-broker server side per stage, the skew table, cost per GiB with the formulas of the comparison doc, and the
apples-to-apples checklist of unmet items. Read the checklist before quoting a number. Also take Grafana screenshots
(free tier retention is 14 days).

**STOP: `terraform destroy` force-deletes the bucket.** Not before `results/<run>/summary.md` exists locally. Then destroy,
run the post-destroy checklist from the terraform README (it includes the NLB and its target groups), and delete the
SSM parameters and Grafana tokens if the account is shared.

### A8. Checklist (tick in order)

- [ ] OMB artifact built, tarball present; `broker_count = 6`, `loadgen_count = 2`; budget alarm set
- [ ] apply + artifact upload done; all hosts `bench-bootstrap.done`; 6 brokers up in Grafana; `bench-ctl.sh check-cp` ok (exactly one mdscontrolplane)
- [ ] topic `omb-<run>` with 288 partitions; workers running on both loadgens
- [ ] smoke passes; compression decision noted (`lz4` expected; `none` would NOT be apples-to-apples)
- [ ] ramp / vanity stages run; ABORT file absent (or understood)
- [ ] summary collected and read; per-broker snapshots present for all six brokers (`metrics/1/` .. `metrics/6/`) and the control plane's in `metrics/cp/`
- [ ] NLB (`use_nlb`) considered: it does not remove skew; pinned `balanced` mode used and stated
- [ ] results saved; `terraform destroy`; post-destroy checklist (including the NLB) all empty

### A9. Known gaps and unverified items

- The Kafka client in OMB (3.6.1) and the broker were exercised together only locally (one broker, mem store, 8
  partitions, produce + assign-consume at 5k msg/s, `compression.type=none`). Nothing here has run on AWS. lz4 was
  verified locally with the Apache Kafka 3.9 perf tool and franz-go only, not with an OMB run.
- The CP is a single point of failure for availability (not for correctness): see A10 (A11 adds a standby).
- The lz4 probe (`kafka-producer-perf-test`, `retries=0`, `max.block.ms=15000`) passes against a broker with decision 45;
  if it ever misclassifies, force `--compression none|lz4`.
- DynamoDB cost and the 98-calls-per-commit behaviour at 288 partitions are predictions (comparison doc, section 5).

### A10. Control-plane loss: what happens when it dies

The control plane is the log store (A11). The old transaction-based store, its single sequencer and the rollback to it were
removed (decision 54). The bullets below apply to a lone CP, or to losing both nodes of the pair.

If the CP dies (instance terminated by the max_runtime guard, crash, host failure):
- Symptom: brokers' produce acks stall or fail (the commit step cannot reach `:8080`), `up{job="mdscontrolplane"}` 0, no new
  `metrics/cp/` files, `bench-ctl.sh check-cp` fails. Segments already stored in S3 are not lost: they are simply not
  committed (visible) until a CP answers; clients retry per their delivery timeouts.
- Process died, host alive: the unit has `Restart=always`, `RestartSec=1`, so systemd brings it back in about a second
  (look at `journalctl -u mdscontrolplane`; `sudo systemctl restart mdscontrolplane` by hand if it gave up). The state is all in DynamoDB (log) and S3 (checkpoints),
  so nothing is lost, but a restart is not free. Real behaviour: (1) every commit that reaches the CP during the gap is
  refused at the connection; the brokers keep retrying it (capped exponential backoff, 20 ms up to 2 s, with jitter) for
  `-commit-deadline` (default 120 s) after the commit's first attempt, so a gap of seconds fails no produce but delays the
  acks and fills the produce buffer (clients are paused by backpressure meanwhile); (2) a commit whose answer was lost
  is resent under the same key and gets the same answer, so it is not duplicated; (3) the new process starts with an
  empty Pebble state and restores the newest checkpoint and replays the log after it before it listens (reads of DynamoDB and S3);
  (4) a gap longer than the deadline fails the segments in flight with REQUEST_TIMED_OUT, and producers that retry may then
  duplicate records whose lost commit had landed (decision 41). A host replacement takes minutes, which is beyond the
  deadline: expect failed produces and re-run the stage.
- Host gone: `terraform apply -replace=aws_instance.cp -var-file=my.tfvars` (replaces only the CP; after upload READY is
  already set, so it boots straight away). The private IP changes, which changes the brokers' user-data (`-cp-url`):
  Terraform replaces the broker hosts too (`user_data_replace_on_change`), which in a benchmark is acceptable but loses the
  run; for a mid-run recovery instead restart the instance in place or re-point a broker by hand
  (`/etc/systemd/system/mdsbroker.service`, `-cp-url`, `systemctl daemon-reload && systemctl restart mdsbroker`).
- After recovery run `check-cp`, then resume or re-run the stage; note the outage in the run notes.

### A11. The log store (stage 4 default): two CP nodes, run id, killing the appender

What runs: the control plane starts `aws_instance.cp` (rank 0, the preferred appender) and `cp_standby_count`
(default 1) `aws_instance.cp_standby` (rank 1, a hot standby that tails and applies the log, serves reads, and answers
commits with 503). Each runs `mdscontrolplane -store dynamolog -rank R -node-name cp-R -takeover-after 5s -checkpoint-bucket <run bucket>
-checkpoint-prefix cp/ -pebble-dir /var/lib/mdscontrolplane/pebble` (`cp_takeover_after`, `cp_extra_args` change them). Brokers get
`-cp-url http://<rank0>:8080,http://<rank1>:8080` (output `cp_urls`) and the default `-max-segment-partitions 4096`
(`broker_extra_args` is empty). The DynamoDB table has TTL on `ttl` and warm throughput `ddb_warm_*` (decision 50; X4: a
cold on-demand table throttles near 4k items/s). Checkpoints are objects under `cp/ckpt/` in the run bucket.

Run id: one `run_id` names the bucket, table, tags and every log line (`-run-id`). The table is new per run, so the log starts
at slot 0. Do not reuse a run id's table with a different `-window-slots` (a cluster constant).

Verify (after apply and upload): `bench-ctl.sh check-cp` (one mdscontrolplane on each CP host, healthz, brokers reach rank 0) and
`bench-ctl.sh cp-roles` (rank 0 `X-CP-Role: active`, rank 1 `standby`). `s3://<bucket>/results/<run>/metrics/cp/` (rank 0)
and `metrics/cp-1/` (standby) get a file every 10 s. `summarize.py` reads `metrics/cp/` only: after a failover the active
node's numbers are in `cp-1/`.

Kill the appender mid-run (the failover measurement): during a stage at steady load, note the time and run
`bench-ctl.sh kill-appender 60` (SIGKILL mdscontrolplane on rank 0, held down 60 s, then started again; it comes back as the
preferred appender and the standby steps down, so you also measure the failback). Expect, in this order:
1. brokers' commits fail to connect to rank 0 or reach it reset, and move to rank 1, which answers 503 until it takes
   over; produce acks stall (`mdsbroker_segment_commit_seconds` p99 rises, buffered bytes grow, backpressure pauses reads);
2. after `takeover-after` (5 s) plus the standby's next tail pass rank 1 becomes active (`mdscontrolplane_log_active` 1,
   `mdscontrolplane_log_role_changes_total{to="active"}`, a few `mdscontrolplane_log_hole_fills_total` if the dead node held a slot, up to
   250 ms each) and the stall ends; in-flight commits are replayed by segment key (the window answers them with the original
   offsets), so there are no duplicates and no lost acked data;
3. when rank 0 restarts it restores the newest checkpoint (or replays) and starts active; rank 1 sees a lower-rank writer
   and demotes (`role_changes_total{to="standby"}`); brokers fail back through a 503 from rank 1.
Look at: Overview, row "Control plane (log store ...)" (slot rate, append p50/p99, lost slots, apply and standby lag, hole
fills and role changes, queue/refused, checkpoint age); Pipeline "Commit" p99 and "Segments in flight";
OMB's own latency chart for the gap; `journalctl -u mdscontrolplane` on both hosts (`log_role`, `log_ready`, `cp call` WARNs).
Budget: an ack gap of `takeover-after` + about 2 s of broker backoff + the 10 s CP call timeout in the worst case (a host
that drops packets instead of refusing: dial 5 s, then 503 from the standby), far below the 120 s `-commit-deadline`;
the it/ test measures about 2 s with a 1.5 s takeover on loopback. If a gap approaches the deadline, produces fail with
REQUEST_TIMED_OUT and the stage is invalid.

There is no rollback to the transaction store (decision 54); going back needs an older git revision.

Standby or whole-CP loss: lose the standby only: nothing visible (alert on `up{job="mdscontrolplane"}`); the active keeps serving. Lose
both: as in A10 (acks stall, everything retried for the deadline). Replace one host with `terraform apply
-replace=aws_instance.cp_standby[0]` (its IP is in the brokers' user-data, so Terraform replaces the brokers too).

# Part B. Quick smoke (single broker)

Everything below is the original single-broker flow (`broker_count=1`, `run-benchmark.sh`). Disposable stack: one broker
host, one load generator host. Known limits:

- The broker accepts only non-idempotent producers (decision 41): the harness passes `enable.idempotence=false` to `kafka-producer-perf-test` (Kafka 3.8 defaults to true and would be rejected with INVALID_REQUEST). Keep it if you override `PRODUCER_PROPS`. Results in `docs/aws-benchmark-results-r1.md` predate this change.
- The load tool is Apache Kafka's `kafka-producer-perf-test`. The WarpStream `benchmark-producer` always compresses
  (no flag to disable); the broker accepts that since decision 45 (earlier brokers answered
  `UNSUPPORTED_COMPRESSION_TYPE`). Never use `cmd/mdsbench` for the AWS run.
- The broker implements neither CreateTopics nor DescribeConfigs, so `kafka-topics.sh --create/--describe`
  fail. The control-plane host creates the 6-partition topic `bench-<run_id>` at boot through its loopback mdscontrolplane API;
  the harness only waits for it (`kafka-topics.sh --list` works).
- Saturating runs (default) make client-side latency mostly client queueing and print no percentiles. Use
  broker-side ack latency (`seal_to_ack`) for saturation, and `--rate N` runs for client p50/p99 below saturation.
- The `aws_*` CloudWatch series names (YACE) were not verified against a live scrape; see "Dashboards".

## 1. Prerequisites

- AWS credentials for the target account (region `ap-south-2` in the examples), AWS CLI v2, the Session Manager plugin,
  Terraform >= 1.6, Go, Python 3, `jq`.
- A Grafana Cloud stack (examples: `https://mdsio.grafana.net`). Known non-secret values:

  | what | value |
  |---|---|
  | metrics push URL | `https://prometheus-prod-36-prod-us-west-0.grafana.net/api/prom/push` |
  | metrics user | `2644751` |
  | Loki push URL | `https://logs-prod-021.grafana.net/loki/api/v1/push` |
  | Loki user | `1318052` |
  | stack URL | `https://mdsio.grafana.net` |

### Grafana tokens (minimal scopes)

Grafana Cloud portal, your org, Security, Access Policies:
1. Create access policy `mds-bench-metrics` with realm = your stack and scope `metrics:write` only. Add a token (expiry: a few days). Save it as `prom_api_key`.
2. Create access policy `mds-bench-logs` with scope `logs:write` only. Add a token. Save it as `loki_api_key`.
   (One policy with both scopes also works: store the same token in `grafana/api_key` and skip the two above.)
3. Annotations and dashboards: in the stack, Administration, Users and access, Service accounts, create one with role Editor and a token. Used by the harness (`grafana/annotation_token`) and by `apply-dashboards.sh` (`GRAFANA_API_TOKEN`). Annotations tolerate being absent.
Never commit tokens; they live only in SSM.

### SSM parameters (create once per account/region, before apply)

```
R=ap-south-2; P=/mds-bench/grafana
aws ssm put-parameter --region $R --name $P/remote_write_url --type String       --value 'https://prometheus-prod-36-prod-us-west-0.grafana.net/api/prom/push'
aws ssm put-parameter --region $R --name $P/user             --type String       --value 2644751
aws ssm put-parameter --region $R --name $P/prom_api_key     --type SecureString --value "$PROM_TOKEN"
aws ssm put-parameter --region $R --name $P/loki_url         --type String       --value 'https://logs-prod-021.grafana.net/loki/api/v1/push'
aws ssm put-parameter --region $R --name $P/loki_user        --type String       --value 1318052
aws ssm put-parameter --region $R --name $P/loki_api_key     --type SecureString --value "$LOKI_TOKEN"
# optional (run annotations)
aws ssm put-parameter --region $R --name $P/url              --type String       --value 'https://mdsio.grafana.net'
aws ssm put-parameter --region $R --name $P/annotation_token --type SecureString --value "$ANNOTATION_TOKEN"
```
Use `read -rs PROM_TOKEN` style prompts so tokens stay out of shell history.

## 2. Apply

```
cd ops/deploy/aws/terraform
cp example.tfvars my.tfvars    # region=ap-south-2, az=ap-south-2a, run_id, owner, budget_usd, budget_email
terraform init && terraform plan -var-file=my.tfvars
terraform apply -var-file=my.tfvars          # you run this; the S3 request-metrics config exists from here on
```
Note the S3 request metrics need about 15 minutes after creation to show data: apply early.

## 3. Upload the artifact

From the repo root, right after apply (instances wait up to 45 minutes for `READY`):
```
$(cd ops/deploy/aws/terraform && terraform output -raw artifact_upload_command)
```
It builds linux/amd64 `mdsbroker`/`mdscontrolplane`, uploads them, the observability directory (`artifacts/observability/`)
and the harness (`artifacts/bench/`), and writes `READY` last.

## 4. Wait for READY and verify health

Every host: `ls /var/lib/bench-bootstrap.done` (takes about 3-6 minutes after READY).
```
$(terraform output -raw ssm_start_session_cp)
  systemctl is-active mdscontrolplane mb-snapshot alloy ; curl -s 127.0.0.1:8080/healthz ; curl -s 127.0.0.1:9101/healthz
$(terraform output -raw ssm_start_session_broker)
  systemctl is-active mdsbroker mb-snapshot alloy
  curl -s 127.0.0.1:9100/healthz
  tail -n 30 /var/log/bench-bootstrap.log ; journalctl -u mdsbroker -n 20 --no-pager
$(terraform output -raw ssm_start_session_loadgen)
  systemctl is-active alloy ; ls /opt/bench ; source /etc/profile.d/bench.sh
  kafka-topics.sh --bootstrap-server $BOOTSTRAP --list     # expect bench-<run_id>
```
Grafana (stack URL): apply dashboards once from your workstation, with the Editor service-account token:
```
cd ops/deploy/aws/observability
GRAFANA_URL=https://mdsio.grafana.net GRAFANA_API_TOKEN=... ./apply-dashboards.sh
```
Then check: Explore (Prometheus) `up{run_id="<run>"}` shows broker, mdscontrolplane (role `cp`), node jobs for all hosts with value 1;
Explore (Loki) `{run_id="<run>"}` shows `mdsbroker` and `mdscontrolplane` JSON lines (labels `service`, `level`, `host`);
dashboards Overview, Pipeline, AWS, Hosts show data for the run (AWS CloudWatch panels lag 2-15 minutes).
The S3 manifests appear in `s3://<bucket>/results/<run>/mdsbroker-<run>.json` and `mdscontrolplane-<run>.json`.

## 5. Smoke test (go/no-go, about 5 minutes, 1 client, 20 s)

On the loadgen host (`sudo -i` first, so `$HOME` is `/root` and logs can go to `/var/log`):
```
/opt/bench/run-benchmark.sh --smoke
```
It waits for the broker, checks the topic, runs a 5 s warm-up and one 20 s cell with 1 client at 1 KB, and prints
`SMOKE PASS` or `SMOKE FAIL`. Go only if all hold:
- [ ] `SMOKE PASS`; `~/bench-out/<run>/loadgen/c1_1k.r1.c1.out` shows records/sec > 0 and no errors.
- [ ] Grafana Pipeline dashboard shows the segment rate and `committed bytes` rising during the window; `segments_failed` stays 0.
- [ ] Loki has no `level=error` lines for the window (Overview log panel).
- [ ] AWS dashboard: `mdsbroker_s3_*` and `mdscontrolplane_dynamo_*` panels show attempts; throttles 0.
- [ ] `s3://<bucket>/results/<run>/metrics/` is receiving `<broker id>/<epoch>-broker.txt` and `cp/<epoch>-mdscontrolplane.txt` files (every 10 s).
- [ ] An annotation "run <run> start" is visible (if you set the annotation parameters).
If it fails, check `journalctl -u mdsbroker` on the broker host (a `UNSUPPORTED_*` or S3/DynamoDB permission error shows up there) before spending more.

## 6. Run the matrix

Detach it so a dropped SSM session does not kill the run:
```
nohup /opt/bench/run-benchmark.sh > /var/log/bench-run.log 2>&1 &   
tail -f /var/log/bench-run.log
```
Matrix: warm-up 20 s; cells `c1_1k` (1 client, 1 KB), `c8_1k`, `c32_1k`, `c1_100b` (1 client, 100 B), 60 s each,
repeated 3 times, 15 s idle between cells; about 17 minutes. Each cell's stdout is saved
(`loadgen/<cell>.r<N>.c<K>.out`, one file per client), uploaded after every cell to
`s3://<bucket>/results/<run>/loadgen/`, with Grafana annotations at run start, each cell start and end, and run end.
mdsbroker `/metrics` snapshots come from `mb-snapshot.service` on the broker host and mdscontrolplane's from the same unit on the control-plane host (every 10 s; the SGs open
no metrics ports and loadgen has no `ssm:SendCommand`, so a push-to-S3 timer was the simplest option). `summarize.py`
picks the snapshots around each cell by timestamp.
Latency run below saturation (optional, afterwards): `run-benchmark.sh --rate 500 --cells "c1_1k c8_1k" --reps 1`
(500 records/s per client; the tool then exits by itself and prints p50/p95/p99).
Options: `--reps --duration --gap --warmup --cells --rate --topic --tool`.

## 7. Collect results (before destroy)

On the workstation:
```
ops/deploy/aws/bench/collect-results.sh $(cd ops/deploy/aws/terraform && terraform output -raw bucket) <run_id> ap-south-2
```
Writes `./results/<run_id>/` (loadgen/, metrics/, manifests, `build-git-sha.txt`, `terraform-outputs.json`) and `summary.md`: per cell and repetition client
throughput and latency, broker segment put/queue/commit/ack averages, S3 and DynamoDB throttles/errors/retries,
DynamoDB write units, then a mean over repetitions. Also keep the Grafana dashboards' time range screenshots if
needed (free tier retention is 14 days).

Collect before `terraform destroy` (`force_destroy` deletes `results/`), and well inside `bucket_expire_days` (default 3
days): the lifecycle rule expires every object, including the `seg/` segments that DynamoDB still points to, so after
expiry a Fetch of old offsets fails with `committed segment missing` (KAFKA_STORAGE_ERROR). That is acceptable for a
disposable stack but means data and results are not durable.

## 8. Destroy and verify nothing is left

**STOP: `terraform destroy` force-deletes the bucket and everything in it (results, metric snapshots, manifests). Do not run it until `./results/<run_id>/summary.md` exists on your workstation (step 7). Also keep `my.tfvars`.** Instance logs survive only in Grafana Cloud Loki (free tier: 14 days).

```
cd ops/deploy/aws/terraform && terraform destroy -var-file=my.tfvars     # you run this
```
Then run every command of "Post-destroy checklist" in `ops/deploy/aws/terraform/README.md` (all must print nothing):
tagged resources, EC2 instances and volumes, VPC and endpoints, unattached Elastic IPs, the bucket, the DynamoDB
table, IAM roles and instance profiles, the budget. Then delete the SSM parameters if the account is shared
(command in that README) and revoke the three Grafana tokens. Keep `terraform.tfstate` until the checklist passes.

## 9. Estimated spend

Fixed cost about $0.29/hour (c7i.xlarge $0.179, c7i.large $0.089, volumes, public IPv4, S3 request metrics).
A session of about 2 hours (apply, 15 min of waiting, smoke, matrix, collection, destroy) is about $0.6 fixed.
Usage: S3 PUTs at 5-60 segments/s for 17 minutes is on the order of 5-60 thousand PUTs per minute at the extreme
($0.005 per 1000), so $0.2-1.5; DynamoDB on-demand writes (about one per slot) similar order, under $1;
CloudWatch GetMetricData by Alloy a few cents. Expect **$2-5 total** (the first minutes of S3 request metrics are a
monthly custom-metric price prorated). Budget alarm thresholds are in `my.tfvars`. Grafana Cloud free tier is not
included. Left running for a day by mistake: about $7.

## 10. Dashboards: names still unconfirmed

All `mdsbroker_*`, `mdscontrolplane_*` and `node_*` series in the dashboards were checked against the Go code and
`docs/observability.md` (the `mdsbroker_s3_*`/`mdscontrolplane_dynamo_*` families are composed from a Subsystem in
`internal/system/awsmetrics`, confirmed in code). Not confirmed against a live scrape: the CloudWatch series from Alloy's embedded YACE
(`aws_s3_first_byte_latency_average`, `aws_s3_total_request_latency_{average,maximum}`, `aws_s3_{all,put,get}_requests_sum`,
`aws_s3_{4xx,5xx}_errors_sum`, `aws_dynamodb_successful_request_latency_{average,maximum}`,
`aws_dynamodb_consumed_{read,write}_capacity_units_sum`, `aws_dynamodb_provisioned_*_capacity_units_average`,
`aws_dynamodb_{read,write}_throttle_events_sum`, `aws_dynamodb_throttled_requests_sum`,
`aws_dynamodb_transaction_conflict_sum`, `aws_dynamodb_conditional_check_failed_requests_sum`, label `dimension_Operation`).
First thing after READY: Explore `{__name__=~"aws_.*"}` and fix `ops/deploy/aws/observability/gen/gen_dashboards.py` if spelled differently.

## Run w2: sequencer, smoke and ramp (2026-10-19, ap-south-2, 6 brokers m6in.4xlarge, 1 CP m6in.xlarge, 288 partitions, lz4, balanced)

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This section is a record.*

Short run (stack up about 24 min, test about 14 min): smoke 100k (2 min + 1 warm-up), ramp 250k and 500k (3 min + 1 warm-up each),
produce-only. Raw data in `results/w2` (git-ignored). Destroyed; post-destroy checks empty.

| stage | target msg/s | achieved msg/s (MiB/s) | pub p50 / p99 ms | skew max/min | groups/s | mean group | queue ms | tx ms | WRU/s | cache hit |
|---|---|---|---|---|---|---|---|---|---|---|
| smoke | 100k | 100.2k (97.9) | 320 / 464 | 1.10 | 19.6 | 1.00 | 3 | 31 | 1,880 | 96.7% |
| ramp 250k | 250k | 250.4k (244.5) | 446 / 628 | 1.10 | 15.5 | 1.01 | 10 | 45 | 2,659 | 98.2% |
| ramp 500k | 500k | ~250k then falling (ABORT, p99 11.7 s) | 6.8 s / 11.7 s at abort | 1.10 | 13.0 | 1.20 | 229 | 60 | 2,462 | 98.4% |

Findings
- Plateau is about 250k msg/s (about 245 MiB/s). The limit is the single sequencer commit rate: about 15-20 commits (groups) per second, each one
  TransactWriteItems of 30-60 ms (rising with load). Each of the six brokers seals about 2.6 segments/s regardless of rate, so segments grow with load
  (about 8 MB at 100k, about 8 MB committed per segment at 250k) but the commit count stays flat; at 500k the commit queue wait on brokers
  reached about 10 s and the p99 rule tripped. S3 PUT p50 time 80-100 ms, no throttles or errors, S3 PUT/s only about 16 in total: S3 is not the limit.
  DynamoDB throttles, errors, cancellations and retries were all 0. Broker CPU was not captured (the top snapshot was taken after the stage ended).
- Audit predictions: single-segment groups held (mean group size 1.0 up to 250k; 1.2 at 500k when the queue built). WRU per commit was
  about 100-190 (not 240): 96 at smoke, 171 at 250k, 190 at 500k. WRU/s stayed about 2-2.7k, so DynamoDB capacity is not the bound.
  Commit-rate bound confirmed: about 1 / (tx + about 20 ms) per second.
- Plan implications: reaching 500k-1M msg/s needs more segments per commit (batch several broker segments into one group: bigger group,
  larger transactions, still under the 100 items cap), or fewer and larger segments (longer seal interval or size trigger), or a pipelined sequencer
  (overlap commits instead of one transaction at a time, partitions are independent). Fewer partitions per segment would also allow group size above 1.
- Bugs found: run-omb.sh called render-driver.sh directly, but S3 upload drops the exec bit, so the stage loop died silently after pre-flight
  (fixed: `bash render-driver.sh`). The runbook workloads use 5 min stages; this run shortened them on the loadgen with sed (not committed).

## W3 experiments: partition-affine producers and a longer segment window

Both are opt-in; the default WarpStream-style run is unchanged.
- Partition-affine producers (patch `0004-affine-partitioner.patch`, rebuild the OMB tarball with `ops/scripts/build-omb.sh`): `run-omb.sh --affine`
  (driver key `mdsAffine`) makes producer k, pinned to broker k % 6, write only partitions p with p % 6 == k % 6 (48 each; sticky per
  ~batch.size bytes like the stock partitioner). Needs balanced mode. NOT comparable to WarpStream: label results "partition-affine producers".
- Segment window: `bench-ctl.sh broker-args -segment-window 1s` adds the flag to every mdsbroker (systemd drop-in) and restarts them;
  `bench-ctl.sh broker-args` with no flags removes the drop-in. (Or set `broker_extra_args` in my.tfvars at apply time.)
- Partition cap (decision 52): the broker default `-max-segment-partitions` is now 4096, valid only for the log store. The old
  transaction store refused a segment of more than 99 partitions with `ErrTooLarge` and needed `-max-segment-partitions 96`; it is
  removed (decision 54), so leave `broker_extra_args` empty.
- Abort threshold: `run-omb.sh --abort-p99-ms 60000` (or env `OMB_ABORT_P99_MS`); default 10000.
- Short stages: `w3-smoke-100k-1min` (1 min), `w3-500k-4min` (4 min + 1 min warm-up). Example:
  `bench-ctl.sh run --stages w3-500k-4min --abort-p99-ms 60000 [--affine]`. Results of such short stages are indicative only.
