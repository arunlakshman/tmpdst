# AWS benchmark runbook

Two flows share the same Terraform stack and observability:

- **WarpStream-comparable run (6 brokers, OpenMessaging Benchmark): part A below.** Read
  `docs/aws-benchmark-warpstream-comparison.md` first: it says what the numbers can and cannot claim.
- **Quick smoke (1 broker, Kafka `kafka-producer-perf-test` matrix): part B, sections 1-10**, the original flow, still
  valid with `broker_count=1` and the fallback when OMB misbehaves (`run-benchmark.sh` accepts the comma-list
  `BOOTSTRAP` of N brokers too).

Stack reference: `deploy/aws/terraform/README.md` (variables, outputs, NLB trade-off, artifact contract, cost, destroy,
post-destroy checklist). Observability: `deploy/aws/observability/README.md`, `docs/observability.md`.
You run apply and destroy; nothing here is automatic. Never run `docker prune` or touch other AWS resources.

## Part A. 6-broker OMB flow

### A0. What is built where

| piece | where it runs | file |
|---|---|---|
| OMB artifact (Kafka driver only, patched) | built locally with Docker | `scripts/build-omb.sh` -> `build/omb/openmessaging-benchmark.tar.gz` (about 29 MB, pinned OMB commit) |
| operator control (topic, workers, start run) | your workstation, through SSM send-command | `deploy/aws/bench/bench-ctl.sh` |
| OMB workers | every loadgen host, `:8080` (HTTP) and `:9090` (stats) | `deploy/aws/bench/start-omb-workers.sh` |
| OMB coordinator + stage plan + abort rules | loadgen host 0 (detached) | `deploy/aws/bench/run-omb.sh` |
| the ONE manicp (single sequencer, decision 46) + `metrics/cp/` snapshots | dedicated control-plane host (`cp_instance_type`, default m6in.xlarge), `:8080` | `files/cp-user-data.sh.tpl` |
| manibroker `/metrics` snapshots every 10 s (`metrics/<broker id>/`) | every broker host (`mb-snapshot.service`) | terraform user-data |
| summary | your workstation | `deploy/aws/bench/collect-results.sh` -> `results/<run>/summary.md` |

Why topic creation goes through SSM to the control-plane host: OMB cannot create topics (the broker has no CreateTopics),
the CP security group admits `:8080` from the broker SG only (loadgen to CP stays closed), the NLB forwards only Kafka
traffic, and loadgen hosts have no `ssm:SendCommand`. The 288-partition topic is one TOPICS item in DynamoDB (`cp/doc.go`), so creation is cheap;
the Metadata response for 288 partitions is a few KB. The patched OMB driver maps every logical topic to the
topic you created (`omb-<run_id>`), so OMB's random topic names never matter.

### A1. Prerequisites (workstation)

AWS CLI v2 with credentials and the Session Manager plugin, Terraform >= 1.6, Go, Docker (running), Python 3,
`jq`, git. Grafana tokens and SSM parameters exactly as in part B section 1 below.
Check the instance types exist in your AZ (terraform README, "Variables"); if you substitute m6i the result is not comparable.

### A2. Build the OMB artifact (before apply, any time)

```
open -a Docker                       # macOS, if the daemon is not running
scripts/build-omb.sh                 # about 5 min the first time (Maven cache lives in docker volume manifester-omb-m2)
python3 -I -m unittest discover -s deploy/aws/bench -p 'test_*.py'   # summarize tests, optional
```
The artifact upload command printed by terraform (`artifact_upload_command`) uploads `build/omb/openmessaging-benchmark.tar.gz`
to `artifacts/omb/` if it exists, so build first. If you built later:
`aws s3 cp build/omb/openmessaging-benchmark.tar.gz s3://$(terraform output -raw bucket)/artifacts/omb/` and, on each loadgen,
`aws s3 cp ... /tmp/omb.tgz && mkdir -p /opt/omb && tar -xzf /tmp/omb.tgz -C /opt/omb` (via SSM sessions).

### A3. Apply with six brokers

```
cd deploy/aws/terraform
cp example.tfvars my.tfvars       # run_id, owner, budget_usd, budget_email; broker_count = 6, loadgen_count = 2
terraform init -upgrade && terraform plan -var-file=my.tfvars
terraform apply -var-file=my.tfvars                                # you run this
$(terraform output -raw artifact_upload_command)                   # from the repo root; READY is written last
```
Apply early: the S3 request metrics take about 15 minutes to show data.

### A4. Verify (about 5-10 minutes after READY)

```
export AWS_REGION=ap-south-2 RUN_ID=<run_id>
deploy/aws/bench/bench-ctl.sh worker-status || true          # not started yet: expected "NOT running"
$(terraform output -json ssm_start_session_brokers | jq -r '.[0]')   # on broker 1:
  systemctl is-active manibroker mb-snapshot alloy ; curl -s 127.0.0.1:9100/healthz ; pgrep -c manicp   # 0: no manicp on brokers
$(terraform output -raw ssm_start_session_cp)                         # on the control-plane host:
  systemctl is-active manicp mb-snapshot alloy ; curl -s 127.0.0.1:8080/healthz ; pgrep -xc manicp   # exactly 1
$(terraform output -json ssm_start_session_loadgens | jq -r '.[0]')  # on loadgen 0:
  source /etc/profile.d/bench.sh; echo $BOOTSTRAP $LOADGEN_INDEX $LOADGEN_COUNT; ls /opt/omb/bin /opt/bench
```
Or in one command from the workstation: `deploy/aws/bench/bench-ctl.sh check-cp` (exactly one manicp process on the CP host,
healthz ok, no manicp on any broker, every broker reaches `<cp_private_ip>:8080`).
Grafana: apply dashboards (`deploy/aws/observability/apply-dashboards.sh`), check `up{run_id="<run>"}` for all six
`broker` values plus one `manicp` series with `role="cp"`, and that the `$broker` variable of the Overview dashboard lists 1..6.
The Overview "Control plane" row (commit rate, queue depth, group size, transaction latency, cancellations, DynamoDB WRU)
reads that one manicp; its cancellations should stay near 0 (see A10).

### A5. Topic and workers (workstation)

```
deploy/aws/bench/bench-ctl.sh --run-id $RUN_ID create-topic 288         # topic omb-<run_id>, 288 partitions, via SSM on the control-plane host
deploy/aws/bench/bench-ctl.sh start-workers                              # benchmark-worker on every loadgen
deploy/aws/bench/bench-ctl.sh worker-status
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
deploy/aws/bench/bench-ctl.sh run --stages smoke
deploy/aws/bench/bench-ctl.sh tail 60          # repeat; also: Grafana Overview and Pipeline dashboards
```
Pre-flight (automatic): the control plane is alive (a `metrics/cp/<epoch>-manicp.txt` snapshot in S3 newer than 90 s;
loadgens cannot reach the CP over the network by design), Kafka ApiVersions on every broker IP (prints node ids, warns on duplicates), the topic
is visible on every broker with 288 partitions, the lz4 probe (one 1 KiB record with `compression.type=lz4`; the brokers accept it since decision 45, and a broker that
rejects it makes the run fall back to `none` with a LOUD "not apples-to-apples" warning). Go only if:
- [ ] `SMOKE PASS`, achieved >= 80% of 100k msg/s, no errors
- [ ] every broker shows traffic in Grafana (Overview, "Per-broker write throughput"), skew max/min about 1 in balanced mode
- [ ] `segments_failed` 0, S3 and DynamoDB throttles 0, `manicp` WRU/s (summary.md "Control plane" table; Overview "Control plane" row) and the mean group size: at 288 partitions with random partition spread a segment holds about 96 partitions (97 items), so
      every group holds ONE segment (a group is capped at 100 items) and the WRU per commit stays about 240 at any load; expect group size 1 and
      240 x commits/s (comparison doc, section 5; `docs/surprises-audit.md` S1/S7). It would fall only with fewer partitions per segment
- [ ] control plane: queue depth not growing, canceled groups about 0 (one writer), commit latency steady
- [ ] no `level=error` Loki lines

Then the careful path or the default:
```
deploy/aws/bench/bench-ctl.sh run --stages smoke,ramp,vanity-produce     # ramp: 100k, 250k, 500k, 750k, 1M msg/s, 5 min each
deploy/aws/bench/bench-ctl.sh run --stages vanity                        # later: adds 64 consumers (Fetch path, least mature)
deploy/aws/bench/bench-ctl.sh run --stages vanity-produce --bootstrap-mode random   # skew as an unmodified client sees it
deploy/aws/bench/bench-ctl.sh run --stages vanity-fanout3 --include-later            # 3x fan-out etc.: workloads/later
```
Rates (1 KiB messages): 100k = 97.7 MiB/s, 250k = 244.1, 500k = 488.3, 750k = 732.4, 1M = 976.6.
Stage names are workload files in `deploy/aws/bench/omb/workloads` (and `later/` with `--include-later`).
Compression: `--compression auto|lz4|none` (default auto = lz4 if the probe passes, as WarpStream; `none` only as a fallback).

Abort rules, checked on every 10 s OMB line: error rate above 0.1% of the publish rate, publish p99 of the interval above
10 s, or (when consumers exist) backlog of 100 K messages or more growing for 2 consecutive intervals. A trip kills OMB,
writes `ABORT` (reason) next to `omb/` on loadgen 0, uploads everything, and skips the remaining stages. Manual stop:
`bench-ctl.sh abort` (touches `/tmp/omb-stop`, kills the coordinator). `--max-minutes` (default 120) refuses an
over-long plan. Results land in `~/bench-out/<run>/omb/` and `s3://<bucket>/results/<run>/omb/` after every stage.

### A7. Collect, then destroy

```
SUMMARIZE_ARGS='--broker-hourly <usd per m6in.4xlarge hour in your region>' \
  deploy/aws/bench/collect-results.sh $(cd deploy/aws/terraform && terraform output -raw bucket) $RUN_ID ap-south-2
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
- [ ] apply + artifact upload done; all hosts `bench-bootstrap.done`; 6 brokers up in Grafana; `bench-ctl.sh check-cp` ok (exactly one manicp)
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
- The single-sequencer control plane (decision 46) and its `manicp_seq_*` metrics are new: the first AWS run with
  them is the evidence. summarize.py shows `-` for sequencer columns when the manicp build exports none.
- The CP is a single point of failure for availability (not for correctness): see A10.
- The lz4 probe (`kafka-producer-perf-test`, `retries=0`, `max.block.ms=15000`) passes against a broker with decision 45;
  if it ever misclassifies, force `--compression none|lz4`.
- DynamoDB cost and the 98-calls-per-commit behaviour at 288 partitions are predictions (comparison doc, section 5).

### A10. The single control plane: verify, and what to do when it dies

Decision 46: the control plane owns sequencing. There is exactly ONE manicp (host `mb-<run>-cp`, `cp_instance_type`,
count 1) with a single sequencing thread, group commit and an in-memory offset cache; every broker runs
`-cp-url http://<cp_private_ip>:8080`. The first 6-broker smoke run with one manicp per broker (6 independent writers on
one DynamoDB table) showed constant compare-and-set cancellations and commit latency spikes: that is what this layout removes.

Verify the single CP (also part of every apply):
- `deploy/aws/bench/bench-ctl.sh check-cp`: exactly one `manicp` process on the CP host, healthz ok, no `manicp` process on
  any broker, every broker reaches the CP.
- Grafana Overview, "Control plane" row: one series for the sequencer; "Cancellations and group retries" about 0. Sustained
  cancels or two `manicp` hosts under `up{job="manicp"}` mean a second manicp is running somewhere (a leftover process, a
  hand-started instance): stop it. Correctness never depends on this (the DynamoDB compare-and-set is the safety net,
  so a second writer cannot corrupt offsets); it is only a performance problem.
- `s3://<bucket>/results/<run>/metrics/cp/` gets a new file every 10 s; run-omb.sh pre-flight refuses to start without it.

If the CP dies (instance terminated by the max_runtime guard, crash, host failure):
- Symptom: brokers' produce acks stall or fail (the commit step cannot reach `:8080`), `up{job="manicp"}` 0, no new
  `metrics/cp/` files, `bench-ctl.sh check-cp` fails. Segments already stored in S3 are not lost: they are simply not
  committed (visible) until a CP answers; clients retry per their delivery timeouts.
- Process died, host alive: the unit has `Restart=always`, `RestartSec=1`, so systemd brings it back in about a second
  (look at `journalctl -u manicp`; `sudo systemctl restart manicp` by hand if it gave up). The state is all in DynamoDB,
  so nothing is lost, but a restart is not free. Real behaviour: (1) every commit that reaches the CP during the gap is
  refused at the connection; the brokers keep retrying it (capped exponential backoff, 20 ms up to 2 s, with jitter) for
  `-commit-deadline` (default 120 s) after the commit's first attempt, so a gap of seconds fails no produce but delays the
  acks and fills the produce buffer (clients are paused by backpressure meanwhile); (2) a commit whose answer was lost
  is resent under the same key and gets the same answer, so it is not duplicated; (3) the new process starts with an
  empty sequencer cache, so the first commit and the first Lookup of every partition read DynamoDB (a burst of reads);
  (4) a gap longer than the deadline fails the segments in flight with REQUEST_TIMED_OUT, and producers that retry may then
  duplicate records whose lost commit had landed (decision 41). A host replacement takes minutes, which is beyond the
  deadline: expect failed produces and re-run the stage.
- Host gone: `terraform apply -replace=aws_instance.cp -var-file=my.tfvars` (replaces only the CP; after upload READY is
  already set, so it boots straight away). The private IP changes, which changes the brokers' user-data (`-cp-url`):
  Terraform replaces the broker hosts too (`user_data_replace_on_change`), which in a benchmark is acceptable but loses the
  run; for a mid-run recovery instead restart the instance in place or re-point a broker by hand
  (`/etc/systemd/system/manibroker.service`, `-cp-url`, `systemctl daemon-reload && systemctl restart manibroker`).
- After recovery run `check-cp`, then resume or re-run the stage; note the outage in the run notes.

HA (future work, not needed for correctness): an active/standby pair, where the standby holds an advisory DynamoDB lease
(heartbeat item with TTL) and takes over by fencing through the existing compare-and-set when the active one stops
renewing. The lease would only prevent two active sequencers (a performance matter, as above); safety stays with the
conditional writes. Not built, not needed for the benchmark.

# Part B. Quick smoke (single broker)

Everything below is the original single-broker flow (`broker_count=1`, `run-benchmark.sh`). Disposable stack: one broker
host, one load generator host. Known limits:

- The broker accepts only non-idempotent producers (decision 41): the harness passes `enable.idempotence=false` to `kafka-producer-perf-test` (Kafka 3.8 defaults to true and would be rejected with INVALID_REQUEST). Keep it if you override `PRODUCER_PROPS`. Results in `docs/aws-benchmark-results-r1.md` predate this change.
- The load tool is Apache Kafka's `kafka-producer-perf-test`. The WarpStream `benchmark-producer` always compresses
  (no flag to disable); the broker accepts that since decision 45 (earlier brokers answered
  `UNSUPPORTED_COMPRESSION_TYPE`). Never use `cmd/manibench` for the AWS run.
- The broker implements neither CreateTopics nor DescribeConfigs, so `kafka-topics.sh --create/--describe`
  fail. The control-plane host creates the 6-partition topic `bench-<run_id>` at boot through its loopback manicp API;
  the harness only waits for it (`kafka-topics.sh --list` works).
- Saturating runs (default) make client-side latency mostly client queueing and print no percentiles. Use
  broker-side ack latency (`seal_to_ack`) for saturation, and `--rate N` runs for client p50/p99 below saturation.
- The `aws_*` CloudWatch series names (YACE) were not verified against a live scrape; see "Dashboards".

## 1. Prerequisites

- AWS credentials for the target account (region `ap-south-2` in the examples), AWS CLI v2, the Session Manager plugin,
  Terraform >= 1.6, Go, Python 3, `jq`.
- A Grafana Cloud stack (examples: `https://manifesterio.grafana.net`). Known non-secret values:

  | what | value |
  |---|---|
  | metrics push URL | `https://prometheus-prod-36-prod-us-west-0.grafana.net/api/prom/push` |
  | metrics user | `2644751` |
  | Loki push URL | `https://logs-prod-021.grafana.net/loki/api/v1/push` |
  | Loki user | `1318052` |
  | stack URL | `https://manifesterio.grafana.net` |

### Grafana tokens (minimal scopes)

Grafana Cloud portal, your org, Security, Access Policies:
1. Create access policy `manifester-bench-metrics` with realm = your stack and scope `metrics:write` only. Add a token (expiry: a few days). Save it as `prom_api_key`.
2. Create access policy `manifester-bench-logs` with scope `logs:write` only. Add a token. Save it as `loki_api_key`.
   (One policy with both scopes also works: store the same token in `grafana/api_key` and skip the two above.)
3. Annotations and dashboards: in the stack, Administration, Users and access, Service accounts, create one with role Editor and a token. Used by the harness (`grafana/annotation_token`) and by `apply-dashboards.sh` (`GRAFANA_API_TOKEN`). Annotations tolerate being absent.
Never commit tokens; they live only in SSM.

### SSM parameters (create once per account/region, before apply)

```
R=ap-south-2; P=/manifester-bench/grafana
aws ssm put-parameter --region $R --name $P/remote_write_url --type String       --value 'https://prometheus-prod-36-prod-us-west-0.grafana.net/api/prom/push'
aws ssm put-parameter --region $R --name $P/user             --type String       --value 2644751
aws ssm put-parameter --region $R --name $P/prom_api_key     --type SecureString --value "$PROM_TOKEN"
aws ssm put-parameter --region $R --name $P/loki_url         --type String       --value 'https://logs-prod-021.grafana.net/loki/api/v1/push'
aws ssm put-parameter --region $R --name $P/loki_user        --type String       --value 1318052
aws ssm put-parameter --region $R --name $P/loki_api_key     --type SecureString --value "$LOKI_TOKEN"
# optional (run annotations)
aws ssm put-parameter --region $R --name $P/url              --type String       --value 'https://manifesterio.grafana.net'
aws ssm put-parameter --region $R --name $P/annotation_token --type SecureString --value "$ANNOTATION_TOKEN"
```
Use `read -rs PROM_TOKEN` style prompts so tokens stay out of shell history.

## 2. Apply

```
cd deploy/aws/terraform
cp example.tfvars my.tfvars    # region=ap-south-2, az=ap-south-2a, run_id, owner, budget_usd, budget_email
terraform init && terraform plan -var-file=my.tfvars
terraform apply -var-file=my.tfvars          # you run this; the S3 request-metrics config exists from here on
```
Note the S3 request metrics need about 15 minutes after creation to show data: apply early.

## 3. Upload the artifact

From the repo root, right after apply (instances wait up to 45 minutes for `READY`):
```
$(cd deploy/aws/terraform && terraform output -raw artifact_upload_command)
```
It builds linux/amd64 `manibroker`/`manicp`, uploads them, the observability directory (`artifacts/observability/`)
and the harness (`artifacts/bench/`), and writes `READY` last.

## 4. Wait for READY and verify health

Every host: `ls /var/lib/bench-bootstrap.done` (takes about 3-6 minutes after READY).
```
$(terraform output -raw ssm_start_session_cp)
  systemctl is-active manicp mb-snapshot alloy ; curl -s 127.0.0.1:8080/healthz ; curl -s 127.0.0.1:9101/healthz
$(terraform output -raw ssm_start_session_broker)
  systemctl is-active manibroker mb-snapshot alloy
  curl -s 127.0.0.1:9100/healthz
  tail -n 30 /var/log/bench-bootstrap.log ; journalctl -u manibroker -n 20 --no-pager
$(terraform output -raw ssm_start_session_loadgen)
  systemctl is-active alloy ; ls /opt/bench ; source /etc/profile.d/bench.sh
  kafka-topics.sh --bootstrap-server $BOOTSTRAP --list     # expect bench-<run_id>
```
Grafana (stack URL): apply dashboards once from your workstation, with the Editor service-account token:
```
cd deploy/aws/observability
GRAFANA_URL=https://manifesterio.grafana.net GRAFANA_API_TOKEN=... ./apply-dashboards.sh
```
Then check: Explore (Prometheus) `up{run_id="<run>"}` shows broker, manicp (role `cp`), node jobs for all hosts with value 1;
Explore (Loki) `{run_id="<run>"}` shows `manibroker` and `manicp` JSON lines (labels `service`, `level`, `host`);
dashboards Overview, Pipeline, AWS, Hosts show data for the run (AWS CloudWatch panels lag 2-15 minutes).
The S3 manifests appear in `s3://<bucket>/results/<run>/manibroker-<run>.json` and `manicp-<run>.json`.

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
- [ ] AWS dashboard: `manibroker_s3_*` and `manicp_dynamo_*` panels show attempts; throttles 0.
- [ ] `s3://<bucket>/results/<run>/metrics/` is receiving `<broker id>/<epoch>-broker.txt` and `cp/<epoch>-manicp.txt` files (every 10 s).
- [ ] An annotation "run <run> start" is visible (if you set the annotation parameters).
If it fails, check `journalctl -u manibroker` on the broker host (a `UNSUPPORTED_*` or S3/DynamoDB permission error shows up there) before spending more.

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
manibroker `/metrics` snapshots come from `mb-snapshot.service` on the broker host and manicp's from the same unit on the control-plane host (every 10 s; the SGs open
no metrics ports and loadgen has no `ssm:SendCommand`, so a push-to-S3 timer was the simplest option). `summarize.py`
picks the snapshots around each cell by timestamp.
Latency run below saturation (optional, afterwards): `run-benchmark.sh --rate 500 --cells "c1_1k c8_1k" --reps 1`
(500 records/s per client; the tool then exits by itself and prints p50/p95/p99).
Options: `--reps --duration --gap --warmup --cells --rate --topic --tool`.

## 7. Collect results (before destroy)

On the workstation:
```
deploy/aws/bench/collect-results.sh $(cd deploy/aws/terraform && terraform output -raw bucket) <run_id> ap-south-2
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
cd deploy/aws/terraform && terraform destroy -var-file=my.tfvars     # you run this
```
Then run every command of "Post-destroy checklist" in `deploy/aws/terraform/README.md` (all must print nothing):
tagged resources, EC2 instances and volumes, VPC and endpoints, unattached Elastic IPs, the bucket, the DynamoDB
table, IAM roles and instance profiles, the budget. Then delete the SSM parameters if the account is shared
(command in that README) and revoke the three Grafana tokens. Keep `terraform.tfstate` until the checklist passes.

## 9. Estimated spend

Fixed cost about $0.29/hour (c7i.xlarge $0.179, c7i.large $0.089, volumes, public IPv4, S3 request metrics).
A session of about 2 hours (apply, 15 min of waiting, smoke, matrix, collection, destroy) is about $0.6 fixed.
Usage: S3 PUTs at 5-60 segments/s for 17 minutes is on the order of 5-60 thousand PUTs per minute at the extreme
($0.005 per 1000), so $0.2-1.5; DynamoDB on-demand writes (transactional, 2 units each) similar order, under $1;
CloudWatch GetMetricData by Alloy a few cents. Expect **$2-5 total** (the first minutes of S3 request metrics are a
monthly custom-metric price prorated). Budget alarm thresholds are in `my.tfvars`. Grafana Cloud free tier is not
included. Left running for a day by mistake: about $7.

## 10. Dashboards: names still unconfirmed

All `manibroker_*`, `manicp_*` and `node_*` series in the dashboards were checked against the Go code and
`docs/observability.md` (the `manibroker_s3_*`/`manicp_dynamo_*` families are composed from a Subsystem in
`sys/awsobs`, confirmed in code). Not confirmed against a live scrape: the CloudWatch series from Alloy's embedded YACE
(`aws_s3_first_byte_latency_average`, `aws_s3_total_request_latency_{average,maximum}`, `aws_s3_{all,put,get}_requests_sum`,
`aws_s3_{4xx,5xx}_errors_sum`, `aws_dynamodb_successful_request_latency_{average,maximum}`,
`aws_dynamodb_consumed_{read,write}_capacity_units_sum`, `aws_dynamodb_provisioned_*_capacity_units_average`,
`aws_dynamodb_{read,write}_throttle_events_sum`, `aws_dynamodb_throttled_requests_sum`,
`aws_dynamodb_transaction_conflict_sum`, `aws_dynamodb_conditional_check_failed_requests_sum`, label `dimension_Operation`).
First thing after READY: Explore `{__name__=~"aws_.*"}` and fix `deploy/aws/observability/gen/gen_dashboards.py` if spelled differently.
