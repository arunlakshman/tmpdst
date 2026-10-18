# AWS benchmark runbook

Disposable stack: one broker host (manibroker + manicp, S3 + DynamoDB), one load generator host,
Grafana Cloud for metrics/logs. You run apply and destroy; nothing here is automatic.
Related: `deploy/aws/terraform/README.md` (stack), `deploy/aws/observability/README.md` (Alloy, dashboards),
`docs/observability.md` (metrics and logs reference).

Known limits to keep in mind before reading numbers:
- The broker accepts only non-idempotent producers (decision 41): the harness passes `enable.idempotence=false` to `kafka-producer-perf-test` (Kafka 3.8 defaults to true and would be rejected with INVALID_REQUEST). Keep it if you override `PRODUCER_PROPS`. Results in `docs/aws-benchmark-results-r1.md` predate this change.
- The load tool is Apache Kafka's `kafka-producer-perf-test`. The WarpStream `benchmark-producer` was tried
  against the broker and cannot be used: it always compresses (no flag to disable) and the broker rejects
  compressed batches with `UNSUPPORTED_COMPRESSION_TYPE`. Never use `cmd/manibench` for the AWS run.
- The broker implements neither CreateTopics nor DescribeConfigs, so `kafka-topics.sh --create/--describe`
  fail. The broker host creates the 6-partition topic `bench-<run_id>` at boot through manicp's loopback API;
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

Both hosts: `ls /var/lib/bench-bootstrap.done` (takes about 3-6 minutes after READY).
```
$(terraform output -raw ssm_start_session_broker)
  systemctl is-active manicp manibroker mb-snapshot alloy
  curl -s 127.0.0.1:9100/healthz ; curl -s 127.0.0.1:9101/healthz
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
Then check: Explore (Prometheus) `up{run_id="<run>"}` shows broker, manicp, node jobs for both hosts with value 1;
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
- [ ] `s3://<bucket>/results/<run>/metrics/` is receiving `<epoch>-broker.txt` and `-manicp.txt` files (every 10 s).
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
Broker and manicp `/metrics` snapshots come from `mb-snapshot.service` on the broker host (every 10 s; the SGs open
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
