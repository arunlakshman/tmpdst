# manifester-bench: disposable AWS benchmark stack

One broker host (manibroker + manicp + a 10 s `/metrics` snapshotter + Alloy) and one load
generator host in a single public subnet of one AZ, plus a run bucket, a
DynamoDB control-plane table, and a budget alarm. Everything is tagged
`Project=manifester-bench`, `RunId`, `Owner`, `auto-destroy-after` (informational;
nothing destroys it for you). **You run apply and destroy.**

## Layout

| Resource | Notes |
| --- | --- |
| VPC 10.42.0.0/16, 1 public subnet, IGW | S3 and DynamoDB gateway endpoints on the route table |
| Security groups | No inbound except TCP 9092 from the loadgen SG to the broker SG. No SSH: use SSM Session Manager. Metrics are scraped by Alloy on each host over loopback, so no metrics ports are open; the broker host pushes /metrics snapshots to S3 itself. IMDSv2 required. |
| S3 `manifester-bench-<run_id>-<account>` | force_destroy, public access blocked, SSE-S3, everything expires after `bucket_expire_days`, whole-bucket request metrics (`FilterId=manifester-bench`, variable `s3_metrics_filter_id`, also handed to Alloy) |
| DynamoDB `manifester_<run_id>_cp` | Same schema as `cp/dynamo` `CreateTables`: `pk` S hash, `sk` S range, on-demand. Created here, so manicp runs **without** `-create-tables` and its role has no CreateTable/DeleteTable. |
| IAM | Broker: bucket RW, table RW, SSM params under the prefix, SSM core, CloudWatch read (from `../observability/iam-cloudwatch-read.json` if present, else `-var cloudwatch_read_policy_json`). Loadgen: SSM, read `artifacts/*`, write `results/*`. |
| Budget | 80% actual and 100% forecast emails. Account-wide (no tag filter, because cost-allocation tags need manual activation), so set `budget_usd` accordingly. |

TransactWriteItems/TransactGetItems have no IAM actions; they are authorized by
PutItem/UpdateItem/DeleteItem/ConditionCheckItem/GetItem, all of which are granted.

## Prerequisites

- Terraform >= 1.6, AWS CLI, Go (to build), credentials for the target account.
- The Session Manager plugin for `aws ssm start-session`.
- SSM parameters (names are variables, defaults under `/manifester-bench/`; full list and how to get the values in `docs/aws-benchmark-runbook.md`):
  `grafana/remote_write_url`, `grafana/user`, `grafana/prom_api_key` and `grafana/loki_api_key` (SecureString; a single combined `grafana/api_key` is accepted as fallback), `grafana/loki_url`, `grafana/loki_user`; optional `grafana/url`, `grafana/annotation_token` (SecureString) for run annotations.
  (SecureString with the default `aws/ssm` key; a customer key needs its own kms:Decrypt grant.)

## Apply

```
cd deploy/aws/terraform
cp example.tfvars my.tfvars     # git-ignored; set region, az, run_id, owner, budget_*
terraform init
terraform plan  -var-file=my.tfvars
terraform apply -var-file=my.tfvars
# right away (instances wait up to artifact_wait_minutes for it):
$(terraform output -raw artifact_upload_command)     # from the repo root
```

### Artifact contract

`scripts/build-and-upload.sh <bucket> <region>` builds `linux/amd64` (matches
c7i) and uploads, `READY` last:

- `s3://BUCKET/artifacts/manifester-linux-amd64.tar.gz`: `manibroker`, `manicp` at the root.
- `s3://BUCKET/artifacts/observability/`: `deploy/aws/observability` without `dashboards/` and `gen/` (`install-alloy.sh`, `alloy/*`). User-data runs `install-alloy.sh` from it (Alloy rpm 1.10.1 from rpm.grafana.com; writes `/etc/alloy/alloy.env`, 0600). If absent, Alloy is not installed.
- `s3://BUCKET/artifacts/bench/`: `deploy/aws/bench` (harness), synced to `/opt/bench` on the loadgen host.
- `s3://BUCKET/artifacts/READY`: marker (holds the git sha).

Alloy's environment (`/etc/alloy/alloy.env`) is documented in `deploy/aws/observability/README.md`. Local scrape targets: manibroker
`127.0.0.1:9100`, manicp `127.0.0.1:9101`, WarpStream benchmark `127.0.0.1:8081` on loadgen; host metrics come from Alloy's embedded unix exporter.
Alloy's own UI: `127.0.0.1:12345`.

Both services run with `-run-id`, `-log-level info` and `-manifest-out s3://BUCKET/results/<run_id>/` (each writes `<service>-<run_id>.json`). The broker's S3 flags are `-s3-bucket`/`-s3-region`; manicp uses `-region`. Snapshots of both `/metrics` pages land in `s3://BUCKET/results/<run_id>/metrics/<epoch>-{broker,manicp}.txt`.

## Verify

```
$(terraform output -raw ssm_start_session_broker)
  systemctl status manicp manibroker mb-snapshot alloy
  tail /var/log/bench-bootstrap.log ; journalctl -u manibroker -n 50
  curl -s 127.0.0.1:9100/metrics | head
$(terraform output -raw ssm_start_session_loadgen)
  ls /var/lib/bench-bootstrap.done ; echo $BOOTSTRAP
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list     # bench-<run_id> (created by the broker host at boot)
  /opt/bench/run-benchmark.sh --smoke                                      # 20 s go/no-go, see docs/aws-benchmark-runbook.md
```

The topic is created by the broker host through manicp's loopback HTTP API because
the broker implements neither CreateTopics nor DescribeConfigs (`kafka-topics.sh --create/--describe` fail).
The load tool is Apache Kafka's `kafka-producer-perf-test.sh`. The WarpStream CLI
(`/usr/local/bin/warpstream`, same tarball as the official installer; best effort) is installed
but its `benchmark-producer` cannot be used against this broker: it always compresses
(no flag to disable) and the broker rejects compressed batches (`UNSUPPORTED_COMPRESSION_TYPE`).
Results go to `s3://$BUCKET/results/$RUN_ID/` (loadgen may write only `results/*`).

## Cost (on-demand, us-east-1 list prices, approximate)

| Item | Per hour |
| --- | --- |
| c7i.xlarge broker | $0.179 |
| c7i.large loadgen | $0.089 |
| 2 x 30 GB gp3 | $0.0066 |
| 2 x public IPv4 | $0.010 |
| S3 request metrics (about 16 metrics at $0.30/month, prorated) | $0.007 |
| Gateway endpoints, SGs, IAM, VPC, budget (first 2 free) | $0 |
| **Fixed subtotal** | **about $0.29** |
| S3 PUT/GET ($0.005 / $0.0004 per 1k), DynamoDB on-demand ($0.625/M write units, transactional writes cost 2x; $0.125/M read units) | load dependent, order of $0.1-0.5 |

A 4 hour run: about **$1.2 fixed + $0.5-2 usage, roughly $2-4 total**. Other
regions differ by 0-30%. Not included: Grafana Cloud, CloudWatch GetMetricData
calls made by Alloy's CloudWatch scraping (about $0.01 per 1000 metrics requested), and
internet egress (negligible). Same-AZ traffic broker<->loadgen and to S3/DynamoDB through
the gateway endpoints is free.

## Destroy

```
cd deploy/aws/terraform
terraform destroy -var-file=my.tfvars
```

The bucket is `force_destroy`, so objects (including `results/`) are deleted:
copy anything you want first (`aws s3 sync s3://BUCKET/results/ ./results/`).
The SSM parameters were created by you and are not managed here; delete them if the account is shared:
`aws ssm delete-parameters --region R --names $(aws ssm get-parameters-by-path --region R --path /manifester-bench --recursive --query 'Parameters[].Name' --output text)`.

## Post-destroy checklist

All should print nothing (or an empty list). Replace `R` and `RUN`.

```
aws resourcegroupstaggingapi get-resources --region R --tag-filters Key=Project,Values=manifester-bench Key=RunId,Values=RUN --query 'ResourceTagMappingList[].ResourceARN'
aws ec2 describe-instances --region R --filters Name=tag:Project,Values=manifester-bench Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
aws ec2 describe-volumes --region R --filters Name=tag:Project,Values=manifester-bench --query 'Volumes[].VolumeId'
aws ec2 describe-vpcs --region R --filters Name=tag:Project,Values=manifester-bench --query 'Vpcs[].VpcId'
aws ec2 describe-vpc-endpoints --region R --filters Name=tag:Project,Values=manifester-bench --query 'VpcEndpoints[].VpcEndpointId'
aws ec2 describe-addresses --region R --query 'Addresses[?AssociationId==`null`].AllocationId'
aws s3api list-buckets --query "Buckets[?starts_with(Name,'manifester-bench-RUN')].Name"
aws dynamodb list-tables --region R --query "TableNames[?starts_with(@,'manifester_')]"
aws iam list-roles --query "Roles[?starts_with(RoleName,'mb-RUN-')].RoleName"
aws iam list-instance-profiles --query "InstanceProfiles[?starts_with(InstanceProfileName,'mb-RUN-')].InstanceProfileName"
aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" --query "Budgets[?Name=='manifester-bench-RUN'].Name"
```

Nothing here creates CloudWatch log groups (logs stay in journald and
`/var/log/bench-bootstrap.log` on the hosts). S3 request-metric data in
CloudWatch cannot be deleted and expires on its own. If a destroy fails on the
VPC, look for ENIs left by SSM or a stopped-then-orphaned instance.
Terraform state is local (`terraform.tfstate` here, git-ignored): keep it until
the checklist passes, since losing it orphans the resources.

## Open assumptions

- AMI is the latest Amazon Linux 2023 x86_64 via public SSM parameter, resolved at plan time.
- Alloy 1.10.1 (rpm repo) and Kafka 3.8.0 (archive.apache.org) are pinned by variable; both URLs were confirmed to exist with HTTP HEAD on 2026-10-18.
- Hosts have public IPs (needed for Grafana Cloud and downloads); nothing inbound is allowed.
- Single broker (`-broker-id 1`); scale-out is out of scope.
