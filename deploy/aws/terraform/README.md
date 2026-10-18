# manifester-bench: disposable AWS benchmark stack

One broker host (manibroker + manicp + node_exporter + Alloy) and one load
generator host in a single public subnet of one AZ, plus a run bucket, a
DynamoDB control-plane table, and a budget alarm. Everything is tagged
`Project=manifester-bench`, `RunId`, `Owner`, `auto-destroy-after` (informational;
nothing destroys it for you). **You run apply and destroy.**

## Layout

| Resource | Notes |
| --- | --- |
| VPC 10.42.0.0/16, 1 public subnet, IGW | S3 and DynamoDB gateway endpoints on the route table |
| Security groups | No inbound except TCP 9092 from the loadgen SG to the broker SG. No SSH: use SSM Session Manager. Metrics are scraped by Alloy on each host over loopback, so no metrics ports are open. IMDSv2 required. |
| S3 `manifester-bench-<run_id>-<account>` | force_destroy, public access blocked, SSE-S3, everything expires after `bucket_expire_days`, whole-bucket request metrics (`FilterId=EntireBucket`, variable `s3_metrics_filter_id`) |
| DynamoDB `manifester_<run_id>_cp` | Same schema as `cp/dynamo` `CreateTables`: `pk` S hash, `sk` S range, on-demand. Created here, so manicp runs **without** `-create-tables` and its role has no CreateTable/DeleteTable. |
| IAM | Broker: bucket RW, table RW, SSM params under the prefix, SSM core, CloudWatch read (from `../observability/iam-cloudwatch-read.json` if present, else `-var cloudwatch_read_policy_json`). Loadgen: SSM, read `artifacts/*`, write `results/*`. |
| Budget | 80% actual and 100% forecast emails. Account-wide (no tag filter, because cost-allocation tags need manual activation), so set `budget_usd` accordingly. |

TransactWriteItems/TransactGetItems have no IAM actions; they are authorized by
PutItem/UpdateItem/DeleteItem/ConditionCheckItem/GetItem, all of which are granted.

## Prerequisites

- Terraform >= 1.6, AWS CLI, Go (to build), credentials for the target account.
- The Session Manager plugin for `aws ssm start-session`.
- Three SSM parameters (names are variables, defaults under `/manifester-bench/`):
  ```
  aws ssm put-parameter --region R --name /manifester-bench/grafana/remote_write_url --type String       --value 'https://prometheus-...grafana.net/api/prom/push'
  aws ssm put-parameter --region R --name /manifester-bench/grafana/user             --type String       --value '<metrics user id>'
  aws ssm put-parameter --region R --name /manifester-bench/grafana/api_key          --type SecureString --value '<token>'
  ```
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
- `s3://BUCKET/artifacts/alloy/*`: contents of `deploy/aws/observability/alloy/`. Alloy runs on that directory (`alloy run /etc/alloy/conf.d`). If absent, Alloy is not started.
- `s3://BUCKET/artifacts/READY`: marker (holds the git sha).

Environment variables the Alloy config can rely on (`/etc/alloy/env`):
`GRAFANA_REMOTE_WRITE_URL`, `GRAFANA_USER`, `GRAFANA_API_KEY`, `RUN_ID`,
`INSTANCE_ROLE` (`broker` or `loadgen`), `AWS_REGION`, `S3_BUCKET`,
`DYNAMODB_TABLE` (broker only). Local scrape targets: manibroker
`127.0.0.1:9100`, manicp `127.0.0.1:9101`, node_exporter `127.0.0.1:9110` (not
9100, which manibroker uses), WarpStream benchmark `:8081`/`:8082` on loadgen.
Alloy's own UI: `127.0.0.1:12345`.

manibroker flags `-run-id`, `-manifest-out` (to `s3://BUCKET/results/<run_id>/manifest.json`)
and `-log-level` are being added to the Go code concurrently; an older binary
will make `manibroker.service` fail on an unknown flag. Note the broker's S3
flags are `-s3-bucket`/`-s3-region` while manicp uses `-region`.

## Verify

```
$(terraform output -raw ssm_start_session_broker)
  systemctl status manicp manibroker node_exporter alloy
  tail /var/log/bench-bootstrap.log ; journalctl -u manibroker -n 50
  curl -s 127.0.0.1:9100/metrics | head
$(terraform output -raw ssm_start_session_loadgen)
  ls /var/lib/bench-bootstrap.done ; echo $BOOTSTRAP
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic t1 --partitions 8
  warpstream cli benchmark-producer -bootstrap-host $BOOTSTRAP_HOST -bootstrap-port 9092 -topic t1 -num-clients 1
  # fallback:
  /opt/kafka/bin/kafka-producer-perf-test.sh --topic t1 --num-records 1000000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP
```

The WarpStream CLI is installed with the official `curl https://console.warpstream.com/install.sh | sh`
(best effort; check `which warpstream`). Its producer/consumer benchmarks are
`warpstream cli benchmark-producer` / `benchmark-consumer` (flags `-topic`,
`-num-clients`, `-record-size`, `-max-records-per-second`, `-bootstrap-host`,
`-bootstrap-port`). Upload results with
`aws s3 cp out.txt s3://$BUCKET/results/$RUN_ID/` (loadgen may write only `results/*`).

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
The three SSM parameters were created by you and are not managed here; delete
them if the account is shared:
`aws ssm delete-parameters --region R --names /manifester-bench/grafana/remote_write_url /manifester-bench/grafana/user /manifester-bench/grafana/api_key`.

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
- Alloy 1.9.2, node_exporter 1.8.2, Kafka 3.8.0 are pinned by variable; confirm the release assets exist.
- Hosts have public IPs (needed for Grafana Cloud and downloads); nothing inbound is allowed.
- Single broker (`-broker-id 1`); scale-out is out of scope.
