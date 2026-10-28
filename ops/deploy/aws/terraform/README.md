# manifester-bench: disposable AWS benchmark stack (WarpStream-comparable)

N stateless brokers (default 6, one AZ; each runs manibroker + a 10 s `/metrics` snapshotter + Alloy),
a log-based control plane (manicp `-store dynamolog`: one active host plus an optional hot standby; brokers
point `-cp-url` at the URL list), `loadgen_count` load generator
hosts (default 2, OpenMessaging Benchmark), an optional internal NLB, one run bucket
and one DynamoDB control-plane table, all in one public subnet, plus a budget alarm.
Everything is tagged `Project=manifester-bench`, `RunId`, `Owner`, `auto-destroy-after`.
**You run apply and destroy.** Hosts also self-terminate `max_runtime_minutes` (default 360) after boot.

The shape follows WarpStream's published TCO benchmark (6 x m6in.4xlarge agents, OMB on
m6in.8xlarge, S3 object store; <https://www.warpstream.com/blog/warpstream-benchmarks-and-tco>).
Apples-to-apples caveats: region ap-south-2 (not us-east-1), brokers here are single-AZ
(WarpStream's agents are spread over AZs for availability), and the object store and
metadata store are plain S3 + DynamoDB rather than WarpStream's.

```
                 +--------------------------- one VPC / one public subnet / one AZ ---------------------------+
 loadgen-0..L-1  |   (OMB driver+workers, Kafka CLI)                                                          |
   SG loadgen ---+--9092--> [optional internal NLB :9092, TCP, no cross-zone] --9092--> broker-1 .. broker-N  |
        |        |                                                                       SG broker            |
        +--8080,8081,9090-9099 (loadgen <-> loadgen only)                                (manibroker :9092)   |
                 |                                          broker SG --8080--> cp (manicp, the only one)     |
                 +-----------------------------------------------------------------------+------------------+
                                                       S3 gateway endpoint / DynamoDB gateway endpoint
                                                       one S3 bucket  <--+--> one DynamoDB table (warm throughput)
 SSM Session Manager only (no SSH), IMDSv2 required, no inter-broker traffic.
```

## Layout

| Resource | Notes |
| --- | --- |
| VPC 10.42.0.0/16, 1 public subnet, IGW | S3 and DynamoDB gateway endpoints on the route table |
| Brokers `aws_instance.broker[0..N-1]` | broker id = index + 1, `-advertise <own private IP>:9092`, `-node-id broker-<id>`, `-cp-url http://<cp private ip>:8080`. manibroker starts after the CP answers `/healthz` (bounded 10 min wait in user-data). No manicp runs on brokers. `instance_initiated_shutdown_behavior = terminate`. |
| Control plane `aws_instance.cp` (rank 0) + `aws_instance.cp_standby` (`cp_standby_count`, default 1) | `cp_instance_type` (default m6in.xlarge), runs ONLY manicp. `-store dynamolog -table T -ckpt-bucket B -ckpt-prefix cp/ -rank R -node-name cp-R -takeover-after 5s -pebble-dir /var/lib/manicp/pebble` (+ `-addr :8080 -region -table-prefix -metrics-addr 127.0.0.1:9101 -run-id -manifest-out ${cp_extra_args}`); rank 0 is the preferred appender and creates topic `bench-<run_id>` on loopback, the standby tails the log. Brokers get `-cp-url` = the URL list, rank 0 first (output `cp_urls`). Pushes `/metrics` to `results/<run_id>/metrics/cp/` (rank 0) and `metrics/cp-<rank>/`, Alloy role `cp` (manicp scrape on each; CloudWatch DynamoDB on rank 0 only), self-terminates like the others. |
| Load generators `aws_instance.loadgen[0..L-1]` | Java 17 (Corretto), Kafka 3.8 CLI in `/opt/kafka`, OMB from `artifacts/omb/openmessaging-benchmark.tar.gz` into `/opt/omb` (skipped with a log line if absent), harness in `/opt/bench`, `/etc/profile.d/bench.sh` |
| Internal NLB (`use_nlb`) | TCP 9092 listener, instance targets = all brokers, TCP health check on 9092, one subnet, no cross-zone, no SG |
| Security groups | Control plane: 8080 from the broker SG only, no other inbound (bench-ctl reaches it with SSM send-command; loadgen to CP is closed). Broker: 9092 from the loadgen SG (plus the subnet CIDR for NLB health checks when `use_nlb`); nothing else, no inter-broker rule. Loadgen: self-referencing TCP 8080, 8081 and 9090-9099 (OMB worker HTTP and stats) and nothing from outside. No SSH. Metrics are scraped by Alloy on each host over loopback; the broker hosts push /metrics snapshots to S3. |
| S3 `manifester-bench-<run_id>-<account>` | force_destroy, public access blocked, SSE-S3, everything expires after `bucket_expire_days`, whole-bucket request metrics (`FilterId=manifester-bench`, variable `s3_metrics_filter_id`, also handed to Alloy) |
| DynamoDB `manifester_<run_id>_cp` | Log-store schema: `pk` S hash, `sk` S range, on-demand with `warm_throughput` (default 30,000 write / 30,000 read units/s). Created here, so every manicp runs **without** `-create-tables` and the role has no CreateTable/DeleteTable. |
| IAM | Control plane: table RW, bucket list + read `artifacts/*` + write `results/*` (manifest, snapshots), SSM params, SSM core, CloudWatch read. Broker: bucket RW, table RW, SSM params under the prefix, SSM core, CloudWatch read (from `../observability/iam-cloudwatch-read.json` if present, else `-var cloudwatch_read_policy_json`). Loadgen: SSM, read `artifacts/*`, write `results/*`. |
| Budget | 80% actual and 100% forecast emails. Account-wide (no tag filter, because cost-allocation tags need manual activation), so set `budget_usd` accordingly. |

TransactWriteItems/TransactGetItems have no IAM actions; they are authorized by
PutItem/UpdateItem/DeleteItem/ConditionCheckItem/GetItem, all of which are granted.

## Variables (the ones you will change)

| Variable | Default | Meaning |
| --- | --- | --- |
| `region`, `az`, `run_id`, `owner`, `budget_usd`, `budget_email` | none | required, see `example.tfvars` |
| `broker_count` | 6 | 1..12 stateless brokers |
| `cp_instance_type` | `m6in.xlarge` | each control-plane host (4 vCPU / 16 GiB) |
| `cp_standby_count` | `1` | hot-standby CP hosts (0..2) |
| `cp_takeover_after` | `5s` | manicp `-takeover-after`; adds to the failover stall, far under the 120 s `-commit-deadline` |
| `broker_extra_args` | `""` | extra manibroker flags. Empty: brokers take the default `-max-segment-partitions 4096`. |
| `cp_extra_args` | empty | extra manicp flags (e.g. `-slot-rate`, `-log-k`); empty keeps the unit valid for any manicp build |
| `broker_instance_type` | `m6in.4xlarge` | 16 vCPU / 64 GiB / up to 40 Gbps, as WarpStream's agents |
| `loadgen_count` | 2 | 1..8 load generator hosts |
| `loadgen_instance_type` | `m6in.8xlarge` | 32 vCPU / 128 GiB / 50 Gbps, as WarpStream's OMB host |
| `use_nlb` | true | internal NLB bootstrap |
| `max_runtime_minutes` | 360 | `shutdown -h +N` on every host, instance terminates. Counts from boot; the full plan with consumers takes about 135-140 min including boot and collection, and the CP host dying stops all writes, so keep this at 240 or more |
| `ddb_warm_write_units`, `ddb_warm_read_units` | 30000, 30000 | table `warm_throughput` |
| `initial_topic_partitions` | 6 | partitions of `bench-<run_id>` |
| `bucket_expire_days`, `root_volume_gb`, `cp_extra_args`, `alloy_version`, `kafka_tools_version`, Grafana SSM param names | see `variables.tf` | unchanged |

The AWS provider is now `~> 6.13` (was `~> 5.80`): `warm_throughput` on `aws_dynamodb_table`
first appears in 6.13.0 (the previously locked 5.100.0 has no such argument; verified from its schema).
Run `terraform init -upgrade` once.

Before applying, confirm the instance types are offered in the AZ (read-only):
`aws ec2 describe-instance-type-offerings --location-type availability-zone --filters Name=instance-type,Values=m6in.4xlarge,m6in.8xlarge Name=location,Values=ap-south-2a --region ap-south-2`.
m6in has an ap-south-2 on-demand price list entry, which means the family is sold there;
the per-AZ check could not be run when this was written (expired SSO token). If it is not
offered in your AZ, set `broker_instance_type`/`loadgen_instance_type` to `m6i.4xlarge`/`m6i.8xlarge`
(same vCPU/RAM, up to 12.5/12.5 Gbps network, so network-bound results are NOT comparable) or pick another AZ.

## Outputs

`cp_private_ip`, `cp_instance_id`, `ssm_start_session_cp`, `broker_private_ips` (list), `broker_instance_ids`, `loadgen_instance_ids`, `kafka_bootstrap`
(`ip:9092,ip:9092,...` of every broker), `kafka_bootstrap_nlb` (null when `use_nlb=false`),
`ssm_start_session_brokers` / `ssm_start_session_loadgens` (lists of commands),
`ssm_start_session_broker` / `ssm_start_session_loadgen` (first host), `bucket`, `dynamodb_table`,
`artifact_upload_command`, `destroy_reminder`.

### Bootstrap choice and the NLB trade-off

Metadata returns only the answering broker (decisions 29/33) and a client keeps using the
broker it bootstrapped to. So with `kafka_bootstrap` (all brokers listed) a client still
lands on whichever of them answers first, and with `kafka_bootstrap_nlb` the NLB
spreads **connections** across brokers (flow hash), never requests. With few client
connections (a handful of OMB producers) per-broker skew is expected and normal
(one broker can carry 2x the load of another); use many producers/connections, or pin one
producer group per broker IP (`BROKER_IPS`) when you want an exact split. The NLB adds a
small latency hop and costs about $0.03/h; set `use_nlb=false` to drop it.

## Prerequisites

- Terraform >= 1.6, AWS CLI, Go (to build), credentials for the target account.
- The Session Manager plugin for `aws ssm start-session`.
- SSM parameters (names are variables, defaults under `/manifester-bench/`; full list and how to get the values in `docs/aws-benchmark-runbook.md`):
  `grafana/remote_write_url`, `grafana/user`, `grafana/prom_api_key` and `grafana/loki_api_key` (SecureString; a single combined `grafana/api_key` is accepted as fallback), `grafana/loki_url`, `grafana/loki_user`; optional `grafana/url`, `grafana/annotation_token` (SecureString) for run annotations.
  (SecureString with the default `aws/ssm` key; a customer key needs its own kms:Decrypt grant.)

## Apply

```
cd ops/deploy/aws/terraform
cp example.tfvars my.tfvars     # git-ignored; set region, az, run_id, owner, budget_*
terraform init -upgrade         # first time after the provider bump to 6.x
terraform plan  -var-file=my.tfvars
terraform apply -var-file=my.tfvars
# right away (instances wait up to artifact_wait_minutes for it):
$(terraform output -raw artifact_upload_command)     # from the repo root
```

### Artifact contract

`scripts/build-and-upload.sh <bucket> <region>` builds `linux/amd64` (x86_64 instance types) and uploads, `READY` last:

- `s3://BUCKET/artifacts/manifester-linux-amd64.tar.gz`: `manibroker`, `manicp` at the root.
- `s3://BUCKET/artifacts/observability/`: `ops/deploy/aws/observability` without `dashboards/` and `gen/` (`install-alloy.sh`, `alloy/*`). User-data runs `install-alloy.sh` from it (Alloy rpm 1.10.1 from rpm.grafana.com; writes `/etc/alloy/alloy.env`, 0600). If absent, Alloy is not installed.
- `s3://BUCKET/artifacts/bench/`: `ops/deploy/aws/bench` (harness), the whole directory, synced to `/opt/bench` on every loadgen host.
- `s3://BUCKET/artifacts/omb/openmessaging-benchmark.tar.gz`: OMB distribution, uploaded from `build/omb/` at the repo root if it exists (the bench build script produces it); extracted to `/opt/omb` on every loadgen.
- `s3://BUCKET/artifacts/READY`: marker (holds the git sha).

Alloy's environment (`/etc/alloy/alloy.env`) is documented in `ops/deploy/aws/observability/README.md`. Local scrape targets: manibroker
`127.0.0.1:9100`, manicp `127.0.0.1:9101`, WarpStream benchmark `127.0.0.1:8081` on loadgen; host metrics come from Alloy's embedded unix exporter.
Alloy's own UI: `127.0.0.1:12345`.

Both services run with `-run-id`, `-log-level info` and `-manifest-out s3://BUCKET/results/<run_id>/` (each writes `<service>-<run_id>.json`). The broker's S3 flags are `-s3-bucket`/`-s3-region`; manicp uses `-region`. Snapshots of `/metrics` land in `s3://BUCKET/results/<run_id>/metrics/<broker id>/<epoch>-broker.txt` (each broker) and `.../metrics/cp/<epoch>-manicp.txt` (the control-plane host).

## Verify

```
terraform output broker_private_ips ; terraform output kafka_bootstrap_nlb
terraform output ssm_start_session_cp ; terraform output cp_private_ip
  systemctl status manicp mb-snapshot alloy ; curl -s 127.0.0.1:8080/healthz ; pgrep -xc manicp   # exactly 1
# pick a command from: terraform output ssm_start_session_brokers
  systemctl status manibroker mb-snapshot alloy ; cat /etc/manifester/host.env   # no manicp here
  tail /var/log/bench-bootstrap.log ; journalctl -u manibroker -n 50 ; sudo shutdown --show
  curl -s 127.0.0.1:9100/metrics | head
# pick a command from: terraform output ssm_start_session_loadgens
  ls /var/lib/bench-bootstrap.done ; source /etc/profile.d/bench.sh ; env | grep -E 'BOOTSTRAP|BROKER_IPS|LOADGEN'
  ls /opt/omb ; java -version
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list     # bench-<run_id> (created by the control-plane host at boot)
  /opt/bench/run-benchmark.sh --smoke                                      # 20 s go/no-go, see docs/aws-benchmark-runbook.md
aws elbv2 describe-target-health --region R --target-group-arn <arn>      # all brokers healthy (use_nlb)
```

`/etc/profile.d/bench.sh` on every loadgen exports `BOOTSTRAP` (all brokers, comma-separated ip:9092),
`BOOTSTRAP_NLB` (empty when `use_nlb=false`), `BROKER_IPS` (comma-separated), `BOOTSTRAP_HOST` (first broker IP),
`LOADGEN_INDEX` (0-based), `LOADGEN_COUNT`, `RUN_ID`, `BUCKET`, `TABLE`, `REGION`, `AWS_REGION`, `SSM_PREFIX`.

The topic is created by the control-plane host through manicp's loopback HTTP API because
the broker implements neither CreateTopics nor DescribeConfigs (`kafka-topics.sh --create/--describe` fail).
The load tool is Apache Kafka's `kafka-producer-perf-test.sh`. The WarpStream CLI
(`/usr/local/bin/warpstream`, same tarball as the official installer; best effort) is installed
and its `benchmark-producer` always compresses (no flag to disable); that works since the broker
stores compressed batches as opaque bytes (decision 45; older brokers answered `UNSUPPORTED_COMPRESSION_TYPE`).
Results go to `s3://$BUCKET/results/$RUN_ID/` (loadgen may write only `results/*`).

## Cost (on-demand; ap-south-2 list prices for EC2, other lines approximate)

EC2 prices are the AWS public price list for Asia Pacific (Hyderabad), Linux, fetched 2026-10-19.

| Item | Per hour |
| --- | --- |
| 6 x m6in.4xlarge brokers ($1.1718 each) | $7.031 |
| 1 x m6in.xlarge control plane (instance + root volume + public IPv4) | about $0.23 |
| 2 x m6in.8xlarge loadgens ($2.3437 each) | $4.687 |
| Internal NLB (about $0.0225 base + about $0.01 LCU) | $0.03 |
| 8 x 30 GB gp3 root volumes (the CP's is in its line above) | $0.03 |
| 8 x public IPv4 ($0.005 each) | $0.04 |
| S3 request metrics (about 16 metrics at $0.30/month, prorated) | $0.007 |
| Gateway endpoints, SGs, IAM, VPC, budget (first 2 free) | $0 |
| **Fixed subtotal** | **about $12.0 / hour** |
| S3 PUT/GET, 16 MiB segments at 1 GiB/s is about 64 PUT/s (about 230k/h, $0.005 per 1k) | up to about $1.2 |
| DynamoDB commits. r1 measured about 190 write units per segment (about 12 segments/s at 108 MB/s), so 64 segments/s is about 12k WRU/s, about 44M WRU/h at about $0.625 per million | up to about $27 at a sustained 1 GiB/s; proportional to load |
| DynamoDB `warm_throughput` 30k/30k | AWS charges for pre-warming once, not hourly; the rate is on the DynamoDB pricing page and was not verified here |

A 4 hour run: about **$48 fixed** (12.0 x 4), **worst case about $160** with a sustained 1 GiB/s
for the whole 4 hours; a realistic mix of idle time and shorter load windows lands between. 6 hours (the
self-destruct limit): about $72 fixed. Not included: Grafana Cloud, CloudWatch GetMetricData calls made
by Alloy, internet egress (negligible), warm-throughput pre-warm charge. Same-AZ traffic and S3/DynamoDB
through gateway endpoints is free. The NLB is in the same AZ as the targets, so no cross-AZ charges.
`example.tfvars` sets `budget_usd = 200`.

## Destroy

```
cd ops/deploy/aws/terraform
terraform destroy -var-file=my.tfvars
```

Destroy order is handled by Terraform (listener, attachments, target group and NLB before
the instances and VPC). Self-termination after `max_runtime_minutes` removes only the EC2 hosts:
the NLB, table, bucket and budget keep existing (and the NLB keeps billing) until `terraform destroy`.
Keep `terraform.tfstate` until the checklist is clean.

The bucket expires every object after `bucket_expire_days` (default 3), including the `seg/` segments that DynamoDB still
points to, so after expiry a Fetch of old offsets fails with `committed segment missing`. That is acceptable for a
disposable stack, but results must be collected before `terraform destroy` (and within the expiry window).

The bucket is `force_destroy`, so objects (including `results/`) are deleted:
copy anything you want first (`aws s3 sync s3://BUCKET/results/ ./results/`).
The SSM parameters were created by you and are not managed here; delete them if the account is shared:
`aws ssm delete-parameters --region R --names $(aws ssm get-parameters-by-path --region R --path /manifester-bench --recursive --query 'Parameters[].Name' --output text)`.

## Post-destroy checklist

All should print nothing (or an empty list). Replace `R` and `RUN`.

```
aws resourcegroupstaggingapi get-resources --region R --tag-filters Key=Project,Values=manifester-bench Key=RunId,Values=RUN --query 'ResourceTagMappingList[].ResourceARN'
aws ec2 describe-instances --region R --filters Name=tag:Project,Values=manifester-bench Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
aws elbv2 describe-load-balancers --region R --query "LoadBalancers[?starts_with(LoadBalancerName,'mb-RUN-')].LoadBalancerArn"
aws elbv2 describe-target-groups --region R --query "TargetGroups[?starts_with(TargetGroupName,'mb-RUN-')].TargetGroupArn"
aws ec2 describe-network-interfaces --region R --filters Name=tag:Project,Values=manifester-bench --query 'NetworkInterfaces[].NetworkInterfaceId'
aws ec2 describe-network-interfaces --region R --filters Name=description,Values='ELB net/mb-RUN-*' --query 'NetworkInterfaces[].NetworkInterfaceId'
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
VPC, look for ENIs left by the NLB (description `ELB net/...`, they disappear a few minutes after the NLB is deleted), SSM or a stopped-then-orphaned instance, and for extra EBS volumes. Any existing "stale resources" checklist you keep elsewhere must include the NLB, its target group and listener, otherwise a leftover internal NLB bills about $0.03/h unnoticed.
Terraform state is local (`terraform.tfstate` here, git-ignored): keep it until
the checklist passes, since losing it orphans the resources.

## Open assumptions

- AMI is the latest Amazon Linux 2023 x86_64 via public SSM parameter, resolved at plan time.
- Alloy 1.10.1 (rpm repo) and Kafka 3.8.0 (archive.apache.org) are pinned by variable; both URLs were confirmed to exist with HTTP HEAD on 2026-10-18.
- Hosts have public IPs (needed for Grafana Cloud and downloads); nothing inbound is allowed.
- vCPU quota: the full stack is 6 x 16 (brokers) + 2 x 32 (loadgens) + 4 (control plane) = 164 vCPUs against the 192 account quota.
- The control plane is an active node plus a hot standby: losing one is a failover of a few seconds (runbook A11), losing both stalls acks until one is back. Standby IPs are in the brokers' user-data: replacing a CP host replaces the brokers. The DynamoDB table has TTL on `ttl` and warm throughput `ddb_warm_*`. Run `terraform init -upgrade && terraform validate` before the first plan.
- Brokers are cattle (decisions 41-44): no registration, fencing or idempotence; ids 1..N come from the instance index, so replacing a host re-uses its id harmlessly.
- Not verified here: `warm_throughput` apply behaviour and pre-warm price, per-AZ m6in availability, and a full plan/apply (never run in this session).
