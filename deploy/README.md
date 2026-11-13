# Deploy

All deployment files of the repo are here.

| Folder | What it is |
|---|---|
| `aws/terraform` | The benchmark stack: control plane hosts, broker hosts, load generator hosts (OMB workers), S3 bucket, DynamoDB table. See its README. |
| `aws/scripts` | `build-and-upload.sh` (builds the binaries, uploads to the run bucket) and `build-omb.sh` (builds the OpenMessaging Benchmark tarball). |
| `aws/bench` | Harness that runs on the load generators and on your laptop (`bench-ctl.sh`, `run-omb.sh`, OMB patches and workloads, `summarize.py`). |
| `aws/observability` | Grafana Alloy install and dashboards. |
| `aws/cp-verify` | Small separate stack to check the control plane on AWS. |
| `container` | Control plane Containerfile, and the compose file for broker integration tests (Podman). |
| `helm` | Control plane Helm chart. |

Run order for a benchmark: see `broker/docs/aws-benchmark-runbook.md`.
