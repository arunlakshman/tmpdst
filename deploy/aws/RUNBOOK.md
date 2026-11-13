# Run the OMB benchmark on AWS

Stock public OpenMessaging Benchmark (no patches) against 6 `mdsbroker` hosts and 1 control plane host
(+ standby). The old patched-OMB runbook is `broker/docs/aws-benchmark-runbook.md` (history only).

## 1. One time on your laptop
    aws sso login --profile hydsandbox
    export AWS_PROFILE=hydsandbox
    aws sts get-caller-identity                 # Account must be 478483354994
    bash deploy/aws/scripts/build-omb.sh        # Podman; builds build/omb/openmessaging-benchmark.tar.gz (OMB_COMMIT inside)
The Grafana SSM parameters must exist under `/manifester-bench/grafana/` (they do in ap-south-2).

## 2. Create the stack
    cd deploy/aws/terraform
    terraform init
    terraform plan  -var-file=my.tfvars
    terraform apply -var-file=my.tfvars
    $(terraform output -raw artifact_upload_command)     # builds both Linux binaries, uploads binaries, bench harness, OMB, Kafka tools, READY

## 3. Run
    deploy/aws/bench/bench-ctl.sh check-cp
    deploy/aws/bench/bench-ctl.sh start-workers
    deploy/aws/bench/bench-ctl.sh run --stages smoke       # 100k msg/s, 64 producers, 64 consumers, 288 partitions
    deploy/aws/bench/bench-ctl.sh tail 60
    deploy/aws/bench/bench-ctl.sh run --stages ramp,vanity # 100k .. 1M msg/s, then the WarpStream vanity workload
OMB creates its own topic (CreateTopics) and its consumers use consumer groups. The driver YAML is
`deploy/aws/bench/omb/driver-kafka.yaml.tmpl` (idempotence off, lz4, WarpStream client settings).

## 4. Results and cleanup
    deploy/aws/bench/collect-results.sh $(cd deploy/aws/terraform && terraform output -raw bucket) <run_id> ap-south-2
    cd deploy/aws/terraform && terraform destroy -var-file=my.tfvars
Then run the "Post-destroy checklist" in `deploy/aws/terraform/README.md`. Hosts also stop themselves after
`max_runtime_minutes`, but the NLB, DynamoDB table and S3 bucket stay until destroy.
