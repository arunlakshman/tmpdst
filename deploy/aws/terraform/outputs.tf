output "cp_private_ip" {
  description = "Private IP of the rank-0 control-plane host (the preferred appender)."
  value       = aws_instance.cp.private_ip
}

output "cp_private_ips" {
  description = "Private IPs of all control-plane hosts, rank 0 first."
  value       = local.cp_ips
}

output "cp_urls" {
  description = "The -cp-url list given to every broker (rank 0 first)."
  value       = local.cp_urls
}

output "cp_standby_instance_ids" {
  description = "Instance ids of the hot-standby control-plane hosts (rank 1..N); empty with cp_standby_count = 0."
  value       = aws_instance.cp_standby[*].id
}

output "ssm_start_session_cp_standby" {
  value = [for id in aws_instance.cp_standby[*].id : "aws ssm start-session --region ${var.region} --target ${id}"]
}

output "cp_instance_id" {
  value = aws_instance.cp.id
}

output "ssm_start_session_cp" {
  value = "aws ssm start-session --region ${var.region} --target ${aws_instance.cp.id}"
}

output "broker_private_ips" {
  value = aws_instance.broker[*].private_ip
}

output "broker_instance_ids" {
  value = aws_instance.broker[*].id
}

output "loadgen_instance_ids" {
  value = aws_instance.loadgen[*].id
}

output "kafka_bootstrap" {
  description = "Comma-separated ip:9092 of all brokers (for clients on the loadgen hosts)."
  value       = join(",", [for ip in aws_instance.broker[*].private_ip : "${ip}:9092"])
}

output "kafka_bootstrap_nlb" {
  description = "Internal NLB bootstrap (connection-level spreading only), or null when use_nlb=false."
  value       = var.use_nlb ? "${aws_lb.kafka[0].dns_name}:9092" : null
}

output "ssm_start_session_brokers" {
  value = [for id in aws_instance.broker[*].id : "aws ssm start-session --region ${var.region} --target ${id}"]
}

output "ssm_start_session_loadgens" {
  value = [for id in aws_instance.loadgen[*].id : "aws ssm start-session --region ${var.region} --target ${id}"]
}

# First host of each kind, kept for the single-host runbook commands.
output "ssm_start_session_broker" {
  value = "aws ssm start-session --region ${var.region} --target ${aws_instance.broker[0].id}"
}

output "ssm_start_session_loadgen" {
  value = "aws ssm start-session --region ${var.region} --target ${aws_instance.loadgen[0].id}"
}

output "bucket" {
  value = aws_s3_bucket.run.bucket
}

output "dynamodb_table" {
  value = aws_dynamodb_table.cp.name
}

output "artifact_upload_command" {
  description = "Run from the repo root right after apply; instances wait for it."
  value       = "deploy/aws/terraform/scripts/build-and-upload.sh ${aws_s3_bucket.run.bucket} ${var.region}"
}

output "destroy_reminder" {
  value = "REMEMBER TO DESTROY: cd deploy/aws/terraform && terraform destroy   # run_id=${var.run_id}; then follow the post-destroy checklist in README.md (incl. NLB and target groups). Hosts also self-terminate ${var.max_runtime_minutes} min after boot, but the NLB, table and bucket stay."
}
