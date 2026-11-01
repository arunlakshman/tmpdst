output "region" {
  value = var.region
}

output "table_prefix" {
  description = "Pass to mdscontrolplane as -table-prefix."
  value       = local.table_prefix
}

output "table_name" {
  value = aws_dynamodb_table.cp.name
}

output "checkpoint_bucket" {
  description = "Pass to mdscontrolplane as -checkpoint-bucket."
  value       = aws_s3_bucket.checkpoints.bucket
}

output "log_ttl" {
  description = "Pass to mdscontrolplane as -log-ttl."
  value       = "${var.log_ttl_days * 24}h"
}

output "instance_id" {
  description = "ID of the test host EC2 instance. Use it to connect with SSM Session Manager."
  value       = aws_instance.test_host.id
}

output "instance_private_ip" {
  description = "Private IP of the test host EC2 instance."
  value       = aws_instance.test_host.private_ip
}

output "throttle_table_name" {
  description = "Name of the throttle test table. Empty when enable_throttle_table is false."
  value       = var.enable_throttle_table ? aws_dynamodb_table.throttle[0].name : ""
}
