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
