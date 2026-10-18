output "broker_private_ip" {
  value = aws_instance.broker.private_ip
}

output "broker_instance_id" {
  value = aws_instance.broker.id
}

output "loadgen_instance_id" {
  value = aws_instance.loadgen.id
}

output "kafka_bootstrap" {
  description = "Bootstrap address for clients on the loadgen host."
  value       = "${aws_instance.broker.private_ip}:9092"
}

output "ssm_start_session_broker" {
  value = "aws ssm start-session --region ${var.region} --target ${aws_instance.broker.id}"
}

output "ssm_start_session_loadgen" {
  value = "aws ssm start-session --region ${var.region} --target ${aws_instance.loadgen.id}"
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
  value = "REMEMBER TO DESTROY: cd deploy/aws/terraform && terraform destroy   # run_id=${var.run_id}; then follow the post-destroy checklist in README.md"
}
