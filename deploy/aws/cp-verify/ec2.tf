# One small EC2 test host. It runs the control plane test processes.
#
# What it is for: a Linux machine in the same AWS account and region as the
# DynamoDB table and S3 bucket, so the control plane tests run close to the real
# services. You reach it with SSM Session Manager. There is no SSH and no key pair.
#
# Cost: about 2 US cents per hour while it is running (instance plus 20 GB disk).
# It is not stopped or removed by any automatic rule.
#
# NOTHING here is destroyed automatically. The instance keeps running until you
# stop it or run terraform destroy. Check it with the AWS console when you are done.
#
# Network: the account's default VPC and one of its default subnets. This file does
# not make a VPC, subnet, NAT or load balancer. It only makes one security group,
# which has no inbound rules.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# The subnet list comes back in no fixed order, so sort it and take the first id.
locals {
  test_host_subnet_id = sort(data.aws_subnets.default.ids)[0]
}

# Amazon Linux 2023, arm64. Its AMI id is kept in a public SSM parameter.
data "aws_ssm_parameter" "al2023_arm64_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

# No inbound rules at all. SSM works over outbound HTTPS, so no inbound port is needed.
resource "aws_security_group" "test_host" {
  name        = "mds-cpverify-${var.run_id}-host"
  description = "Test host for the control plane. No inbound traffic."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Allow all outbound traffic (SSM, package downloads, AWS APIs)."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "test_host" {
  ami                         = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type               = var.instance_type
  subnet_id                   = local.test_host_subnet_id
  vpc_security_group_ids      = [aws_security_group.test_host.id]
  iam_instance_profile        = aws_iam_instance_profile.test_host.name
  associate_public_ip_address = true

  # Tiny first-boot script: make the work folders and give them to ec2-user.
  user_data                   = <<-EOT
    #!/bin/bash
    mkdir -p /opt/cpv/{bin,scripts,run,logs,data,results}
    chown -R ec2-user:ec2-user /opt/cpv
  EOT
  user_data_replace_on_change = false

  # A newer public AMI must never replace a running test box.
  lifecycle {
    ignore_changes = [ami]
  }

  # Require IMDSv2 (session tokens) for the instance metadata service.
  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "mds-cpverify-${var.run_id}"
  }
}
