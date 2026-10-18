locals {
  common_vars = {
    region                  = var.region
    run_id                  = var.run_id
    bucket                  = aws_s3_bucket.run.bucket
    artifact_wait_minutes   = var.artifact_wait_minutes
    alloy_version           = var.alloy_version
    grafana_url_param       = var.grafana_remote_write_url_param
    grafana_user_param      = var.grafana_user_param
    grafana_api_key_param   = var.grafana_api_key_param
    grafana_loki_url_param  = var.grafana_loki_url_param
    grafana_loki_user_param = var.grafana_loki_user_param
    ssm_prefix              = trimsuffix(var.allowed_ssm_param_prefix, "/")
    metrics_filter_id       = var.s3_metrics_filter_id
  }

  common = templatefile("${path.module}/files/common.sh.tpl", local.common_vars)
}

resource "aws_instance" "broker" {
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = var.broker_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.broker.id]
  iam_instance_profile        = aws_iam_instance_profile.broker.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/files/broker-user-data.sh.tpl", merge(local.common_vars, {
    common            = local.common
    table_prefix      = local.table_prefix
    table_name        = local.table_name
    broker_extra_args = var.broker_extra_args
    cp_extra_args     = var.cp_extra_args
  }))

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gb
    encrypted   = true
    tags        = merge(local.tags, { Name = "${local.name}-broker-root" })
  }

  tags = { Name = "${local.name}-broker", Role = "broker" }

  depends_on = [
    terraform_data.ssm_prefix_check,
    aws_route_table_association.public,
    aws_vpc_endpoint.s3,
    aws_vpc_endpoint.dynamodb,
    aws_iam_role_policy.broker,
    aws_iam_role_policy.broker_ssm_read,
  ]
}

resource "aws_instance" "loadgen" {
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = var.loadgen_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.loadgen.id]
  iam_instance_profile        = aws_iam_instance_profile.loadgen.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/files/loadgen-user-data.sh.tpl", merge(local.common_vars, {
    common              = local.common
    broker_private_ip   = aws_instance.broker.private_ip
    kafka_tools_version = var.kafka_tools_version
  }))

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gb
    encrypted   = true
    tags        = merge(local.tags, { Name = "${local.name}-loadgen-root" })
  }

  tags = { Name = "${local.name}-loadgen", Role = "loadgen" }

  depends_on = [
    terraform_data.ssm_prefix_check,
    aws_route_table_association.public,
    aws_iam_role_policy.loadgen,
    aws_iam_role_policy.loadgen_ssm_read,
  ]
}
