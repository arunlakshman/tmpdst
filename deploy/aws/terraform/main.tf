data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  name = "mb-${var.run_id}"

  tags = {
    Project            = "manifester-bench"
    RunId              = var.run_id
    Owner              = var.owner
    auto-destroy-after = var.auto_destroy_after
  }

  bucket_name  = "manifester-bench-${var.run_id}-${data.aws_caller_identity.current.account_id}"
  table_prefix = "manifester_${replace(var.run_id, "-", "_")}_"
  table_name   = "${local.table_prefix}cp"

  ssm_arn_prefix = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${trimsuffix(var.allowed_ssm_param_prefix, "/")}"

  grafana_params = [
    var.grafana_remote_write_url_param,
    var.grafana_user_param,
    var.grafana_api_key_param,
    var.grafana_prom_api_key_param,
    var.grafana_loki_api_key_param,
    var.grafana_loki_url_param,
    var.grafana_loki_user_param,
  ]
}

# ---------------------------------------------------------------- network

resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = local.name }
}

# The default SG is emptied so nothing can use it by accident.
resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-default-empty" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = local.name }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = var.az
  map_public_ip_on_launch = false
  tags                    = { Name = "${local.name}-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-public" }

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${local.name}-s3" }
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${local.name}-dynamodb" }
}

# No inbound from anywhere except Kafka from the loadgen SG. Access to the
# hosts is SSM Session Manager (outbound only). Metrics are scraped by the
# Alloy agent on each host over loopback, so no metrics ports are opened.
resource "aws_security_group" "loadgen" {
  name_prefix = "${local.name}-loadgen-"
  description = "manifester-bench load generator: no inbound"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${local.name}-loadgen" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "broker" {
  name_prefix = "${local.name}-broker-"
  description = "manifester-bench broker: Kafka from loadgen only"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${local.name}-broker" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "kafka_from_loadgen" {
  security_group_id            = aws_security_group.broker.id
  referenced_security_group_id = aws_security_group.loadgen.id
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
  description                  = "Kafka from loadgen"
}

resource "aws_vpc_security_group_egress_rule" "broker_all" {
  security_group_id = aws_security_group.broker.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "outbound: S3/DynamoDB endpoints, SSM, Grafana Cloud, package downloads"
}

resource "aws_vpc_security_group_egress_rule" "loadgen_all" {
  security_group_id = aws_security_group.loadgen.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "outbound: broker, SSM, Grafana Cloud, package downloads"
}

# Fail early if the Grafana parameter names fall outside the IAM-allowed prefix.
resource "terraform_data" "ssm_prefix_check" {
  lifecycle {
    precondition {
      condition     = alltrue([for p in local.grafana_params : startswith(p, var.allowed_ssm_param_prefix)])
      error_message = "Every grafana_*_param must start with allowed_ssm_param_prefix, or the instance role cannot read it."
    }
  }
}

# ---------------------------------------------------------------- budget

resource "aws_budgets_budget" "this" {
  name         = "manifester-bench-${var.run_id}"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}
