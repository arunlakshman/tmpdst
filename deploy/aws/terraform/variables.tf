# No secrets live in this stack. Grafana credentials are SSM parameters that the
# operator creates by hand; only their NAMES are variables.

variable "region" {
  description = "AWS region (no default: pass it explicitly)."
  type        = string
}

variable "az" {
  description = "Availability zone for the single public subnet, e.g. eu-west-1a (no default)."
  type        = string

  validation {
    condition     = startswith(var.az, var.region)
    error_message = "az must be inside var.region."
  }
}

variable "run_id" {
  description = "Short id of this benchmark run. Lowercase letters, digits, hyphens. Becomes part of the bucket, table and tag names."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,23}$", var.run_id))
    error_message = "run_id must match ^[a-z0-9][a-z0-9-]{1,23}$ (2-24 chars)."
  }
}

variable "owner" {
  description = "Value of the Owner tag (who to ask about leftovers)."
  type        = string
}

variable "auto_destroy_after" {
  description = "Value of the auto-destroy-after tag, e.g. 2026-10-19T18:00Z. Informational only: nothing destroys the stack automatically."
  type        = string
  default     = "manual"
}

variable "broker_count" {
  description = "Number of stateless broker hosts (each runs manibroker only and points -cp-url at the single control-plane host), all in the one AZ and sharing one bucket and one table. WarpStream's published benchmark uses 6."
  type        = number
  default     = 6

  validation {
    condition     = var.broker_count >= 1 && var.broker_count <= 12 && floor(var.broker_count) == var.broker_count
    error_message = "broker_count must be an integer in 1..12."
  }
}

variable "cp_instance_type" {
  description = "Control-plane host: exactly ONE instance running only manicp (the single sequencer, decision 46). x86_64 only. 4 vCPU / 16 GiB is enough for the sequencer; counts 4 vCPUs against the account quota."
  type        = string
  default     = "m6in.xlarge"
}

variable "broker_instance_type" {
  description = "Broker host (manibroker only). x86_64 only: the artifact is linux/amd64. Default matches WarpStream's published agents (16 vCPU / 64 GiB / up to 40 Gbps)."
  type        = string
  default     = "m6in.4xlarge"
}

variable "loadgen_count" {
  description = "Number of load generator hosts (OpenMessaging Benchmark workers/driver are spread over them)."
  type        = number
  default     = 2

  validation {
    condition     = var.loadgen_count >= 1 && var.loadgen_count <= 8 && floor(var.loadgen_count) == var.loadgen_count
    error_message = "loadgen_count must be an integer in 1..8."
  }
}

variable "loadgen_instance_type" {
  description = "Load generator host. x86_64 only. Default matches WarpStream's OMB host (32 vCPU / 128 GiB / 50 Gbps)."
  type        = string
  default     = "m6in.8xlarge"
}

variable "use_nlb" {
  description = "Create an internal Network Load Balancer (TCP 9092) in front of the brokers as an optional single bootstrap address (output kafka_bootstrap_nlb). Connection-level spreading only."
  type        = bool
  default     = true
}

variable "max_runtime_minutes" {
  description = "Self-destruct guard: user-data schedules 'shutdown -h +N' on every host and instance_initiated_shutdown_behavior is terminate, so each instance terminates N minutes after boot even if you forget to destroy."
  type        = number
  default     = 360

  validation {
    condition     = var.max_runtime_minutes >= 30 && var.max_runtime_minutes <= 1440
    error_message = "max_runtime_minutes must be 30..1440."
  }
}

variable "ddb_warm_write_units" {
  description = "DynamoDB on-demand warm_throughput write units/s (a new table starts at 4,000). Pre-warming is billed once by AWS, see README. Needs aws provider >= 6.13."
  type        = number
  default     = 30000
}

variable "ddb_warm_read_units" {
  description = "DynamoDB on-demand warm_throughput read units/s (a new table starts at 12,000)."
  type        = number
  default     = 30000
}

variable "initial_topic_partitions" {
  description = "Partitions of the topic bench-<run_id> that the control-plane host creates through its loopback manicp at boot (the broker implements no CreateTopics)."
  type        = number
  default     = 6
}

variable "root_volume_gb" {
  description = "Root EBS volume size (gp3, encrypted) of both hosts."
  type        = number
  default     = 30
}

variable "bucket_expire_days" {
  description = "Every object in the run bucket expires after this many days (S3 minimum is 1). force_destroy handles the rest at destroy."
  type        = number
  default     = 3
}

variable "s3_metrics_filter_id" {
  description = "Id of the whole-bucket S3 request-metrics configuration. CloudWatch exposes it as the FilterId dimension; also passed to Alloy as S3_METRICS_FILTER_ID, so the YACE query and this resource always agree."
  type        = string
  default     = "manifester-bench"
}

variable "budget_usd" {
  description = "AWS Budget limit (USD, monthly period) for the alarm."
  type        = number
}

variable "budget_email" {
  description = "Email address notified by the budget alarm."
  type        = string
}

variable "allowed_ssm_param_prefix" {
  description = "SSM parameter path prefix both hosts may read (GetParameter). Must start with /."
  type        = string
  default     = "/manifester-bench/"

  validation {
    condition     = startswith(var.allowed_ssm_param_prefix, "/")
    error_message = "allowed_ssm_param_prefix must start with /."
  }
}

variable "grafana_remote_write_url_param" {
  description = "SSM parameter NAME holding the Grafana Cloud Prometheus remote_write URL. Must be under allowed_ssm_param_prefix."
  type        = string
  default     = "/manifester-bench/grafana/remote_write_url"
}

variable "grafana_user_param" {
  description = "SSM parameter NAME holding the Grafana Cloud metrics user (instance id)."
  type        = string
  default     = "/manifester-bench/grafana/user"
}

variable "grafana_api_key_param" {
  description = "SSM parameter NAME of an optional single combined Grafana Cloud token (SecureString), used only for whichever of the two keys below is missing."
  type        = string
  default     = "/manifester-bench/grafana/api_key"
}

variable "grafana_prom_api_key_param" {
  description = "SSM parameter NAME holding the metrics:write token (SecureString)."
  type        = string
  default     = "/manifester-bench/grafana/prom_api_key"
}

variable "grafana_loki_api_key_param" {
  description = "SSM parameter NAME holding the logs:write token (SecureString)."
  type        = string
  default     = "/manifester-bench/grafana/loki_api_key"
}

variable "grafana_loki_url_param" {
  description = "SSM parameter NAME holding the Grafana Cloud Loki push URL (.../loki/api/v1/push)."
  type        = string
  default     = "/manifester-bench/grafana/loki_url"
}

variable "grafana_loki_user_param" {
  description = "SSM parameter NAME holding the Grafana Cloud Loki user (instance id)."
  type        = string
  default     = "/manifester-bench/grafana/loki_user"
}

variable "cloudwatch_read_policy_json" {
  description = "Fallback CloudWatch read policy (JSON) for the broker host when deploy/aws/observability/iam-cloudwatch-read.json does not exist. null: attach nothing."
  type        = string
  default     = null
}

variable "alloy_version" {
  description = "Grafana Alloy rpm version from rpm.grafana.com (1.10.1 is the version the configs were validated with)."
  type        = string
  default     = "1.10.1"
}

variable "kafka_tools_version" {
  description = "Apache Kafka release for the fallback CLI tools (archive.apache.org, Scala 2.13 build)."
  type        = string
  default     = "3.8.0"
}

variable "broker_extra_args" {
  description = "Extra flags appended to the manibroker command line (e.g. \"-segment-bytes 32MiB\")."
  type        = string
  default     = ""
}

variable "cp_extra_args" {
  description = "Extra flags appended to the manicp command line on the control-plane host (e.g. sequencer tuning flags once manicp has them). Empty by default so it stays valid for any manicp build."
  type        = string
  default     = ""
}

variable "artifact_wait_minutes" {
  description = "How long user-data waits for artifacts/READY in the run bucket before giving up."
  type        = number
  default     = 45
}
