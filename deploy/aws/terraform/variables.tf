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

variable "broker_instance_type" {
  description = "Broker host (manibroker + manicp). x86_64 only: the artifact is linux/amd64."
  type        = string
  default     = "c7i.xlarge"
}

variable "loadgen_instance_type" {
  description = "Load generator host. x86_64 only."
  type        = string
  default     = "c7i.large"
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
  description = "Extra flags appended to the manicp command line."
  type        = string
  default     = ""
}

variable "artifact_wait_minutes" {
  description = "How long user-data waits for artifacts/READY in the run bucket before giving up."
  type        = number
  default     = 45
}
