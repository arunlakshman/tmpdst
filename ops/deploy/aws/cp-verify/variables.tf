variable "region" {
  description = "AWS region. ap-south-2 is Hyderabad."
  type        = string
  default     = "ap-south-2"
}

variable "aws_profile" {
  description = "AWS SSO profile name from ~/.aws/config."
  type        = string
  default     = "hydsandbox"
}

variable "run_id" {
  description = "Short name for this test run. It is part of the table and bucket names."
  type        = string
  default     = "cpv1"
}

variable "owner" {
  description = "Tag value: who owns these resources."
  type        = string
  default     = "arun"
}

variable "log_ttl_days" {
  description = "Items with a ttl attribute are deleted by DynamoDB after this time. Only a backstop."
  type        = number
  default     = 1
}
