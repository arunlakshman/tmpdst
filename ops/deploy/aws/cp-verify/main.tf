# Small stack to test the control plane alone against real AWS.
# Only two things are made: one DynamoDB table and one S3 bucket. No VPC, no EC2, no load balancer.
# Both cost almost nothing when idle (pay per request, objects expire after 1 day).

data "aws_caller_identity" "current" {}

locals {
  tags = {
    Project = "mds-cp-verify"
    RunId   = var.run_id
    Owner   = var.owner
  }

  table_prefix = "mds_${replace(var.run_id, "-", "_")}_"
  table_name   = "${local.table_prefix}cp"
  bucket_name  = "mds-cpverify-${var.run_id}-${data.aws_caller_identity.current.account_id}"
}

# Same shape as the benchmark table (pk, sk, TTL on "ttl"), but NOT pre-warmed:
# warm throughput is only needed for high load tests.
resource "aws_dynamodb_table" "cp" {
  name         = local.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  deletion_protection_enabled = false

  point_in_time_recovery {
    enabled = false
  }
}

resource "aws_s3_bucket" "checkpoints" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "checkpoints" {
  bucket                  = aws_s3_bucket.checkpoints.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Cost guard: nothing stays more than 1 day, even if we forget to destroy.
resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  rule {
    id     = "expire-everything"
    status = "Enabled"
    filter {}

    expiration {
      days = 1
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
