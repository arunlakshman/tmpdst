resource "aws_s3_bucket" "run" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "run" {
  bucket                  = aws_s3_bucket.run.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "run" {
  bucket = aws_s3_bucket.run.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "run" {
  bucket = aws_s3_bucket.run.id

  rule {
    id     = "expire-everything"
    status = "Enabled"
    filter {}

    expiration {
      days = var.bucket_expire_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# Whole-bucket request metrics (paid CloudWatch metrics, ~1 min granularity).
resource "aws_s3_bucket_metric" "all" {
  bucket = aws_s3_bucket.run.id
  name   = var.s3_metrics_filter_id
}

# Identical to cp/dynamo CreateTables: table <prefix>cp, pk (S) hash,
# sk (S) range, pay-per-request. manicp runs without -create-tables.
resource "aws_dynamodb_table" "cp" {
  name         = local.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  # Pre-warm the on-demand table so ~1 GiB/s of segment commits is not throttled
  # while the table doubles up from the 4,000 WRU/s default.
  warm_throughput {
    read_units_per_second  = var.ddb_warm_read_units
    write_units_per_second = var.ddb_warm_write_units
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
