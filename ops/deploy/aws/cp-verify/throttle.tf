# Optional small table for throttling tests. Only made when enable_throttle_table is true.
# It has 1 read and 1 write per second, so requests above that are throttled.
# Same keys and TTL as the main table (aws_dynamodb_table.cp).

resource "aws_dynamodb_table" "throttle" {
  count = var.enable_throttle_table ? 1 : 0

  name           = "${local.table_prefix}throttle_cp"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "pk"
  range_key      = "sk"

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
