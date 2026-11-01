# IAM for the test host (see ec2.tf).
# The host can only use the one control plane table, the throttle table when it
# exists, and the checkpoint bucket. Nothing else in the account.

data "aws_iam_policy_document" "test_host_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "test_host" {
  name               = "mds-cpverify-${var.run_id}-host"
  assume_role_policy = data.aws_iam_policy_document.test_host_assume.json
}

# Lets SSM Session Manager connect to the host. This is the only way in.
resource "aws_iam_role_policy_attachment" "test_host_ssm" {
  role       = aws_iam_role.test_host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# The throttle table is only in the list when it exists (count is 0 or 1).
locals {
  test_host_table_arns = concat(
    [aws_dynamodb_table.cp.arn],
    aws_dynamodb_table.throttle[*].arn,
  )
}

data "aws_iam_policy_document" "test_host_access" {
  statement {
    sid = "ControlPlaneTables"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:DescribeTable",
    ]
    resources = local.test_host_table_arns
  }

  # Objects in the checkpoint bucket.
  statement {
    sid = "CheckpointObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.checkpoints.arn}/*"]
  }

  # Listing needs the bucket ARN itself, not the objects.
  statement {
    sid       = "CheckpointBucketList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.checkpoints.arn]
  }
}

resource "aws_iam_role_policy" "test_host_access" {
  name   = "cp-verify-access"
  role   = aws_iam_role.test_host.id
  policy = data.aws_iam_policy_document.test_host_access.json
}

# The instance profile is how EC2 gives the role to the instance.
resource "aws_iam_instance_profile" "test_host" {
  name = "mds-cpverify-${var.run_id}-host"
  role = aws_iam_role.test_host.name
}
