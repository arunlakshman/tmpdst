data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

locals {
  cw_policy_file = "${path.module}/../observability/iam-cloudwatch-read.json"
  cw_policy_json = fileexists(local.cw_policy_file) ? file(local.cw_policy_file) : var.cloudwatch_read_policy_json
}

data "aws_iam_policy_document" "ssm_read" {
  statement {
    sid       = "ReadParams"
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = ["${local.ssm_arn_prefix}*"]
  }

  statement {
    sid       = "DecryptSecureStrings"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:${var.region}:${data.aws_caller_identity.current.account_id}:key/*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
}

# ------------------------------------------------------ control-plane host

resource "aws_iam_role" "cp" {
  name_prefix        = "${local.name}-cp-"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy_attachment" "cp_ssm_core" {
  role       = aws_iam_role.cp.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Least privilege: the table, artifacts read, results write (run manifest + metrics snapshots), and the checkpoint
# objects of the log store (cp/ in the run bucket). No segment data access.
data "aws_iam_policy_document" "cp" {
  statement {
    sid       = "BucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.run.arn]
  }

  statement {
    sid       = "ReadArtifacts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.run.arn}/artifacts/*"]
  }

  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.run.arn}/results/*"]
  }

  statement {
    sid       = "LogStoreCheckpoints"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.run.arn}/cp/*"]
  }

  statement {
    sid = "ControlPlaneTable"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:Query",
      "dynamodb:DescribeTable",
    ]
    resources = [aws_dynamodb_table.cp.arn]
  }
}

resource "aws_iam_role_policy" "cp" {
  name   = "cp-data"
  role   = aws_iam_role.cp.id
  policy = data.aws_iam_policy_document.cp.json
}

resource "aws_iam_role_policy" "cp_ssm_read" {
  name   = "ssm-read"
  role   = aws_iam_role.cp.id
  policy = data.aws_iam_policy_document.ssm_read.json
}

# Alloy on the CP host scrapes CloudWatch (DynamoDB table metrics) with the same read policy as the brokers.
resource "aws_iam_role_policy" "cp_cloudwatch_read" {
  count  = local.cw_policy_json == null ? 0 : 1
  name   = "cloudwatch-read"
  role   = aws_iam_role.cp.id
  policy = local.cw_policy_json
}

resource "aws_iam_instance_profile" "cp" {
  name_prefix = "${local.name}-cp-"
  role        = aws_iam_role.cp.name
}

# ------------------------------------------------------------ broker host

resource "aws_iam_role" "broker" {
  name_prefix        = "${local.name}-broker-"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy_attachment" "broker_ssm_core" {
  role       = aws_iam_role.broker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "broker" {
  statement {
    sid       = "BucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.run.arn]
  }

  statement {
    sid = "ObjectRW"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.run.arn}/*"]
  }
}

resource "aws_iam_role_policy" "broker" {
  name   = "broker-data"
  role   = aws_iam_role.broker.id
  policy = data.aws_iam_policy_document.broker.json
}

resource "aws_iam_role_policy" "broker_ssm_read" {
  name   = "ssm-read"
  role   = aws_iam_role.broker.id
  policy = data.aws_iam_policy_document.ssm_read.json
}

resource "aws_iam_role_policy" "broker_cloudwatch_read" {
  count  = local.cw_policy_json == null ? 0 : 1
  name   = "cloudwatch-read"
  role   = aws_iam_role.broker.id
  policy = local.cw_policy_json
}

resource "aws_iam_instance_profile" "broker" {
  name_prefix = "${local.name}-broker-"
  role        = aws_iam_role.broker.name
}

# ----------------------------------------------------------- loadgen host

resource "aws_iam_role" "loadgen" {
  name_prefix        = "${local.name}-loadgen-"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy_attachment" "loadgen_ssm_core" {
  role       = aws_iam_role.loadgen.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "loadgen" {
  statement {
    sid       = "ListArtifactsAndResults"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.run.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["artifacts/*", "artifacts", "results/*"]
    }
  }

  statement {
    sid       = "ReadArtifacts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.run.arn}/artifacts/*"]
  }

  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.run.arn}/results/*"]
  }
}

resource "aws_iam_role_policy" "loadgen" {
  name   = "loadgen-data"
  role   = aws_iam_role.loadgen.id
  policy = data.aws_iam_policy_document.loadgen.json
}

resource "aws_iam_role_policy" "loadgen_ssm_read" {
  name   = "ssm-read"
  role   = aws_iam_role.loadgen.id
  policy = data.aws_iam_policy_document.ssm_read.json
}

resource "aws_iam_instance_profile" "loadgen" {
  name_prefix = "${local.name}-loadgen-"
  role        = aws_iam_role.loadgen.name
}
