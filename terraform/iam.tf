# Description: This file contains the IAM roles and policies for the lambda functions
########################################################################
# IAM MULTI-ROLE SETUP
########################################################################

# DEFINE MULTI-SERVICE ROLE (lambda, s3, cloudwatch, events)
resource "aws_iam_role" "multi_service_role" {
  name = "multi_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "scheduler.amazonaws.com"
          ]
        }
      }
    ]
  })
}


########################################################################
# S3 SETUP                                                        
# Description: allows allows retention/tagging/access control settings
# Lambda IAM Policy for S3
########################################################################

# S3 DEFINE POLICY
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectRetention",
      "s3:PutObjectTagging",
      "s3:PutObjectAcl",
      "s3:ListObjects",
      "s3:ListObjectsV2",
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.extract_bucket.arn}/*",
      "${aws_s3_bucket.transform_bucket.arn}/*",
      "${aws_s3_bucket.lambda_code_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:ListAllMyBuckets",
      "s3:ListObjectsV2",
      "s3:ListObjects"
    ]
    resources = [
      "arn:aws:s3:::*",
    ]
  }
}


########################################################################
# LAMBDA SETUP
# Description: Allows Lambda permission to write to Cloudwatch logs
########################################################################

resource "aws_iam_policy" "lambda_execution_policy" {
  name        = "lambda_execution_policy"
  path        = "/"
  description = "IAM policy for Lambda execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
          "lambda:GetFunction"
        ]
        Resource = "*"
      }
    ]
    }
  )
}

########################################################################
# CLOUDWATCH SETUP
# Description: Give permission for Lambda to write to CloudWatch logs
########################################################################

data "aws_iam_policy_document" "cw_document" {
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:CreateLogGroup",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
    ]
  }
}

resource "aws_iam_policy" "cw_policy" {
  name   = "cw_policy"
  policy = data.aws_iam_policy_document.cw_document.json
}

########################################################################
# POLICY WRITE & ATTACH
########################################################################

# S3 WRITE POLICY
resource "aws_iam_policy" "s3_write_policy" {
  policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "s3_attachment" {
  role       = aws_iam_role.multi_service_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_attachment" {
  role       = aws_iam_role.multi_service_role.name
  policy_arn = aws_iam_policy.lambda_execution_policy.arn
}

resource "aws_iam_role_policy_attachment" "cw_attachment" {
  role       = aws_iam_role.multi_service_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

###################
# EVENTS POLICIES #
###################

data "aws_iam_policy_document" "cloudwatch_events_policy" {
  statement {
    actions = [
      "events:PutRule",
      "events:PutTargets",
      "events:RemoveTargets",
      "events:DeleteRule",
      "events:PutEvents"
    ]
    resources = ["*"]
    effect    = "Allow"
  }
}

resource "aws_iam_policy" "cloudwatch_events_policy" {
  name   = "cloudwatch_events_policy"
  policy = data.aws_iam_policy_document.cloudwatch_events_policy.json
}

resource "aws_iam_role_policy_attachment" "cloudwatch_events_attachment" {
  role       = aws_iam_role.multi_service_role.name
  policy_arn = aws_iam_policy.cloudwatch_events_policy.arn
}

#########################
# SECRETS MANAGER SETUP #
#########################

# Policy Doc
data "aws_iam_policy_document" "secrets_manager_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = ["arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:bentley-secrets-Na0yc8"]
  }
}

# SM Policy Resource
resource "aws_iam_policy" "secrets_manager_policy" {
  name   = "secrets_manager_policy"
  policy = data.aws_iam_policy_document.secrets_manager_policy_doc.json
}

# Attach SM Policy to Role
resource "aws_iam_role_policy_attachment" "secrets_manager_attachment" {
  role       = aws_iam_role.multi_service_role.name
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
}
