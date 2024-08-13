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
            "states.amazonaws.com",
            "events.amazonaws.com",
            "s3.amazonaws.com"
          ]
        }
      }
    ]
  })
}



########################################################################
# S3 SETUP                                                        
# Description: allows allows retention/tagging/access control settings
# Lambda IAM Policy for S3 Write
########################################################################

# S3 DEFINE POLICY
resource "aws_iam_policy" "s3_access_policy" {
  name        = "s3_access_policy"
  path        = "/"
  description = "IAM policy for S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        resources = [
          "${aws_s3_bucket.extract_bucket.arn}/*",
          "${aws_s3_bucket.transform_bucket.arn}/*",
          "${aws_s3_bucket.lambda_bucket.arn}/*"
          ]
        }
      ] 
    }
  )
}

# S3 WRITE POLICY
resource "aws_iam_policy" "s3_write_policy" {
  policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# S3 ATTACH POLICY
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

########################################################################
# LAMBDA SETUP
# Description: Allows Lambda permission to write to Cloudwatch logs
########################################################################



# Uses Iam policy document to assume role for lambda functions
resource "aws_iam_role" "lambda_role" {
  assume_role_policy = data.aws_iam_policy_document.bentley_service_role.json
}