resource "aws_iam_role" "bentley_service_role" {
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com",
                        "s3.amazonaws.com",
                        "cloudwatch.amazonaws.com",
                        "events.amazonaws.com",
                    ]
                }
            }
        ]
    }
    EOF
}

# lambda setup
resource "aws_iam_role" "lambda_role" {
  assume_role_policy = data.aws_iam_policy_document.bentley_service_role.json
}


# s3 setup
# allows allows retention/tagging/access control settings
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:PutObjectRetention",
      "s3:PutObjectTagging",
      "s3:PutObjectAcl"
    ]
    resources = [
      "${aws_s3_bucket.extract_bucket.arn}/*",
      "${aws_s3_bucket.transform_bucket.arn}/*",
      "${aws_s3_bucket.lambda_bucket.arn}/*",
    ]
  }
}

# write policy
resource "aws_iam_policy" "s3_write_policy" {
    policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# attach policy to role
resource "aws_iam_role_policy_attachment" "s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_write_policy.arn
}

# lambda setup
