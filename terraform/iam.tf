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

# s3 setup
# allows to list and retrieve s3 buckets, and allows retention/tagging/access control settings
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    actions = [
      "s3:ListAllMyBuckets",
      "s3:GetBucketLocation"
      ]
    resources = ["arn:aws:s3:::*"]
  }
  
  statement {
    actions = [
      "s3:PutObject",
      "s3:PutObjectRetention",
      "s3:PutObjectTagging",
      "s3:PutObjectAcl"
    ]
    resources = [
      "${aws_s3_bucket.data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*"
    ]
  }
}