# define

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

# create

# attach