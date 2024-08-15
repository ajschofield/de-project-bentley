resource "random_string" "eventbridge_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "random_string" "s3_ingestion_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "random_string" "s3_transform_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "aws_cloudwatch_event_rule" "lambda_trigger" {
  name                = "lambda-scheduled-trigger"
  description         = "Schedule to trigger the Lambda function"
  schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "extract_lambda_cw_event" {
  rule       = aws_cloudwatch_event_rule.lambda_trigger.name
  target_id  = "TargetFunctionV1"
  arn        = aws_lambda_function.extract_lambda.arn #replaced lambda name placeholder
  depends_on = [aws_lambda_permission.allow_eventbridge]
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge${random_string.eventbridge_suffix.result}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_trigger.arn

  lifecycle {
    replace_triggered_by = [random_string.eventbridge_suffix]
  }
}

# below is step function 1
resource "aws_lambda_permission" "allow_s3_ingestion" {
  statement_id  = "AllowS3InvokeLambdaTransform${random_string.s3_ingestion_suffix.result}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.function_name #replaced lambda name placeholder
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.extract_bucket.arn #replaced bucket name placeholder

  lifecycle {
    replace_triggered_by = [random_string.s3_ingestion_suffix]
  }
}


resource "aws_s3_bucket_notification" "extract_bucket_notification" {
  bucket = aws_s3_bucket.extract_bucket.id #replaced bucket name placeholder

  lambda_function {
    events              = ["s3:ObjectCreated:*"]
    lambda_function_arn = aws_lambda_function.transform_lambda.arn #replaced lambda name placeholder
  }

  depends_on = [aws_lambda_permission.allow_s3_ingestion]
}

resource "aws_lambda_permission" "allow_s3_transform_bucket" {
  statement_id  = "AllowS3InvokeLambdaTransform${random_string.s3_transform_suffix.result}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.function_name #replaced lambda name placeholder
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.transform_bucket.arn #replaced bucket name placeholder

  lifecycle {
    replace_triggered_by = [random_string.s3_transform_suffix]
  }
}


resource "aws_s3_bucket_notification" "transform_bucket_notification" {
  bucket = aws_s3_bucket.transform_bucket.id #replaced bucket name placeholder

  lambda_function {
    events              = ["s3:ObjectCreated:*"]
    lambda_function_arn = aws_lambda_function.transform_lambda.arn #replaced lambda name placeholder
  }

  depends_on = [aws_lambda_permission.allow_s3_transform_bucket]
}