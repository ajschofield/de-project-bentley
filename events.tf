resource "aws_cloudwatch_event_rule" "lambda_trigger" {
  name                = "lambda-scheduled-trigger"
  description         = "Schedule to trigger the Lambda function"
  schedule_expression = "rate(30 minutes)"
  
#   event_pattern = jsonencode({
#     detail-type = [
#       "AWS Console Sign In via CloudTrail"
#     ]
#   })
}


resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.lambda_trigger.name
  target_id = "TargetFunctionV1"
  arn       = aws_lambda_function.my_lambda_function.arn
}



resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.my_lambda_function.function_name 
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_trigger.arn  
}


# below is step function 1
resource "aws_lambda_permission" "allow_s3_ingestion" {
  statement_id  = "AllowS3InvokeLambdaTransform"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_transform.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.extract.arn
}


resource "aws_s3_bucket_notification" "extract_bucket_notification" {
  bucket = aws_s3_bucket.extract.id

  lambda_function {
    events             = ["s3:ObjectCreated:*"]  
    lambda_function_arn = aws_lambda_function.lambda_transform.arn
  }

  depends_on = [aws_lambda_permission.allow_s3_ingestion]
}

# need to duplicate and replace "2" with "3"