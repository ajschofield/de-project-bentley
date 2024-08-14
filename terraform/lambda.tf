### EXTRACT LAMBDA SET UP
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  output_path = "${path.module}/../extract_function.zip"
}

resource "aws_lambda_function" "extract_lambda" {
    function_name = "${var.extract_lambda_name}"
    s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
    s3_key = "extract-lambda/extract_function.zip"
    role = aws_iam_role.multi_service_role.arn #<< lambda role placehodler
    handler = "extract_lambda.extract" # << check that the function is called lambda handler
    runtime = "python3.11"
    environment {
        variables = {
            output = aws_s3_bucket.extract_bucket.bucket
        }
    }
}

resource "aws_lambda_permission" "allow_to_write_to_s3_extract_bucket" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.extract_bucket.arn
}


### TRANSFORM LAMBDA SET UP
data "archive_file" "transform_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda.py"
  output_path = "${path.module}/../transform_function.zip"
}

resource "aws_lambda_function" "transform_lambda" {
    function_name = "${var.transform_lambda_name}"
    s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
    s3_key = "transform-lambda/transform_function.zip"
    role = aws_iam_role.multi_service_role.arn # << lambda role placehodler
    handler = "transform_lambda.lambda_handler" # << check that the function is called lambda handler
    runtime = "python3.11"
    environment {
        variables = {
            output = aws_s3_bucket.transform_bucket.bucket
        }
    }
}

resource "aws_lambda_permission" "allow_to_write_to_s3_transform_bucket" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.transform_bucket.arn
}

### LOAD LAMBDA SET UP
data "archive_file" "load_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/load_lambda.py"
  output_path = "${path.module}/../load_function.zip"
}

resource "aws_lambda_function" "load_lambda" {
    function_name = "${var.load_lambda_name}"
    s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
    s3_key = "load-lambda/load_function.zip"
    role = aws_iam_role.multi_service_role.arn # << lambda role placehodler
    handler = "load_lambda.lambda_handler" # << check that the function is called lambda handler
    runtime = "python3.11"
}

