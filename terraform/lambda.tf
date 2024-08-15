# Extract Lambda Function
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  output_path = "${path.module}/../extract_function.zip"
}
resource "aws_s3_object" "extract_lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "${var.extract_lambda_name}/extract_function.zip"
  source = data.archive_file.extract_lambda_zip.output_path
  etag   = filemd5(data.archive_file.extract_lambda_zip.output_path)
}

resource "aws_lambda_function" "extract_lambda" {
  function_name = var.extract_lambda_name
  s3_bucket     = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key        = aws_s3_object.extract_lambda_code.key
  role          = aws_iam_role.multi_service_role.arn
  handler       = "extract_lambda.extract"
  runtime       = "python3.11"

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_s3_object.extract_lambda_code]
}

# Transform Lambda Function
data "archive_file" "transform_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda.py"
  output_path = "${path.module}/../transform_function.zip"
}
resource "aws_s3_object" "transform_lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "${var.transform_lambda_name}/transform_function.zip"
  source = data.archive_file.transform_lambda_zip.output_path
  etag   = filemd5(data.archive_file.transform_lambda_zip.output_path)
}

resource "aws_lambda_function" "transform_lambda" {
  function_name = var.transform_lambda_name
  s3_bucket     = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key        = aws_s3_object.transform_lambda_code.key
  role          = aws_iam_role.multi_service_role.arn
  handler       = "transform_lambda.transform"
  runtime       = "python3.11"

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_s3_object.transform_lambda_code]
}

# Load Lambda Function
data "archive_file" "load_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/load_lambda.py"
  output_path = "${path.module}/../load_function.zip"
}
resource "aws_s3_object" "load_lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "${var.load_lambda_name}/load_function.zip"
  source = data.archive_file.load_lambda_zip.output_path
  etag   = filemd5(data.archive_file.load_lambda_zip.output_path)
}

resource "aws_lambda_function" "load_lambda" {
  function_name = var.load_lambda_name
  s3_bucket     = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key        = aws_s3_object.load_lambda_code.key
  role          = aws_iam_role.multi_service_role.arn
  handler       = "load_lambda.load"
  runtime       = "python3.11"

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_s3_object.load_lambda_code]
}
