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

locals {
  layer_dir    = "${path.module}/.."
  requirements = "${path.module}/../requirements.txt"
  layer_zip    = "${path.module}/../layer.zip"
}

resource "null_resource" "prepare_layer" {
  triggers = {
    requirements_hash = filesha1(local.requirements)
  }
  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ${local.layer_dir}/python/lib/python3.11/site-packages/
      pip install -r ${local.requirements} -t ${local.layer_dir}/python/lib/python3.11/site-packages/
      cd ${local.layer_dir} && zip -r ${local.layer_zip} .
    EOT
  }
}

resource "aws_s3_object" "layer_zip" {
  bucket     = aws_s3_bucket.lambda_code_bucket.bucket
  key        = "layer.zip"
  source     = local.layer_zip
  depends_on = [null_resource.prepare_layer]
}

resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name          = "lambda_layer"
  compatible_runtimes = ["python3.11"]
  s3_bucket           = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key              = aws_s3_object.layer_zip.key
  skip_destroy        = true
  depends_on          = [aws_s3_object.layer_zip]
}
