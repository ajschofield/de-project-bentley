resource "aws_s3_bucket" "extract_bucket" {
    bucket = "${var.s3_extract_bucket_name}"
}

resource "aws_s3_bucket" "transform_bucket" {
    bucket = "${var.s3_transform_bucket_name}"
}

resource "aws_s3_bucket" "lambda_bucket" {
    bucket = "${var.s3_code_bucket_name}"
}

resource "aws_s3_object" "extract_lambda_code" {
  bucket = aws_s3_bucket.s3_code_bucket_name.bucket
  key = "${var.extract_lambda_name}/function_e.zip"
  source = "${path.module}/../function_e.zip"
}