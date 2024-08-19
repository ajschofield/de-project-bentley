### EXTRACT BUCKET SET-UP
resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = "${var.s3_extract_bucket_name}-"
}

### TRANSFORM BUCKET SET-UP
resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = "${var.s3_transform_bucket_name}-"
}

### LAMBDA BUCKET
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "${var.s3_code_bucket_name}-"
}

### LAMBDA LAYER BUCKET
resource "aws_s3_bucket" "lambda_layer_bucket" {
  bucket_prefix = "lambda-layer-dev-"
}