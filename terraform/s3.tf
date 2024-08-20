########################
# EXTRACT BUCKET SETUP #
########################

resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = "${var.s3_extract_bucket_name}-"
  force_destroy = true
  tags = {
    Name = "Ingestion Bucket"
  }
}

resource "aws_s3_bucket_versioning" "extract_bucket_versioning" {
  bucket = aws_s3_bucket.extract_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

##########################
# TRANSFORM BUCKET SETUP #
##########################

resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = "${var.s3_transform_bucket_name}-"
  force_destroy = true
  tags = {
    Name = "Transform Bucket"
  }
}


resource "aws_s3_bucket_versioning" "transform_bucket_versioning" {
  bucket = aws_s3_bucket.transform_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

#######################
# LAMBDA BUCKET SETUP #
#######################

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "${var.s3_code_bucket_name}-"
  force_destroy = true
  tags = {
    Name = "Lambda Bucket"
  }
}

resource "aws_s3_bucket_versioning" "lambda_bucket_versioning" {
  bucket = aws_s3_bucket.lambda_code_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}
