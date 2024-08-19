########################
# EXTRACT BUCKET SETUP #
########################

resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = "${var.s3_extract_bucket_name}-"

  tags = {
    Name = "Ingestion Bucket"
  }
}

##########################
# TRANSFORM BUCKET SETUP #
##########################

resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = "${var.s3_transform_bucket_name}-"
  tags = {
    Name = "Transform Bucket"
  }
}

#######################
# LAMBDA BUCKET SETUP #
#######################

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "${var.s3_code_bucket_name}-"
  tags = {
    Name = "Lambda Bucket"
  }
}
