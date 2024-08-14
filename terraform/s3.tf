### EXTRACT BUCKET SET-UP
resource "aws_s3_bucket" "extract_bucket" {
    bucket_prefix = "${var.s3_extract_bucket_name}-"
}

# resource "aws_s3_bucket_notification" "extract_bucket_notification" {
#   bucket = aws_s3_bucket.extract_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.extract_lambda.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
#   depends_on = [aws_lambda_permission.allow_to_write_to_s3_extract_bucket]
# }  # << is this the correct permission dependency?

### TRANSFORM BUCKET SET-UP
resource "aws_s3_bucket" "transform_bucket" {
    bucket_prefix = "${var.s3_transform_bucket_name}-"
}

# resource "aws_s3_bucket_notification" "transform_bucket_notification" {
#   bucket = aws_s3_bucket.transform_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.transform_lambda.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
#   depends_on = [aws_lambda_permission.allow_to_write_to_s3_transform_bucket]
# }  # << is this the correct permission dependency?


### LAMBDA BUCKET
resource "aws_s3_bucket" "lambda_code_bucket" {
    bucket_prefix = "${var.s3_code_bucket_name}-"
}

# resource "aws_s3_object" "extract_lambda_code" {
#   bucket = aws_s3_bucket.lambda_code_bucket.bucket
#   key = "${var.extract_lambda_name}/extract_function.zip"
#   source = "${path.module}/../extract_function.zip"
# } # << can't figure out how this is being used but we seem to need it

# resource "aws_s3_object" "transform_lambda_code" {
#   bucket = aws_s3_bucket.lambda_code_bucket.bucket
#   key = "${var.transform_lambda_name}/transform_function.zip"
#   source = "${path.module}/../transform_function.zip"
# } # << can't figure out how this is being used but we seem to need it

# resource "aws_s3_object" "load_lambda_code" {
#   bucket = aws_s3_bucket.lambda_code_bucket.bucket
#   key = "${var.load_lambda_name}/load_function.zip"
#   source = "${path.module}/../load_function.zip"
# }