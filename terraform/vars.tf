variable "s3_extract_bucket_name" {
  type    = string
  default = "extract-bucket"
}

variable "s3_transform_bucket_name" {
  type    = string
  default = "transform-bucket"
}

variable "s3_code_bucket_name" {
  type    = string
  default = "lambda-bucket"
}

variable "extract_lambda_name" {
  type    = string
  default = "extract-lambda"
}

variable "transform_lambda_name" {
  type    = string
  default = "transform-lambda"
}

variable "load_lambda_name" {
  type    = string
  default = "load-lambda"
}

variable "project_name" {
  type    = string
  default = "tt"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "github_repo" {
  type    = string
  default = "de-project-bentley"
}

variable "team_name" {
  type    = string
  default = "Team-Bentley"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
