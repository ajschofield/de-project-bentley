terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~>3.2.2"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~>2.5.0"
    }
  }
  backend "s3" {
    bucket = "bentley-project-secrets"
    key    = "bentley-project/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      GitHubRepo  = var.github_repo
      Team        = var.team_name
    }
  }
}
