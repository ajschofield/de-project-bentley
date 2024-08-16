# data "aws_availability_zones" "available" {}

# module "vpc" {
#   source  = "terraform-aws-modules/vpc/aws"
#   version = "5.12.1"

#   name                 = var.project_name
#   cidr                 = "10.0.0.0/16"
#   azs                  = data.aws_availability_zones.available.names
#   public_subnets       = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]
#   enable_dns_hostnames = true
#   enable_dns_support   = true
# }

# resource "aws_db_subnet_group" "Terrific-Totes-sub-gr" {
#   name       = "tt-db-subnet"
#   subnet_ids = module.vpc.public_subnets

#   tags = {
#     Name = "${var.project_name}"
#   }
# }

# resource "aws_security_group" "rds" {
#   name   = "${var.project_name}-rds"
#   vpc_id = module.vpc.vpc_id

#   ingress {
#     from_port   = 5432
#     to_port     = 5432
#     protocol    = "tcp"
#     cidr_blocks = ["0.0.0.0/0"]
#   }

#   egress {
#     from_port   = 5432
#     to_port     = 5432
#     protocol    = "tcp"
#     cidr_blocks = ["0.0.0.0/0"]
#   }

#   tags = {
#     Name = "${var.project_name}-rds"
#   }
# }

# resource "aws_db_parameter_group" "Terrific-Totes-param-gr" {
#   name   = "tt-db-param"
#   family = "postgres14"

#   parameter {
#     name  = "log_connections"
#     value = "1"
#   }
# }

# resource "aws_db_instance" "terrific-totes-rds" {
#   db_name           = var.project_name
#   instance_class    = "db.t3.micro"
#   allocated_storage = 5
#   engine            = "postgres"
#   engine_version    = "14.10"
#   username          = ""
#   password          = ""
#   db_subnet_group_name   = aws_db_subnet_group.Terrific-Totes-sub-gr.name
#   vpc_security_group_ids = [aws_security_group.rds.id]
#   parameter_group_name   = aws_db_parameter_group.Terrific-Totes-param-gr.name
#   publicly_accessible    = false
#   skip_final_snapshot    = true
# }
