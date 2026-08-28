terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for CSV Data Storage
## force_destroy argument allows the 'terraform destroy' action to remove bucket even if populated
### NOTE: S3 bucket names have to be globally unique, so the bucket name may need updated
resource "aws_s3_bucket" "csv_storage" {
  bucket        = "${var.project_name}-bucket-182484"
  force_destroy = true
}

# Security Group to allow PostgreSQL inbound access
resource "aws_security_group" "rds_sg" {
  name        = "${var.project_name}-rds-sg"
  description = "Allow inbound PostgreSQL access"

# Since testing IPs aren't known, we will leave all IPs open for communication with the DB server
# In Prod, that would be replaced with an internal subset to allow only specific internall traffic
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Defining RDS PostgreSQL Instance
## Storage is in GiB
## Uses t3.micro, a free-teir elligible DB in AWS
## publicly_accessible allows CLI/local scripts to access instance
## skip_final_snapshot speeds up deletion in 'terraform destory' step
resource "aws_db_instance" "postgres_db" {
  allocated_storage      = 20
  max_allocated_storage  = 100
  engine                 = "postgres"
  engine_version         = "15"
  instance_class         = "db.t3.micro"
  db_name                = "etldb"
  username               = "dbadmin"
  password               = var.db_password
  skip_final_snapshot    = true
  publicly_accessible    = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
}