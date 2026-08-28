## ENVIRONMENT CONFIGURATION
variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Used as the prefix in infrastructure resources"
  type        = string
  default     = "ms-hasbro-dpe-interview"
}

## DATABASE CREDENTIALS
variable "db_password" {
  description = "User Password for accessing the RDS PostgreSQL database"
  type        = string
  sensitive   = true
}