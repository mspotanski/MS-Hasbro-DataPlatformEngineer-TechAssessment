output "rds_endpoint" {
  value       = split(":", aws_db_instance.postgres_db.endpoint)[0]
  description = "Host endpoint for RDS"
}

output "rds_dbname" {
  value       = aws_db_instance.postgres_db.db_name
  description = "Database name"
}

output "rds_username" {
  value       = aws_db_instance.postgres_db.username
  description = "Database master username"
}