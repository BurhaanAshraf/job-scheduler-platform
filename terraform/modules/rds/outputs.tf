output "db_endpoint" {
  value       = aws_db_instance.main.endpoint
  description = "RDS endpoint"
  sensitive   = true
}

output "db_port" {
  value       = aws_db_instance.main.port
  description = "RDS port"
}

output "db_instance_id" {
  value       = aws_db_instance.main.id
  description = "RDS instance identifier"
}

output "db_secret_arn" {
  value       = aws_secretsmanager_secret.db_password.arn
  description = "Secrets Manager ARN for DB password"
}

output "db_secret_name" {
  value       = aws_secretsmanager_secret.db_password.name
  description = "Secrets Manager secret name"
}

output "rds_kms_key_arn" {
  value       = aws_kms_key.rds.arn
  description = "RDS KMS key ARN"
}