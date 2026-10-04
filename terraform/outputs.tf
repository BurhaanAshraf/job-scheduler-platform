output "alb_dns_name" {
  value       = module.ecs_alb.alb_dns_name
  description = "ALB DNS name for the API"
}

output "api_ecr_url" {
  value       = module.ecr_iam.api_repo_url
  description = "ECR repository URL for API"
}

output "scheduler_ecr_url" {
  value       = module.ecr_iam.scheduler_repo_url
  description = "Scheduler ECR repository URL"
}

output "worker_ecr_url" {
  value       = module.ecr_iam.worker_repo_url
  description = "Worker ECR repository URL"
}

output "db_endpoint" {
  value       = module.rds.db_endpoint
  description = "RDS endpoint"
  sensitive   = true
}

output "redis_endpoint" {
  value       = module.elasticache.redis_endpoint
  description = "ElastiCache Redis endpoint"
  sensitive   = true
}

output "deploy_role_arn" {
  value       = module.github_oidc.deploy_role_arn
  description = "GitHub Actions OIDC deploy role ARN (put in deploy.yml)"
}

output "sns_topic_arn" {
  value       = module.ecs_alb.sns_topic_arn
  description = "SNS topic ARN for alarm notifications"
}
