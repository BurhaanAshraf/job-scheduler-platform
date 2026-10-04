output "api_repo_url" {
  value       = aws_ecr_repository.api.repository_url
  description = "API ECR repository URL"
}

output "scheduler_repo_url" {
  value       = aws_ecr_repository.scheduler.repository_url
  description = "Scheduler ECR repository URL"
}

output "worker_repo_url" {
  value       = aws_ecr_repository.worker.repository_url
  description = "Worker ECR repository URL"
}

output "execution_role_arn" {
  value       = aws_iam_role.ecs_execution.arn
  description = "ECS execution role ARN"
}

output "api_task_role_arn" {
  value       = aws_iam_role.api_task.arn
  description = "API task role ARN"
}

output "scheduler_task_role_arn" {
  value       = aws_iam_role.scheduler_task.arn
  description = "Scheduler task role ARN"
}

output "worker_task_role_arn" {
  value       = aws_iam_role.worker_task.arn
  description = "Worker task role ARN"
}

output "ecr_repo_arns" {
  value = [
    aws_ecr_repository.api.arn,
    aws_ecr_repository.scheduler.arn,
    aws_ecr_repository.worker.arn
  ]
  description = "ECR repository ARNs (for deploy-role scoping)"
}