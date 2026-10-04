output "alb_dns_name" {
  value       = aws_lb.main.dns_name
  description = "ALB DNS name"
}

output "alb_arn" {
  value       = aws_lb.main.arn
  description = "ALB ARN"
}

output "cluster_id" {
  value       = aws_ecs_cluster.main.id
  description = "ECS cluster ID"
}

output "api_task_definition_arn" {
  value       = aws_ecs_task_definition.api.arn
  description = "API task definition ARN"
}

output "scheduler_task_definition_arn" {
  value       = aws_ecs_task_definition.scheduler.arn
  description = "Scheduler task definition ARN"
}

output "worker_task_definition_arn" {
  value       = aws_ecs_task_definition.worker.arn
  description = "Worker task definition ARN"
}

output "sns_topic_arn" {
  value       = aws_sns_topic.alarms.arn
  description = "SNS topic ARN for alarm notifications"
}

output "cluster_arn" {
  value       = aws_ecs_cluster.main.arn
  description = "ECS cluster ARN"
}