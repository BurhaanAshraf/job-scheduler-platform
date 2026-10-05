variable "environment" {
  type        = string
  description = "Environment name"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "ap-south-2"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "public_subnet_ids" {
  type        = list(string)
  description = "Public subnet IDs"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs"
}

variable "alb_security_group_id" {
  type        = string
  description = "ALB security group ID"
}

variable "ecs_task_sg_id" {
  type        = string
  description = "ECS tasks security group ID"
}

variable "db_endpoint" {
  type        = string
  description = "RDS endpoint"
}

variable "redis_endpoint" {
  type        = string
  description = "Redis endpoint"
}

variable "db_secret_arn" {
  type        = string
  description = "Secrets Manager ARN for DB password"
}

variable "api_image_uri" {
  type        = string
  description = "API Docker image URI"
}

variable "scheduler_image_uri" {
  type        = string
  description = "Scheduler Docker image URI"
}

variable "worker_image_uri" {
  type        = string
  description = "Worker Docker image URI"
}

variable "execution_role_arn" {
  type        = string
  description = "ECS execution role ARN"
}

variable "api_task_role_arn" {
  type        = string
  description = "API task role ARN"
}

variable "scheduler_task_role_arn" {
  type        = string
  description = "Scheduler task role ARN"
}

variable "worker_task_role_arn" {
  type        = string
  description = "Worker task role ARN"
}

variable "api_desired_count" {
  type        = number
  description = "API desired count"
}

variable "scheduler_desired_count" {
  type        = number
  description = "Scheduler desired count"
}

variable "worker_desired_count" {
  type        = number
  description = "Worker desired count"
}

variable "worker_min_count" {
  type        = number
  description = "Worker min count"
}

variable "worker_max_count" {
  type        = number
  description = "Worker max count"
}

variable "cpu" {
  type        = number
  description = "Fargate CPU units"
}

variable "memory" {
  type        = number
  description = "Fargate memory in MiB"
}

variable "random_suffix" {
  type        = string
  description = "Random suffix"
}

variable "acm_certificate_arn" {
  type        = string
  description = "ACM certificate ARN for HTTPS (optional)"
  default     = ""
}
variable "monthly_budget_limit_usd" {
  description = "Monthly AWS cost budget (USD) for the billing alarm"
  type        = number
  default     = 20
}
