variable "environment" {
  type        = string
  description = "Environment name"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs"
}

variable "redis_security_group_ids" {
  type        = list(string)
  description = "Redis security group IDs"
}

variable "node_type" {
  type        = string
  description = "ElastiCache node type"
}

variable "num_cache_nodes" {
  type        = number
  description = "Number of cache nodes"
}

variable "engine_version" {
  type        = string
  description = "Redis engine version"
}

variable "random_suffix" {
  type        = string
  description = "Random suffix for unique naming"
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

variable "ecs_execution_role_arn" {
  type        = string
  description = "ECS execution role ARN"
}