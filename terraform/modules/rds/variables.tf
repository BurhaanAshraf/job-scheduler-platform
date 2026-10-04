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

variable "db_security_group_ids" {
  type        = list(string)
  description = "DB security group IDs"
}

variable "db_instance_class" {
  type        = string
  description = "RDS instance class"
}

variable "db_allocated_storage" {
  type        = number
  description = "Allocated storage in GB"
}

variable "db_name" {
  type        = string
  description = "Database name"
}

variable "db_username" {
  type        = string
  description = "Master username"
}

variable "db_password" {
  type        = string
  description = "Master password"
  sensitive   = true
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