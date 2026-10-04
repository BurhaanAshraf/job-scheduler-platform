variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-2"
}

variable "github_repo" {
  description = "GitHub repo (OWNER/NAME) allowed to assume the deploy role via OIDC"
  type        = string
  default     = "BurhaanAshraf/job-scheduler-platform"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24"]
}

variable "availability_zones" {
  description = "Availability zones to use"
  type        = list(string)
  default     = ["ap-south-2a", "ap-south-2b"]
}

# RDS variables
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "job_scheduler"
}

variable "db_username" {
  description = "Database master username"
  type        = string
  default     = "burhaan"
}

variable "db_password" {
  description = "Database master password (stored in Secrets Manager)"
  type        = string
  sensitive   = true
}

# ElastiCache variables
variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.t4g.micro"
}

variable "redis_num_nodes" {
  description = "Number of cache nodes (1 for single, 2+ for cluster)"
  type        = number
  default     = 1
}

variable "redis_engine_version" {
  description = "Redis engine version"
  type        = string
  default     = "7.1"
}

# ECS/Fargate variables
variable "fargate_cpu" {
  description = "CPU units for Fargate tasks (256 = 0.25 vCPU)"
  type        = number
  default     = 256
}

variable "fargate_memory" {
  description = "Memory in MiB for Fargate tasks"
  type        = number
  default     = 512
}

variable "image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}

variable "api_desired_count" {
  description = "Desired count for API service"
  type        = number
  default     = 2
}

variable "scheduler_desired_count" {
  description = "Desired count for Scheduler service"
  type        = number
  default     = 1
}

variable "worker_desired_count" {
  description = "Initial desired count for Worker service"
  type        = number
  default     = 2
}

variable "worker_min_count" {
  description = "Minimum worker count for autoscaling"
  type        = number
  default     = 2
}

variable "worker_max_count" {
  description = "Maximum worker count for autoscaling"
  type        = number
  default     = 6
}