variable "environment" {
  type        = string
  description = "Environment name"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "ap-south-2"
}

variable "random_suffix" {
  type        = string
  description = "Random suffix for unique naming"
}

variable "rds_kms_key_arn" {
  type        = string
  description = "RDS KMS key ARN"
}

variable "redis_kms_key_arn" {
  type        = string
  description = "Redis KMS key ARN"
}