variable "environment" {
  type        = string
  description = "Environment name"
}

variable "github_repo" {
  type        = string
  description = "GitHub repo allowed to assume the role (OWNER/NAME)"
}

variable "create_oidc_provider" {
  type        = bool
  description = "Create the account OIDC provider (false if one already exists)"
  default     = true
}

variable "existing_oidc_provider_arn" {
  type        = string
  description = "ARN of an existing GitHub OIDC provider (used when create_oidc_provider=false)"
  default     = ""
}

variable "ecr_repo_arns" {
  type        = list(string)
  description = "ECR repository ARNs this role may push to"
}

variable "execution_role_arn" {
  type        = string
  description = "ECS execution role ARN (PassRole target)"
}

variable "api_task_role_arn" {
  type        = string
  description = "API task role ARN (PassRole target)"
}

variable "scheduler_task_role_arn" {
  type        = string
  description = "Scheduler task role ARN (PassRole target)"
}

variable "worker_task_role_arn" {
  type        = string
  description = "Worker task role ARN (PassRole target)"
}
