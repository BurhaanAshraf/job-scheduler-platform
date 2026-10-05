provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "job-scheduler"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Random suffix for unique resource names
resource "random_id" "suffix" {
  byte_length = 4
}

data "aws_caller_identity" "current" {}

# S3 bucket for Terraform state (already exists)
data "aws_s3_bucket" "terraform_state" {
  bucket = "job-scheduler-terraform-state-burhaan"
}

# DynamoDB table for state locking (already exists)
data "aws_dynamodb_table" "terraform_locks" {
  name = "terraform-locks-job-scheduler"
}

module "networking" {
  source = "./modules/networking"

  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  availability_zones   = var.availability_zones
  aws_region           = var.aws_region
  random_suffix        = random_id.suffix.hex
}

module "rds" {
  source = "./modules/rds"

  environment             = var.environment
  vpc_id                  = module.networking.vpc_id
  private_subnet_ids      = module.networking.private_subnet_ids
  db_security_group_ids   = module.networking.db_security_group_ids
  db_instance_class       = var.db_instance_class
  db_allocated_storage    = var.db_allocated_storage
  db_name                 = var.db_name
  db_username             = var.db_username
  db_password             = var.db_password
  api_task_role_arn       = module.ecr_iam.api_task_role_arn
  scheduler_task_role_arn = module.ecr_iam.scheduler_task_role_arn
  worker_task_role_arn    = module.ecr_iam.worker_task_role_arn
  ecs_execution_role_arn  = module.ecr_iam.execution_role_arn
  random_suffix           = random_id.suffix.hex
}

module "elasticache" {
  source = "./modules/elasticache"

  environment              = var.environment
  vpc_id                   = module.networking.vpc_id
  private_subnet_ids       = module.networking.private_subnet_ids
  redis_security_group_ids = module.networking.redis_security_group_ids
  node_type                = var.redis_node_type
  num_cache_nodes          = var.redis_num_nodes
  engine_version           = var.redis_engine_version
  api_task_role_arn        = module.ecr_iam.api_task_role_arn
  scheduler_task_role_arn  = module.ecr_iam.scheduler_task_role_arn
  worker_task_role_arn     = module.ecr_iam.worker_task_role_arn
  ecs_execution_role_arn   = module.ecr_iam.execution_role_arn
  random_suffix            = random_id.suffix.hex
}

module "ecr_iam" {
  source = "./modules/ecr-iam"

  environment       = var.environment
  random_suffix     = random_id.suffix.hex
  rds_kms_key_arn   = module.rds.rds_kms_key_arn
  redis_kms_key_arn = module.elasticache.redis_kms_key_arn
}

module "github_oidc" {
  source = "./modules/github-oidc"

  environment                = var.environment
  github_repo                = var.github_repo
  create_oidc_provider       = true
  existing_oidc_provider_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
  ecr_repo_arns              = module.ecr_iam.ecr_repo_arns
  execution_role_arn         = module.ecr_iam.execution_role_arn
  api_task_role_arn          = module.ecr_iam.api_task_role_arn
  scheduler_task_role_arn    = module.ecr_iam.scheduler_task_role_arn
  worker_task_role_arn       = module.ecr_iam.worker_task_role_arn
}

module "ecs_alb" {
  source = "./modules/ecs-alb"

  environment              = var.environment
  vpc_id                   = module.networking.vpc_id
  public_subnet_ids        = module.networking.public_subnet_ids
  private_subnet_ids       = module.networking.private_subnet_ids
  alb_security_group_id    = module.networking.alb_security_group_id
  ecs_task_sg_id           = module.networking.ecs_task_sg_id
  db_endpoint              = module.rds.db_endpoint
  redis_endpoint           = module.elasticache.redis_endpoint
  db_secret_arn            = module.rds.db_secret_arn
  api_image_uri            = "${module.ecr_iam.api_repo_url}:${var.image_tag}"
  scheduler_image_uri      = "${module.ecr_iam.scheduler_repo_url}:${var.image_tag}"
  worker_image_uri         = "${module.ecr_iam.worker_repo_url}:${var.image_tag}"
  execution_role_arn       = module.ecr_iam.execution_role_arn
  api_task_role_arn        = module.ecr_iam.api_task_role_arn
  scheduler_task_role_arn  = module.ecr_iam.scheduler_task_role_arn
  worker_task_role_arn     = module.ecr_iam.worker_task_role_arn
  api_desired_count        = var.api_desired_count
  scheduler_desired_count  = var.scheduler_desired_count
  worker_desired_count     = var.worker_desired_count
  worker_min_count         = var.worker_min_count
  worker_max_count         = var.worker_max_count
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  random_suffix            = random_id.suffix.hex
  monthly_budget_limit_usd = var.monthly_budget_limit_usd
}