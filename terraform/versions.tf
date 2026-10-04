terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  # Partial S3 backend: bucket/region/lock table here, state `key` per env.
  #   terraform init -reconfigure -backend-config=backend-dev.hcl
  #   terraform init -reconfigure -backend-config=backend-prod.hcl
  # Never share one key between envs: dev and prod must not fight over state.
  backend "s3" {
    bucket         = "job-scheduler-terraform-state-burhaan"
    region         = "ap-south-2"
    encrypt        = true
    dynamodb_table = "terraform-locks-job-scheduler"
  }
}
