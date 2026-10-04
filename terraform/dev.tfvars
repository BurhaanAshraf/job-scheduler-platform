# Development Environment Configuration
# Run: terraform apply -var-file=dev.tfvars

aws_region  = "ap-south-2"
environment = "dev"

# Network (no NAT: tasks run in public subnets; see modules/networking)
vpc_cidr             = "10.1.0.0/16"
public_subnet_cidrs  = ["10.1.1.0/24", "10.1.2.0/24"]
private_subnet_cidrs = ["10.1.11.0/24", "10.1.12.0/24"]
availability_zones   = ["ap-south-2a", "ap-south-2b"]

# Database (smaller for dev)
db_instance_class    = "db.t4g.micro"
db_allocated_storage = 20
db_name              = "job_scheduler"
db_username          = "burhaan"
# db_password is REQUIRED: export TF_VAR_db_password before plan/apply.
# (No Redis password: ElastiCache runs without auth, SG-isolated. See README.)

# Redis (single node for dev)
redis_node_type      = "cache.t4g.micro"
redis_num_nodes      = 1
redis_engine_version = "7.1"

# Fargate (smaller for dev)
fargate_cpu    = 256
fargate_memory = 512

# Service counts
api_desired_count       = 1
scheduler_desired_count = 1
worker_desired_count    = 1
worker_min_count        = 1
worker_max_count        = 2

# Image
image_tag = "dev-latest"

# Optional: ACM certificate ARN for HTTPS
# acm_certificate_arn = "arn:aws:acm:ap-south-2:123456789012:certificate/..."

# Optional: SNS topic for alarms
# sns_topic_arn = "arn:aws:sns:ap-south-2:123456789012:job-scheduler-alarms"