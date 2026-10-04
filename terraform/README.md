# Terraform Infrastructure for Job Scheduler Platform

This directory contains the Terraform configuration to provision the complete AWS infrastructure for the Job Scheduler Platform.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        VPC (10.0.0.0/16)                     │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │   Public Subnets    │    │      Private Subnets        │ │
│  │  (10.0.1.0/24,      │    │  (10.0.11.0/24,             │ │
│  │   10.0.2.0/24)      │    │   10.0.12.0/24)             │ │
│  │                     │    │                             │ │
│  │  ALB (port 80/443)  │    │  ECS Tasks (API, Scheduler, │ │
│  │                     │    │  Worker)                    │ │
│  │                     │    │  RDS PostgreSQL             │ │
│  │                     │    │  ElastiCache Redis          │ │
│  └─────────────────────┘    └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Terraform >= 1.5
- AWS CLI configured with appropriate credentials
- AWS account with permissions for VPC, RDS, ElastiCache, ECS, ECR, IAM, ALB, Secrets Manager

### Initialize (pick ONE environment per working copy)

Backend state keys are per environment (`dev/terraform.tfstate`,
`prod/terraform.tfstate`). Never share one key between envs.

```bash
cd terraform
terraform init -reconfigure -backend-config=backend-dev.hcl   # dev
# or
terraform init -reconfigure -backend-config=backend-prod.hcl   # prod
```

### Plan

```bash
# Development
terraform plan -var-file=dev.tfvars

# Production
terraform plan -var-file=prod.tfvars
```

### Apply

```bash
# Development
terraform apply -var-file=dev.tfvars

# Production
terraform apply -var-file=prod.tfvars
```

### Required Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `db_password` | Database master password (also becomes the DSN in Secrets Manager) | Yes |

```bash
export TF_VAR_db_password='...'   # never commit this value
terraform plan -var-file=dev.tfvars
```

There is intentionally NO Redis password: ElastiCache runs without AUTH
(the Go client has no TLS support) and is isolated by security group in
private subnets. See "Secrets Management" below.

### Outputs

After apply, key outputs:
- `alb_dns_name` - ALB DNS name for API access
- `api_ecr_url`, `scheduler_ecr_url`, `worker_ecr_url` - ECR repository URLs for Docker pushes
- `db_endpoint` - RDS endpoint
- `redis_endpoint` - ElastiCache Redis endpoint

## Modules

- `networking` - VPC, subnets, IGW, route tables, security groups
- `rds` - PostgreSQL RDS instance in private subnets
- `elasticache` - Redis ElastiCache cluster in private subnets
- `ecr-iam` - ECR repositories and IAM roles
- `ecs-alb` - ECS cluster, Fargate services, ALB, autoscaling

## Cost profile (personal project, ~$60/mo dev)

Deliberately lean: ECS tasks run in **public subnets** on **Fargate Spot**,
with **no NAT gateway and no interface VPC endpoints** (those two alone would
add ~$110/mo). RDS/Redis stay in private subnets behind security groups.
First-year accounts also get the RDS `db.t4g.micro` free tier (~−$15/mo).

| Service | $/mo |
|---|---|
| ALB | ~18 |
| Fargate Spot (3× 0.25vCPU/0.5GB) | ~9 |
| ElastiCache `cache.t4g.micro` | ~16 |
| RDS `db.t4g.micro` + 20 GB | ~17 (free-tier yr 1) |
| KMS, Secrets, ECR, logs | ~3 |

## Secrets Management

The database DSN is stored in AWS Secrets Manager as a ready-to-use URL
(the app expects `JOB_SCHEDULER_DB_DSN` to be a `postgres://` URL, not JSON):

- `${environment}/job-scheduler/db-password` → `postgres://user:pass@host:5432/db?sslmode=require`

ECS task definitions reference it via `valueFrom` (secret ARN), so no
plaintext credentials appear in task definitions or Dockerfiles.

## CI/CD Integration

The GitHub Actions workflow includes a `terraform-plan` job that runs on PRs modifying `.tf` files. Requires:
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in GitHub secrets
- OIDC role for production deployments (configured separately)

## Destroy

```bash
terraform destroy -var-file=dev.tfvars
# or
terraform destroy -var-file=prod.tfvars
```

**Warning**: This will destroy ALL resources including RDS data. Ensure you have backups!