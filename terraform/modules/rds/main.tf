# DB Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "${var.environment}-db-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name        = "${var.environment}-db-subnet-group"
    Environment = var.environment
  }
}

# KMS Key for RDS encryption
resource "aws_kms_key" "rds" {
  description             = "KMS key for RDS encryption"
  deletion_window_in_days = 10
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Allow administration of the key"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = [
          "kms:*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow RDS to use the key"
        Effect = "Allow"
        Principal = {
          Service = "rds.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow ECS tasks to decrypt secrets"
        Effect = "Allow"
        Principal = {
          AWS = [
            var.api_task_role_arn,
            var.scheduler_task_role_arn,
            var.worker_task_role_arn,
            var.ecs_execution_role_arn
          ]
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-rds-key"
    Environment = var.environment
  }
}

data "aws_caller_identity" "current" {}

# DB Parameter Group (PostgreSQL 16 to match local compose stack)
resource "aws_db_parameter_group" "main" {
  name        = "${var.environment}-pg16-params"
  family      = "postgres16"
  description = "Parameter group for PostgreSQL 16"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  tags = {
    Name        = "${var.environment}-pg-params"
    Environment = var.environment
  }
}

# RDS Instance
resource "aws_db_instance" "main" {
  identifier                  = "${var.environment}-job-scheduler-db"
  engine                      = "postgres"
  engine_version              = "16.15"
  instance_class              = var.db_instance_class
  allocated_storage           = var.db_allocated_storage
  max_allocated_storage       = 100
  storage_encrypted           = true
  kms_key_id                  = aws_kms_key.rds.arn
  db_name                     = var.db_name
  username                    = var.db_username
  password                    = var.db_password
  db_subnet_group_name        = aws_db_subnet_group.main.name
  vpc_security_group_ids      = var.db_security_group_ids
  parameter_group_name        = aws_db_parameter_group.main.name
  allow_major_version_upgrade = true
  # Dev applies immediately; prod waits for the maintenance window.
  apply_immediately = var.environment == "prod" ? false : true

  backup_retention_period = var.environment == "dev" ? 1 : 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  skip_final_snapshot          = var.environment == "dev"
  deletion_protection          = var.environment == "prod"
  copy_tags_to_snapshot        = true
  performance_insights_enabled = var.environment == "prod"
  monitoring_interval          = 60
  monitoring_role_arn          = aws_iam_role.rds_monitoring.arn

  tags = {
    Name        = "${var.environment}-job-scheduler-db"
    Environment = var.environment
  }
}

# Secrets Manager secret for the DB connection.
# Stored as a ready-to-use postgres DSN because the app (internal/config)
# expects JOB_SCHEDULER_DB_DSN to be a URL, not a JSON document.
resource "aws_secretsmanager_secret" "db_password" {
  name        = "${var.environment}/job-scheduler/db-password"
  description = "Postgres DSN for Job Scheduler (JOB_SCHEDULER_DB_DSN)"
  kms_key_id  = aws_kms_key.rds.arn

  tags = {
    Name        = "${var.environment}-db-password"
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id = aws_secretsmanager_secret.db_password.id
  # NOTE: use .address (bare hostname), not .endpoint (hostname:port).
  secret_string = "postgres://${var.db_username}:${urlencode(var.db_password)}@${aws_db_instance.main.address}:5432/${var.db_name}?sslmode=require"
}

# CloudWatch Log Group for RDS
resource "aws_cloudwatch_log_group" "rds" {
  name              = "/aws/rds/${var.environment}/job-scheduler/postgresql"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = {
    Name        = "${var.environment}-rds-logs"
    Environment = var.environment
  }
}

# IAM role for RDS enhanced monitoring
resource "aws_iam_role" "rds_monitoring" {
  name = "${var.environment}-rds-monitoring-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "monitoring.rds.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}