# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.environment}-job-scheduler"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name        = "${var.environment}-job-scheduler"
    Environment = var.environment
  }
}

# CloudWatch Log Groups for each service
resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${var.environment}-job-scheduler-api"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = {
    Name        = "${var.environment}-api-logs"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "scheduler" {
  name              = "/ecs/${var.environment}-job-scheduler-scheduler"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = {
    Name        = "${var.environment}-scheduler-logs"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/ecs/${var.environment}-job-scheduler-worker"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = {
    Name        = "${var.environment}-worker-logs"
    Environment = var.environment
  }
}

# ALB
resource "aws_lb" "main" {
  name               = "${var.environment}-job-scheduler-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [var.alb_security_group_id]
  subnets            = var.public_subnet_ids

  enable_deletion_protection = var.environment == "prod"

  tags = {
    Name        = "${var.environment}-job-scheduler-alb"
    Environment = var.environment
  }
}

# Target Group for API
resource "aws_lb_target_group" "api" {
  name        = "${var.environment}-api-tg"
  port        = 4000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/healthz"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
    matcher             = "200"
  }

  tags = {
    Name        = "${var.environment}-api-tg"
    Environment = var.environment
  }
}

# ALB Listener (HTTP -> HTTPS redirect if cert provided, else just HTTP)
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ALB Listener for HTTPS (if ACM cert provided)
resource "aws_lb_listener" "https" {
  count = var.acm_certificate_arn != "" ? 1 : 0

  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# API Task Definition
resource "aws_ecs_task_definition" "api" {
  family                   = "${var.environment}-job-scheduler-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.api_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "api"
      image     = var.api_image_uri
      essential = true
      portMappings = [{
        containerPort = 4000
        protocol      = "tcp"
      }]
      environment = [
        # NOTE: redis_endpoint output is a bare hostname; the app requires host:port.
        { name = "REDIS_ADDR", value = "${var.redis_endpoint}:6379" },
        { name = "API_PORT", value = "4000" },
        { name = "LOG_LEVEL", value = "info" },
        { name = "DB_MAX_CONNS", value = "10" },
        { name = "SCHEDULER_POLL_INTERVAL", value = "500ms" }
      ]
      secrets = [
        { name = "JOB_SCHEDULER_DB_DSN", valueFrom = var.db_secret_arn }
      ]
      healthCheck = {
        command     = ["/api", "-healthcheck"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 30
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.environment}-job-scheduler-api"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "api"
        }
      }
    }
  ])

  tags = {
    Name        = "${var.environment}-api-task-def"
    Environment = var.environment
  }
}

# Scheduler Task Definition
resource "aws_ecs_task_definition" "scheduler" {
  family                   = "${var.environment}-job-scheduler-scheduler"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.scheduler_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "scheduler"
      image     = var.scheduler_image_uri
      essential = true
      environment = [
        # NOTE: redis_endpoint output is a bare hostname; the app requires host:port.
        { name = "REDIS_ADDR", value = "${var.redis_endpoint}:6379" },
        { name = "API_PORT", value = "4000" },
        { name = "LOG_LEVEL", value = "info" },
        { name = "DB_MAX_CONNS", value = "10" },
        { name = "SCHEDULER_POLL_INTERVAL", value = "500ms" },
        { name = "SCHEDULER_INSTANCE_ID", value = "${var.environment}-scheduler-${var.random_suffix}" }
      ]
      secrets = [
        { name = "JOB_SCHEDULER_DB_DSN", valueFrom = var.db_secret_arn }
      ]
      healthCheck = {
        command     = ["/scheduler", "-healthcheck"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 30
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.environment}-job-scheduler-scheduler"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "scheduler"
        }
      }
    }
  ])

  tags = {
    Name        = "${var.environment}-scheduler-task-def"
    Environment = var.environment
  }
}

# Worker Task Definition
resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.environment}-job-scheduler-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.worker_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "worker"
      image     = var.worker_image_uri
      essential = true
      environment = [
        # NOTE: redis_endpoint output is a bare hostname; the app requires host:port.
        { name = "REDIS_ADDR", value = "${var.redis_endpoint}:6379" },
        { name = "API_PORT", value = "4000" },
        { name = "LOG_LEVEL", value = "info" },
        { name = "DB_MAX_CONNS", value = "10" },
        { name = "SCHEDULER_POLL_INTERVAL", value = "500ms" }
      ]
      secrets = [
        { name = "JOB_SCHEDULER_DB_DSN", valueFrom = var.db_secret_arn }
      ]
      healthCheck = {
        command     = ["/worker", "-healthcheck"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 30
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.environment}-job-scheduler-worker"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "worker"
        }
      }
    }
  ])

  tags = {
    Name        = "${var.environment}-worker-task-def"
    Environment = var.environment
  }
}

# API Service
# Cost-lean: PUBLIC subnets + public IPs (no NAT bill) on Fargate Spot (~70%
# cheaper). RDS/Redis stay in private subnets, reachable only from the task SG.
resource "aws_ecs_service" "api" {
  name            = "${var.environment}-job-scheduler-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [var.ecs_task_sg_id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 4000
  }

  deployment_controller {
    type = "ECS"
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  tags = {
    Name        = "${var.environment}-api-service"
    Environment = var.environment
  }
}

# Scheduler Service (single instance for leader election)
resource "aws_ecs_service" "scheduler" {
  name            = "${var.environment}-job-scheduler-scheduler"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.scheduler.arn
  desired_count   = var.scheduler_desired_count
  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [var.ecs_task_sg_id]
    assign_public_ip = true
  }

  deployment_controller {
    type = "ECS"
  }

  tags = {
    Name        = "${var.environment}-scheduler-service"
    Environment = var.environment
  }
}

# Worker Service
resource "aws_ecs_service" "worker" {
  name            = "${var.environment}-job-scheduler-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [var.ecs_task_sg_id]
    assign_public_ip = true
  }

  deployment_controller {
    type = "ECS"
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  tags = {
    Name        = "${var.environment}-worker-service"
    Environment = var.environment
  }
}

# Worker Auto Scaling
resource "aws_appautoscaling_target" "worker" {
  max_capacity       = var.worker_max_count
  min_capacity       = var.worker_min_count
  resource_id        = "service/${aws_ecs_cluster.main.id}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "worker_scale_up" {
  name               = "${var.environment}-worker-scale-up"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  service_namespace  = aws_appautoscaling_target.worker.service_namespace

  target_tracking_scaling_policy_configuration {
    target_value = 70.0
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    scale_out_cooldown = 60
    scale_in_cooldown  = 60
  }
}

# SNS topic for alarm notifications. Subscribe an email (or PagerDuty/OpsGenie)
# to actually receive them:
#   aws sns subscribe --topic-arn <arn> --protocol email --notification-endpoint you@example.com
resource "aws_sns_topic" "alarms" {
  name = "${var.environment}-job-scheduler-alarms"

  tags = {
    Name        = "${var.environment}-job-scheduler-alarms"
    Environment = var.environment
  }
}

# Job-failure alarm (14.6): the worker logs {"message":"job processing failed"}
# at ERROR on every failed attempt, so a log metric filter counts failures.
# Fires when jobs repeatedly fail (e.g. bad callback URLs).
resource "aws_cloudwatch_log_metric_filter" "job_failures" {
  name           = "${var.environment}-job-failures"
  log_group_name = aws_cloudwatch_log_group.worker.name
  pattern        = "{ $.message = \"job processing failed\" }"

  metric_transformation {
    name          = "${var.environment}-job-failures"
    namespace     = "JobScheduler/${var.environment}"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "job_failure_rate" {
  alarm_name          = "${var.environment}-job-failure-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.job_failures.metric_transformation[0].name
  namespace           = "JobScheduler/${var.environment}"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Job failures > 5 in 5 minutes (bad callbacks or downstream outage)"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

# API 5xx alarm: real ALB metric. Fires when targets return 5xx.
resource "aws_cloudwatch_metric_alarm" "api_failure_rate" {
  alarm_name          = "${var.environment}-api-failure-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "HTTPCode_Target_5XX_Count"
  namespace           = "AWS/ApplicationELB"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "API targets returned > 5 HTTP 5xx in 5 minutes"

  dimensions = {
    TargetGroup  = aws_lb_target_group.api.arn_suffix
    LoadBalancer = aws_lb.main.arn_suffix
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

# Queue-depth metric (14.7): the scheduler logs {"msg":"queue depth","depth":N}
# every minute, so a log metric filter turns it into a real CloudWatch metric.
resource "aws_cloudwatch_log_metric_filter" "queue_depth" {
  name           = "${var.environment}-queue-depth"
  log_group_name = aws_cloudwatch_log_group.scheduler.name
  pattern        = "{ $.message = \"queue depth\" }"

  metric_transformation {
    name      = "${var.environment}-queue-depth"
    namespace = "JobScheduler/${var.environment}"
    value     = "$.depth"
  }
}

resource "aws_cloudwatch_metric_alarm" "queue_depth" {
  alarm_name          = "${var.environment}-queue-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 5
  metric_name         = aws_cloudwatch_log_metric_filter.queue_depth.metric_transformation[0].name
  namespace           = "JobScheduler/${var.environment}"
  period              = 60
  statistic           = "Average"
  threshold           = 100
  alarm_description   = "Ready-queue depth > 100 for 5 minutes (workers can't keep up)"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

# Task-health alarm (14.8): EventBridge catches any ECS task in this cluster
# that stops with a non-zero container exit (crashing image, failed health
# check, OOM) and fans out to SNS.
resource "aws_cloudwatch_event_rule" "task_stopped" {
  name        = "${var.environment}-ecs-task-stopped"
  description = "Fires when an ECS task stops with a non-zero exit code"

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.main.arn]
      lastStatus = ["STOPPED"]
      containers = {
        exitCode = [{ "anything-but" = 0 }]
      }
    }
  })

  tags = {
    Name        = "${var.environment}-ecs-task-stopped"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_target" "task_stopped_sns" {
  rule      = aws_cloudwatch_event_rule.task_stopped.name
  target_id = "sns"
  arn       = aws_sns_topic.alarms.arn
}

resource "aws_sns_topic_policy" "alarms" {
  arn = aws_sns_topic.alarms.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgePublish"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alarms.arn
    }]
  })
}