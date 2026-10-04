output "deploy_role_arn" {
  value       = aws_iam_role.github_deploy.arn
  description = "ARN for GitHub Actions OIDC deploys (put in deploy.yml)"
}
