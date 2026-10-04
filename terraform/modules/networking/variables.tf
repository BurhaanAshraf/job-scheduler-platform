variable "environment" {
  type        = string
  description = "Environment name"
}

variable "vpc_cidr" {
  type        = string
  description = "VPC CIDR block"
}

variable "public_subnet_cidrs" {
  type        = list(string)
  description = "Public subnet CIDRs"
}

variable "private_subnet_cidrs" {
  type        = list(string)
  description = "Private subnet CIDRs"
}

variable "availability_zones" {
  type        = list(string)
  description = "Availability zones"
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