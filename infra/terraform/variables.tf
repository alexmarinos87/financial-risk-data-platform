variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "financial-risk-data-platform"
}

variable "ecr_repository_name" {
  type    = string
  default = "financial-risk-data-platform"
}

variable "github_repository" {
  type    = string
  default = "alexmarinos87/financial-risk-data-platform"
}

variable "github_oidc_provider_arn" {
  type        = string
  default     = ""
  description = "Existing GitHub Actions OIDC provider ARN. Leave empty to skip deploy role creation."
}

variable "github_deploy_environments" {
  type    = set(string)
  default = ["dev", "prod"]
}

variable "eks_cluster_arn" {
  type        = string
  default     = ""
  description = "Optional EKS cluster ARN to scope DescribeCluster access."
}
