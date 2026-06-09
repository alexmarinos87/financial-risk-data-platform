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

variable "environment" {
  type    = string
  default = "dev"
}

variable "vpc_id" {
  type        = string
  default     = ""
  description = "VPC ID for optional managed database resources."
}

variable "private_subnet_ids" {
  type        = list(string)
  default     = []
  description = "Private subnet IDs for optional RDS, Aurora, and DocumentDB resources."
}

variable "database_allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "CIDR blocks allowed to connect to optional database security groups. Keep empty unless deliberately testing."
}

variable "create_rds_postgres" {
  type        = bool
  default     = false
  description = "Create an optional single-instance RDS PostgreSQL warehouse. Disabled by default to avoid cost."
}

variable "create_aurora_postgres" {
  type        = bool
  default     = false
  description = "Create an optional Aurora PostgreSQL cluster. Disabled by default to avoid cost."
}

variable "create_documentdb_cluster" {
  type        = bool
  default     = false
  description = "Create an optional Amazon DocumentDB cluster. Disabled by default to avoid cost."
}

variable "postgres_database_name" {
  type    = string
  default = "risk_platform"
}

variable "postgres_master_username" {
  type    = string
  default = "risk_admin"
}

variable "rds_postgres_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "rds_postgres_allocated_storage_gb" {
  type    = number
  default = 20
}

variable "database_deletion_protection" {
  type        = bool
  default     = false
  description = "Enable deletion protection for optional managed databases."
}

variable "aurora_postgres_engine_version" {
  type        = string
  default     = null
  description = "Optional Aurora PostgreSQL engine version. Leave null to let AWS choose the default supported version."
}

variable "aurora_min_acu" {
  type    = number
  default = 0.5
}

variable "aurora_max_acu" {
  type    = number
  default = 1
}

variable "aurora_instance_count" {
  type    = number
  default = 1
}

variable "documentdb_master_username" {
  type    = string
  default = "docdb_admin"
}

variable "documentdb_master_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Master password for optional DocumentDB. Required only when create_documentdb_cluster is true."
}

variable "documentdb_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "documentdb_instance_count" {
  type    = number
  default = 1
}
