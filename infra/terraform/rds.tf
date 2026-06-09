resource "aws_db_instance" "postgres" {
  count = var.create_rds_postgres ? 1 : 0

  identifier = "${var.project_name}-${var.environment}-postgres"

  engine         = "postgres"
  instance_class = var.rds_postgres_instance_class

  allocated_storage     = var.rds_postgres_allocated_storage_gb
  max_allocated_storage = max(var.rds_postgres_allocated_storage_gb, 100)
  storage_encrypted     = true
  storage_type          = "gp3"

  db_name  = var.postgres_database_name
  username = var.postgres_master_username

  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.postgres[0].name
  vpc_security_group_ids = [aws_security_group.postgres[0].id]
  publicly_accessible    = false

  backup_retention_period = 1
  deletion_protection     = var.database_deletion_protection
  skip_final_snapshot     = true

  auto_minor_version_upgrade = true
  copy_tags_to_snapshot      = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Workload    = "warehouse"
  }
}

output "rds_postgres_endpoint" {
  value = try(aws_db_instance.postgres[0].address, null)
}
