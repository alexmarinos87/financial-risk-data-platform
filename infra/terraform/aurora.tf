resource "aws_rds_cluster" "aurora_postgres" {
  count = var.create_aurora_postgres ? 1 : 0

  cluster_identifier = "${var.project_name}-${var.environment}-aurora-postgres"
  engine             = "aurora-postgresql"
  engine_version     = var.aurora_postgres_engine_version
  database_name      = var.postgres_database_name
  master_username    = var.postgres_master_username

  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.postgres[0].name
  vpc_security_group_ids = [aws_security_group.postgres[0].id]
  storage_encrypted      = true

  serverlessv2_scaling_configuration {
    min_capacity = var.aurora_min_acu
    max_capacity = var.aurora_max_acu
  }

  backup_retention_period = 1
  deletion_protection     = var.database_deletion_protection
  skip_final_snapshot     = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Workload    = "warehouse"
  }
}

resource "aws_rds_cluster_instance" "aurora_postgres" {
  count = var.create_aurora_postgres ? var.aurora_instance_count : 0

  identifier         = "${var.project_name}-${var.environment}-aurora-postgres-${count.index + 1}"
  cluster_identifier = aws_rds_cluster.aurora_postgres[0].id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.aurora_postgres[0].engine
  engine_version     = aws_rds_cluster.aurora_postgres[0].engine_version

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Workload    = "warehouse"
  }
}

output "aurora_postgres_endpoint" {
  value = try(aws_rds_cluster.aurora_postgres[0].endpoint, null)
}
