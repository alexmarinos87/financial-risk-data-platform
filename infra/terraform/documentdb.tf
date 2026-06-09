resource "aws_docdb_cluster" "source" {
  count = var.create_documentdb_cluster ? 1 : 0

  cluster_identifier = "${var.project_name}-${var.environment}-documentdb"
  engine             = "docdb"

  master_username = var.documentdb_master_username
  master_password = var.documentdb_master_password

  db_subnet_group_name   = aws_docdb_subnet_group.documentdb[0].name
  vpc_security_group_ids = [aws_security_group.documentdb[0].id]
  storage_encrypted      = true

  backup_retention_period = 1
  deletion_protection     = var.database_deletion_protection
  skip_final_snapshot     = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Workload    = "source-documents"
  }
}

resource "aws_docdb_cluster_instance" "source" {
  count = var.create_documentdb_cluster ? var.documentdb_instance_count : 0

  identifier         = "${var.project_name}-${var.environment}-documentdb-${count.index + 1}"
  cluster_identifier = aws_docdb_cluster.source[0].id
  instance_class     = var.documentdb_instance_class

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Workload    = "source-documents"
  }
}

output "documentdb_endpoint" {
  value = try(aws_docdb_cluster.source[0].endpoint, null)
}
