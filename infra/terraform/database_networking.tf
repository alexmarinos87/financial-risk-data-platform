locals {
  create_postgres_network   = var.create_rds_postgres || var.create_aurora_postgres
  create_documentdb_network = var.create_documentdb_cluster
}

resource "aws_security_group" "postgres" {
  count = local.create_postgres_network ? 1 : 0

  name        = "${var.project_name}-${var.environment}-postgres"
  description = "PostgreSQL access for ${var.project_name} ${var.environment}"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.database_allowed_cidr_blocks
    content {
      description = "PostgreSQL client access"
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_db_subnet_group" "postgres" {
  count = local.create_postgres_network ? 1 : 0

  name       = "${var.project_name}-${var.environment}-postgres"
  subnet_ids = var.private_subnet_ids

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_security_group" "documentdb" {
  count = local.create_documentdb_network ? 1 : 0

  name        = "${var.project_name}-${var.environment}-documentdb"
  description = "DocumentDB access for ${var.project_name} ${var.environment}"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.database_allowed_cidr_blocks
    content {
      description = "DocumentDB client access"
      from_port   = 27017
      to_port     = 27017
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_docdb_subnet_group" "documentdb" {
  count = local.create_documentdb_network ? 1 : 0

  name       = "${var.project_name}-${var.environment}-documentdb"
  subnet_ids = var.private_subnet_ids

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
