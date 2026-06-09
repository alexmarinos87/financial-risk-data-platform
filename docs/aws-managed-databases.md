# AWS Managed Database Scaffold

This project includes disabled-by-default Terraform for managed AWS databases.
It is intended as infrastructure design evidence, not something to leave running
unattended.

## What Is Included

PostgreSQL serving options:

1. Single-instance RDS PostgreSQL.
2. Aurora PostgreSQL Serverless v2.

Document source option:

1. Amazon DocumentDB for MongoDB-compatible source documents.

Supporting resources:

1. Private subnet groups.
2. Database security groups.
3. Encrypted storage.
4. AWS-managed master password for RDS/Aurora PostgreSQL.
5. Disabled-by-default creation flags to avoid accidental cost.

## Default Behaviour

The defaults create no managed databases:

```hcl
create_rds_postgres      = false
create_aurora_postgres   = false
create_documentdb_cluster = false
```

This means `terraform plan` can include the scaffolding without creating RDS,
Aurora, or DocumentDB until an environment explicitly opts in.

## Example RDS PostgreSQL Opt-In

```hcl
environment = "dev"

vpc_id             = "vpc-xxxxxxxx"
private_subnet_ids = ["subnet-aaa", "subnet-bbb"]

create_rds_postgres = true

database_allowed_cidr_blocks = [
  "10.0.0.0/16"
]
```

RDS PostgreSQL is the simplest first managed target for this project because it
maps directly to the local PostgreSQL playground and the `risk_platform` schema.

## Example Aurora PostgreSQL Opt-In

```hcl
environment = "dev"

vpc_id             = "vpc-xxxxxxxx"
private_subnet_ids = ["subnet-aaa", "subnet-bbb"]

create_aurora_postgres = true
aurora_min_acu         = 0.5
aurora_max_acu         = 1
aurora_instance_count  = 1
```

Aurora is more production-like, but it is also more infrastructure than this
portfolio workload needs. Use it to discuss scaling and managed operations, not
as the first thing to deploy.

## Example DocumentDB Opt-In

```hcl
environment = "dev"

vpc_id             = "vpc-xxxxxxxx"
private_subnet_ids = ["subnet-aaa", "subnet-bbb"]

create_documentdb_cluster  = true
documentdb_master_password = "replace-with-secure-value"

database_allowed_cidr_blocks = [
  "10.0.0.0/16"
]
```

DocumentDB is MongoDB-compatible, but it is not identical to MongoDB. For this
repo, the important design point is the source pattern:

```text
nested source documents
  -> incremental extraction
  -> flattening and validation
  -> warehouse serving tables
```

## Cost And Safety Notes

1. Keep all creation flags set to `false` unless deliberately testing.
2. Use private subnets and avoid public database access.
3. Keep `database_allowed_cidr_blocks` narrow.
4. Set a budget alarm before applying any managed database resources.
5. Destroy test resources immediately after validation.
6. Prefer RDS PostgreSQL first if the goal is to demonstrate warehouse serving.
7. Use DocumentDB only when testing source-document behaviour is worth the cost.

## How It Maps To The Local Playground

| Local | AWS |
| --- | --- |
| Docker PostgreSQL | RDS PostgreSQL or Aurora PostgreSQL |
| Docker MongoDB | Amazon DocumentDB |
| `sql/postgres_schema.sql` | Database migration/init script |
| `src.warehouse.postgres_loader` | Batch load job or scheduled task |
| `sql/consistency_checks.sql` | Reconciliation/monitoring query |

## Interview Explanation

Use this version:

> I kept managed databases as disabled-by-default Terraform because RDS, Aurora,
> and DocumentDB have real running costs. The local Docker playground proves the
> source-to-warehouse contract. The Terraform shows how I would move the same
> pattern into AWS using private subnets, security groups, encrypted storage,
> and opt-in creation flags.
