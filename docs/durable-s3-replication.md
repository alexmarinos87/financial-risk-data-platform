# Disabled Durable S3 Replication

## Outcome

This adapter copies already-published local raw and curated Parquet into an existing S3 bucket without changing the local analytical contracts:

```text
configured local raw and curated paths
  -> bounded regular-file inventory
  -> SHA-256 and deterministic object keys
  -> plan-only manifest by default
  -> explicit immutable S3 upload
  -> metadata verification
  -> replay or conflict refusal
```

The implementation is `src/storage/s3_replication.py`. The committed configuration is `config/object_storage.yaml`.

## Disabled-by-default activation

The committed S3 configuration has `enabled: false` and contains no bucket name, account, credential, or endpoint. Normal invocation builds a plan only and does not construct or call an S3 client.

Execution requires all of:

1. a reviewed configuration change setting `enabled: true`;
2. an existing bucket supplied through `RISK_PLATFORM_S3_BUCKET`;
3. an AWS region supplied through `AWS_REGION`;
4. locally configured AWS credentials or workload identity; and
5. an explicit `--execute` invocation.

The adapter does not create a bucket, alter bucket policy, add lifecycle rules, change encryption settings, provision IAM, deploy infrastructure, or run `terraform apply`.

## Immutable key contract

Object keys preserve the local path below `storage.base_dir`:

```text
<prefix>/<raw or curated relative path>
```

For example:

```text
financial-risk-data-platform/curated/daily_returns/year=2026/.../batch.parquet
```

Each upload stores user metadata containing:

```text
sha256
relative-path
model-version
```

and requests S3-managed `AES256` server-side encryption.

Before uploading, the adapter performs `HeadObject`:

- missing object: upload, then verify metadata and content length;
- matching size, SHA-256, and relative path: count as already present;
- existing but non-matching object: fail and never overwrite.

This contract makes reruns convergent while keeping object-key conflicts visible. It does not depend on ETag semantics, which can differ for multipart or encrypted uploads.

## Inventory bounds

The adapter inventories only `*.parquet` beneath the configured raw and curated base paths. It rejects symbolic-link bases, directories, and files; paths outside `storage.base_dir`; and unsafe relative paths.

The committed bounds are:

```text
4,096 files
1 GB total local input
```

A request exceeding either limit fails before any remote operation. Hashing streams files in 1 MB chunks.

## Evidence and redaction

The plan or execution summary includes:

- configuration and manifest fingerprints;
- selected file and byte counts;
- object keys, relative paths, sizes, and SHA-256 values;
- uploaded and already-present counts;
- encryption and prefix; and
- a one-way fingerprint showing whether a bucket was configured.

The bucket name, AWS credentials, session tokens, account identifiers, and endpoint configuration are not written to the summary.

## Commands

Plan without installing an S3 client or making a cloud request:

```bash
.venv/bin/python -m src.storage.s3_replication \
  --summary-json .demo/s3-replication-plan.json
```

Install the optional client only on an operator environment:

```bash
.venv/bin/python -m pip install -e '.[s3]'
```

After reviewing and enabling the configuration:

```bash
export RISK_PLATFORM_S3_BUCKET='existing-private-bucket'
export AWS_REGION='eu-west-2'

.venv/bin/python -m src.storage.s3_replication \
  --execute \
  --summary-json .demo/s3-replication-run.json
```

## Retention and recovery boundary

This increment provides durable replication, not lifecycle ownership. No object is deleted or replaced. Retention periods, legal holds, object lock, archival classes, cross-region replication, disaster-recovery objectives, and restore testing require explicit later decisions tied to the target environment.

Local Parquet remains the immediate source for existing commands. Reading analytics directly from S3 and transactional lakehouse semantics are separate changes.

## CI boundary

CI uses fake clients and plan-only behavior. It creates no AWS resource, makes no S3 call, uploads no object, and applies no infrastructure.
