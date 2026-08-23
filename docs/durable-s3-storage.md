# Disabled Immutable S3 Storage

## Outcome

One local artifact can be published to a content-addressed S3 key through an explicitly enabled adapter:

```text
regular local file
  -> bounded read
  -> SHA-256 content identity
  -> deterministic immutable object key
  -> conditional S3 PutObject
  -> checksum and server-side encryption
  -> written or verified-already-present evidence
```

The adapter is `src/storage/durable_s3.py`. The operator command is `src/orchestration/publish_durable_artifact.py`.

## Disabled default

`config/durable_storage.yaml` contains:

```yaml
enabled: false
```

Publication also requires:

```text
--enable-durable-write
```

Both gates must be enabled. Normal CI, readiness checks, and local development perform no S3 request.

## Runtime configuration

The repository stores environment-variable names rather than a bucket, KMS key, or credentials:

```text
RISK_PLATFORM_DURABLE_BUCKET
AWS_REGION
optional configured KMS key environment variable
```

The boto3 client uses the normal AWS credential chain of the enabled runtime. Static credentials are not accepted by the adapter and are not written to configuration or summaries.

## Immutable identity

Object keys use:

```text
<prefix>/<dataset>/sha256=<first-two-hex>/<full-sha256>.<extension>
```

The write sends:

```text
If-None-Match: *
ChecksumSHA256
sha256 metadata
server-side encryption
```

A precondition failure is treated as replay only after `HeadObject` confirms both the stored SHA-256 metadata and content length. A key collision or mismatched existing object fails closed.

The adapter exposes no overwrite or delete operation.

## Explicit publication

After reviewing and enabling the store:

```bash
export RISK_PLATFORM_DURABLE_BUCKET='reviewed-private-bucket'
export AWS_REGION='eu-west-2'

.venv/bin/python -m src.orchestration.publish_durable_artifact \
  --store-id primary-s3 \
  --dataset portfolio-risk-attribution \
  --file data/curated/portfolio_risk_attribution/example.parquet \
  --enable-durable-write \
  --summary-json .demo/durable-publication.json
```

The enabled runtime must provide boto3. The import is deliberately lazy so disabled validation and normal CI do not require AWS SDK installation or credentials.

## Inventory

`inventory_immutable_objects` lists only the configured prefix and dataset. Pagination is bounded by `max_list_objects`, with a hard maximum of 10,000. Any returned key outside the requested prefix fails the inventory.

## Bounds

The bundled store caps one object at 100 MB and one inventory at 1,000 objects. Code-level hard limits are 5 GB and 10,000 objects. Local input must be a non-empty regular file, must not be a symbolic link, and must not change size while being read.

## Boundary

This increment does not create a bucket, KMS key, IAM role, lifecycle rule, replication policy, retention lock, inventory schedule, or credential. It does not migrate existing local datasets automatically, activate cloud storage by default, deploy infrastructure, or run `terraform apply`. Those require explicit infrastructure and operating decisions.
