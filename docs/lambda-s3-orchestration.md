# Lambda And S3 Orchestration

This note shows how the local pipeline maps to a small AWS workflow using S3
and Lambda.

## Local Shape

The local demo writes partitioned parquet output under:

```text
data/raw/
data/curated/
```

The same layout maps directly to S3 prefixes:

```text
s3://<raw-bucket>/market_events/year=YYYY/month=MM/day=DD/hour=HH/
s3://<curated-bucket>/returns_1m/year=YYYY/month=MM/day=DD/hour=HH/
s3://<curated-bucket>/volatility_5m/year=YYYY/month=MM/day=DD/hour=HH/
s3://<curated-bucket>/data_quality_metrics/year=YYYY/month=MM/day=DD/hour=HH/
s3://<curated-bucket>/risk_summary/year=YYYY/month=MM/day=DD/hour=HH/
```

## Small-Batch Lambda Flow

```text
source extract lands file in S3
  -> S3 ObjectCreated event
  -> Lambda validates event metadata
  -> Lambda starts the pipeline for the affected partition
  -> raw and curated outputs are written to S3
  -> summary JSON is emitted for monitoring
```

Lambda is a good fit for small batches, orchestration glue, and validation
around object events. Larger transformations should move to a container task,
Glue job, or scheduled Kubernetes job.

## Example Handler Shape

```python
from pathlib import Path

from src.orchestration.run_pipeline import run_pipeline


def handler(event, context):
    records = event.get("Records", [])
    if not records:
        raise ValueError("No S3 records received")

    summaries = []
    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        input_path = download_to_tmp(bucket, key)
        summary = run_pipeline(
            input_path=Path(input_path),
            thresholds_path=Path("/var/task/config/risk_thresholds.yaml"),
            late_seconds=300,
            window_minutes=5,
            vol_window=5,
            storage_config_path=Path("/var/task/config/storage.yaml"),
            lock_owner=f"lambda:{context.aws_request_id}",
        )
        summaries.append({"bucket": bucket, "key": key, "summary": summary})

    publish_run_summary(summaries)
    return {"processed": len(summaries), "summaries": summaries}
```

The helper functions would own S3 download and monitoring output. The core
pipeline logic remains testable outside AWS.

## Reliability Considerations

1. Use idempotent target paths so retries do not duplicate output.
2. Preserve raw source files for replay.
3. Lock partitions while a live job or backfill is writing.
4. Emit run summaries with row counts, duplicate rate, late rate, and quality
   status.
5. Route failed events to a dead-letter queue.
6. Use CloudWatch alarms on failed invocations and stale output.
7. Keep transformation code independent from the Lambda handler.

## When Not To Use Lambda

Use a scheduled container, Kubernetes CronJob, or Glue job when:

1. Batch size regularly exceeds Lambda memory or timeout limits.
2. Transformations require distributed processing.
3. The job needs heavy Python dependencies or long warm-up time.
4. Backfills must replay many partitions in one run.

The current repository already includes a Kubernetes CronJob deployment scaffold
for that heavier scheduled-batch path.

## Interview Talking Point

The practical split is:

```text
Lambda: small orchestration and event handling
S3: durable raw and curated storage
PostgreSQL: operational warehouse serving layer
Scheduled job: larger batch and backfill processing
```

That split keeps the pipeline simple while giving each component a clear
responsibility.
