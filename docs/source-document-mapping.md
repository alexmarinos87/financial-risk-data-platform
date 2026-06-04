# Source Document Mapping

This note shows how nested upstream documents can be flattened into the
warehouse-ready event shape used by the pipeline.

## Example Upstream Document

```json
{
  "_id": "665f4a731d7f2d9f0a9d0001",
  "candidateId": "cand_123",
  "check": {
    "id": "check_987",
    "type": "employment_reference",
    "status": "completed"
  },
  "employer": {
    "name": "Example Ltd",
    "country": "GB"
  },
  "timestamps": {
    "createdAt": "2026-06-04T09:01:00Z",
    "updatedAt": "2026-06-04T09:05:30Z",
    "completedAt": "2026-06-04T09:05:00Z"
  },
  "source": "application"
}
```

## Flattened Warehouse Record

```json
{
  "source_document_id": "665f4a731d7f2d9f0a9d0001",
  "candidate_id": "cand_123",
  "check_id": "check_987",
  "check_type": "employment_reference",
  "check_status": "completed",
  "employer_name": "Example Ltd",
  "employer_country": "GB",
  "ts_created": "2026-06-04T09:01:00Z",
  "ts_updated": "2026-06-04T09:05:30Z",
  "ts_completed": "2026-06-04T09:05:00Z",
  "source": "application"
}
```

## Mapping Rules

| Source path | Warehouse column | Rule |
| --- | --- | --- |
| `_id` | `source_document_id` | Preserve as source primary key |
| `candidateId` | `candidate_id` | Rename to snake case |
| `check.id` | `check_id` | Flatten nested identifier |
| `check.type` | `check_type` | Flatten nested attribute |
| `check.status` | `check_status` | Keep source status value |
| `employer.name` | `employer_name` | Flatten nested object |
| `employer.country` | `employer_country` | Keep ISO-like country value |
| `timestamps.createdAt` | `ts_created` | Parse as UTC timestamp |
| `timestamps.updatedAt` | `ts_updated` | Parse as UTC timestamp |
| `timestamps.completedAt` | `ts_completed` | Nullable business timestamp |
| `source` | `source` | Preserve source system label |

## Validation Rules

Required fields:

```text
source_document_id
candidate_id
check_id
check_type
check_status
ts_created
ts_updated
source
```

Useful checks:

1. `ts_updated >= ts_created`.
2. `ts_completed IS NULL OR ts_completed >= ts_created`.
3. `check_status` belongs to the accepted status set.
4. `source_document_id` is unique in the raw table.
5. `check_id` is stable across re-syncs.

## Incremental Loading

For a document source, use `timestamps.updatedAt` as the extraction cursor when
available. Keep `_id` as the stable source key and use it for idempotent loading.

The warehouse load pattern mirrors the event pipeline:

1. Land raw documents or flattened raw rows.
2. Validate required fields and timestamp ordering.
3. Deduplicate by source key.
4. Load curated tables with `ON CONFLICT`.
5. Emit data quality metrics for missing fields, invalid statuses, and stale
   source updates.

## Relationship To This Project

The project uses market events as a simple domain because the records are easy
to understand. The same engineering pattern applies to nested operational
documents:

```text
nested source record
  -> flattened raw contract
  -> validation
  -> deduplication
  -> curated warehouse tables
  -> operational SQL
```

The important point is not the domain. The important point is maintaining a
clear contract between source data, transformation code, and warehouse
consumers.
