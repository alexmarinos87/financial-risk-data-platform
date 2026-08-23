# Verified Durable Dataset Restore

## Outcome

This path restores one exact immutable replication manifest to the configured
local raw and curated dataset layout:

```text
completed durable replication result
  -> strict local summary validation
  -> exact canonical manifest identity
  -> plan-only local target inventory
  -> explicit dual-gated S3 reads
  -> remote manifest and object metadata verification
  -> bounded payload digest verification
  -> atomic no-overwrite local publication
  -> deterministic replay evidence
```

The runner is:

```text
src/orchestration/restore_durable_datasets.py
```

It consumes the JSON result produced by
`src/orchestration/replicate_durable_datasets.py`. It never lists an S3 prefix
and never chooses a mutable "latest" manifest. The supplied result must identify
one exact `durable-dataset-replication-v1` manifest.

## Plan First

The default command performs only local validation and inventory:

```bash
.venv/bin/python -m src.orchestration.restore_durable_datasets \
  --replication-summary .demo/durable-replication-result.json \
  --store-id primary-s3 \
  --summary-json .demo/durable-restore-plan.json
```

Planning:

- reads a strict bounded local JSON file;
- loads durable and local storage configuration;
- reconstructs the canonical replication manifest;
- verifies its manifest ID and immutable object key;
- maps every manifest entry to one configured local dataset root;
- classifies each target as `missing`, `already_present`, or `conflict`;
- calculates a deterministic `durable-restore-*` plan identity; and
- reports whether the supplied replication result proves completed immutable
  publication.

Planning does **not**:

- read bucket or region environment values;
- create a boto3 client;
- call S3;
- create local directories; or
- modify a local file.

The summary contains store, manifest, dataset, relative-path, object-key, size and
SHA-256 evidence. It excludes bucket, region, KMS and credential values.

## Exact Manifest Contract

Every artifact entry must contain exactly:

```text
content_length
dataset
object_key
relative_path
remote_dataset
sha256
```

The runner reconstructs:

```json
{
  "contract": "durable-dataset-replication-v1",
  "entries": [],
  "store_id": "primary-s3"
}
```

It then derives the manifest ID from canonical JSON, adds that ID to the manifest
document, and derives the content-addressed manifest object key with the existing
immutable S3 adapter. The locally supplied `manifest_id`,
`manifest_object_key`, artifact count, dataset count and total bytes must all
match the reconstruction.

Execution also requires the replication result to prove:

- `replication.performed = true`;
- one result for every ordered artifact;
- every artifact result is `written` or `already_present` with matching identity;
- written plus already-present counts reconcile; and
- the published manifest result matches the reconstructed key, digest, size and
  identity.

A plan-only replication summary may be inspected, but it cannot authorize object
reads.

## Local Target Mapping

Targets are derived only from `config/storage.yaml`.

A manifest entry for:

```text
dataset = market_events
remote_dataset = raw-market_events
relative_path = year=2026/month=08/batch.parquet
```

maps beneath the configured raw `market_events` root.

A manifest entry for:

```text
dataset = daily_returns
remote_dataset = curated-daily_returns
relative_path = year=2026/batch.parquet
```

maps beneath the configured curated `daily_returns` root.

The runner rejects:

- unconfigured dataset/remote-dataset pairs;
- absolute paths;
- `.` or `..` segments;
- backslashes, doubled separators and control characters;
- non-Parquet targets;
- duplicate object keys or local targets;
- symbolic-link configured bases, dataset roots, parent directories or targets;
- non-directory parents; and
- non-regular existing targets.

Existing regular files are read only within the declared object-size bound. A
matching size and SHA-256 is `already_present`; any mismatch is `conflict`.

## Explicit Read Gate

S3 reads require both:

1. `enabled: true` for the selected store in
   `config/durable_storage.yaml`; and
2. the explicit `--enable-durable-read` command flag.

For example:

```bash
export RISK_PLATFORM_DURABLE_BUCKET='configured-outside-git'
export AWS_REGION='eu-west-2'

.venv/bin/python -m src.orchestration.restore_durable_datasets \
  --replication-summary .demo/durable-replication-result.json \
  --store-id primary-s3 \
  --max-total-bytes 1000000000 \
  --enable-durable-read \
  --summary-json .demo/durable-restore-result.json
```

The bundled store remains disabled. Missing environment values, local conflicts
and incomplete replication evidence all fail before client creation.

## Remote Verification

Execution resolves only the configured bucket and region and creates one S3
client. It performs no list operation.

The exact manifest object is verified first with `head_object`:

- metadata `storage-contract` must be
  `immutable-content-addressed-v1`;
- metadata `sha256` must equal the reconstructed manifest digest; and
- `ContentLength` must equal the canonical manifest byte length.

The body is then read with an expected-length-plus-one bound. Its exact length,
SHA-256, canonical bytes and parsed document must match the locally reconstructed
manifest.

Before any artifact body is accepted, every declared artifact receives the same
metadata, storage-contract and content-length verification. Only exact declared
keys are used.

A missing local artifact is downloaded with an expected-length-plus-one bound.
The payload length, SHA-256 and immutable object key are recomputed before local
publication. An already-present local artifact still receives remote metadata
verification but does not require another artifact-body read.

## Atomic No-Overwrite Publication

A verified missing payload is written to a temporary regular file in the final
directory, flushed and `fsync`ed. The runner then creates the final path through
an atomic same-directory hard link.

This provides no-overwrite behavior:

- a missing target becomes `restored`;
- a concurrently created matching target converges to `already_present`; and
- a concurrently created conflicting target fails without replacement.

The temporary file is always removed. The final file is reread and verified
before the result is accepted.

A failure after some files are restored is replay-safe: immutable matching files
become `already_present` on the next execution, while remaining missing files are
restored.

## Bounds

The restore applies all of these bounds before S3 client creation:

- strict replication-summary file cap: 16 MB;
- canonical manifest cap: 10 MB;
- artifact count no greater than the smaller of 10,000 and the selected store's
  `max_list_objects`;
- every object no greater than `max_object_bytes`;
- hard aggregate restore cap: 100 GB; and
- default request aggregate cap: 1 GB.

The artifact count does not authorize S3 listing. It bounds the exact manifest
entries and exact key reads.

## Evidence And Replay

`restore_plan_id` binds:

```text
restore contract
store ID
manifest ID
ordered dataset / relative path / SHA-256 identities
```

It is stable across machines and local states. Local status and execution results
remain explicit but do not change the selected manifest identity.

An execution result reports:

- manifest verification;
- exact remote object count verified;
- artifact results by dataset and relative path;
- restored count; and
- already-present count.

No bucket, region, KMS key or credential value is returned.

## Boundary

This increment does not:

- discover manifests by prefix;
- select a mutable latest object;
- restore an unconfirmed plan-only replication;
- overwrite or delete a local file;
- write, delete or mutate S3 objects;
- create a bucket, KMS key, role or credential;
- schedule restore;
- deploy infrastructure; or
- run `terraform apply`.

It restores exact immutable bytes. It does not reinterpret or recalculate the
Parquet records.
