# Bounded Durable Dataset Replication

## Outcome

This path adds a thin orchestration layer over the immutable S3 adapter already
implemented in `src/storage/durable_s3.py`:

```text
configured local raw and curated Parquet
  -> bounded regular-file inventory
  -> deterministic content hashes and object keys
  -> stable replication manifest
  -> plan-only evidence by default
  -> explicit dual-gated immutable S3 publication
```

The runner is:

```text
src/orchestration/replicate_durable_datasets.py
```

It does not introduce another S3 object identity. Every object write and replay
verification delegates to `put_immutable_object`.

## Plan First

The default command does not resolve AWS environment values, create an S3 client,
or perform a network call:

```bash
.venv/bin/python -m src.orchestration.replicate_durable_datasets \
  --store-id primary-s3 \
  --dataset daily_returns \
  --dataset portfolio_risk_attribution \
  --summary-json .demo/durable-replication-plan.json
```

With no `--dataset` arguments, the runner inventories every configured raw and
curated dataset that currently exists locally.

A plan reports:

- selected local artifact count and bytes;
- local dataset and relative Parquet path;
- durable dataset name;
- SHA-256 content identity;
- deterministic immutable object key;
- deterministic manifest ID and manifest object key; and
- only the names of environment variables that an enabled execution would use.

Bucket, region, KMS-key and credential values are never included in the summary.

## Explicit Execution Gate

Publication requires both:

1. `enabled: true` for the selected store in
   `config/durable_storage.yaml`; and
2. the explicit `--enable-durable-write` command flag.

For example:

```bash
export RISK_PLATFORM_DURABLE_BUCKET='configured-outside-git'
export AWS_REGION='eu-west-2'

.venv/bin/python -m src.orchestration.replicate_durable_datasets \
  --store-id primary-s3 \
  --dataset daily_returns \
  --max-files 1000 \
  --max-total-bytes 1000000000 \
  --enable-durable-write \
  --summary-json .demo/durable-replication-result.json
```

The bundled configuration remains disabled and contains no bucket, key,
credential or account identifier.

## Local Inventory Contract

The runner derives paths only from `config/storage.yaml`.

Raw Parquet is grouped under:

```text
raw-<configured raw dataset>
```

Curated Parquet is grouped under:

```text
curated-<configured curated dataset key>
```

The inventory:

- includes only `*.parquet`;
- is sorted by durable dataset, relative path and content hash;
- rejects symbolic-link base paths, dataset paths and files;
- rejects non-regular and empty files;
- rejects objects above the store's `max_object_bytes`;
- rejects unconfigured dataset selectors;
- applies explicit file and total-byte caps; and
- verifies that a file's size is unchanged while it is read.

The requested file cap cannot exceed either the hard 10,000-file bound or the
selected store's `max_list_objects`. The hard total-byte bound is 100 GB; the
default request bound is 1 GB.

## Immutable Publication

For each planned file, execution calls the existing adapter with its durable
dataset, payload, extension, content type and configured encryption controls.
The adapter owns:

- the SHA-256 object identity;
- content-addressed object keys;
- `If-None-Match: *`;
- S3 checksum submission;
- server-side encryption;
- matching-object replay verification; and
- overwrite refusal.

Before each call, the runner rereads the local file and verifies the planned
length and SHA-256 digest. A file changed after planning fails before that object
is submitted.

Partial execution is replay-safe. Successfully written objects remain immutable;
rerunning verifies or writes the remaining content.

## Replication Manifest

After every selected artifact has either been written or verified as already
present, the runner publishes one canonical JSON manifest under:

```text
replication-manifests
```

The manifest contract is `durable-dataset-replication-v1` and contains only
stable evidence:

- contract version;
- store ID;
- local dataset;
- durable dataset;
- relative source path;
- content length;
- SHA-256 digest; and
- immutable object key.

It contains no run timestamp, random run ID, bucket, region, KMS key or
credential. The manifest ID is a SHA-256-derived identity over its ordered
entries. Repeating the same local state converges on the same manifest object. A
changed file, selection or path creates a distinguishable manifest.

The manifest is published last. Its presence means every referenced object
completed the immutable adapter contract for that execution.

The completed result JSON is also the explicit input to the verified restore
path documented in `docs/durable-s3-restore.md`. Restore reconstructs this exact
manifest identity; it does not list a prefix or select a mutable latest object.

## Failure And Replay Semantics

| Condition | Behaviour |
| --- | --- |
| No local artifacts match | Return `no_artifacts`; make no client |
| Explicit flag absent | Return the complete plan; make no client |
| Store disabled | Return the complete plan; make no client |
| Required environment missing | Fail before client creation |
| Local file changes after planning | Fail before publishing that object |
| Existing matching object | Report `already_present` |
| Existing identity mismatch | Fail closed through the adapter |
| Artifact publication fails | Stop; do not publish the manifest |
| Manifest publication fails | Artifact objects remain safe; rerun converges |

## Boundary

Replication does not:

- create an S3 bucket, KMS key, IAM role or credential;
- activate the bundled store;
- upload anything in CI;
- preserve a mutable partition hierarchy in object keys;
- delete or overwrite remote objects;
- schedule replication;
- deploy infrastructure; or
- run `terraform apply`.

The source partition path is retained in the immutable manifest. Object keys stay
content-addressed rather than becoming mutable copies of local paths. Verified
local restore is a separate explicit read workflow, not an implicit side effect
of replication.
