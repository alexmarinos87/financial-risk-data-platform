from pathlib import Path


def test_durable_restore_is_plan_first_exact_manifest_and_no_overwrite() -> None:
    restore_documentation = Path("docs/durable-s3-restore.md").read_text(
        encoding="utf-8"
    )
    replication_documentation = Path(
        "docs/durable-s3-replication.md"
    ).read_text(encoding="utf-8")
    runner = Path(
        "src/orchestration/restore_durable_datasets.py"
    ).read_text(encoding="utf-8")

    for required in (
        "durable-dataset-restore-v1",
        "durable-dataset-replication-v1",
        "--enable-durable-read",
        "plan-only",
        "exact immutable replication manifest",
        "head_object",
        "expected-length-plus-one",
        "Atomic No-Overwrite Publication",
        "already_present",
        "conflict",
        "restore_plan_id",
    ):
        assert required in restore_documentation

    assert "docs/durable-s3-restore.md" in replication_documentation
    assert "restore objects to local storage" not in replication_documentation
    assert "list_objects_v2" not in runner
    assert "get_object" in runner
    assert runner.index("if not enable_durable_read") < runner.index(
        "client = selected_factory"
    )
    assert runner.index("if any(artifact.local_status == \"conflict\"") < (
        runner.index("client = selected_factory")
    )


def test_restore_documentation_keeps_cloud_mutation_out_of_scope() -> None:
    documentation = Path("docs/durable-s3-restore.md").read_text(
        encoding="utf-8"
    )

    for prohibited in (
        "select a mutable latest",
        "overwrite or delete a local file",
        "write, delete or mutate S3 objects",
        "run `terraform apply`",
    ):
        assert prohibited in documentation

    for false_claim in (
        "restore is automatic",
        "CI downloads production objects",
        "creates an S3 bucket",
    ):
        assert false_claim not in documentation
