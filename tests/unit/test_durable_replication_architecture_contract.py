from pathlib import Path


def test_durable_replication_documentation_matches_runtime_contract() -> None:
    documentation = Path("docs/durable-s3-replication.md").read_text(
        encoding="utf-8"
    )
    runtime = Path(
        "src/orchestration/replicate_durable_datasets.py"
    ).read_text(encoding="utf-8")

    assert "replicate_durable_datasets" in documentation
    assert "def replicate_local_datasets" in runtime
    assert "--max-files" in documentation
    assert "--max-total-bytes" in documentation
    assert "max_files" in runtime
    assert "max_total_bytes" in runtime

    for required in (
        "put_immutable_object",
        "--enable-durable-write",
        "replication-manifests",
        "durable-dataset-replication-v1",
        "no_artifacts",
    ):
        assert required in documentation
        assert required in runtime

    for prohibited in (
        "terraform apply",
        "create an S3 bucket",
        "delete or overwrite remote objects",
    ):
        assert prohibited in documentation
