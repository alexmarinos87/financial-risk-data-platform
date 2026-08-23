from pathlib import Path


def test_s3_replication_is_disabled_immutable_and_secret_safe() -> None:
    source = Path("src/storage/s3_replication.py").read_text(encoding="utf-8")
    docs = Path("docs/durable-s3-replication.md").read_text(
        encoding="utf-8"
    )
    config = Path("config/object_storage.yaml").read_text(encoding="utf-8")
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert "enabled: false" in config
    assert "RISK_PLATFORM_S3_BUCKET" in config
    assert "AWS_REGION" in config
    assert "boto3>=1.35,<2" in pyproject

    for required in (
        "head_object",
        "put_object",
        "overwrite is forbidden",
        "ServerSideEncryption",
        "bucket_fingerprint",
        '"bucket_created": False',
        '"objects_deleted": 0',
        '"infrastructure_applied": False',
    ):
        assert required in source

    for required in (
        "Disabled-by-default",
        "does not create a bucket",
        "fail and never overwrite",
        "bucket name",
        "CI uses fake clients",
        "no object is deleted or replaced",
    ):
        assert required.lower() in docs.lower()
