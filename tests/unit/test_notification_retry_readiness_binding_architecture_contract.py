from pathlib import Path


def test_retry_readiness_binding_is_deterministic_local_and_secret_safe() -> None:
    contract = Path(
        "src/warehouse/notification_retry_readiness_binding_contract.py"
    ).read_text(encoding="utf-8")
    runner = Path(
        "src/orchestration/build_notification_retry_readiness_binding.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/notification-retry-readiness-binding.md").read_text(
        encoding="utf-8"
    )
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "validate_retry_execution_record",
        "validate_notification_execution_readiness_enforcement",
        "canonical_retry_execution_record_bytes",
        "readiness_enforcement_sha256",
        "during the terminal execution window",
        "requestful terminal evidence must retain delivery lock identity",
        "retry readiness binding_id does not match content",
    ):
        assert required in contract

    for required in (
        "--terminal-record",
        "--execution-summary",
        "--recorded-at",
        "execution_readiness",
        "must not be a symbolic link",
        "exceeds 1 mb",
    ):
        assert required in runner.casefold()

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.",
        "psycopg",
    ):
        assert forbidden not in contract
        assert forbidden not in runner

    for required in (
        "primary arc42 blocks: `orchestration` and `warehouse`",
        "exact terminal record",
        "exact readiness enforcement",
        "does not persist",
        "no endpoint value",
        "no network request",
        "terraform apply",
    ):
        assert required in normalized_docs
