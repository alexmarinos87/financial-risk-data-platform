from pathlib import Path


def test_retry_governance_bundle_is_atomic_and_side_effect_bounded() -> None:
    terminal_recorder = Path(
        "src/warehouse/notification_retry_execution_recorder.py"
    ).read_text(encoding="utf-8")
    bundle = Path(
        "src/warehouse/notification_retry_governance_bundle_recorder.py"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/notification_retry_governance_bundle_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-retry-atomic-governance-persistence.md"
    ).read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "record_notification_retry_execution_with_cursor",
        "record_notification_retry_execution_with_cursor(",
        "connection.commit()",
    ):
        assert required in terminal_recorder

    for required in (
        "validate_notification_retry_governance_bundle",
        "build_notification_retry_readiness_binding",
        "record_notification_retry_execution_with_cursor",
        "record_notification_retry_readiness_binding_with_cursor",
        "existing retry terminal has no readiness binding",
        "atomic replay cannot backfill legacy history",
        "connection.commit()",
        "connection.rollback()",
        "terminal_created != readiness_created",
    ):
        assert required in bundle

    assert bundle.index("record_notification_retry_execution_with_cursor(") < bundle.index(
        "record_notification_retry_readiness_binding_with_cursor("
    )
    assert bundle.index("record_notification_retry_readiness_binding_with_cursor(") < (
        bundle.index("connection.commit()")
    )

    for required in (
        "exact_replay_converged",
        "legacy_backfill_rejected",
        "second_write_failure_observed",
        "first_write_rolled_back",
    ):
        assert required in fixture

    assert (
        "notification_retry_governance_bundle_postgres_contract_check" in makefile
    )

    for required in (
        "primary arc42 blocks: `warehouse` and `orchestration`",
        "commit both or roll back both",
        "exact replay requires both rows",
        "not silently backfilled",
        "performs no network request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in bundle
        assert forbidden not in fixture
