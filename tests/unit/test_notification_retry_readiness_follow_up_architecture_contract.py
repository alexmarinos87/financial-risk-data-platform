from pathlib import Path


def test_readiness_aware_retry_views_preserve_current_selection_and_safety() -> None:
    schema = Path("sql/notification_retry_readiness_follow_up_schema.sql").read_text(
        encoding="utf-8"
    )
    checks = Path(
        "sql/notification_retry_readiness_follow_up_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/notification_retry_readiness_follow_up_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/notification-retry-readiness-follow-up.md").read_text(
        encoding="utf-8"
    )
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for view in (
        "notification_retry_readiness_execution_history",
        "latest_notification_retry_readiness_by_event",
        "current_notification_retry_readiness_follow_up",
        "current_notification_retry_readiness_failures",
        "current_notification_retry_readiness_ambiguities",
        "current_notification_retry_readiness_binding_reviews",
        "current_notification_retry_readiness_bound",
    ):
        assert f"risk_platform.{view}" in schema

    for required in (
        "readiness.terminal_record_id = history.record_id",
        "readiness.terminal_record_id = latest.record_id",
        "readiness.record_id = follow_up.latest_execution_record_id",
        "readiness_binding_missing",
        "readiness_destination_mismatch",
        "where delivery_failure",
        "where ambiguous_outcome",
    ):
        assert required in schema.casefold()

    for required in (
        "notification_retry_readiness_history_preserves_grain",
        "notification_retry_readiness_latest_selection_matches",
        "notification_retry_readiness_follow_up_covers_current",
        "notification_retry_readiness_review_partition_matches",
        "notification_retry_readiness_failure_partition_matches",
        "notification_retry_readiness_ambiguity_partition_matches",
        "notification_retry_readiness_superseded_binding_excluded",
    ):
        assert required in checks

    for required in (
        "_assert_bound_row",
        "_assert_superseded_row",
        "binding-history-terminal-001",
        "readiness-follow-up-superseding",
        "superseded_readiness_binding_excluded",
        "connection.rollback()",
    ):
        assert required in fixture.casefold()

    assert "27_notification_retry_readiness_follow_up_schema.sql:ro" in compose

    for required in (
        "primary arc42 block: `warehouse`",
        "one readiness-aware current row per event",
        "readiness_binding_missing",
        "older superseded execution cannot leak",
        "ordinary ci performs no network request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "requests.",
        "urllib.request",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in fixture.casefold()
