from pathlib import Path


def test_destination_retry_follow_up_is_current_read_only_and_reconciled() -> None:
    schema = Path(
        "sql/portfolio_risk_notification_retry_destination_follow_up_schema.sql"
    ).read_text(encoding="utf-8")
    checks = Path(
        "sql/portfolio_risk_notification_retry_destination_follow_up_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/notification_retry_follow_up_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-retry-destination-follow-up.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for view in (
        "notification_retry_destination_execution_history",
        "latest_notification_retry_destination_by_event",
        "current_notification_retry_destination_follow_up",
        "current_notification_retry_destination_failures",
        "current_notification_retry_destination_ambiguities",
        "current_notification_retry_destination_binding_reviews",
    ):
        assert f"risk_platform.{view}" in schema

    for required in (
        "binding.record_id = history.record_id",
        "binding.record_id = latest.record_id",
        "destination_binding_missing",
        "destination_review_required",
        "where delivery_failure",
        "where ambiguous_outcome",
    ):
        assert required in schema.casefold()

    for required in (
        "notification_retry_destination_history_preserves_grain",
        "notification_retry_destination_latest_selection_matches",
        "notification_retry_destination_follow_up_covers_current",
        "notification_retry_destination_binding_review_partition_matches",
        "notification_retry_destination_failure_partition_matches",
        "notification_retry_destination_ambiguity_partition_matches",
    ):
        assert required in checks

    for required in (
        "_insert_destination_binding",
        "_assert_destination_rows",
        "follow-up-event-uncertain",
        "follow-up-event-superseded",
        "superseded_destination_binding_excluded",
        "connection.rollback()",
    ):
        assert required in fixture

    assert (
        "21_portfolio_risk_notification_retry_destination_follow_up_schema.sql:ro"
        in compose
    )

    for required in (
        "primary arc42 block: `warehouse`",
        "one row per current pending notification",
        "destination_binding_missing",
        "older superseded retry record cannot leak",
        "ordinary ci performs no external webhook request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "requests.post(",
        "urllib.request",
        "httpx.",
    ):
        assert forbidden not in fixture.casefold()
