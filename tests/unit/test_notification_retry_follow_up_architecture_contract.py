from pathlib import Path


def test_retry_follow_up_is_current_read_only_and_postgres_validated() -> None:
    schema = Path(
        "sql/portfolio_risk_notification_retry_follow_up_schema.sql"
    ).read_text(encoding="utf-8")
    checks = Path(
        "sql/portfolio_risk_notification_retry_follow_up_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/notification_retry_follow_up_contract_check.py"
    ).read_text(encoding="utf-8")
    postgres_check = Path(
        "src/warehouse/notification_retry_follow_up_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-retry-follow-up.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for view in (
        "notification_retry_execution_event_history",
        "latest_notification_retry_execution_by_event",
        "current_notification_retry_request_failures",
        "current_notification_retry_persistence_uncertainty",
        "current_notification_retry_follow_up",
        "current_notification_delivery_failures",
        "current_notification_ambiguous_outcomes",
    ):
        assert f"risk_platform.{view}" in schema

    for required in (
        "jsonb_array_elements_text(execution.requested_event_ids_json)",
        "finished_at desc",
        "record_id desc",
        "event_ordinal desc",
        "persistence_review_required",
        "initial_delivery_required",
        "execution_review_required",
        "retry_plan_required",
        "follow_up_reason not in ('delivered', 'acknowledged')",
    ):
        assert required in schema.casefold()

    assert schema.index("WHEN evidence.delivered") < schema.index(
        "WHEN evidence.acknowledgement_id IS NOT NULL"
    )
    assert schema.index("WHEN evidence.acknowledgement_id IS NOT NULL") < schema.index(
        "WHEN evidence.uncertainty_record_id IS NOT NULL"
    )
    assert schema.index("WHEN evidence.uncertainty_record_id IS NOT NULL") < schema.index(
        "WHEN evidence.attempt_count = 0"
    )

    for required in (
        "notification_retry_latest_event_selection_current",
        "notification_retry_current_uncertainty_valid",
        "notification_retry_follow_up_covers_pending_events",
        "notification_retry_delivery_failure_partition_matches",
        "notification_retry_ambiguous_partition_matches",
    ):
        assert required in checks

    for required in (
        "_insert_evaluation_and_event",
        "_insert_attempt",
        "_insert_execution_record",
        "_assert_follow_up_rows",
        "connection.rollback()",
    ):
        assert required in fixture or required in postgres_check

    assert "follow-up-event-uncertain" in postgres_check
    assert "follow-up-event-superseded" in postgres_check
    assert "superseded_uncertainty_excluded" in postgres_check
    assert "external_request_performed" in postgres_check
    assert "20_portfolio_risk_notification_retry_follow_up_schema.sql:ro" in compose
    assert "notification_retry_follow_up_postgres_contract_check" in makefile

    for required in (
        "primary arc42 block: `warehouse`",
        "one row per current notification",
        "supersedes an older `persistence_uncertain`",
        "initial_delivery_required",
        "ordinary ci performs no external request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "requests.post(",
        "urllib.request",
        "httpx.",
        "terraform apply",
    ):
        assert forbidden not in postgres_check.casefold()
