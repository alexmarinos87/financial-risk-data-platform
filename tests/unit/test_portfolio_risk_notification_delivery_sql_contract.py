from pathlib import Path


def test_delivery_schema_is_append_only_and_status_is_derived() -> None:
    sql = Path(
        "sql/portfolio_risk_notification_delivery_schema.sql"
    ).read_text(encoding="utf-8")

    for required in (
        "portfolio_risk_notification_delivery_attempts",
        "UNIQUE (event_id, channel, attempt_number)",
        "idx_portfolio_notification_delivery_one_success",
        "portfolio_risk_notification_delivery_status",
        "portfolio_risk_notification_delivery_pending",
        "portfolio_risk_notification_delivery_succeeded",
        "COALESCE(",
        "BOOL_OR(attempt.outcome = 'succeeded')",
        "idempotency_key = event_id",
        "payload_sha256 ~ '^[0-9a-f]{64}$'",
    ):
        assert required in sql

    assert "UPDATE risk_platform.portfolio_risk_notification_outbox" not in sql


def test_delivery_reconciliation_covers_identity_attempts_and_status() -> None:
    sql = Path(
        "sql/portfolio_risk_notification_delivery_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for required in (
        "notification_delivery_attempts_reference_outbox",
        "notification_delivery_identity_valid",
        "notification_delivery_outcomes_valid",
        "notification_delivery_attempt_numbers_unique",
        "notification_delivery_success_unique",
        "notification_delivery_attempt_numbers_contiguous",
        "notification_delivery_status_matches_attempts",
        "notification_delivery_status_partitions_current_outbox",
    ):
        assert required in sql
