from pathlib import Path


def test_retry_destination_authority_is_locked_and_durable() -> None:
    executor = Path(
        "src/orchestration/execute_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    wrapper = Path(
        "src/orchestration/run_recorded_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    schema = Path(
        "sql/portfolio_risk_notification_retry_destination_binding_schema.sql"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-retry-destination-authority.md"
    ).read_text(encoding="utf-8")
    normalized = " ".join(docs.split()).casefold()

    for required in (
        "resolve_notification_destination_authority",
        "assert_retry_plan_is_current",
        "destination_authority_observer",
        "destination_authority_id",
        "destination_fingerprint",
        "--destination-config",
        "--destination-id",
    ):
        assert required in executor
    assert executor.index("assert_retry_plan_is_current") < executor.index(
        "selected_authority_resolver("
    )
    assert executor.index("selected_authority_resolver(") < executor.index(
        "selected_transport("
    )

    for required in (
        "observe_destination_authority",
        "record_notification_retry_destination_binding",
        "destination_history",
        "destination_authority_observer=observe_destination_authority",
    ):
        assert required in wrapper
    for required in (
        "portfolio_risk_notification_retry_destination_bindings",
        "record_id TEXT NOT NULL UNIQUE REFERENCES",
        "BEFORE UPDATE OR DELETE",
    ):
        assert required in schema
    for required in (
        "primary arc42 block: `orchestration`",
        "refreshed under the lock",
        "append-only destination binding",
        "performs no external webhook request",
        "terraform apply",
    ):
        assert required in normalized
