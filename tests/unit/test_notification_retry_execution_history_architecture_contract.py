from pathlib import Path


def test_retry_execution_history_is_append_only_and_side_effect_bounded() -> None:
    schema = Path(
        "sql/portfolio_risk_notification_retry_execution_schema.sql"
    ).read_text(encoding="utf-8")
    contract = Path(
        "src/warehouse/notification_retry_execution_contract.py"
    ).read_text(encoding="utf-8")
    recorder = Path(
        "src/warehouse/notification_retry_execution_recorder.py"
    ).read_text(encoding="utf-8")
    wrapper = Path(
        "src/orchestration/run_recorded_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-retry-execution-history.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-retry-execution-record-v1",
        "failed_before_request",
        "failed_after_request",
        "persistence_uncertain",
        "requested_event_ids",
        "persisted_event_ids",
        "persisted_attempt_ids",
        "document_sha256",
    ):
        assert required in contract

    for required in (
        "portfolio_risk_notification_retry_executions",
        "reject_notification_retry_execution_mutation",
        "BEFORE UPDATE",
        "BEFORE DELETE",
        "UNIQUE",
    ):
        assert required in schema

    for required in (
        "record_notification_retry_execution",
        "request_id already exists with different retry execution evidence",
        "ON CONFLICT DO NOTHING",
    ):
        assert required in recorder

    for required in (
        "execute_portfolio_risk_notification_retries",
        "record_notification_retry_execution",
        "persistence_uncertain",
        "failed_after_request",
        "failed_before_request",
        "attempt_persistence_uncertain",
    ):
        assert required in wrapper

    for required in (
        "append-only",
        "exact retry convergence",
        "ambiguous remote outcome",
        "no webhook request in ordinary ci",
        "terraform apply",
    ):
        assert required in normalized_docs
