from pathlib import Path


def test_retry_execution_history_is_append_only_bounded_and_secret_safe() -> None:
    contract = Path(
        "src/warehouse/notification_retry_execution_contract.py"
    ).read_text(encoding="utf-8")
    reader = Path(
        "src/warehouse/notification_retry_execution_reader.py"
    ).read_text(encoding="utf-8")
    recorder = Path(
        "src/warehouse/notification_retry_execution_recorder.py"
    ).read_text(encoding="utf-8")
    wrapper = Path(
        "src/orchestration/run_recorded_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    lock_check = Path(
        "src/warehouse/notification_delivery_lock_contract_check.py"
    ).read_text(encoding="utf-8")
    schema = Path(
        "sql/portfolio_risk_notification_retry_execution_schema.sql"
    ).read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
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
        "execution_summary",
        "lock_key_fingerprint",
        "validate_retry_execution_record",
    ):
        assert required in contract

    for required in (
        "read_notification_retry_execution_request",
        "where request_id = %s",
        "document_sha256",
        "validate_retry_execution_record",
        "lookup failed before execution",
    ):
        assert required in reader.casefold()

    for required in (
        "record_notification_retry_execution",
        "request_id already exists with different retry evidence",
        "record_id already exists with different retry evidence",
        "document_sha256",
    ):
        assert required in recorder

    for required in (
        "execute_portfolio_risk_notification_retries",
        "read_notification_retry_execution_request",
        "tracking_transport",
        "tracking_writer",
        "record_notification_retry_execution",
        "RecordedRetryExecutionError",
        "request_id already exists for a different notification retry plan",
        "--execute",
    ):
        assert required in wrapper

    assert wrapper.index("if execute is not True") < wrapper.index(
        "selected_history_reader"
    )
    assert wrapper.index("selected_history_reader") < wrapper.index(
        "selected_clock ="
    )
    assert "except BaseException" not in wrapper
    assert "response body" not in wrapper.casefold()

    for required in (
        "portfolio_risk_notification_retry_executions",
        "requested_event_ids_json",
        "persisted_event_ids_json",
        "reject_notification_retry_execution_mutation",
        "before update or delete",
        "request_id text not null unique",
    ):
        assert required in schema.casefold()

    assert "19_portfolio_risk_notification_retry_execution_schema.sql" in compose
    assert "retry_history_exact_retry_converged" in lock_check
    assert "retry_history_preflight_read_validated" in lock_check
    assert "retry_history_append_only" in lock_check

    for required in (
        "primary arc42 block: `warehouse`",
        "exact retry convergence",
        "persistence_uncertain",
        "does not claim success",
        "stable notification `idempotency-key`",
        "full endpoint urls",
        "ordinary ci performs no webhook request",
        "terraform apply",
        "separate follow-up pr",
    ):
        assert required in normalized_docs
