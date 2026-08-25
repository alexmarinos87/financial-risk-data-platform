from pathlib import Path


def test_manual_retry_execution_is_exact_disabled_and_one_attempt_only() -> None:
    source = Path(
        "src/orchestration/execute_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    delivery_source = Path(
        "src/orchestration/deliver_portfolio_risk_notifications.py"
    ).read_text(encoding="utf-8")
    plan_contract = Path(
        "src/orchestration/portfolio_risk_notification_retry_plan_contract.py"
    ).read_text(encoding="utf-8")
    policy_source = Path(
        "src/orchestration/portfolio_risk_notification_retry_execution_policy.py"
    ).read_text(encoding="utf-8")
    config = Path("config/notification_delivery.yaml").read_text(
        encoding="utf-8"
    )
    docs = Path(
        "docs/portfolio-risk-notification-retry-execution.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    assert "retry_execution:" in config
    retry_execution = config.split("retry_execution:", maxsplit=1)[1]
    assert "enabled: false" in retry_execution

    for required in (
        "--plan",
        "--confirm-plan-id",
        "--request-id",
        "--execute",
        "execution_started_at",
        "assert_retry_plan_is_current",
        "Idempotency-Key",
        "response_bodies_recorded",
        "plan_mutated",
        "acknowledgement_mutated",
        "dead_letter_mutated",
    ):
        assert required in source

    assert "--executed-at" not in source
    assert "time_module.sleep" not in source
    assert "Sleeper" not in source
    assert "UPDATE risk_platform" not in source
    assert "DELETE FROM risk_platform" not in source

    for required in (
        "portfolio_risk_notification_delivery_attempts",
        "HAVING COALESCE(MAX(attempt.attempt_number), 0) = 0",
        "events with existing delivery attempts require an exact retry plan",
        "initial_attempts_only",
        "environment if environment is not None else os.environ",
    ):
        assert required in delivery_source
    assert "time_module.sleep" not in delivery_source
    assert "for attempt_number in range" not in delivery_source
    assert "ON CONFLICT (attempt_id) DO NOTHING" not in delivery_source

    for required in (
        "MAX_PLAN_FILE_BYTES",
        "exact_mapping",
        "retryable_event_ids",
        "event_document_sha256",
        "_plan_id",
        "assert_retry_plan_is_current",
    ):
        assert required in plan_contract

    for required in (
        "portfolio-risk-manual-retry-execution-v1",
        "policy.max_events > retry_policy.max_plan_events",
        "policy.max_events > delivery_config.max_batch_events",
        "policy.max_plan_age_seconds > retry_policy.max_event_age_seconds",
    ):
        assert required in policy_source

    for required in (
        "# Explicit Manual Notification Retry Execution",
        "Primary arc42 block: `orchestration`",
        "disabled-by-default",
        "before the first network request",
        "operator cannot supply or backdate",
        "one new attempt",
        "no internal retry loop",
        "stable `Idempotency-Key`",
        "fake readers, transports, clocks and attempt writers",
        "performs no external delivery",
        "does not",
        "terraform apply",
        "P4d",
    ):
        assert required.casefold() in normalized_docs
