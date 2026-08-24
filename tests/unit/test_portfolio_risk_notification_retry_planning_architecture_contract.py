from pathlib import Path


def test_dead_letter_retry_planning_is_deterministic_and_delivery_free() -> None:
    source = Path(
        "src/orchestration/plan_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-retry-planning.md"
    ).read_text(encoding="utf-8")
    config = Path("config/notification_delivery.yaml").read_text(
        encoding="utf-8"
    )

    for required in (
        "portfolio-risk-dead-letter-retry-plan-v1",
        "retryable",
        "not_yet_eligible",
        "attempts_exhausted",
        "expired",
        "acknowledged",
        "invalid",
        "max_candidate_rows",
        "max_plan_events",
        "delivery_attempt_written",
        "dead_letter_mutated",
        "portfolio_risk_notification_delivery_attempts",
        "portfolio_risk_limit_acknowledgements",
    ):
        assert required in source

    assert "--execute" not in source
    assert "urllib.request" not in source
    assert "INSERT INTO" not in source
    assert "UPDATE risk_platform" not in source
    assert "DELETE FROM risk_platform" not in source

    for required in (
        "# Bounded Dead-Letter Retry Planning",
        "Primary arc42 block: `orchestration`",
        "classification precedence",
        "exact event identities",
        "no webhook request",
        "no delivery attempt",
        "no dead-letter mutation",
        "P4c",
    ):
        assert required.casefold() in docs.casefold()

    for required in (
        "retry_planning:",
        "max_candidate_rows:",
        "max_plan_events:",
        "max_event_age_seconds:",
        "max_backoff_seconds:",
        "retryable_http_statuses:",
        "retryable_error_codes:",
    ):
        assert required in config


def test_postgres_contract_executes_delivery_free_retry_planning() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "src.orchestration.plan_portfolio_risk_notification_retries" in workflow
    assert "--planned-at 2026-12-31T23:59:59Z" in workflow
    assert "notification-retry-plan-contract.json" in workflow
