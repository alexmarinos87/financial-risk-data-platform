from pathlib import Path


def test_initial_delivery_requires_current_refreshed_readiness_under_shared_lock() -> None:
    enforcement = Path(
        "src/warehouse/notification_execution_readiness_enforcement.py"
    ).read_text(encoding="utf-8")
    delivery = Path(
        "src/orchestration/deliver_portfolio_risk_notifications.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-execution-readiness-enforcement.md"
    ).read_text(encoding="utf-8")
    config = Path("config/notification_delivery.yaml").read_text(encoding="utf-8")

    for required in (
        "portfolio-risk-notification-execution-readiness-enforcement-v1",
        "current_notification_execution_readiness_review",
        "notification_execution_readiness_decisions",
        "validate_notification_execution_readiness_record",
        "run_notification_execution_readiness_gate",
        "MAX_READINESS_AGE = timedelta(minutes=5)",
        "substantive_evidence_match",
        "readiness serving row does not match its retained record",
        "superseded during preflight",
    ):
        assert required in enforcement

    for required in (
        "_enforce_initial_delivery_readiness",
        "_validate_initial_delivery_readiness",
        'execution_kind="initial"',
        "execution_readiness",
        "lock_evidence=lock_evidence",
    ):
        assert required in delivery

    lock_index = delivery.index("with selected_lock_factory(dsn=dsn) as lock_evidence:")
    readiness_index = delivery.index(
        "execution_readiness = _validate_initial_delivery_readiness("
    )
    transport_index = delivery.index('summary["execution"] = _execute_initial_attempts(')
    assert lock_index < readiness_index < transport_index

    for required in (
        "Primary arc42 blocks",
        "current allowed serving row",
        "append-only readiness record",
        "fresh gate evaluation",
        "same shared delivery lock",
        "substantive governed evidence",
        "before the first network request",
        "disabled by default",
    ):
        assert required.lower() in docs.lower()

    assert "enabled: false" in config
