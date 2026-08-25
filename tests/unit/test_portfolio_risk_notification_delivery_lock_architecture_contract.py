from pathlib import Path


def test_notification_delivery_is_serialized_before_evidence_and_network_io() -> None:
    lock_source = Path(
        "src/orchestration/portfolio_risk_notification_delivery_lock.py"
    ).read_text(encoding="utf-8")
    contract_source = Path(
        "src/warehouse/notification_delivery_lock_contract_check.py"
    ).read_text(encoding="utf-8")
    initial_source = Path(
        "src/orchestration/deliver_portfolio_risk_notifications.py"
    ).read_text(encoding="utf-8")
    retry_source = Path(
        "src/orchestration/execute_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-delivery-locking.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-delivery-lock-v1",
        "pg_try_advisory_lock",
        "pg_advisory_unlock",
        "Another notification delivery execution already holds the global lock",
        "key_fingerprint",
    ):
        assert required in lock_source
    assert "SELECT pg_advisory_lock" not in lock_source

    for source in (initial_source, retry_source):
        for required in (
            "DeliveryLockFactory",
            "acquire_notification_delivery_lock",
            "with selected_lock_factory(dsn=dsn)",
            "held_through_attempt_persistence",
            "concurrency_control",
        ):
            assert required in source

    initial_lock = initial_source.index("with selected_lock_factory(dsn=dsn)")
    initial_read = initial_source.index(
        "candidates = _read_and_validate_candidates",
        initial_lock,
    )
    initial_send = initial_source.index(
        "summary[\"execution\"] = _execute_initial_attempts",
        initial_lock,
    )
    assert initial_lock < initial_read < initial_send

    retry_lock = retry_source.index("with selected_lock_factory(dsn=dsn)")
    retry_read = retry_source.index("candidates = selected_reader", retry_lock)
    retry_revalidation = retry_source.index(
        "assert_retry_plan_is_current",
        retry_read,
    )
    retry_send = retry_source.index("response_status = selected_transport", retry_lock)
    assert retry_lock < retry_read < retry_revalidation < retry_send
    assert "held_through_revalidation" in retry_source

    for required in (
        "contender_rejected",
        "lock_reacquired_after_release",
        "with acquire_notification_delivery_lock(dsn=dsn) as first",
        "with acquire_notification_delivery_lock(dsn=dsn) as second",
        "external_request_performed",
        "delivery_attempt_written",
    ):
        assert required in contract_source
    assert "src.warehouse.notification_delivery_lock_contract_check" in makefile
    assert makefile.index("notification_delivery_lock_contract_check") < makefile.index(
        "postgres_consistency",
        makefile.index("postgres-contract-check:"),
    )

    for required in (
        "# PostgreSQL Notification Delivery Concurrency Control",
        "Primary arc42 block: `orchestration`",
        "non-blocking",
        "before candidate selection",
        "before current-evidence revalidation",
        "held through current-evidence revalidation and attempt persistence",
        "stable notification `Idempotency-Key`",
        "PostgreSQL 16 contention contract",
        "performs no webhook request",
        "terraform apply",
    ):
        assert required.casefold() in normalized_docs
