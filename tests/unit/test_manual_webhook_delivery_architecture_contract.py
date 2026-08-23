from pathlib import Path


def test_manual_webhook_delivery_is_disabled_secret_safe_and_manual() -> None:
    source = Path(
        "src/orchestration/deliver_portfolio_risk_notifications.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/manual-webhook-delivery.md").read_text(
        encoding="utf-8"
    )
    config = Path("config/notification_delivery.yaml").read_text(
        encoding="utf-8"
    )

    assert "enabled: false" in config
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in config
    assert "https://" not in config

    for required in (
        "Idempotency-Key",
        "--execute",
        "response_bodies_recorded",
        "max_attempts_per_event",
        "initial_backoff_seconds",
        "portfolio_risk_notification_delivery_attempts",
    ):
        assert required in source

    for required in (
        "Disabled-by-default",
        "append-only",
        "Idempotency-Key",
        "Response bodies",
        "CI uses fake transports",
        "does not schedule itself",
    ):
        assert required.lower() in docs.lower()
