from pathlib import Path


def test_manual_webhook_delivery_is_disabled_secret_safe_and_manual() -> None:
    source = Path(
        "src/orchestration/deliver_portfolio_risk_notifications.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/manual-webhook-delivery.md").read_text(encoding="utf-8")
    config = Path("config/notification_delivery.yaml").read_text(encoding="utf-8")
    destination_config = Path(
        "config/notification_destinations.yaml"
    ).read_text(encoding="utf-8")

    assert "enabled: false" in config
    assert "enabled: false" in destination_config
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in config
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in destination_config
    assert "https://" not in config
    assert "https://" not in destination_config

    for required in (
        "Idempotency-Key",
        "--execute",
        "--destination-config",
        "--destination-id",
        "response_bodies_recorded",
        "max_attempts_per_event",
        "initial_backoff_seconds",
        "portfolio_risk_notification_delivery_attempts",
        "resolve_notification_destination_authority",
        "destination_authority",
        "require_active=True",
        "event_types=_event_types(candidates)",
    ):
        assert required in source

    assert source.index("resolve_notification_destination_authority(") < source.index(
        'summary["execution"] = _execute_initial_attempts('
    )

    for required in (
        "Disabled-by-default",
        "append-only",
        "Idempotency-Key",
        "Response bodies",
        "CI uses fake transports",
        "does not schedule itself",
        "clock-derived execution",
        "endpoint-environment identity",
        "reviewed allow-list",
        "before the first request",
    ):
        assert required.lower() in docs.lower()
