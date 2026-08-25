from pathlib import Path


def test_destination_contract_is_secret_free_reviewed_and_delivery_free() -> None:
    source = Path(
        "src/orchestration/portfolio_risk_notification_destination_contract.py"
    ).read_text(encoding="utf-8")
    config = Path("config/notification_destinations.yaml").read_text(
        encoding="utf-8"
    )
    docs = Path(
        "docs/portfolio-risk-notification-destination-ownership.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-destination-v1",
        "endpoint_env",
        "owner",
        "recipient_scope",
        "allowed_event_types",
        "change_request_id",
        "review_expires_at",
        "external_request_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
    ):
        assert required in source

    assert "--execute" not in source
    assert "urllib" not in source
    assert "requests" not in source
    assert "https://" not in config
    assert "enabled: false" in config
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in config

    for required in (
        "primary arc42 block: `governance`",
        "secret-free endpoint reference",
        "disabled-by-default activation",
        "independent reviewer",
        "controlled local receiver or fake transport",
        "does not itself authorize",
        "no external request",
        "terraform apply",
    ):
        assert required in normalized_docs
