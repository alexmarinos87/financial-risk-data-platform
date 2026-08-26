from pathlib import Path


def test_destination_authority_is_secret_free_and_side_effect_free() -> None:
    source = Path(
        "src/orchestration/portfolio_risk_notification_destination_authority.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-destination-authority.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-destination-authority-v1",
        "resolve_notification_destination_authority",
        "delivery_endpoint_env",
        "require_active",
        "evaluated_event_types",
        "destination_fingerprint",
        "endpoint_value_recorded",
        "external_request_performed",
    ):
        assert required in source

    for forbidden in (
        "urllib.request",
        "requests.post(",
        "httpx.",
        "--execute",
        "terraform apply",
    ):
        assert forbidden not in source.casefold()

    for required in (
        "primary arc42 block: `orchestration`",
        "clock-derived",
        "endpoint-environment identity",
        "event allow-list",
        "no endpoint value",
        "ordinary ci performs no external request",
        "terraform apply",
        "separate dependent pr",
    ):
        assert required in normalized_docs
