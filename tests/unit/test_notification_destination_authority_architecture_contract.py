from pathlib import Path


def test_destination_authority_is_bounded_active_and_side_effect_free() -> None:
    source = Path(
        "src/orchestration/portfolio_risk_notification_destination_authority.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/portfolio-risk-notification-destination-authority.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-destination-authority-v1",
        "load_notification_destinations",
        "evaluate_destination_activation",
        "notification destination is not active",
        "endpoint environment does not match delivery configuration",
        "outside the destination allow-list",
        "payload_sha256",
        "authority_id does not match content",
        "destination authority must not be a symbolic link",
    ):
        assert required in source

    for required in (
        '"external_request_performed": false',
        '"delivery_attempt_written": false',
        '"outbox_mutated": false',
        '"acknowledgement_mutated": false',
    ):
        assert required in source.casefold()

    for forbidden in (
        "requests.post(",
        "urllib.request",
        "httpx.",
        "psycopg.connect",
    ):
        assert forbidden not in source.casefold()

    for required in (
        "primary arc42 block: `orchestration`",
        "committed destination is disabled",
        "endpoint environment-variable name, but not its value",
        "does not yet grant",
        "terraform apply",
    ):
        assert required in normalized_docs
