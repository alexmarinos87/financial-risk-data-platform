from pathlib import Path


def test_controlled_receiver_is_no_network_bounded_and_documented() -> None:
    source = Path(
        "src/orchestration/controlled_notification_receiver.py"
    ).read_text(encoding="utf-8")
    tests = Path(
        "tests/unit/test_controlled_notification_receiver.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/controlled-notification-receiver.md").read_text(
        encoding="utf-8"
    )
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "ControlledNotificationReceiver",
        "validate_notification_activation_checklist",
        "Idempotency-Key must equal the payload event_id",
        "Idempotency-Key was reused with a different payload",
        "MAX_PAYLOAD_BYTES = 65_536",
        "MAX_REQUESTS = 100",
        "MAX_TIMEOUT_SECONDS = 30.0",
        '"external_request_performed": False',
        '"socket_opened": False',
        '"dns_lookup_performed": False',
        '"delivery_attempt_written": False',
    ):
        assert required in source

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.create_connection",
        "asyncio.open_connection",
    ):
        assert forbidden not in source.casefold()

    for required in (
        "same-content duplicate",
        "same key + different payload sha-256",
        "headers are exactly",
        "no payload body",
        "ordinary ci no-network",
        "terraform apply",
    ):
        assert required in normalized_docs

    for required in (
        "test_receiver_accepts_same_content_duplicate_without_network",
        "test_idempotency_key_reuse_with_changed_payload_fails_closed",
        "test_checklist_endpoint_and_header_controls_fail_closed",
        "test_payload_event_and_bound_controls_fail_closed",
    ):
        assert required in tests
