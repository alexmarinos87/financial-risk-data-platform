from pathlib import Path


def test_transition_rehearsal_is_chained_no_network_and_secret_free() -> None:
    source = Path(
        "src/orchestration/notification_destination_transition_rehearsal.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-destination-transition-rehearsal.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "validate_notification_destination_transition_plan",
        "validate_target_destination_authority",
        "ControlledNotificationReceiver",
        "rotation target does not equal disablement current state",
        "disablement target does not equal rollback current state",
        "rollback does not reference the exact disablement plan",
        "_assert_stale_authority_rejected",
        '"request_count": 0',
        '"receiver_summary": None',
        '"target_authority_required": False',
        '"external_request_performed": False',
        '"socket_opened": False',
        '"dns_lookup_performed": False',
    ):
        assert required in source

    for required in (
        "primary arc42 block: `orchestration`",
        "complete destination rotation, disablement, and rollback chain",
        "stale authority therefore cannot authorise",
        "authority-free and request-free",
        "opens no socket",
        "performs no dns lookup",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "requests.post(",
        "urllib.request",
        "httpx.",
        "socket.socket",
    ):
        assert forbidden not in source.casefold()
