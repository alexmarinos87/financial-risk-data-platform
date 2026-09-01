from pathlib import Path


def test_transition_history_is_append_only_current_and_no_network() -> None:
    contract = Path(
        "src/warehouse/notification_destination_transition_rehearsal_contract.py"
    ).read_text(encoding="utf-8")
    recorder = Path(
        "src/warehouse/notification_destination_transition_rehearsal_recorder.py"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/"
        "notification_destination_transition_rehearsal_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    schema = Path(
        "sql/notification_destination_transition_rehearsal_schema.sql"
    ).read_text(encoding="utf-8")
    checks = Path(
        "sql/notification_destination_transition_rehearsal_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-destination-transition-rehearsal-history.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "validate_notification_destination_transition_rehearsal",
        "disable stage must be authority-free and request-free",
        "receiver receipt ordinals are not contiguous",
        "duplicate receipt does not reuse identical content",
        "stale authority rejection evidence is incomplete",
        "transition rehearsal record is not canonical",
    ):
        assert required in contract

    for required in (
        "where request_id = %s",
        "request_id already exists with different transition evidence",
        "rehearsal_id already exists under different evidence",
        "on conflict do nothing",
        "record_json, document_sha256",
    ):
        assert required in recorder.casefold()

    for view in (
        "latest_notification_destination_transition_rehearsals",
        "current_notification_destination_transition_review",
        "current_notification_destination_transition_review_failures",
        "current_notification_destination_transition_ready",
    ):
        assert f"risk_platform.{view}" in schema

    for status in (
        "activation_not_ready",
        "transition_rehearsal_missing",
        "transition_rehearsal_superseded",
        "ready",
    ):
        assert status in schema
        assert status in docs

    for required in (
        "notification_destination_transition_request_ids_unique",
        "notification_destination_transition_disable_stages_safe",
        "notification_destination_transition_latest_selection_current",
        "notification_destination_transition_review_status_reconciles",
        "notification_destination_transition_append_only_triggers_enabled",
    ):
        assert required in checks

    for required in (
        "initial_review_status",
        "transition_rehearsal_superseded",
        "conflicting_request_rejected",
        "update_rejected",
        "delete_rejected",
        "external_request_performed",
    ):
        assert required in fixture

    for required in (
        "primary arc42 blocks: `orchestration` and `warehouse`",
        "independently checks",
        "authority-free and request-free disablement stage",
        "newer receiver checklist and successful controlled rehearsal",
        "ordinary validation performs no dns lookup",
        "terraform apply",
    ):
        assert required in normalized_docs

    for source in (contract, recorder, fixture):
        lowered = source.casefold()
        for forbidden in (
            "requests.post(",
            "urllib.request",
            "httpx.",
            "socket.socket",
        ):
            assert forbidden not in lowered
