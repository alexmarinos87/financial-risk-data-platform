from pathlib import Path


def test_controlled_receiver_rehearsal_history_is_append_only_and_no_network() -> None:
    contract = Path(
        "src/warehouse/controlled_receiver_rehearsal_contract.py"
    ).read_text(encoding="utf-8")
    recorder = Path(
        "src/warehouse/controlled_receiver_rehearsal_recorder.py"
    ).read_text(encoding="utf-8")
    checker = Path(
        "src/warehouse/controlled_receiver_rehearsal_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    checks = Path(
        "sql/controlled_notification_receiver_rehearsal_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    schema = Path(
        "sql/controlled_notification_receiver_rehearsal_schema.sql"
    ).read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    docs = Path(
        "docs/controlled-notification-receiver-rehearsal-history.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for status in (
        "completed",
        "rejected_before_request",
        "failed_during_rehearsal",
    ):
        assert status in contract
        assert status in schema
        assert status in checker
        assert status in docs

    for required in (
        "validate_notification_activation_checklist",
        "same_content_duplicate_count",
        "review window",
        "payload_bodies_recorded",
        "external_request_performed",
    ):
        assert required in contract

    for required in (
        "notification_activation_checklists",
        "controlled_notification_receiver_rehearsals",
        "ON CONFLICT DO NOTHING",
        "request_id already exists with different rehearsal evidence",
    ):
        assert required in recorder

    for required in (
        "reject_notification_activation_checklist_mutation",
        "reject_controlled_receiver_rehearsal_mutation",
        "BEFORE UPDATE OR DELETE",
        "document_sha256",
    ):
        assert required in schema

    for required in (
        "CONTRACT-REHEARSAL-COMPLETED",
        "CONTRACT-REHEARSAL-FAILED",
        "CONTRACT-REHEARSAL-REJECTED",
        "exact controlled receiver retry did not converge",
        "append-only rehearsal mutation was accepted",
        "external_request_performed",
    ):
        assert required in checker

    for required in (
        "controlled_receiver_completed_rows_valid",
        "controlled_receiver_failed_rows_valid",
        "controlled_receiver_rejected_rows_valid",
        "controlled_receiver_rehearsal_side_effects_safe",
        "controlled_receiver_rehearsals_inside_review_window",
    ):
        assert required in checks

    assert (
        "22_controlled_notification_receiver_rehearsal_schema.sql:ro"
        in compose
    )
    assert "controlled_receiver_rehearsal_postgres_contract_check" in makefile

    for required in (
        "primary arc42 blocks: `orchestration` and `warehouse`",
        "exact request replay convergence",
        "append-only postgresql history",
        "ordinary validation performs no dns lookup",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "urllib.request",
        "requests.post(",
        "httpx.",
    ):
        assert forbidden not in contract.casefold()
        assert forbidden not in recorder.casefold()
        assert forbidden not in checker.casefold()
