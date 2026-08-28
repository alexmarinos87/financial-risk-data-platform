from pathlib import Path


def test_controlled_receiver_review_views_are_current_read_only_and_reconciled() -> None:
    schema = Path(
        "sql/controlled_notification_receiver_review_schema.sql"
    ).read_text(encoding="utf-8")
    checks = Path(
        "sql/controlled_notification_receiver_review_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/controlled_receiver_review_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    docs = Path(
        "docs/controlled-notification-receiver-review-views.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for view in (
        "latest_notification_activation_checklists",
        "latest_controlled_notification_receiver_rehearsals",
        "current_notification_activation_rehearsal_review",
        "current_notification_activation_review_failures",
        "current_notification_activation_ready",
    ):
        assert f"risk_platform.{view}" in schema

    for status in (
        "checklist_incomplete",
        "checklist_not_yet_active",
        "checklist_expired",
        "rehearsal_missing",
        "rehearsal_evidence_conflict",
        "rehearsal_superseded",
        "rehearsal_rejected",
        "rehearsal_failed",
        "ready",
    ):
        assert status in schema
        assert status in fixture
        assert status in docs

    for required in (
        "reviewed_at DESC, checklist.checklist_id DESC",
        "recorded_at DESC, rehearsal.record_id DESC",
        "incomplete_controls_json",
        "rehearsal_reference_consistent",
        "rehearsal_matches_current_checklist",
        "operational_activation_ready",
    ):
        assert required in schema

    for required in (
        "notification_activation_latest_checklist_selection_current",
        "controlled_receiver_latest_rehearsal_selection_current",
        "notification_activation_incomplete_controls_reconcile",
        "notification_activation_review_status_reconciles",
        "notification_activation_failure_partition_matches",
        "notification_activation_ready_partition_matches",
    ):
        assert required in checks

    for required in (
        "review-ready",
        "review-incomplete",
        "review-not-yet",
        "review-expired",
        "review-missing",
        "review-rejected",
        "review-failed",
        "review-superseded",
        "review_failure_rows",
        "external_request_performed",
    ):
        assert required in fixture

    assert "23_controlled_notification_receiver_review_schema.sql:ro" in compose
    assert "controlled_receiver_review_postgres_contract_check" in makefile

    for required in (
        "primary arc42 block: `warehouse`",
        "one current, reviewer-oriented state per notification destination",
        "historical rows remain immutable",
        "newer checklist can therefore supersede a successful rehearsal",
        "ordinary validation performs no dns lookup",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "urllib.request",
        "requests.post(",
        "httpx.",
    ):
        assert forbidden not in fixture.casefold()
