from __future__ import annotations

from pathlib import Path


CONTRACT = Path("src/warehouse/notification_execution_readiness_history_contract.py")
RECORDER = Path("src/warehouse/notification_execution_readiness_recorder.py")
SCHEMA = Path("sql/notification_execution_readiness_schema.sql")
CHECKS = Path("sql/notification_execution_readiness_consistency_checks.sql")
DOC = Path("docs/notification-execution-readiness-history.md")


def test_history_contract_and_schema_are_present() -> None:
    contract = CONTRACT.read_text(encoding="utf-8")
    schema = SCHEMA.read_text(encoding="utf-8")

    assert "portfolio-risk-notification-execution-readiness-record-v1" in contract
    assert "notification_execution_readiness_decisions" in schema
    assert "latest_notification_execution_readiness_decisions" in schema
    assert "current_notification_execution_readiness_review" in schema


def test_current_views_partition_all_review_states() -> None:
    schema = SCHEMA.read_text(encoding="utf-8")

    for status in (
        "decision_missing",
        "decision_superseded",
        "decision_stale",
        "blocked",
        "allowed",
    ):
        assert status in schema
    for suffix in ("allowed", "blocked", "stale", "superseded", "missing"):
        assert f"current_notification_execution_readiness_{suffix}" in schema


def test_history_is_append_only_and_reconciled() -> None:
    schema = SCHEMA.read_text(encoding="utf-8")
    checks = CHECKS.read_text(encoding="utf-8")

    assert "notification_execution_readiness_reject_update" in schema
    assert "notification_execution_readiness_reject_delete" in schema
    assert "append-only" in schema
    assert "notification_execution_readiness_append_only_triggers_enabled" in checks
    assert "notification_execution_readiness_review_status_reconciles" in checks


def test_history_code_has_no_notification_transport() -> None:
    source = CONTRACT.read_text(encoding="utf-8") + RECORDER.read_text(
        encoding="utf-8"
    )
    lowered = source.lower()

    assert "urllib" not in source
    assert "import socket" not in source
    assert "urlopen" not in source
    assert "terraform apply" not in lowered
    assert "delivery_attempts" not in source
    assert "notification_outbox" not in source


def test_documentation_preserves_disabled_default_and_next_boundary() -> None:
    document = DOC.read_text(encoding="utf-8")

    assert "P4d5b" in document
    assert "P4d5c" in document
    assert "disabled by default" in document
    assert "five minutes" in document
