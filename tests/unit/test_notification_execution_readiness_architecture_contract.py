from __future__ import annotations

from pathlib import Path


MODULE = Path("src/warehouse/notification_execution_readiness_gate.py")
DOC = Path("docs/notification-execution-readiness-gate.md")


def test_readiness_gate_is_present_in_warehouse_boundary() -> None:
    source = MODULE.read_text(encoding="utf-8")

    assert "portfolio-risk-notification-execution-readiness-gate-v1" in source
    assert "current_notification_activation_rehearsal_review" in source
    assert "current_notification_destination_transition_review" in source
    assert "current_notification_retry_destination_ambiguities" in source
    assert "BLOCKING_REASON_ORDER" in source


def test_readiness_gate_is_read_only_and_has_no_transport() -> None:
    source = MODULE.read_text(encoding="utf-8")
    lowered = source.lower()

    assert "urllib" not in source
    assert "import socket" not in source
    assert "urlopen" not in source
    assert "insert into" not in lowered
    assert "update risk_platform" not in lowered
    assert "delete from" not in lowered
    assert "terraform apply" not in lowered
    assert '"external_request_performed": False' in source
    assert '"delivery_attempt_written": False' in source
    assert '"outbox_mutated": False' in source
    assert '"acknowledgement_mutated": False' in source


def test_readiness_gate_has_no_execute_cli_option() -> None:
    source = MODULE.read_text(encoding="utf-8")

    assert 'add_argument("--execute"' not in source
    assert "action=\"store_true\"" not in source


def test_readiness_gate_documentation_preserves_disabled_default() -> None:
    document = DOC.read_text(encoding="utf-8")

    assert "read-only" in document.lower()
    assert "disabled by default" in document.lower()
    assert "P4d5a" in document
    assert "persistence_ambiguity" in document
