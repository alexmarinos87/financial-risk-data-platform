from pathlib import Path


def test_retry_readiness_binding_history_is_append_only_and_source_reconciled() -> None:
    schema = Path("sql/notification_retry_readiness_binding_schema.sql").read_text(
        encoding="utf-8"
    )
    checks = Path(
        "sql/notification_retry_readiness_binding_consistency_checks.sql"
    ).read_text(encoding="utf-8")
    recorder = Path(
        "src/warehouse/notification_retry_readiness_binding_recorder.py"
    ).read_text(encoding="utf-8")
    fixture = Path(
        "src/warehouse/notification_retry_readiness_binding_postgres_contract_check.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/notification-retry-readiness-binding-history.md").read_text(
        encoding="utf-8"
    )
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    makefile = Path("Makefile").read_text(encoding="utf-8")
    normalized_schema = schema.casefold()
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "notification_retry_readiness_bindings",
        "notification_retry_readiness_binding_status",
        "notification_retry_readiness_bound",
        "notification_retry_readiness_binding_missing",
        "notification_retry_readiness_binding_reject_update",
        "notification_retry_readiness_binding_reject_delete",
        "references\n        risk_platform.portfolio_risk_notification_retry_executions",
        "references\n        risk_platform.notification_execution_readiness_decisions",
    ):
        assert required in normalized_schema

    for required in (
        "record_notification_retry_readiness_binding_with_cursor",
        "validate_notification_retry_readiness_binding",
        "validate_retry_execution_record",
        "validate_notification_execution_readiness_record",
        "retry readiness binding terminal source is missing",
        "retry readiness binding readiness source is missing",
        "does not allow execution",
        "already has different readiness evidence",
    ):
        assert required in recorder

    for required in (
        "notification_retry_readiness_terminal_sources_reconcile",
        "notification_retry_readiness_decision_sources_reconcile",
        "notification_retry_readiness_missing_partition_matches",
        "notification_retry_readiness_history_is_append_only",
    ):
        assert required in checks

    for required in (
        "exact_replay_converged",
        "conflicting_terminal_binding_rejected",
        "missing_readiness_source_rejected",
        "legacy_binding_missing_visible",
        "update_rejected",
        "delete_rejected",
    ):
        assert required in fixture

    assert "26_notification_retry_readiness_binding_schema.sql:ro" in compose
    assert (
        "notification_retry_readiness_binding_postgres_contract_check" in makefile
    )

    for required in (
        "primary arc42 blocks: `warehouse` and `orchestration`",
        "independently reloads and validates both source documents",
        "exact replay converges",
        "binding_missing",
        "performs no network request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in recorder
        assert forbidden not in fixture
