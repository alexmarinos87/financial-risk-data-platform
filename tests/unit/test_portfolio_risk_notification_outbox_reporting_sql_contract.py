from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _extract_statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    end = sql.index(";", start) + 1
    return sql[start:end]


def _create_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA risk_platform")
    connection.execute(
        """
        CREATE TABLE risk_platform.portfolio_risk_limit_actionable_transitions (
            calculation_id TEXT,
            previous_calculation_id TEXT,
            transition_type TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE risk_platform.portfolio_risk_notification_outbox (
            event_id TEXT,
            model_version TEXT,
            event_type TEXT,
            transition_type TEXT,
            delivery_disposition TEXT,
            suppression_reason TEXT,
            source_evaluation_calculation_id TEXT,
            source_previous_evaluation_calculation_id TEXT,
            risk_limit_model_version TEXT,
            policy_id TEXT,
            policy_fingerprint TEXT,
            portfolio_id TEXT,
            base_currency TEXT,
            definition_fingerprint TEXT,
            attribution_model_version TEXT,
            weighting_method TEXT,
            covariance_method TEXT,
            correlation_method TEXT,
            covariance_window INTEGER,
            annualization_days INTEGER,
            ts_event TIMESTAMPTZ,
            ts_ingest TIMESTAMPTZ,
            metric_name TEXT,
            subject_type TEXT,
            subject_key TEXT,
            previous_subject_key TEXT,
            subject_changed BOOLEAN,
            unit TEXT,
            previous_status TEXT,
            current_status TEXT,
            severity_rank INTEGER,
            observed_value DOUBLE,
            observed_signed_value DOUBLE,
            warning_threshold DOUBLE,
            critical_threshold DOUBLE,
            breach_excess DOUBLE,
            payload_json TEXT,
            loaded_at TIMESTAMPTZ
        )
        """
    )


def _outbox_row(
    *,
    event_id: str,
    transition_type: str,
    disposition: str,
    source_id: str,
    previous_id: str | None,
    event_time: datetime,
) -> tuple[object, ...]:
    current_status = (
        "critical"
        if transition_type == "escalated"
        else "warning"
        if transition_type in {"opened", "deescalated"}
        else "ok"
    )
    previous_status = (
        None
        if previous_id is None
        else "warning"
        if transition_type in {"escalated", "resolved"}
        else "critical"
    )
    return (
        event_id,
        "portfolio-risk-notification-outbox-v1",
        {
            "opened": "breach_opened",
            "escalated": "breach_escalated",
            "deescalated": "breach_deescalated",
            "resolved": "breach_resolved",
        }[transition_type],
        transition_type,
        disposition,
        "deescalation_not_routed" if disposition == "suppressed" else None,
        source_id,
        previous_id,
        "portfolio-risk-limits-v1",
        "us-tech-standard",
        "risk-limit-policy-a",
        "us-tech-equal",
        "USD",
        "definition-a",
        "portfolio-attribution-v1",
        "constant_weight_daily_rebalanced",
        "sample_annualized",
        "pearson",
        20,
        252,
        event_time,
        event_time,
        "portfolio_volatility_annualized",
        "portfolio",
        "us-tech-equal",
        None if previous_id is None else "us-tech-equal",
        False,
        "annualized_decimal",
        previous_status,
        current_status,
        {"ok": 0, "warning": 1, "critical": 2}[current_status],
        0.35,
        0.35,
        0.30,
        0.45,
        0.05 if current_status != "ok" else 0.0,
        "{}",
        event_time,
    )


def test_notification_schema_exposes_immutable_outbox_contract() -> None:
    schema_sql = _read_sql(
        "sql/portfolio_risk_notification_outbox_schema.sql"
    )

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "risk_platform.portfolio_risk_notification_outbox"
    ) in schema_sql
    assert "event_id TEXT PRIMARY KEY" in schema_sql
    assert "delivery_disposition IN ('pending', 'suppressed')" in schema_sql
    assert "deescalation_not_routed" in schema_sql
    assert "jsonb_typeof(payload_json) = 'object'" in schema_sql
    assert (
        "source_evaluation_calculation_id TEXT NOT NULL REFERENCES"
        in schema_sql
    )

    for view_name in (
        "current_portfolio_risk_notification_outbox",
        "portfolio_risk_notification_pending",
        "portfolio_risk_notification_suppressed",
        "portfolio_risk_notification_outbox_summary",
    ):
        assert f"risk_platform.{view_name}" in schema_sql


def test_current_pending_suppressed_and_summary_views_execute() -> None:
    schema_sql = _read_sql(
        "sql/portfolio_risk_notification_outbox_schema.sql"
    )
    view_names = (
        "current_portfolio_risk_notification_outbox",
        "portfolio_risk_notification_pending",
        "portfolio_risk_notification_suppressed",
        "portfolio_risk_notification_outbox_summary",
    )

    with duckdb.connect() as connection:
        _create_tables(connection)
        connection.executemany(
            """
            INSERT INTO risk_platform.portfolio_risk_limit_actionable_transitions
            VALUES (?, ?, ?)
            """,
            [
                ("evaluation-current", None, "opened"),
                (
                    "evaluation-deescalated",
                    "evaluation-critical",
                    "deescalated",
                ),
            ],
        )
        event_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
        connection.executemany(
            """
            INSERT INTO risk_platform.portfolio_risk_notification_outbox
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                _outbox_row(
                    event_id="event-current",
                    transition_type="opened",
                    disposition="pending",
                    source_id="evaluation-current",
                    previous_id=None,
                    event_time=event_time,
                ),
                _outbox_row(
                    event_id="event-stale",
                    transition_type="opened",
                    disposition="pending",
                    source_id="evaluation-stale",
                    previous_id=None,
                    event_time=event_time,
                ),
                _outbox_row(
                    event_id="event-suppressed",
                    transition_type="deescalated",
                    disposition="suppressed",
                    source_id="evaluation-deescalated",
                    previous_id="evaluation-critical",
                    event_time=event_time,
                ),
            ],
        )
        for view_name in view_names:
            prefix = (
                "CREATE OR REPLACE VIEW\n"
                "risk_platform.current_portfolio_risk_notification_outbox AS"
                if view_name == "current_portfolio_risk_notification_outbox"
                else f"CREATE OR REPLACE VIEW risk_platform.{view_name} AS"
            )
            connection.execute(_extract_statement(schema_sql, prefix))

        assert connection.execute(
            """
            SELECT event_id
            FROM risk_platform.current_portfolio_risk_notification_outbox
            ORDER BY event_id
            """
        ).fetchall() == [
            ("event-current",),
            ("event-suppressed",),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM "
            "risk_platform.portfolio_risk_notification_pending"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM "
            "risk_platform.portfolio_risk_notification_suppressed"
        ).fetchone() == (1,)
        assert connection.execute(
            """
            SELECT SUM(event_count)
            FROM risk_platform.portfolio_risk_notification_outbox_summary
            """
        ).fetchone() == (2,)


def test_notification_reconciliation_and_documentation_contract() -> None:
    consistency_sql = _read_sql(
        "sql/portfolio_risk_notification_outbox_consistency_checks.sql"
    )
    documentation = _read_sql("docs/portfolio-risk-notification-outbox.md")

    for check_name in (
        "portfolio_notification_current_rows_match_actionable_transitions",
        "portfolio_notification_pending_view_matches",
        "portfolio_notification_suppressed_view_matches",
        "portfolio_notification_summary_rows_reconcile",
        "portfolio_notification_source_evaluations_exist",
        "portfolio_notification_previous_evaluations_exist",
        "portfolio_notification_transition_metadata_matches",
        "portfolio_notification_event_ids_unique",
        "portfolio_notification_dispositions_valid",
        "portfolio_notification_payload_identity_matches",
        "portfolio_notification_current_candidates_complete",
        "portfolio_notification_current_candidates_not_stale",
    ):
        assert check_name in consistency_sql

    assert "does not deliver" in documentation
    assert '"performed": false' in documentation
    assert "ON CONFLICT (event_id) DO NOTHING" in documentation
    assert "`pending` means eligible candidate—not sent" in documentation
