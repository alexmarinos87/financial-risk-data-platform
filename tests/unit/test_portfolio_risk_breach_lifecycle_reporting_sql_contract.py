from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

VOLATILITY_METRIC = "portfolio_volatility_annualized"
CONCENTRATION_METRIC = "largest_absolute_component_contribution_share"


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _extract_statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    end = sql.index(";", start) + 1
    return sql[start:end]


def _evaluation_row(
    *,
    metric_name: str,
    day: int,
    status: str,
    observed_value: float,
    subject_key: str,
    observed_signed_value: float | None = None,
) -> tuple[object, ...]:
    is_volatility = metric_name == VOLATILITY_METRIC
    warning_threshold = 0.30 if is_volatility else 0.65
    critical_threshold = 0.45 if is_volatility else 0.80
    is_breach = status != "ok"
    breach_threshold = (
        None
        if status == "ok"
        else critical_threshold if status == "critical" else warning_threshold
    )
    breach_excess = (
        0.0
        if breach_threshold is None
        else observed_value - breach_threshold
    )
    ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
    ts_ingest = datetime(2026, 2, 1, 12, day, tzinfo=timezone.utc)
    return (
        f"{metric_name}-{day}",
        "portfolio-risk-limits-v1",
        "us-tech-standard",
        "risk-limit-policy-a",
        "us-tech-equal",
        "USD",
        "definition-a",
        f"attribution-{day}",
        "portfolio-attribution-v1",
        "constant_weight_daily_rebalanced",
        "sample_annualized",
        "pearson",
        20,
        252,
        ts_event,
        ts_ingest,
        metric_name,
        "portfolio" if is_volatility else "constituent",
        subject_key,
        "annualized_decimal" if is_volatility else "absolute_share",
        observed_value,
        (
            observed_value
            if observed_signed_value is None
            else observed_signed_value
        ),
        warning_threshold,
        critical_threshold,
        status,
        is_breach,
        breach_threshold,
        breach_excess,
    )


def _create_latest_evaluation_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA risk_platform")
    connection.execute(
        """
        CREATE TABLE risk_platform.latest_portfolio_risk_limit_evaluations (
            calculation_id TEXT,
            model_version TEXT,
            policy_id TEXT,
            policy_fingerprint TEXT,
            portfolio_id TEXT,
            base_currency TEXT,
            definition_fingerprint TEXT,
            attribution_calculation_id TEXT,
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
            unit TEXT,
            observed_value DOUBLE,
            observed_signed_value DOUBLE,
            warning_threshold DOUBLE,
            critical_threshold DOUBLE,
            status TEXT,
            is_breach BOOLEAN,
            breach_threshold DOUBLE,
            breach_excess DOUBLE
        )
        """
    )


def _insert_evaluation_series(connection: duckdb.DuckDBPyConnection) -> None:
    rows = [
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=1,
            status="ok",
            observed_value=0.20,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=2,
            status="warning",
            observed_value=0.35,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=3,
            status="critical",
            observed_value=0.50,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=4,
            status="warning",
            observed_value=0.34,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=5,
            status="ok",
            observed_value=0.25,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=VOLATILITY_METRIC,
            day=6,
            status="warning",
            observed_value=0.32,
            subject_key="us-tech-equal",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=1,
            status="critical",
            observed_value=0.90,
            observed_signed_value=-0.90,
            subject_key="alpha_vantage:AAPL",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=2,
            status="critical",
            observed_value=0.85,
            observed_signed_value=0.85,
            subject_key="alpha_vantage:MSFT",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=3,
            status="ok",
            observed_value=0.50,
            observed_signed_value=0.50,
            subject_key="alpha_vantage:MSFT",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=4,
            status="ok",
            observed_value=0.40,
            observed_signed_value=0.40,
            subject_key="alpha_vantage:MSFT",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=5,
            status="warning",
            observed_value=0.70,
            observed_signed_value=0.70,
            subject_key="alpha_vantage:AAPL",
        ),
        _evaluation_row(
            metric_name=CONCENTRATION_METRIC,
            day=6,
            status="critical",
            observed_value=0.81,
            observed_signed_value=0.81,
            subject_key="alpha_vantage:AAPL",
        ),
    ]
    placeholders = ", ".join(["?"] * len(rows[0]))
    connection.executemany(
        "INSERT INTO "
        "risk_platform.latest_portfolio_risk_limit_evaluations VALUES "
        f"({placeholders})",
        rows,
    )


def test_breach_lifecycle_schema_exposes_expected_views() -> None:
    schema_sql = _read_sql("sql/portfolio_risk_breach_lifecycle_schema.sql")

    for view_name in (
        "portfolio_risk_limit_metric_transitions",
        "portfolio_risk_limit_actionable_transitions",
        "portfolio_risk_limit_breach_episodes",
        "portfolio_risk_limit_open_episodes",
    ):
        assert f"CREATE OR REPLACE VIEW risk_platform.{view_name}" in schema_sql

    for transition_type in (
        "initial_ok",
        "opened",
        "escalated",
        "deescalated",
        "resolved",
        "unchanged",
    ):
        assert f"'{transition_type}'" in schema_sql

    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in schema_sql
    assert "subject_change_observations" in schema_sql
    assert "peak_breach_excess" in schema_sql


def test_transition_and_episode_views_execute_deterministically() -> None:
    schema_sql = _read_sql("sql/portfolio_risk_breach_lifecycle_schema.sql")
    view_names = (
        "portfolio_risk_limit_metric_transitions",
        "portfolio_risk_limit_actionable_transitions",
        "portfolio_risk_limit_breach_episodes",
        "portfolio_risk_limit_open_episodes",
    )

    with duckdb.connect() as connection:
        _create_latest_evaluation_table(connection)
        _insert_evaluation_series(connection)
        for view_name in view_names:
            connection.execute(
                _extract_statement(
                    schema_sql,
                    f"CREATE OR REPLACE VIEW risk_platform.{view_name} AS",
                )
            )

        volatility_transitions = connection.execute(
            """
            SELECT transition_type
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE metric_name = ?
            ORDER BY ts_event
            """,
            [VOLATILITY_METRIC],
        ).fetchall()
        assert volatility_transitions == [
            ("initial_ok",),
            ("opened",),
            ("escalated",),
            ("deescalated",),
            ("resolved",),
            ("opened",),
        ]

        concentration_subject_change = connection.execute(
            """
            SELECT subject_changed
            FROM risk_platform.portfolio_risk_limit_metric_transitions
            WHERE metric_name = ? AND ts_event = ?
            """,
            [
                CONCENTRATION_METRIC,
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
        ).fetchone()
        assert concentration_subject_change == (True,)

        assert connection.execute(
            "SELECT COUNT(*) FROM "
            "risk_platform.portfolio_risk_limit_actionable_transitions"
        ).fetchone() == (9,)

        episodes = connection.execute(
            """
            SELECT
                metric_name,
                episode_sequence,
                episode_status,
                opening_status,
                latest_breach_status,
                peak_status,
                breach_observations,
                warning_observations,
                critical_observations,
                peak_subject_key,
                resolution_date IS NULL
            FROM risk_platform.portfolio_risk_limit_breach_episodes
            ORDER BY metric_name, episode_sequence
            """
        ).fetchall()
        assert episodes == [
            (
                CONCENTRATION_METRIC,
                1,
                "resolved",
                "critical",
                "critical",
                "critical",
                2,
                0,
                2,
                "alpha_vantage:AAPL",
                False,
            ),
            (
                CONCENTRATION_METRIC,
                2,
                "open",
                "warning",
                "critical",
                "critical",
                2,
                1,
                1,
                "alpha_vantage:AAPL",
                True,
            ),
            (
                VOLATILITY_METRIC,
                1,
                "resolved",
                "warning",
                "warning",
                "critical",
                3,
                2,
                1,
                "us-tech-equal",
                False,
            ),
            (
                VOLATILITY_METRIC,
                2,
                "open",
                "warning",
                "warning",
                "warning",
                1,
                1,
                0,
                "us-tech-equal",
                True,
            ),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM "
            "risk_platform.portfolio_risk_limit_open_episodes"
        ).fetchone() == (2,)


def test_lifecycle_reconciliation_and_documentation_contract() -> None:
    consistency_sql = _read_sql(
        "sql/portfolio_risk_breach_lifecycle_consistency_checks.sql"
    )
    documentation = Path("docs/portfolio-risk-breach-lifecycle.md").read_text(
        encoding="utf-8"
    )

    for check_name in (
        "portfolio_risk_limit_transition_rows_match_latest",
        "portfolio_risk_limit_actionable_transition_rows_match",
        "portfolio_risk_limit_opened_transitions_match_episodes",
        "portfolio_risk_limit_resolutions_match_resolved_episodes",
        "portfolio_risk_limit_episode_counts_reconcile",
        "portfolio_risk_limit_episode_resolution_reconciles",
        "portfolio_risk_limit_episode_boundaries_reference_transitions",
        "portfolio_risk_limit_episode_keys_unique",
        "portfolio_risk_limit_episodes_do_not_overlap",
        "portfolio_risk_limit_open_episodes_are_current",
    ):
        assert check_name in consistency_sql

    assert "consecutive **available evaluation observations**" in documentation
    assert "current corrected series" in documentation
    assert "does not add" in documentation
    assert "notification delivery" in documentation
