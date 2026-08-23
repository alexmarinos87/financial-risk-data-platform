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


def test_model_approval_schema_is_append_only_and_method_bound() -> None:
    sql = _read_sql("sql/model_approval_schema.sql")

    for table_name in ("model_approvals", "model_approval_revocations"):
        assert f"CREATE TABLE IF NOT EXISTS risk_platform.{table_name}" in sql
    for view_name in (
        "model_approval_event_history",
        "current_model_approval_status",
        "current_model_approvals",
        "revoked_model_approvals",
    ):
        assert f"CREATE OR REPLACE VIEW risk_platform.{view_name}" in sql

    assert "model-approval-v1" in sql
    assert "model-approval-revocation-v1" in sql
    assert "portfolio-attribution-v1" in sql
    assert "portfolio-attribution-ewma-v1" in sql
    assert "sample_annualized" in sql
    assert "ewma_zero_mean_lambda_0_94_annualized" in sql
    assert "prevent_model_governance_mutation" in sql
    assert sql.count("BEFORE UPDATE") == 2
    assert sql.count("BEFORE DELETE") == 2
    assert "validate_model_approval_revocation_insert" in sql


def test_current_model_approval_views_select_latest_and_partition_status() -> None:
    sql = _read_sql("sql/model_approval_schema.sql")
    current_view = _extract_statement(
        sql,
        "CREATE OR REPLACE VIEW risk_platform.current_model_approval_status AS",
    )
    approved_view = _extract_statement(
        sql,
        "CREATE OR REPLACE VIEW risk_platform.current_model_approvals AS",
    )
    revoked_view = _extract_statement(
        sql,
        "CREATE OR REPLACE VIEW risk_platform.revoked_model_approvals AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.model_approvals (
                approval_id TEXT,
                model_version TEXT,
                use_case TEXT,
                contract_fingerprint TEXT,
                attribution_model_version TEXT,
                weighting_method TEXT,
                covariance_method TEXT,
                correlation_method TEXT,
                fixed_parameters_json TEXT,
                request_id TEXT,
                approved_at TIMESTAMPTZ,
                approved_by TEXT,
                reason TEXT,
                created_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE risk_platform.model_approval_revocations (
                revocation_id TEXT,
                model_version TEXT,
                approval_id TEXT,
                request_id TEXT,
                revoked_at TIMESTAMPTZ,
                revoked_by TEXT,
                reason TEXT,
                created_at TIMESTAMPTZ
            )
            """
        )
        january = datetime(2026, 1, 1, tzinfo=timezone.utc)
        february = datetime(2026, 2, 1, tzinfo=timezone.utc)
        march = datetime(2026, 3, 1, tzinfo=timezone.utc)
        approvals = [
            (
                "ewma-old",
                "model-approval-v1",
                "portfolio-risk-limit-evaluation",
                "ewma-contract",
                "portfolio-attribution-ewma-v1",
                "constant_weight_daily_rebalanced",
                "ewma_zero_mean_lambda_0_94_annualized",
                "implied_from_ewma_covariance",
                "{}",
                "EWMA-OLD",
                january,
                "reviewer",
                "old approval",
                january,
            ),
            (
                "ewma-new",
                "model-approval-v1",
                "portfolio-risk-limit-evaluation",
                "ewma-contract",
                "portfolio-attribution-ewma-v1",
                "constant_weight_daily_rebalanced",
                "ewma_zero_mean_lambda_0_94_annualized",
                "implied_from_ewma_covariance",
                "{}",
                "EWMA-NEW",
                march,
                "reviewer",
                "new approval",
                march,
            ),
            (
                "sample-only",
                "model-approval-v1",
                "portfolio-risk-limit-evaluation",
                "sample-contract",
                "portfolio-attribution-v1",
                "constant_weight_daily_rebalanced",
                "sample_annualized",
                "pearson",
                "{}",
                "SAMPLE-ONE",
                january,
                "reviewer",
                "sample approval",
                january,
            ),
        ]
        placeholders = ", ".join(["?"] * len(approvals[0]))
        connection.executemany(
            f"INSERT INTO risk_platform.model_approvals VALUES ({placeholders})",
            approvals,
        )
        revocations = [
            (
                "revoke-ewma-old",
                "model-approval-revocation-v1",
                "ewma-old",
                "REVOKE-OLD",
                february,
                "reviewer",
                "old approval revoked",
                february,
            ),
            (
                "revoke-sample",
                "model-approval-revocation-v1",
                "sample-only",
                "REVOKE-SAMPLE",
                february,
                "reviewer",
                "sample approval revoked",
                february,
            ),
        ]
        revocation_placeholders = ", ".join(["?"] * len(revocations[0]))
        connection.executemany(
            "INSERT INTO risk_platform.model_approval_revocations VALUES "
            f"({revocation_placeholders})",
            revocations,
        )
        connection.execute(current_view)
        connection.execute(approved_view)
        connection.execute(revoked_view)

        current = connection.execute(
            """
            SELECT
                contract_fingerprint,
                approval_id,
                approval_status,
                approval_count,
                revocation_count,
                epoch_us(approved_at)
            FROM risk_platform.current_model_approval_status
            ORDER BY contract_fingerprint
            """
        ).fetchall()
        approved = connection.execute(
            "SELECT approval_id FROM risk_platform.current_model_approvals"
        ).fetchall()
        revoked = connection.execute(
            "SELECT approval_id FROM risk_platform.revoked_model_approvals"
        ).fetchall()

    assert current == [
        (
            "ewma-contract",
            "ewma-new",
            "approved",
            2,
            0,
            int(march.timestamp() * 1_000_000),
        ),
        (
            "sample-contract",
            "sample-only",
            "revoked",
            1,
            1,
            int(january.timestamp() * 1_000_000),
        ),
    ]
    assert approved == [("ewma-new",)]
    assert revoked == [("sample-only",)]


def test_model_approval_consistency_suite_covers_governance_invariants() -> None:
    sql = _read_sql("sql/model_approval_consistency_checks.sql")
    for check_name in (
        "model_approval_history_matches_event_tables",
        "model_approval_revocations_reference_approvals",
        "model_approval_revocation_times_are_valid",
        "model_approval_request_grains_are_unique",
        "model_revocation_request_grains_are_unique",
        "current_model_approval_grain_is_unique",
        "current_model_approval_selects_latest_approval",
        "current_model_approval_status_matches_revocations",
        "model_approval_contracts_are_supported",
        "current_model_approval_views_partition_status",
        "model_approval_append_only_triggers_enabled",
    ):
        assert check_name in sql
