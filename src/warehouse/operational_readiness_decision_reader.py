from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_readiness_decision_recorder import (
    canonical_operational_readiness_decision_bytes,
    validate_operational_readiness_decision,
)

CurrentRowReader = Callable[..., list[Mapping[str, Any]]]


def _quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("schema_name must be non-empty text")
    return '"' + value.replace('"', '""') + '"'


def _read_current_rows_postgres(
    *,
    dsn: str,
    gate_id: str,
    gate_fingerprint: str,
    operational_policy_id: str,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
    latest_expected_session: date,
    schema_name: str,
) -> list[Mapping[str, Any]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    schema = _quote_identifier(schema_name)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Readiness-aware planning requires psycopg") from exc

    query = f"""
        SELECT
            history.decision_json,
            latest.document_sha256,
            latest.recorded_at
        FROM {schema}.latest_operational_readiness_decisions latest
        JOIN {schema}.operational_readiness_decisions history
          ON history.decision_id = latest.decision_id
        WHERE latest.gate_id = %s
          AND latest.gate_fingerprint = %s
          AND latest.operational_policy_id = %s
          AND latest.operational_policy_fingerprint = %s
          AND latest.schedule_id = %s
          AND latest.schedule_fingerprint = %s
          AND latest.calendar_id = %s
          AND latest.portfolio_id = %s
          AND latest.risk_limit_policy_id = %s
          AND latest.mandate_fingerprint = %s
          AND latest.latest_expected_session = %s
        ORDER BY latest.evaluated_at DESC, latest.decision_id DESC
        LIMIT 2
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        gate_id,
                        gate_fingerprint,
                        operational_policy_id,
                        operational_policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_fingerprint,
                        latest_expected_session,
                    ),
                )
                return [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read the current operational readiness decision"
        ) from None


def read_current_operational_readiness_decision(
    *,
    dsn: str,
    gate_id: str,
    gate_fingerprint: str,
    operational_policy_id: str,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
    latest_expected_session: date,
    schema_name: str = "risk_platform",
    row_reader: CurrentRowReader | None = None,
) -> dict[str, Any] | None:
    selected_reader = row_reader or _read_current_rows_postgres
    rows = selected_reader(
        dsn=dsn,
        gate_id=gate_id,
        gate_fingerprint=gate_fingerprint,
        operational_policy_id=operational_policy_id,
        operational_policy_fingerprint=operational_policy_fingerprint,
        schedule_id=schedule_id,
        schedule_fingerprint=schedule_fingerprint,
        calendar_id=calendar_id,
        portfolio_id=portfolio_id,
        risk_limit_policy_id=risk_limit_policy_id,
        mandate_fingerprint=mandate_fingerprint,
        latest_expected_session=latest_expected_session,
        schema_name=schema_name,
    )
    if not isinstance(rows, list):
        raise StorageError("current operational readiness query returned invalid rows")
    if len(rows) > 1:
        raise StorageError("current operational readiness decision grain is not unique")
    if not rows:
        return None

    row = rows[0]
    if not isinstance(row, Mapping) or set(row) != {
        "decision_json",
        "document_sha256",
        "recorded_at",
    }:
        raise StorageError("current operational readiness row is incompatible")
    decision_json = row.get("decision_json")
    if not isinstance(decision_json, Mapping):
        raise StorageError("current operational readiness document is incompatible")
    decision = validate_operational_readiness_decision(decision_json)
    expected_contract = {
        "gate_id": gate_id,
        "gate_fingerprint": gate_fingerprint,
        "operational_policy_id": operational_policy_id,
        "operational_policy_fingerprint": operational_policy_fingerprint,
        "schedule_id": schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "portfolio_id": portfolio_id,
        "risk_limit_policy_id": risk_limit_policy_id,
        "mandate_fingerprint": mandate_fingerprint,
        "latest_expected_session": latest_expected_session.isoformat(),
    }
    for key, expected in expected_contract.items():
        if decision.get(key) != expected:
            raise StorageError(
                f"current operational readiness {key} does not match the plan"
            )

    document_sha256 = row.get("document_sha256")
    if not isinstance(document_sha256, str) or len(document_sha256) != 64:
        raise StorageError("current operational readiness digest is incompatible")
    canonical_sha256 = hashlib.sha256(
        canonical_operational_readiness_decision_bytes(decision)
    ).hexdigest()
    if document_sha256 != canonical_sha256:
        raise StorageError("current operational readiness digest does not reconcile")
    recorded_at = row.get("recorded_at")
    if not isinstance(recorded_at, datetime):
        raise StorageError("current operational readiness recorded_at is incompatible")
    if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
        raise StorageError("current operational readiness recorded_at must be aware")

    return {
        **decision,
        "document_sha256": document_sha256,
        "recorded_at": recorded_at.astimezone(timezone.utc).isoformat(),
    }
