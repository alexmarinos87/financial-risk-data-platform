from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_readiness_decision_recorder import (
    validate_operational_readiness_decision,
)
from src.warehouse.operational_readiness_override_contract import (
    OperationalReadinessOverride,
    OperationalReadinessOverrideRevocation,
    aware_utc,
    bounded_text,
    build_operational_readiness_override,
    build_operational_readiness_override_revocation,
    request_id,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CurrentOverrideRowReader = Callable[..., list[Mapping[str, Any]]]

OVERRIDE_COLUMNS = """
    override_id,
    model_version,
    decision_id,
    decision_document_sha256,
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
    request_id,
    approved_at,
    expires_at,
    approved_by,
    reason
"""
REVOCATION_COLUMNS = """
    revocation_id,
    model_version,
    override_id,
    request_id,
    revoked_at,
    revoked_by,
    reason
"""


def _optional_timestamp(value: datetime | str | None, label: str) -> datetime | None:
    return None if value is None else aware_utc(value, label)


def _override_from_row(row: tuple[Any, ...]) -> OperationalReadinessOverride:
    latest_session = row[14]
    approved_at = row[16]
    expires_at = row[17]
    if not isinstance(latest_session, date) or isinstance(latest_session, datetime):
        raise StorageError("Stored readiness override session is incompatible")
    if not isinstance(approved_at, datetime) or not isinstance(expires_at, datetime):
        raise StorageError("Stored readiness override timestamps are incompatible")
    return OperationalReadinessOverride(
        override_id=str(row[0]),
        model_version=str(row[1]),
        decision_id=str(row[2]),
        decision_document_sha256=str(row[3]),
        gate_id=str(row[4]),
        gate_fingerprint=str(row[5]),
        operational_policy_id=str(row[6]),
        operational_policy_fingerprint=str(row[7]),
        schedule_id=str(row[8]),
        schedule_fingerprint=str(row[9]),
        calendar_id=str(row[10]),
        portfolio_id=str(row[11]),
        risk_limit_policy_id=str(row[12]),
        mandate_fingerprint=str(row[13]),
        latest_expected_session=latest_session.isoformat(),
        request_id=str(row[15]),
        approved_at=approved_at.astimezone(timezone.utc),
        expires_at=expires_at.astimezone(timezone.utc),
        approved_by=str(row[18]),
        reason=str(row[19]),
    )


def _revocation_from_row(
    row: tuple[Any, ...],
) -> OperationalReadinessOverrideRevocation:
    revoked_at = row[4]
    if not isinstance(revoked_at, datetime):
        raise StorageError("Stored readiness override revocation is incompatible")
    return OperationalReadinessOverrideRevocation(
        revocation_id=str(row[0]),
        model_version=str(row[1]),
        override_id=str(row[2]),
        request_id=str(row[3]),
        revoked_at=revoked_at.astimezone(timezone.utc),
        revoked_by=str(row[5]),
        reason=str(row[6]),
    )


def _override_signature(value: OperationalReadinessOverride) -> tuple[Any, ...]:
    return (
        value.model_version,
        value.decision_id,
        value.decision_document_sha256,
        value.gate_id,
        value.gate_fingerprint,
        value.operational_policy_id,
        value.operational_policy_fingerprint,
        value.schedule_id,
        value.schedule_fingerprint,
        value.calendar_id,
        value.portfolio_id,
        value.risk_limit_policy_id,
        value.mandate_fingerprint,
        value.latest_expected_session,
        value.request_id,
        value.approved_at,
        value.expires_at,
        value.approved_by,
        value.reason,
    )


def _revocation_signature(
    value: OperationalReadinessOverrideRevocation,
) -> tuple[Any, ...]:
    return (
        value.model_version,
        value.override_id,
        value.request_id,
        value.revoked_at,
        value.revoked_by,
        value.reason,
    )


def _override_summary(
    value: OperationalReadinessOverride,
    *,
    created: bool,
) -> dict[str, Any]:
    return {
        "override_id": value.override_id,
        "model_version": value.model_version,
        "decision_id": value.decision_id,
        "decision_document_sha256": value.decision_document_sha256,
        "gate_id": value.gate_id,
        "schedule_id": value.schedule_id,
        "schedule_fingerprint": value.schedule_fingerprint,
        "portfolio_id": value.portfolio_id,
        "mandate_fingerprint": value.mandate_fingerprint,
        "latest_expected_session": value.latest_expected_session,
        "request_id": value.request_id,
        "approved_at": value.approved_at.isoformat(),
        "expires_at": value.expires_at.isoformat(),
        "approved_by": value.approved_by,
        "created": created,
    }


def _revocation_summary(
    value: OperationalReadinessOverrideRevocation,
    *,
    created: bool,
) -> dict[str, Any]:
    return {
        "revocation_id": value.revocation_id,
        "model_version": value.model_version,
        "override_id": value.override_id,
        "request_id": value.request_id,
        "revoked_at": value.revoked_at.isoformat(),
        "revoked_by": value.revoked_by,
        "created": created,
    }


def approve_operational_readiness_override(
    *,
    dsn: str,
    decision_id: str,
    request_identifier: str,
    expires_at: datetime | str,
    approved_by: str,
    reason: str,
    approved_at: datetime | str | None = None,
) -> dict[str, Any]:
    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(approved_by, "approved_by", 320)
    canonical_reason = bounded_text(reason, "reason", 2_000)
    supplied_approved_at = _optional_timestamp(approved_at, "approved_at")
    canonical_expires_at = aware_utc(expires_at, "expires_at")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational readiness override requires psycopg") from exc

    stored: OperationalReadinessOverride | None = None
    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT decision_json, document_sha256
                    FROM risk_platform.operational_readiness_decisions
                    WHERE decision_id = %s
                    """,
                    (decision_id,),
                )
                decision_row = cursor.fetchone()
                if decision_row is None:
                    raise ValidationError(
                        "decision_id does not identify retained readiness evidence"
                    )
                decision_json = decision_row[0]
                if not isinstance(decision_json, Mapping):
                    raise StorageError("Stored readiness decision is incompatible")
                decision = validate_operational_readiness_decision(decision_json)
                document_sha256 = str(decision_row[1])

                cursor.execute(
                    f"""
                    SELECT {OVERRIDE_COLUMNS}
                    FROM risk_platform.operational_readiness_overrides
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is not None:
                    stored = _override_from_row(existing_row)
                    expected = build_operational_readiness_override(
                        decision=decision,
                        decision_document_sha256=document_sha256,
                        request_identifier=canonical_request_id,
                        approved_at=supplied_approved_at or stored.approved_at,
                        expires_at=canonical_expires_at,
                        approved_by=canonical_actor,
                        reason=canonical_reason,
                    )
                    if _override_signature(stored) != _override_signature(expected):
                        raise ValidationError(
                            "request_id already exists with different override content"
                        )
                    return _override_summary(stored, created=False)

                approval_time = supplied_approved_at or datetime.now(timezone.utc)
                override = build_operational_readiness_override(
                    decision=decision,
                    decision_document_sha256=document_sha256,
                    request_identifier=canonical_request_id,
                    approved_at=approval_time,
                    expires_at=canonical_expires_at,
                    approved_by=canonical_actor,
                    reason=canonical_reason,
                )
                cursor.execute(
                    """
                    INSERT INTO risk_platform.operational_readiness_overrides (
                        override_id,
                        model_version,
                        decision_id,
                        decision_document_sha256,
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
                        request_id,
                        approved_at,
                        expires_at,
                        approved_by,
                        reason
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING override_id
                    """,
                    (
                        override.override_id,
                        override.model_version,
                        override.decision_id,
                        override.decision_document_sha256,
                        override.gate_id,
                        override.gate_fingerprint,
                        override.operational_policy_id,
                        override.operational_policy_fingerprint,
                        override.schedule_id,
                        override.schedule_fingerprint,
                        override.calendar_id,
                        override.portfolio_id,
                        override.risk_limit_policy_id,
                        override.mandate_fingerprint,
                        date.fromisoformat(override.latest_expected_session),
                        override.request_id,
                        override.approved_at,
                        override.expires_at,
                        override.approved_by,
                        override.reason,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    SELECT {OVERRIDE_COLUMNS}
                    FROM risk_platform.operational_readiness_overrides
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                stored_row = cursor.fetchone()
                if stored_row is None:
                    raise StorageError("Readiness override is unavailable after insert")
                stored = _override_from_row(stored_row)
                if _override_signature(stored) != _override_signature(override):
                    raise ValidationError(
                        "request_id already exists with different override content"
                    )
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("Readiness override database operation failed") from None

    if stored is None:  # pragma: no cover - guarded above.
        raise StorageError("Readiness override result is unavailable")
    return _override_summary(stored, created=created)


def revoke_operational_readiness_override(
    *,
    dsn: str,
    override_id: str,
    request_identifier: str,
    revoked_by: str,
    reason: str,
    revoked_at: datetime | str | None = None,
) -> dict[str, Any]:
    canonical_override_id = bounded_text(override_id, "override_id", 256)
    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(revoked_by, "revoked_by", 320)
    canonical_reason = bounded_text(reason, "reason", 2_000)
    supplied_revoked_at = _optional_timestamp(revoked_at, "revoked_at")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational readiness revocation requires psycopg") from exc

    stored: OperationalReadinessOverrideRevocation | None = None
    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {OVERRIDE_COLUMNS}
                    FROM risk_platform.operational_readiness_overrides
                    WHERE override_id = %s
                    """,
                    (canonical_override_id,),
                )
                override_row = cursor.fetchone()
                if override_row is None:
                    raise ValidationError(
                        "override_id does not identify readiness override evidence"
                    )
                override = _override_from_row(override_row)

                cursor.execute(
                    f"""
                    SELECT {REVOCATION_COLUMNS}
                    FROM risk_platform.operational_readiness_override_revocations
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is not None:
                    stored = _revocation_from_row(existing_row)
                    expected = build_operational_readiness_override_revocation(
                        override=override,
                        request_identifier=canonical_request_id,
                        revoked_at=supplied_revoked_at or stored.revoked_at,
                        revoked_by=canonical_actor,
                        reason=canonical_reason,
                    )
                    if _revocation_signature(stored) != _revocation_signature(expected):
                        raise ValidationError(
                            "request_id already exists with different revocation content"
                        )
                    return _revocation_summary(stored, created=False)

                revocation_time = supplied_revoked_at or datetime.now(timezone.utc)
                revocation = build_operational_readiness_override_revocation(
                    override=override,
                    request_identifier=canonical_request_id,
                    revoked_at=revocation_time,
                    revoked_by=canonical_actor,
                    reason=canonical_reason,
                )
                cursor.execute(
                    """
                    INSERT INTO
                        risk_platform.operational_readiness_override_revocations (
                            revocation_id,
                            model_version,
                            override_id,
                            request_id,
                            revoked_at,
                            revoked_by,
                            reason
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING revocation_id
                    """,
                    (
                        revocation.revocation_id,
                        revocation.model_version,
                        revocation.override_id,
                        revocation.request_id,
                        revocation.revoked_at,
                        revocation.revoked_by,
                        revocation.reason,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    SELECT {REVOCATION_COLUMNS}
                    FROM risk_platform.operational_readiness_override_revocations
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                stored_row = cursor.fetchone()
                if stored_row is None:
                    raise StorageError(
                        "Readiness override revocation is unavailable after insert"
                    )
                stored = _revocation_from_row(stored_row)
                if _revocation_signature(stored) != _revocation_signature(revocation):
                    raise ValidationError(
                        "request_id already exists with different revocation content"
                    )
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError(
            "Readiness override revocation database operation failed"
        ) from None

    if stored is None:  # pragma: no cover - guarded above.
        raise StorageError("Readiness override revocation result is unavailable")
    return _revocation_summary(stored, created=created)


def _read_current_override_rows_postgres(
    *,
    dsn: str,
    decision_id: str,
) -> list[Mapping[str, Any]]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational readiness override lookup requires psycopg") from exc
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        override_id,
                        model_version,
                        decision_id,
                        decision_document_sha256,
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
                        request_id,
                        approved_at,
                        expires_at,
                        approved_by,
                        revocation_id,
                        revoked_at
                    FROM risk_platform.current_operational_readiness_override_status
                    WHERE decision_id = %s
                    LIMIT 2
                    """,
                    (decision_id,),
                )
                return [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError("Unable to read current readiness override") from None


def read_active_operational_readiness_override(
    *,
    dsn: str,
    decision_id: str,
    evaluated_at: datetime | str,
    row_reader: CurrentOverrideRowReader | None = None,
) -> dict[str, Any] | None:
    evaluation_time = aware_utc(evaluated_at, "evaluated_at")
    selected_reader = row_reader or _read_current_override_rows_postgres
    rows = selected_reader(dsn=dsn, decision_id=decision_id)
    if not isinstance(rows, list):
        raise StorageError("current readiness override query returned invalid rows")
    if len(rows) > 1:
        raise StorageError("current readiness override grain is not unique")
    if not rows:
        return None
    row = rows[0]
    required = {
        "override_id",
        "model_version",
        "decision_id",
        "decision_document_sha256",
        "gate_id",
        "gate_fingerprint",
        "operational_policy_id",
        "operational_policy_fingerprint",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_fingerprint",
        "latest_expected_session",
        "request_id",
        "approved_at",
        "expires_at",
        "approved_by",
        "revocation_id",
        "revoked_at",
    }
    if not isinstance(row, Mapping) or set(row) != required:
        raise StorageError("current readiness override row is incompatible")
    if row.get("decision_id") != decision_id:
        raise StorageError("current readiness override targets another decision")
    approved_at = row.get("approved_at")
    expires_at = row.get("expires_at")
    revoked_at = row.get("revoked_at")
    if not isinstance(approved_at, datetime) or not isinstance(expires_at, datetime):
        raise StorageError("current readiness override timestamps are incompatible")
    approved_at = approved_at.astimezone(timezone.utc)
    expires_at = expires_at.astimezone(timezone.utc)
    if revoked_at is not None:
        if not isinstance(revoked_at, datetime):
            raise StorageError("current readiness revocation timestamp is incompatible")
        revoked_at = revoked_at.astimezone(timezone.utc)
    if evaluation_time < approved_at or evaluation_time >= expires_at:
        return None
    if revoked_at is not None and revoked_at <= evaluation_time:
        return None
    latest_session = row.get("latest_expected_session")
    if not isinstance(latest_session, date) or isinstance(latest_session, datetime):
        raise StorageError("current readiness override session is incompatible")
    return {
        "override_id": row["override_id"],
        "model_version": row["model_version"],
        "decision_id": row["decision_id"],
        "decision_document_sha256": row["decision_document_sha256"],
        "gate_id": row["gate_id"],
        "gate_fingerprint": row["gate_fingerprint"],
        "operational_policy_id": row["operational_policy_id"],
        "operational_policy_fingerprint": row[
            "operational_policy_fingerprint"
        ],
        "schedule_id": row["schedule_id"],
        "schedule_fingerprint": row["schedule_fingerprint"],
        "calendar_id": row["calendar_id"],
        "portfolio_id": row["portfolio_id"],
        "risk_limit_policy_id": row["risk_limit_policy_id"],
        "mandate_fingerprint": row["mandate_fingerprint"],
        "latest_expected_session": latest_session.isoformat(),
        "request_id": row["request_id"],
        "approved_at": approved_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "approved_by": row["approved_by"],
        "revocation_id": row["revocation_id"],
        "revoked_at": revoked_at.isoformat() if revoked_at is not None else None,
        "evaluated_at": evaluation_time.isoformat(),
        "active": True,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append operational readiness override or revocation evidence."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    approve = subparsers.add_parser("approve")
    approve.add_argument("--decision-id", required=True)
    approve.add_argument("--request-id", required=True)
    approve.add_argument("--expires-at", required=True)
    approve.add_argument("--approved-by", required=True)
    approve.add_argument("--reason", required=True)
    approve.add_argument("--approved-at")

    revoke = subparsers.add_parser("revoke")
    revoke.add_argument("--override-id", required=True)
    revoke.add_argument("--request-id", required=True)
    revoke.add_argument("--revoked-by", required=True)
    revoke.add_argument("--reason", required=True)
    revoke.add_argument("--revoked-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "approve":
            summary = approve_operational_readiness_override(
                dsn=args.dsn,
                decision_id=args.decision_id,
                request_identifier=args.request_id,
                expires_at=args.expires_at,
                approved_by=args.approved_by,
                reason=args.reason,
                approved_at=args.approved_at,
            )
        else:
            summary = revoke_operational_readiness_override(
                dsn=args.dsn,
                override_id=args.override_id,
                request_identifier=args.request_id,
                revoked_by=args.revoked_by,
                reason=args.reason,
                revoked_at=args.revoked_at,
            )
    except ValidationError as exc:
        print(f"Readiness override evidence rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Readiness override evidence failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
