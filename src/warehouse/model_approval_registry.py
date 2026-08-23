from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.model_approval_contract import (
    ModelApproval,
    ModelApprovalRevocation,
    ModelContract,
    aware_utc,
    bounded_text,
    build_model_approval,
    build_model_approval_revocation,
    build_model_contract,
    request_id,
    use_case,
)

DEFAULT_POSTGRES_DSN = (
    "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
)


def _optional_timestamp(value: datetime | str | None, label: str) -> datetime | None:
    if value is None:
        return None
    return aware_utc(value, label)


def _approval_from_row(row: tuple[Any, ...]) -> ModelApproval:
    timestamp = row[10]
    if not isinstance(timestamp, datetime):
        raise StorageError("Stored model approval timestamp is incompatible")
    contract = ModelContract(
        attribution_model_version=str(row[4]),
        weighting_method=str(row[5]),
        covariance_method=str(row[6]),
        correlation_method=str(row[7]),
        fixed_parameters_json=json.dumps(
            row[8],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        contract_fingerprint=str(row[3]),
    )
    return ModelApproval(
        approval_id=str(row[0]),
        model_version=str(row[1]),
        use_case=str(row[2]),
        contract=contract,
        request_id=str(row[9]),
        approved_at=timestamp.astimezone(timezone.utc),
        approved_by=str(row[11]),
        reason=str(row[12]),
    )


def _revocation_from_row(row: tuple[Any, ...]) -> ModelApprovalRevocation:
    timestamp = row[4]
    if not isinstance(timestamp, datetime):
        raise StorageError("Stored model approval revocation timestamp is incompatible")
    return ModelApprovalRevocation(
        revocation_id=str(row[0]),
        model_version=str(row[1]),
        approval_id=str(row[2]),
        request_id=str(row[3]),
        revoked_at=timestamp.astimezone(timezone.utc),
        revoked_by=str(row[5]),
        reason=str(row[6]),
    )


def _approval_signature(approval: ModelApproval) -> tuple[Any, ...]:
    return (
        approval.model_version,
        approval.use_case,
        approval.contract.contract_fingerprint,
        approval.contract.attribution_model_version,
        approval.contract.weighting_method,
        approval.contract.covariance_method,
        approval.contract.correlation_method,
        approval.contract.fixed_parameters_json,
        approval.request_id,
        approval.approved_at,
        approval.approved_by,
        approval.reason,
    )


def _revocation_signature(revocation: ModelApprovalRevocation) -> tuple[Any, ...]:
    return (
        revocation.model_version,
        revocation.approval_id,
        revocation.request_id,
        revocation.revoked_at,
        revocation.revoked_by,
        revocation.reason,
    )


def _approval_summary(approval: ModelApproval, *, created: bool) -> dict[str, Any]:
    return {
        "approval_id": approval.approval_id,
        "model_version": approval.model_version,
        "use_case": approval.use_case,
        "contract_fingerprint": approval.contract.contract_fingerprint,
        "attribution_model_version": approval.contract.attribution_model_version,
        "weighting_method": approval.contract.weighting_method,
        "covariance_method": approval.contract.covariance_method,
        "correlation_method": approval.contract.correlation_method,
        "approved_at": approval.approved_at.isoformat(),
        "approved_by": approval.approved_by,
        "request_id": approval.request_id,
        "created": created,
    }


def _revocation_summary(
    revocation: ModelApprovalRevocation,
    *,
    created: bool,
) -> dict[str, Any]:
    return {
        "revocation_id": revocation.revocation_id,
        "model_version": revocation.model_version,
        "approval_id": revocation.approval_id,
        "request_id": revocation.request_id,
        "revoked_at": revocation.revoked_at.isoformat(),
        "revoked_by": revocation.revoked_by,
        "created": created,
    }


def approve_model_contract(
    *,
    dsn: str,
    use_case_name: str,
    attribution_model_version: str,
    weighting_method: str,
    covariance_method: str,
    correlation_method: str,
    request_identifier: str,
    approved_by: str,
    reason: str,
    approved_at: datetime | str | None = None,
) -> dict[str, Any]:
    canonical_use_case = use_case(use_case_name)
    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(approved_by, "approved_by")
    canonical_reason = bounded_text(reason, "reason", 2_000)
    supplied_timestamp = _optional_timestamp(approved_at, "approved_at")
    contract = build_model_contract(
        attribution_model_version=attribution_model_version,
        weighting_method=weighting_method,
        covariance_method=covariance_method,
        correlation_method=correlation_method,
    )

    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Model approval requires psycopg") from exc

    approval_columns = """
        approval_id,
        model_version,
        use_case,
        contract_fingerprint,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        fixed_parameters_json,
        request_id,
        approved_at,
        approved_by,
        reason
    """
    stored: ModelApproval | None = None
    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {approval_columns}
                    FROM risk_platform.model_approvals
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is not None:
                    stored = _approval_from_row(existing_row)
                    expected_timestamp = supplied_timestamp or stored.approved_at
                    expected = build_model_approval(
                        use_case_name=canonical_use_case,
                        contract=contract,
                        request_identifier=canonical_request_id,
                        approved_at=expected_timestamp,
                        approved_by=canonical_actor,
                        reason=canonical_reason,
                    )
                    if _approval_signature(stored) != _approval_signature(expected):
                        raise ValidationError(
                            "request_id already exists with different model approval content"
                        )
                    return _approval_summary(stored, created=False)

                timestamp = supplied_timestamp or datetime.now(timezone.utc)
                approval = build_model_approval(
                    use_case_name=canonical_use_case,
                    contract=contract,
                    request_identifier=canonical_request_id,
                    approved_at=timestamp,
                    approved_by=canonical_actor,
                    reason=canonical_reason,
                )
                cursor.execute(
                    """
                    INSERT INTO risk_platform.model_approvals (
                        approval_id,
                        model_version,
                        use_case,
                        contract_fingerprint,
                        attribution_model_version,
                        weighting_method,
                        covariance_method,
                        correlation_method,
                        fixed_parameters_json,
                        request_id,
                        approved_at,
                        approved_by,
                        reason
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING approval_id
                    """,
                    (
                        approval.approval_id,
                        approval.model_version,
                        approval.use_case,
                        approval.contract.contract_fingerprint,
                        approval.contract.attribution_model_version,
                        approval.contract.weighting_method,
                        approval.contract.covariance_method,
                        approval.contract.correlation_method,
                        Jsonb(json.loads(approval.contract.fixed_parameters_json)),
                        approval.request_id,
                        approval.approved_at,
                        approval.approved_by,
                        approval.reason,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    SELECT {approval_columns}
                    FROM risk_platform.model_approvals
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                stored_row = cursor.fetchone()
                if stored_row is None:
                    raise StorageError("Model approval could not be read after insert")
                stored = _approval_from_row(stored_row)
                if _approval_signature(stored) != _approval_signature(approval):
                    raise ValidationError(
                        "request_id already exists with different model approval content"
                    )
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("Model approval database operation failed") from None

    if stored is None:  # pragma: no cover - guarded by the transaction above.
        raise StorageError("Model approval result is unavailable")
    return _approval_summary(stored, created=created)


def revoke_model_approval(
    *,
    dsn: str,
    approval_id: str,
    request_identifier: str,
    revoked_by: str,
    reason: str,
    revoked_at: datetime | str | None = None,
) -> dict[str, Any]:
    canonical_approval_id = bounded_text(approval_id, "approval_id", 256)
    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(revoked_by, "revoked_by")
    canonical_reason = bounded_text(reason, "reason", 2_000)
    supplied_timestamp = _optional_timestamp(revoked_at, "revoked_at")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Model approval revocation requires psycopg") from exc

    revocation_columns = """
        revocation_id,
        model_version,
        approval_id,
        request_id,
        revoked_at,
        revoked_by,
        reason
    """
    stored: ModelApprovalRevocation | None = None
    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT approved_at
                    FROM risk_platform.model_approvals
                    WHERE approval_id = %s
                    """,
                    (canonical_approval_id,),
                )
                approval_row = cursor.fetchone()
                if approval_row is None:
                    raise ValidationError("approval_id does not identify a model approval")
                approved_at_value = approval_row[0]
                if not isinstance(approved_at_value, datetime):
                    raise StorageError("Stored model approval timestamp is incompatible")
                target_approved_at = approved_at_value.astimezone(timezone.utc)

                cursor.execute(
                    f"""
                    SELECT {revocation_columns}
                    FROM risk_platform.model_approval_revocations
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is not None:
                    stored = _revocation_from_row(existing_row)
                    expected_timestamp = supplied_timestamp or stored.revoked_at
                    expected = build_model_approval_revocation(
                        approval_id=canonical_approval_id,
                        request_identifier=canonical_request_id,
                        revoked_at=expected_timestamp,
                        revoked_by=canonical_actor,
                        reason=canonical_reason,
                    )
                    if _revocation_signature(stored) != _revocation_signature(expected):
                        raise ValidationError(
                            "request_id already exists with different model revocation content"
                        )
                    return _revocation_summary(stored, created=False)

                timestamp = supplied_timestamp or datetime.now(timezone.utc)
                if timestamp < target_approved_at:
                    raise ValidationError(
                        "revoked_at must be on or after the approval timestamp"
                    )
                revocation = build_model_approval_revocation(
                    approval_id=canonical_approval_id,
                    request_identifier=canonical_request_id,
                    revoked_at=timestamp,
                    revoked_by=canonical_actor,
                    reason=canonical_reason,
                )
                cursor.execute(
                    """
                    INSERT INTO risk_platform.model_approval_revocations (
                        revocation_id,
                        model_version,
                        approval_id,
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
                        revocation.approval_id,
                        revocation.request_id,
                        revocation.revoked_at,
                        revocation.revoked_by,
                        revocation.reason,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    SELECT {revocation_columns}
                    FROM risk_platform.model_approval_revocations
                    WHERE request_id = %s
                    """,
                    (canonical_request_id,),
                )
                stored_row = cursor.fetchone()
                if stored_row is None:
                    raise StorageError(
                        "Model approval revocation could not be read after insert"
                    )
                stored = _revocation_from_row(stored_row)
                if _revocation_signature(stored) != _revocation_signature(revocation):
                    raise ValidationError(
                        "request_id already exists with different model revocation content"
                    )
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError(
            "Model approval revocation database operation failed"
        ) from None

    if stored is None:  # pragma: no cover - guarded by the transaction above.
        raise StorageError("Model approval revocation result is unavailable")
    return _revocation_summary(stored, created=created)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append idempotent model approval or revocation evidence to PostgreSQL."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    approve = subparsers.add_parser("approve")
    approve.add_argument("--use-case", required=True)
    approve.add_argument("--attribution-model-version", required=True)
    approve.add_argument("--weighting-method", required=True)
    approve.add_argument("--covariance-method", required=True)
    approve.add_argument("--correlation-method", required=True)
    approve.add_argument("--request-id", required=True)
    approve.add_argument("--approved-by", required=True)
    approve.add_argument("--reason", required=True)
    approve.add_argument("--approved-at")

    revoke = subparsers.add_parser("revoke")
    revoke.add_argument("--approval-id", required=True)
    revoke.add_argument("--request-id", required=True)
    revoke.add_argument("--revoked-by", required=True)
    revoke.add_argument("--reason", required=True)
    revoke.add_argument("--revoked-at")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        if args.command == "approve":
            summary = approve_model_contract(
                dsn=args.dsn,
                use_case_name=args.use_case,
                attribution_model_version=args.attribution_model_version,
                weighting_method=args.weighting_method,
                covariance_method=args.covariance_method,
                correlation_method=args.correlation_method,
                request_identifier=args.request_id,
                approved_by=args.approved_by,
                reason=args.reason,
                approved_at=args.approved_at,
            )
        else:
            summary = revoke_model_approval(
                dsn=args.dsn,
                approval_id=args.approval_id,
                request_identifier=args.request_id,
                revoked_by=args.revoked_by,
                reason=args.reason,
                revoked_at=args.revoked_at,
            )
    except ValidationError as exc:
        print(f"Model governance evidence rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Model governance evidence failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
