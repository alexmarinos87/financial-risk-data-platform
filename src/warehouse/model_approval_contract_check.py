from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.model_approval_gate import resolve_model_approval_gate
from src.warehouse.model_approval_registry import (
    DEFAULT_POSTGRES_DSN,
    approve_model_contract,
    revoke_model_approval,
)

USE_CASE = "portfolio-risk-limit-evaluation"
METHOD_POLICY_FINGERPRINT = "risk-limit-method-policy-contract-check"
EWMA_MODEL_VERSION = "portfolio-attribution-ewma-v1"
WEIGHTING_METHOD = "constant_weight_daily_rebalanced"
COVARIANCE_METHOD = "ewma_zero_mean_lambda_0_94_annualized"
CORRELATION_METHOD = "implied_from_ewma_covariance"


def _resolve_gate(*, dsn: str):
    return resolve_model_approval_gate(
        method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
        attribution_model_version=EWMA_MODEL_VERSION,
        weighting_method=WEIGHTING_METHOD,
        covariance_method=COVARIANCE_METHOD,
        correlation_method=CORRELATION_METHOD,
        dsn=dsn,
    )


def _expect_gate_rejection(*, dsn: str, message: str) -> None:
    try:
        _resolve_gate(dsn=dsn)
    except ValidationError as exc:
        if message not in str(exc):
            raise RuntimeError(
                "model approval gate failed with an unexpected reason"
            ) from exc
        return
    raise RuntimeError("model approval gate unexpectedly accepted the request")


def _expect_append_only_rejection(
    *,
    dsn: str,
    statement: str,
    parameters: tuple[Any, ...],
) -> None:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Model approval contract check requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
            connection.commit()
    except psycopg.Error:
        return
    raise RuntimeError("append-only model governance mutation was unexpectedly allowed")


def run_model_approval_contract_check(*, dsn: str) -> dict[str, Any]:
    first_approved_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    revoked_at = datetime(2026, 2, 10, 12, tzinfo=timezone.utc)
    second_approved_at = datetime(2026, 3, 10, 12, tzinfo=timezone.utc)

    _expect_gate_rejection(dsn=dsn, message="current model approval is required")

    first = approve_model_contract(
        dsn=dsn,
        use_case_name=USE_CASE,
        attribution_model_version=EWMA_MODEL_VERSION,
        weighting_method=WEIGHTING_METHOD,
        covariance_method=COVARIANCE_METHOD,
        correlation_method=CORRELATION_METHOD,
        request_identifier="MODEL-APPROVAL-2026-001",
        approved_by="model-risk@example.test",
        reason="Approve the fixed-decay EWMA contract for local risk-limit evidence.",
        approved_at=first_approved_at,
    )
    first_retry = approve_model_contract(
        dsn=dsn,
        use_case_name=USE_CASE,
        attribution_model_version=EWMA_MODEL_VERSION,
        weighting_method=WEIGHTING_METHOD,
        covariance_method=COVARIANCE_METHOD,
        correlation_method=CORRELATION_METHOD,
        request_identifier="MODEL-APPROVAL-2026-001",
        approved_by="model-risk@example.test",
        reason="Approve the fixed-decay EWMA contract for local risk-limit evidence.",
        approved_at=first_approved_at,
    )
    if first_retry["created"] or first_retry["approval_id"] != first["approval_id"]:
        raise RuntimeError("model approval retry did not converge")

    first_gate = _resolve_gate(dsn=dsn)
    if first_gate.approval_id != first["approval_id"]:
        raise RuntimeError("model approval gate did not bind the first approval")

    revocation = revoke_model_approval(
        dsn=dsn,
        approval_id=str(first["approval_id"]),
        request_identifier="MODEL-REVOCATION-2026-001",
        revoked_by="model-risk@example.test",
        reason="Exercise append-only revocation and reapproval in the contract check.",
        revoked_at=revoked_at,
    )
    revocation_retry = revoke_model_approval(
        dsn=dsn,
        approval_id=str(first["approval_id"]),
        request_identifier="MODEL-REVOCATION-2026-001",
        revoked_by="model-risk@example.test",
        reason="Exercise append-only revocation and reapproval in the contract check.",
        revoked_at=revoked_at,
    )
    if (
        revocation_retry["created"]
        or revocation_retry["revocation_id"] != revocation["revocation_id"]
    ):
        raise RuntimeError("model approval revocation retry did not converge")

    _expect_gate_rejection(dsn=dsn, message="current model approval is revoked")

    second = approve_model_contract(
        dsn=dsn,
        use_case_name=USE_CASE,
        attribution_model_version=EWMA_MODEL_VERSION,
        weighting_method=WEIGHTING_METHOD,
        covariance_method=COVARIANCE_METHOD,
        correlation_method=CORRELATION_METHOD,
        request_identifier="MODEL-APPROVAL-2026-002",
        approved_by="model-risk@example.test",
        reason="Reapprove the same immutable model contract after review.",
        approved_at=second_approved_at,
    )
    second_gate = _resolve_gate(dsn=dsn)
    if second_gate.approval_id != second["approval_id"]:
        raise RuntimeError("model approval gate did not bind the reapproval")
    if second_gate.gate_evidence_id == first_gate.gate_evidence_id:
        raise RuntimeError("model approval gate evidence did not change on reapproval")

    try:
        approve_model_contract(
            dsn=dsn,
            use_case_name=USE_CASE,
            attribution_model_version=EWMA_MODEL_VERSION,
            weighting_method=WEIGHTING_METHOD,
            covariance_method=COVARIANCE_METHOD,
            correlation_method=CORRELATION_METHOD,
            request_identifier="MODEL-APPROVAL-2026-002",
            approved_by="model-risk@example.test",
            reason="Conflicting content must not reuse the same request ID.",
            approved_at=second_approved_at,
        )
    except ValidationError:
        pass
    else:
        raise RuntimeError("conflicting model approval request reuse was accepted")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Model approval contract check requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        connection.read_only = True
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    approval_id,
                    approval_status,
                    approval_count,
                    revocation_count,
                    approved_at
                FROM risk_platform.current_model_approval_status
                WHERE use_case = %s
                  AND contract_fingerprint = %s
                """,
                (USE_CASE, second["contract_fingerprint"]),
            )
            current = cursor.fetchone()
            if current is None:
                raise RuntimeError("current model approval status was not produced")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM risk_platform.model_approval_event_history
                WHERE use_case = %s
                  AND contract_fingerprint = %s
                """,
                (USE_CASE, second["contract_fingerprint"]),
            )
            history_count_row = cursor.fetchone()
            if history_count_row is None:
                raise RuntimeError("model approval history count is unavailable")
            history_count = int(history_count_row[0])

    if current[0] != second["approval_id"]:
        raise RuntimeError("current model approval did not select the latest approval")
    if current[1] != "approved" or int(current[2]) != 2 or int(current[3]) != 0:
        raise RuntimeError("current model approval status is inconsistent")
    if not isinstance(current[4], datetime) or current[4].astimezone(
        timezone.utc
    ) != second_approved_at:
        raise RuntimeError("current model approval timestamp is inconsistent")
    if history_count != 3:
        raise RuntimeError("model approval event history did not retain all events")

    _expect_append_only_rejection(
        dsn=dsn,
        statement=(
            "UPDATE risk_platform.model_approvals "
            "SET reason = reason WHERE approval_id = %s"
        ),
        parameters=(second["approval_id"],),
    )
    _expect_append_only_rejection(
        dsn=dsn,
        statement=(
            "DELETE FROM risk_platform.model_approval_revocations "
            "WHERE revocation_id = %s"
        ),
        parameters=(revocation["revocation_id"],),
    )

    return {
        "use_case": USE_CASE,
        "contract_fingerprint": second["contract_fingerprint"],
        "first_approval_id": first["approval_id"],
        "first_gate_evidence_id": first_gate.gate_evidence_id,
        "revocation_id": revocation["revocation_id"],
        "current_approval_id": second["approval_id"],
        "current_gate_evidence_id": second_gate.gate_evidence_id,
        "current_status": current[1],
        "approval_count": int(current[2]),
        "history_event_count": history_count,
        "missing_gate_rejected": True,
        "revoked_gate_rejected": True,
        "append_only_verified": True,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise model approval idempotency, revocation, current status, "
            "runtime gating and append-only PostgreSQL triggers."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        summary = run_model_approval_contract_check(dsn=args.dsn)
    except Exception as exc:
        print(f"Model approval contract check failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
