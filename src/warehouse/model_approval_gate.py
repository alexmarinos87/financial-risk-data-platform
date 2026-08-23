from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.model_approval_contract import (
    SAMPLE_CONTRACT,
    ModelContract,
    build_model_contract,
    bounded_text,
)

GATE_MODEL_VERSION = "model-approval-gate-v1"
RISK_LIMIT_USE_CASE = "portfolio-risk-limit-evaluation"

ApprovalStatusReader = Callable[..., Mapping[str, Any] | None]


@dataclass(frozen=True, slots=True)
class ModelApprovalGateEvidence:
    gate_evidence_id: str
    model_version: str
    use_case: str
    method_policy_fingerprint: str
    contract_fingerprint: str
    attribution_model_version: str
    weighting_method: str
    covariance_method: str
    correlation_method: str
    approval_required: bool
    decision: str
    approval_id: str | None
    approved_at: datetime | None
    approved_by: str | None


def _evidence_id(
    *,
    method_policy_fingerprint: str,
    contract: ModelContract,
    decision: str,
    approval_id: str | None,
) -> str:
    payload = {
        "approval_id": approval_id,
        "contract_fingerprint": contract.contract_fingerprint,
        "decision": decision,
        "method_policy_fingerprint": method_policy_fingerprint,
        "model_version": GATE_MODEL_VERSION,
        "use_case": RISK_LIMIT_USE_CASE,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{GATE_MODEL_VERSION}-{digest}"


def _read_current_approval(
    *,
    dsn: str,
    use_case: str,
    contract_fingerprint: str,
) -> Mapping[str, Any] | None:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required dependency in CI.
        raise RuntimeError("Model approval gating requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            connection.read_only = True
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        approval_id,
                        approval_status,
                        approved_at,
                        approved_by,
                        revocation_id,
                        revoked_at
                    FROM risk_platform.current_model_approval_status
                    WHERE use_case = %s
                      AND contract_fingerprint = %s
                    """,
                    (use_case, contract_fingerprint),
                )
                rows = cursor.fetchall()
    except Exception:
        raise StorageError("Unable to read current model approval status") from None

    if len(rows) > 1:
        raise StorageError("Current model approval status returned duplicate grains")
    if not rows:
        return None
    row = rows[0]
    return {
        "approval_id": row[0],
        "approval_status": row[1],
        "approved_at": row[2],
        "approved_by": row[3],
        "revocation_id": row[4],
        "revoked_at": row[5],
    }


def _contract(
    *,
    attribution_model_version: str,
    weighting_method: str,
    covariance_method: str,
    correlation_method: str,
) -> ModelContract:
    return build_model_contract(
        attribution_model_version=attribution_model_version,
        weighting_method=weighting_method,
        covariance_method=covariance_method,
        correlation_method=correlation_method,
    )


def resolve_model_approval_gate(
    *,
    method_policy_fingerprint: str,
    attribution_model_version: str,
    weighting_method: str,
    covariance_method: str,
    correlation_method: str,
    dsn: str,
    approval_status_reader: ApprovalStatusReader | None = None,
) -> ModelApprovalGateEvidence:
    policy_fingerprint = bounded_text(
        method_policy_fingerprint,
        "method_policy_fingerprint",
        256,
    )
    contract = _contract(
        attribution_model_version=attribution_model_version,
        weighting_method=weighting_method,
        covariance_method=covariance_method,
        correlation_method=correlation_method,
    )

    if (
        contract.attribution_model_version,
        contract.weighting_method,
        contract.covariance_method,
        contract.correlation_method,
    ) == SAMPLE_CONTRACT:
        decision = "baseline_exempt"
        return ModelApprovalGateEvidence(
            gate_evidence_id=_evidence_id(
                method_policy_fingerprint=policy_fingerprint,
                contract=contract,
                decision=decision,
                approval_id=None,
            ),
            model_version=GATE_MODEL_VERSION,
            use_case=RISK_LIMIT_USE_CASE,
            method_policy_fingerprint=policy_fingerprint,
            contract_fingerprint=contract.contract_fingerprint,
            attribution_model_version=contract.attribution_model_version,
            weighting_method=contract.weighting_method,
            covariance_method=contract.covariance_method,
            correlation_method=contract.correlation_method,
            approval_required=False,
            decision=decision,
            approval_id=None,
            approved_at=None,
            approved_by=None,
        )

    reader = approval_status_reader or _read_current_approval
    try:
        current = reader(
            dsn=dsn,
            use_case=RISK_LIMIT_USE_CASE,
            contract_fingerprint=contract.contract_fingerprint,
        )
    except (ValidationError, StorageError, RuntimeError):
        raise
    except Exception:
        raise StorageError("Unable to resolve current model approval") from None

    if current is None:
        raise ValidationError(
            "a current model approval is required for the non-baseline method policy"
        )
    if not isinstance(current, Mapping):
        raise StorageError("Current model approval status is incompatible")

    status = current.get("approval_status")
    if status == "revoked":
        raise ValidationError(
            "the current model approval is revoked for the non-baseline method policy"
        )
    if status != "approved":
        raise StorageError("Current model approval status is invalid")

    approval_id = bounded_text(current.get("approval_id"), "approval_id", 256)
    approved_by = bounded_text(current.get("approved_by"), "approved_by")
    approved_at = current.get("approved_at")
    if (
        not isinstance(approved_at, datetime)
        or approved_at.tzinfo is None
        or approved_at.utcoffset() is None
    ):
        raise StorageError("Current model approval timestamp is incompatible")
    approved_at = approved_at.astimezone(timezone.utc)
    decision = "approved"
    return ModelApprovalGateEvidence(
        gate_evidence_id=_evidence_id(
            method_policy_fingerprint=policy_fingerprint,
            contract=contract,
            decision=decision,
            approval_id=approval_id,
        ),
        model_version=GATE_MODEL_VERSION,
        use_case=RISK_LIMIT_USE_CASE,
        method_policy_fingerprint=policy_fingerprint,
        contract_fingerprint=contract.contract_fingerprint,
        attribution_model_version=contract.attribution_model_version,
        weighting_method=contract.weighting_method,
        covariance_method=contract.covariance_method,
        correlation_method=contract.correlation_method,
        approval_required=True,
        decision=decision,
        approval_id=approval_id,
        approved_at=approved_at,
        approved_by=approved_by,
    )


def model_approval_gate_metadata(
    evidence: ModelApprovalGateEvidence,
) -> dict[str, Any]:
    return {
        "gate_evidence_id": evidence.gate_evidence_id,
        "model_version": evidence.model_version,
        "use_case": evidence.use_case,
        "method_policy_fingerprint": evidence.method_policy_fingerprint,
        "contract_fingerprint": evidence.contract_fingerprint,
        "attribution_model_version": evidence.attribution_model_version,
        "weighting_method": evidence.weighting_method,
        "covariance_method": evidence.covariance_method,
        "correlation_method": evidence.correlation_method,
        "approval_required": evidence.approval_required,
        "decision": evidence.decision,
        "approval_id": evidence.approval_id,
        "approved_at": (
            evidence.approved_at.isoformat()
            if evidence.approved_at is not None
            else None
        ),
        "approved_by": evidence.approved_by,
    }
