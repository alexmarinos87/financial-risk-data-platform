from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.model_approval_gate import (
    GATE_MODEL_VERSION,
    RISK_LIMIT_USE_CASE,
    model_approval_gate_metadata,
    resolve_model_approval_gate,
)

METHOD_POLICY_FINGERPRINT = "risk-limit-method-policy-test"
SAMPLE = {
    "attribution_model_version": "portfolio-attribution-v1",
    "weighting_method": "constant_weight_daily_rebalanced",
    "covariance_method": "sample_annualized",
    "correlation_method": "pearson",
}
EWMA = {
    "attribution_model_version": "portfolio-attribution-ewma-v1",
    "weighting_method": "constant_weight_daily_rebalanced",
    "covariance_method": "ewma_zero_mean_lambda_0_94_annualized",
    "correlation_method": "implied_from_ewma_covariance",
}


def test_sample_baseline_is_exempt_without_reading_postgres() -> None:
    calls = 0

    def reader(**_: Any):
        nonlocal calls
        calls += 1
        raise AssertionError("baseline exemption must not query approvals")

    evidence = resolve_model_approval_gate(
        method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
        dsn="postgresql://unused",
        approval_status_reader=reader,
        **SAMPLE,
    )
    replay = resolve_model_approval_gate(
        method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
        dsn="postgresql://different-unused",
        approval_status_reader=reader,
        **SAMPLE,
    )

    assert evidence == replay
    assert calls == 0
    assert evidence.model_version == GATE_MODEL_VERSION
    assert evidence.use_case == RISK_LIMIT_USE_CASE
    assert evidence.approval_required is False
    assert evidence.decision == "baseline_exempt"
    assert evidence.approval_id is None
    assert evidence.gate_evidence_id.startswith(f"{GATE_MODEL_VERSION}-")


def test_ewma_requires_and_binds_one_current_approval() -> None:
    approved_at = datetime(2026, 3, 10, 12, tzinfo=timezone.utc)
    calls: list[dict[str, Any]] = []

    def reader(**kwargs: Any):
        calls.append(kwargs)
        return {
            "approval_id": "model-approval-v1-0123456789abcdef01234567",
            "approval_status": "approved",
            "approved_at": approved_at,
            "approved_by": "model-risk@example.test",
            "revocation_id": None,
            "revoked_at": None,
        }

    evidence = resolve_model_approval_gate(
        method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
        dsn="postgresql://example",
        approval_status_reader=reader,
        **EWMA,
    )
    metadata = model_approval_gate_metadata(evidence)

    assert len(calls) == 1
    assert calls[0]["dsn"] == "postgresql://example"
    assert calls[0]["use_case"] == RISK_LIMIT_USE_CASE
    assert calls[0]["contract_fingerprint"].startswith("model-contract-v1-")
    assert evidence.approval_required is True
    assert evidence.decision == "approved"
    assert evidence.approval_id == "model-approval-v1-0123456789abcdef01234567"
    assert evidence.approved_at == approved_at
    assert metadata["approval_id"] == evidence.approval_id
    assert metadata["approved_at"] == approved_at.isoformat()
    assert "reason" not in metadata


def test_ewma_fails_when_current_approval_is_missing_or_revoked() -> None:
    with pytest.raises(ValidationError, match="current model approval is required"):
        resolve_model_approval_gate(
            method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
            dsn="postgresql://unused",
            approval_status_reader=lambda **_: None,
            **EWMA,
        )

    with pytest.raises(ValidationError, match="current model approval is revoked"):
        resolve_model_approval_gate(
            method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
            dsn="postgresql://unused",
            approval_status_reader=lambda **_: {
                "approval_id": "model-approval-v1-0123456789abcdef01234567",
                "approval_status": "revoked",
                "approved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "approved_by": "model-risk@example.test",
                "revocation_id": "model-approval-revocation-v1-abcdef0123456789abcdef01",
                "revoked_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
            },
            **EWMA,
        )


def test_gate_rejects_incompatible_approval_evidence() -> None:
    with pytest.raises(StorageError, match="status is invalid"):
        resolve_model_approval_gate(
            method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
            dsn="postgresql://unused",
            approval_status_reader=lambda **_: {
                "approval_id": "model-approval-v1-0123456789abcdef01234567",
                "approval_status": "pending",
                "approved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "approved_by": "model-risk@example.test",
            },
            **EWMA,
        )

    with pytest.raises(StorageError, match="timestamp is incompatible"):
        resolve_model_approval_gate(
            method_policy_fingerprint=METHOD_POLICY_FINGERPRINT,
            dsn="postgresql://unused",
            approval_status_reader=lambda **_: {
                "approval_id": "model-approval-v1-0123456789abcdef01234567",
                "approval_status": "approved",
                "approved_at": datetime(2026, 1, 1),
                "approved_by": "model-risk@example.test",
            },
            **EWMA,
        )
