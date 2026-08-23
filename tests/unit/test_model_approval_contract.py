from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.model_approval_contract import (
    APPROVAL_MODEL_VERSION,
    REVOCATION_MODEL_VERSION,
    build_model_approval,
    build_model_approval_revocation,
    build_model_contract,
)


def _sample_contract():
    return build_model_contract(
        attribution_model_version="portfolio-attribution-v1",
        weighting_method="constant_weight_daily_rebalanced",
        covariance_method="sample_annualized",
        correlation_method="pearson",
    )


def _ewma_contract():
    return build_model_contract(
        attribution_model_version="portfolio-attribution-ewma-v1",
        weighting_method="constant_weight_daily_rebalanced",
        covariance_method="ewma_zero_mean_lambda_0_94_annualized",
        correlation_method="implied_from_ewma_covariance",
    )


def test_model_contract_fingerprints_bind_methods_and_fixed_parameters() -> None:
    sample = _sample_contract()
    sample_replay = _sample_contract()
    ewma = _ewma_contract()

    assert sample == sample_replay
    assert sample.contract_fingerprint.startswith("model-contract-v1-")
    assert ewma.contract_fingerprint.startswith("model-contract-v1-")
    assert sample.contract_fingerprint != ewma.contract_fingerprint
    assert sample.fixed_parameters_json == (
        '{"annualization_days":252,"degrees_of_freedom":1,"estimator":"sample"}'
    )
    assert ewma.fixed_parameters_json == (
        '{"annualization_days":252,"decay":0.94,'
        '"mean_assumption":"zero_daily"}'
    )


def test_approval_identity_binds_use_case_request_contract_and_timestamp() -> None:
    contract = _ewma_contract()
    approved_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    approval = build_model_approval(
        use_case_name="portfolio-risk-limit-evaluation",
        contract=contract,
        request_identifier="MODEL-2026-001",
        approved_at=approved_at,
        approved_by="reviewer@example.test",
        reason="Approve fixed-decay EWMA for the declared use.",
    )
    replay = build_model_approval(
        use_case_name="portfolio-risk-limit-evaluation",
        contract=contract,
        request_identifier="MODEL-2026-001",
        approved_at=approved_at,
        approved_by="reviewer@example.test",
        reason="Approve fixed-decay EWMA for the declared use.",
    )
    later = build_model_approval(
        use_case_name="portfolio-risk-limit-evaluation",
        contract=contract,
        request_identifier="MODEL-2026-001",
        approved_at=approved_at + timedelta(seconds=1),
        approved_by="reviewer@example.test",
        reason="Approve fixed-decay EWMA for the declared use.",
    )
    other_request = build_model_approval(
        use_case_name="portfolio-risk-limit-evaluation",
        contract=contract,
        request_identifier="MODEL-2026-002",
        approved_at=approved_at,
        approved_by="reviewer@example.test",
        reason="Approve fixed-decay EWMA for the declared use.",
    )

    assert approval == replay
    assert approval.approval_id.startswith(f"{APPROVAL_MODEL_VERSION}-")
    assert len({approval.approval_id, later.approval_id, other_request.approval_id}) == 3


def test_revocation_identity_binds_approval_request_and_timestamp() -> None:
    revoked_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    revocation = build_model_approval_revocation(
        approval_id="model-approval-v1-0123456789abcdef01234567",
        request_identifier="REVOKE-2026-001",
        revoked_at=revoked_at,
        revoked_by="reviewer@example.test",
        reason="Withdraw the approval after model review.",
    )
    replay = build_model_approval_revocation(
        approval_id="model-approval-v1-0123456789abcdef01234567",
        request_identifier="REVOKE-2026-001",
        revoked_at=revoked_at,
        revoked_by="reviewer@example.test",
        reason="Withdraw the approval after model review.",
    )

    assert revocation == replay
    assert revocation.revocation_id.startswith(f"{REVOCATION_MODEL_VERSION}-")


def test_contract_rejects_mixed_methods_and_invalid_evidence() -> None:
    with pytest.raises(ValidationError, match="not supported"):
        build_model_contract(
            attribution_model_version="portfolio-attribution-ewma-v1",
            weighting_method="constant_weight_daily_rebalanced",
            covariance_method="sample_annualized",
            correlation_method="pearson",
        )

    with pytest.raises(ValidationError, match="invalid format"):
        build_model_approval(
            use_case_name="Portfolio Risk Limits",
            contract=_sample_contract(),
            request_identifier="MODEL-2026-001",
            approved_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            approved_by="reviewer@example.test",
            reason="Valid reason.",
        )

    with pytest.raises(ValidationError, match="timezone-aware"):
        build_model_approval(
            use_case_name="portfolio-risk-limit-evaluation",
            contract=_sample_contract(),
            request_identifier="MODEL-2026-001",
            approved_at="2026-01-01T12:00:00",
            approved_by="reviewer@example.test",
            reason="Valid reason.",
        )

    with pytest.raises(ValidationError, match="control characters"):
        build_model_approval(
            use_case_name="portfolio-risk-limit-evaluation",
            contract=_sample_contract(),
            request_identifier="MODEL-2026-001",
            approved_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            approved_by="reviewer@example.test",
            reason="invalid\nreason",
        )
