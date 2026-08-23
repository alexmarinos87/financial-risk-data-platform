from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError

APPROVAL_MODEL_VERSION = "model-approval-v1"
REVOCATION_MODEL_VERSION = "model-approval-revocation-v1"
USE_CASE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MAX_TEXT_LENGTH = 128
MAX_REASON_LENGTH = 2_000
MAX_PARAMETER_DOCUMENT_BYTES = 8_192

SAMPLE_CONTRACT = (
    "portfolio-attribution-v1",
    "constant_weight_daily_rebalanced",
    "sample_annualized",
    "pearson",
)
EWMA_CONTRACT = (
    "portfolio-attribution-ewma-v1",
    "constant_weight_daily_rebalanced",
    "ewma_zero_mean_lambda_0_94_annualized",
    "implied_from_ewma_covariance",
)
SUPPORTED_CONTRACTS = frozenset({SAMPLE_CONTRACT, EWMA_CONTRACT})


@dataclass(frozen=True, slots=True)
class ModelContract:
    attribution_model_version: str
    weighting_method: str
    covariance_method: str
    correlation_method: str
    fixed_parameters_json: str
    contract_fingerprint: str


@dataclass(frozen=True, slots=True)
class ModelApproval:
    approval_id: str
    model_version: str
    use_case: str
    contract: ModelContract
    request_id: str
    approved_at: datetime
    approved_by: str
    reason: str


@dataclass(frozen=True, slots=True)
class ModelApprovalRevocation:
    revocation_id: str
    model_version: str
    approval_id: str
    request_id: str
    revoked_at: datetime
    revoked_by: str
    reason: str


def bounded_text(value: Any, label: str, maximum: int = MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip()
    if not parsed or len(parsed) > maximum:
        raise ValidationError(
            f"{label} must contain between 1 and {maximum} characters"
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def use_case(value: Any) -> str:
    parsed = bounded_text(value, "use_case")
    if USE_CASE_PATTERN.fullmatch(parsed) is None:
        raise ValidationError("use_case has an invalid format")
    return parsed


def request_id(value: Any) -> str:
    parsed = bounded_text(value, "request_id")
    if REQUEST_ID_PATTERN.fullmatch(parsed) is None:
        raise ValidationError("request_id has an invalid format")
    return parsed


def aware_utc(value: datetime | str, label: str) -> datetime:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def _normalise_parameter_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 5:
        raise ValidationError("fixed model parameters exceed the nesting limit")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("fixed model parameters must be finite JSON values")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = bounded_text(raw_key, "fixed parameter key")
            if key in result:
                raise ValidationError("fixed model parameters contain duplicate keys")
            result[key] = _normalise_parameter_value(item, depth=depth + 1)
        return {key: result[key] for key in sorted(result)}
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValidationError("fixed model parameter arrays exceed the item limit")
        return [
            _normalise_parameter_value(item, depth=depth + 1)
            for item in value
        ]
    raise ValidationError("fixed model parameters must contain JSON-compatible values")


def _canonical_json(value: Mapping[str, Any]) -> str:
    normalised = _normalise_parameter_value(value)
    if not isinstance(normalised, dict):
        raise ValidationError("fixed model parameters must be a JSON object")
    document = json.dumps(
        normalised,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(document.encode("utf-8")) > MAX_PARAMETER_DOCUMENT_BYTES:
        raise ValidationError("fixed model parameter document exceeds the size limit")
    return document


def _contract_parameters(contract_tuple: tuple[str, str, str, str]) -> dict[str, Any]:
    if contract_tuple == SAMPLE_CONTRACT:
        return {
            "annualization_days": 252,
            "degrees_of_freedom": 1,
            "estimator": "sample",
        }
    if contract_tuple == EWMA_CONTRACT:
        return {
            "annualization_days": 252,
            "decay": 0.94,
            "mean_assumption": "zero_daily",
        }
    raise ValidationError("model contract is not supported")


def build_model_contract(
    *,
    attribution_model_version: str,
    weighting_method: str,
    covariance_method: str,
    correlation_method: str,
) -> ModelContract:
    contract_tuple = (
        bounded_text(attribution_model_version, "attribution_model_version"),
        bounded_text(weighting_method, "weighting_method"),
        bounded_text(covariance_method, "covariance_method"),
        bounded_text(correlation_method, "correlation_method"),
    )
    if contract_tuple not in SUPPORTED_CONTRACTS:
        raise ValidationError("model contract is not supported")
    parameters_json = _canonical_json(_contract_parameters(contract_tuple))
    fingerprint_payload = {
        "attribution_model_version": contract_tuple[0],
        "correlation_method": contract_tuple[3],
        "covariance_method": contract_tuple[2],
        "fixed_parameters": json.loads(parameters_json),
        "weighting_method": contract_tuple[1],
    }
    digest = hashlib.sha256(
        json.dumps(
            fingerprint_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return ModelContract(
        attribution_model_version=contract_tuple[0],
        weighting_method=contract_tuple[1],
        covariance_method=contract_tuple[2],
        correlation_method=contract_tuple[3],
        fixed_parameters_json=parameters_json,
        contract_fingerprint=f"model-contract-v1-{digest}",
    )


def build_model_approval(
    *,
    use_case_name: str,
    contract: ModelContract,
    request_identifier: str,
    approved_at: datetime | str,
    approved_by: str,
    reason: str,
) -> ModelApproval:
    canonical_use_case = use_case(use_case_name)
    canonical_request_id = request_id(request_identifier)
    timestamp = aware_utc(approved_at, "approved_at")
    actor = bounded_text(approved_by, "approved_by")
    canonical_reason = bounded_text(reason, "reason", MAX_REASON_LENGTH)
    payload = {
        "approved_at": timestamp.isoformat(),
        "contract_fingerprint": contract.contract_fingerprint,
        "model_version": APPROVAL_MODEL_VERSION,
        "request_id": canonical_request_id,
        "use_case": canonical_use_case,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return ModelApproval(
        approval_id=f"{APPROVAL_MODEL_VERSION}-{digest}",
        model_version=APPROVAL_MODEL_VERSION,
        use_case=canonical_use_case,
        contract=contract,
        request_id=canonical_request_id,
        approved_at=timestamp,
        approved_by=actor,
        reason=canonical_reason,
    )


def build_model_approval_revocation(
    *,
    approval_id: str,
    request_identifier: str,
    revoked_at: datetime | str,
    revoked_by: str,
    reason: str,
) -> ModelApprovalRevocation:
    canonical_approval_id = bounded_text(approval_id, "approval_id", 256)
    canonical_request_id = request_id(request_identifier)
    timestamp = aware_utc(revoked_at, "revoked_at")
    actor = bounded_text(revoked_by, "revoked_by")
    canonical_reason = bounded_text(reason, "reason", MAX_REASON_LENGTH)
    payload = {
        "approval_id": canonical_approval_id,
        "model_version": REVOCATION_MODEL_VERSION,
        "request_id": canonical_request_id,
        "revoked_at": timestamp.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return ModelApprovalRevocation(
        revocation_id=f"{REVOCATION_MODEL_VERSION}-{digest}",
        model_version=REVOCATION_MODEL_VERSION,
        approval_id=canonical_approval_id,
        request_id=canonical_request_id,
        revoked_at=timestamp,
        revoked_by=actor,
        reason=canonical_reason,
    )
