from __future__ import annotations

import copy
import hashlib
from datetime import timedelta
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.notification_execution_readiness_history_contract import (
    MODEL_VERSION, build_notification_execution_readiness_record,
)
from src.warehouse.notification_worker_readiness_source import (
    source_bytes, source_identifier, source_time, verify_worker_readiness_record,
)
from test_notification_execution_readiness_gate import (
    DESTINATION_ID, EVALUATED_AT, _delivery, _evaluate,
)


def readiness_record(kind: str = "initial", *, blocked: bool = False) -> dict[str, Any]:
    return build_notification_execution_readiness_record(
        request_id="source-" + kind, recorded_at=EVALUATED_AT + timedelta(seconds=1),
        decision=_evaluate(execution_kind=kind, delivery=_delivery(enabled=not blocked)),
    )


def verify(record: dict[str, Any] | None = None, **changes: Any) -> dict[str, Any]:
    selected = readiness_record() if record is None else record
    args = {
        "record": selected, "document_sha256": hashlib.sha256(source_bytes(selected)).hexdigest(),
        "expected_record_id": selected["record_id"], "destination_id": DESTINATION_ID,
        "execution_kind": selected["decision"]["execution_kind"] if isinstance(selected["decision"], dict) else "initial",
        "observed_at": EVALUATED_AT + timedelta(seconds=2),
    }
    args.update(changes)
    return verify_worker_readiness_record(**args)


@pytest.mark.parametrize("kind", ["initial", "retry"])
def test_reopens_real_canonical_record_but_never_grants_current_permission(kind: str) -> None:
    source = readiness_record(kind)
    result = verify(source)
    assert result["record"] == source
    assert result["retained_status"] == "allowed"
    assert result["age_seconds"] == 2
    assert result["current_evidence_verified"] is False
    assert result["runtime_permission_granted"] is False
    assert result == verify(source)
    source["decision"]["blocking_reasons"].append("changed")
    assert result["record"]["decision"]["blocking_reasons"] == []


def test_retained_block_cannot_be_presented_as_allow() -> None:
    assert verify(readiness_record(blocked=True))["retained_status"] == "blocked"


@pytest.mark.parametrize("age,status", [(300, "allowed"), (300.000001, "stale")])
def test_recomputes_age_at_explicit_observation_instant(age: float, status: str) -> None:
    assert verify(observed_at=EVALUATED_AT + timedelta(seconds=age))["retained_status"] == status


@pytest.mark.parametrize("field,value", [
    ("expected_record_id", "wrong"), ("destination_id", "other-destination"),
    ("execution_kind", "retry"), ("document_sha256", "0" * 64),
    ("document_sha256", "not-a-digest"), ("execution_kind", "execute"),
])
def test_wrong_selected_identity_or_digest_is_rejected(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        verify(**{field: value})


@pytest.mark.parametrize("age", [-1, 0, 0.999999])
def test_recording_time_cannot_postdate_observation(age: float) -> None:
    with pytest.raises(ValidationError, match="postdates"):
        verify(observed_at=EVALUATED_AT + timedelta(seconds=age))


@pytest.mark.parametrize("maximum", [True, False, 0, 301, 1.5, "300"])
def test_age_limit_is_strictly_typed_and_bounded(maximum: Any) -> None:
    with pytest.raises(ValidationError):
        verify(max_age_seconds=maximum)


def test_rehashed_record_still_requires_original_semantic_validator() -> None:
    record = readiness_record(blocked=True)
    record["decision"]["decision"] = "allow"
    identity = {key: value for key, value in record.items() if key != "record_id"}
    record["record_id"] = f"{MODEL_VERSION}-record-{hashlib.sha256(source_bytes(identity)).hexdigest()[:24]}"
    with pytest.raises(ValidationError):
        verify(record)


@pytest.mark.parametrize("change", ["unknown", "bad_time", "wrong_model", "not_object"])
def test_malformed_retained_document_is_rejected(change: str) -> None:
    record = readiness_record()
    if change == "unknown":
        record["endpoint_value"] = "not-permitted"
    elif change == "bad_time":
        record["recorded_at"] = "2026-06-01T12:00:01"
    elif change == "wrong_model":
        record["model_version"] = "other"
    else:
        record["decision"] = []
    with pytest.raises(ValidationError):
        verify(record, execution_kind="initial")


def test_helpers_reject_unbounded_noncanonical_or_naive_inputs() -> None:
    for value in ({"x": "a" * 1_048_576}, {"x": float("nan")}, {"x": object()}, []):
        with pytest.raises(ValidationError):
            source_bytes(value)  # type: ignore[arg-type]
    for value in ("", "https://not-an-identifier", "a" * 513, None):
        with pytest.raises(ValidationError):
            source_identifier(value)
    with pytest.raises(ValidationError):
        source_time("2026-06-01T12:00:00")
    assert source_time("2026-06-01T13:00:00+01:00") == EVALUATED_AT


def test_validation_preserves_caller_owned_input() -> None:
    record = readiness_record()
    previous = copy.deepcopy(record)
    verify(record)
    assert record == previous
