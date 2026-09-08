from __future__ import annotations

import copy
import hashlib
from datetime import timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.warehouse import notification_worker_attempt_attribution_contract as contract
from test_notification_worker_authority_contract import grant
from test_worker_attempt_observer import attempt
from test_worker_execution_context import context


def record(kind: str = "initial", **changes: Any) -> dict[str, Any]:
    arguments = {"authority": grant(), "context": context(kind),
                 "attempt": attempt(1 if kind == "initial" else 2)}
    arguments.update(changes)
    return contract.build_worker_attempt_attribution(**arguments)


def rehash(value: dict[str, Any]) -> None:
    identity = {key: item for key, item in value.items() if key != "attribution_id"}
    value["attribution_id"] = f"{contract.MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"


@pytest.mark.parametrize("kind", ["initial", "retry"])
@pytest.mark.parametrize("failed", [False, True])
def test_record_binds_exact_sources_and_observed_outcome(kind: str, failed: bool) -> None:
    source = attempt(1 if kind == "initial" else 2)
    if failed:
        source.update(outcome="failed", http_status=None, error_code="network_error")
    result = record(kind, attempt=source)
    assert contract.validate_worker_attempt_attribution(result) == result
    assert result == record(kind, attempt=source)
    assert result["receipt"]["source_attempt_sha256"] == hashlib.sha256(canonical_bytes(result["attempt"])).hexdigest()
    assert result["receipt"]["context_id"] == result["context"]["context_id"]
    assert result["receipt"]["outcome"] == ("failed" if failed else "succeeded")
    assert result["failure_history_complete"] is False
    assert result["runtime_permission_granted"] is False


def test_timezone_normalization_and_detached_input() -> None:
    source = attempt()
    prior, selected = grant(), context()
    before = copy.deepcopy((prior, selected, source))
    result = record(authority=prior, context=selected, attempt=source)
    assert (prior, selected, source) == before
    source["attempted_at"] = source["attempted_at"].astimezone(timezone(timedelta(hours=1))).isoformat()
    assert record(attempt=source) == result
    source["endpoint_host"] = "changed.invalid"
    selected["worker_id"] = "other"
    assert result["attempt"]["endpoint_host"] != source["endpoint_host"]
    assert result["context"]["worker_id"] != selected["worker_id"]


@pytest.mark.parametrize("group,field,value", [
    ("receipt", "worker_id", "other"), ("receipt", "context_id", "other"),
    ("receipt", "source_attempt_sha256", "0" * 64), ("receipt", "outcome", "failed"),
    ("attempt", "endpoint_host", "other.invalid"), ("attempt", "payload_sha256", "b" * 64),
    ("attempt", "attempt_number", True), ("attempt", "outcome", "failed"),
    ("context", "runtime_permission_granted", True), ("context", "scope_id", "other"),
    ("authority", "request_id", "OTHER"),
])
def test_rehashed_contradictory_relationship_is_rejected(group: str, field: str, value: Any) -> None:
    result = record()
    result[group][field] = value
    rehash(result)
    with pytest.raises(ValidationError):
        contract.validate_worker_attempt_attribution(result)


@pytest.mark.parametrize("field", ["failure_history_complete", "runtime_permission_granted"])
def test_rehashed_permission_or_coverage_promotion_is_rejected(field: str) -> None:
    result = record()
    result[field] = True
    rehash(result)
    with pytest.raises(ValidationError):
        contract.validate_worker_attempt_attribution(result)


@pytest.mark.parametrize("value", [None, [], {}, {"unexpected": True}])
def test_malformed_records_are_fixed_validation_failures(value: Any) -> None:
    with pytest.raises(ValidationError):
        contract.validate_worker_attempt_attribution(value)


@pytest.mark.parametrize("bad", ["host\x00name", "host\ud800name", "host\udfffname"])
def test_database_incompatible_text_is_rejected_without_repair(bad: str) -> None:
    with pytest.raises(ValidationError) as caught:
        record(attempt=attempt(endpoint_host=bad))
    assert bad not in str(caught.value)


def test_source_and_record_byte_limits_are_enforced(monkeypatch: Any) -> None:
    result = record()
    monkeypatch.setattr(contract, "MAX_ATTEMPT_BYTES", len(canonical_bytes(result["attempt"])) - 1)
    with pytest.raises(ValidationError, match="byte limit"):
        record()
    monkeypatch.setattr(contract, "MAX_ATTEMPT_BYTES", 8192)
    monkeypatch.setattr(contract, "MAX_RECORD_BYTES", len(canonical_bytes(result)) - 1)
    with pytest.raises(ValidationError, match="byte limit"):
        record()
    monkeypatch.setattr(contract, "MAX_RECORD_BYTES", len(canonical_bytes(result)))
    assert record() == result


def test_valid_source_change_gets_new_identity_not_same_receipt() -> None:
    original = record()
    changed = record(attempt=attempt(endpoint_host="other.invalid"))
    assert changed["attribution_id"] != original["attribution_id"]
    assert changed["receipt"]["receipt_id"] != original["receipt"]["receipt_id"]
    assert changed["context"] == original["context"]


def test_existing_observer_receipt_agrees_with_persistence_record() -> None:
    from src.orchestration.notification_worker_attempt_observer import WorkerAttemptObserver
    watch = WorkerAttemptObserver(authority=grant(), context=context(), writer=lambda row: None)
    watch(attempt())
    assert record()["receipt"] == watch.seal()["receipts"][0]
    extra = record()
    extra["unknown"] = None
    with pytest.raises(ValidationError):
        contract.validate_worker_attempt_attribution(extra)
