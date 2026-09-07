from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta
from threading import Thread
from typing import Any

import pytest

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import MODEL_VERSION as DELIVERY_MODEL, _attempt_id
from src.orchestration.notification_worker_attempt_observer import WorkerAttemptObserver
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from test_notification_worker_authority_contract import grant, plan_fixture, rehash
from test_worker_execution_context import context


def attempt(number: int = 1, event_id: str = "event-1", **changes: Any) -> dict[str, Any]:
    result = {
        "model_version": DELIVERY_MODEL, "attempt_id": _attempt_id(event_id, number),
        "event_id": event_id, "attempt_number": number, "channel": "webhook",
        "idempotency_key": event_id, "attempted_at": datetime.fromisoformat(context()["started_at"]),
        "outcome": "succeeded", "http_status": 204, "error_code": None,
        "endpoint_host": "receiver.example.invalid", "payload_sha256": "a" * 64,
    }
    return {**result, **changes}


def observer(kind: str = "initial", writer: Any = None, **changes: Any) -> WorkerAttemptObserver:
    return WorkerAttemptObserver(authority=grant(), context=context(kind), writer=writer or (lambda row: None), **changes)


@pytest.mark.parametrize("kind,number", [("initial", 1), ("retry", 2), ("retry", 10)])
def test_observes_actual_writer_returns_but_never_claims_durable_or_complete(kind: str, number: int) -> None:
    writes = []
    watch = observer(kind, writes.append)
    source = attempt(number)
    watch(source)
    report = watch.seal()
    assert writes == [source]
    assert report["writer_calls"] == report["writer_returns"] == report["attributed_attempts"] == 1
    assert report["unattributed_writer_returns"] == 0
    assert report["observed_succeeded_attempts"] == 1
    assert report["status"] == "sealed"
    receipt = report["receipts"][0]
    expected = {**source, "attempted_at": source["attempted_at"].isoformat()}
    assert receipt["source_attempt_sha256"] == hashlib.sha256(canonical_bytes(expected)).hexdigest()
    assert receipt["execution_kind"] == kind
    assert "endpoint_host" not in receipt and "receiver.example.invalid" not in canonical_bytes(report).decode()
    for field in ("attribution_durable", "database_commit_verified", "failure_history_complete", "runtime_permission_granted", "writer_outcome_uncertain"):
        assert report[field] is False
    assert report == watch.seal()


@pytest.mark.parametrize("status,error", [(None, "network_error"), (429, "http_429"), (503, "http_503")])
def test_failed_transport_is_not_a_failed_write(status: Any, error: str) -> None:
    watch = observer()
    watch(attempt(outcome="failed", http_status=status, error_code=error))
    report = watch.snapshot()
    assert report["observed_failed_attempts"] == 1
    assert report["writer_outcome_uncertain"] is False
    assert report["database_commit_verified"] is False


@pytest.mark.parametrize("field,value", [
    ("attempt_id", "wrong"), ("idempotency_key", "wrong"), ("model_version", "wrong"),
    ("channel", "email"), ("attempt_number", True), ("attempt_number", 11),
    ("attempt_number", 2), ("http_status", True), ("http_status", 600),
    ("outcome", "failed"), ("error_code", "private-detail"), ("payload_sha256", "wrong"),
    ("endpoint_host", ""), ("event_id", "x\nprivate"), ("attempted_at", "2026-09-05T20:05:00"),
])
def test_invalid_attribution_never_suppresses_the_original_write(field: str, value: Any) -> None:
    writes = []
    watch = observer(writer=writes.append)
    source = attempt(**{field: value})
    with pytest.raises(ValidationError, match="after writer returned"):
        watch(source)
    assert writes == [source]
    report = watch.snapshot()
    assert report["writer_returns"] == report["unattributed_writer_returns"] == 1
    assert report["attributed_attempts"] == 0
    assert report["failure_code"] == "attribution_rejected_after_writer"
    assert "private-detail" not in canonical_bytes(report).decode()
    with pytest.raises(ValidationError, match="no longer"):
        watch(attempt(event_id="next"))
    assert len(writes) == 1


@pytest.mark.parametrize("offset,valid", [(-0.000001, False), (0, True), (119.999999, True), (120, False)])
def test_attempt_time_is_bound_to_context(offset: float, valid: bool) -> None:
    source = attempt()
    source["attempted_at"] += timedelta(seconds=offset)
    watch = observer()
    if valid:
        watch(source)
        assert watch.snapshot()["attributed_attempts"] == 1
    else:
        with pytest.raises(ValidationError):
            watch(source)
        assert watch.snapshot()["writer_returns"] == 1


@pytest.mark.parametrize("error", [StorageError, ValidationError, RuntimeError])
def test_ambiguous_writer_failure_is_redacted_latched_and_not_retried(error: Any) -> None:
    calls = []
    def writer(row: Any) -> None:
        calls.append(row)
        raise error("private-dsn-detail")
    watch = observer(writer=writer)
    with pytest.raises(StorageError, match="unconfirmed") as caught:
        watch(attempt())
    assert "private-dsn-detail" not in str(caught.value)
    with pytest.raises(ValidationError):
        watch(attempt())
    report = watch.seal()
    assert len(calls) == report["writer_calls"] == 1
    assert report["writer_returns"] == report["attributed_attempts"] == 0
    assert report["writer_outcome_uncertain"] is True
    assert report["status"] == "observation_failed"


def test_writer_can_commit_then_raise_without_invented_success() -> None:
    committed = []
    def writer(row: Any) -> None:
        committed.append(copy.deepcopy(row))
        raise RuntimeError("failure-after-commit")
    watch = observer(writer=writer)
    with pytest.raises(StorageError):
        watch(attempt())
    assert len(committed) == 1
    assert watch.snapshot()["writer_outcome_uncertain"] is True
    assert watch.snapshot()["database_commit_verified"] is False


def test_keyboard_interrupt_keeps_uncertain_state_and_propagates() -> None:
    def interrupt(row: Any) -> None:
        raise KeyboardInterrupt
    watch = observer(writer=interrupt)
    with pytest.raises(KeyboardInterrupt):
        watch(attempt())
    assert watch.snapshot()["writer_outcome_uncertain"] is True
    with pytest.raises(ValidationError):
        watch(attempt())


@pytest.mark.parametrize("ack", [True, False, 1, {}, "committed"])
def test_unexpected_writer_return_is_not_commit_proof(ack: Any) -> None:
    watch = observer(writer=lambda row: ack)
    with pytest.raises(StorageError, match="unsupported"):
        watch(attempt())
    report = watch.snapshot()
    assert report["writer_returns"] == 1 and report["writer_outcome_uncertain"] is True
    assert report["attributed_attempts"] == 0


def test_duplicate_event_and_overflow_do_not_inflate_receipts() -> None:
    writes = []
    watch = observer("retry", writes.append)
    watch(attempt(2))
    with pytest.raises(ValidationError):
        watch(attempt(3))
    assert len(writes) == 2
    assert watch.snapshot()["attributed_attempts"] == 1
    plan = plan_fixture()
    plan["execution"]["work_items"][0]["max_events"] = 1
    prior = grant(plan=rehash(plan))
    limited = WorkerAttemptObserver(authority=prior, context=context(authority=prior), writer=writes.append)
    limited(attempt())
    with pytest.raises(ValidationError):
        limited(attempt(event_id="event-2"))
    assert limited.snapshot()["unattributed_writer_returns"] == 1


def test_writer_mutation_does_not_change_caller_or_attributed_source() -> None:
    source = attempt()
    before = copy.deepcopy(source)
    def mutate(row: Any) -> None:
        row["outcome"] = "failed"
    watch = observer(writer=mutate)
    watch(source)
    assert source == before
    report = watch.snapshot()
    report["receipts"].clear()
    assert watch.snapshot()["observed_succeeded_attempts"] == 1
    assert watch.snapshot()["database_commit_verified"] is False


def test_empty_sealed_observation_is_not_complete_failure_history() -> None:
    watch = observer()
    result = watch.seal()
    assert result["attributed_attempts"] == 0 and result["failure_history_complete"] is False
    with pytest.raises(ValidationError):
        watch(attempt())
    assert watch.snapshot()["writer_calls"] == 0


def test_reentrant_and_cross_thread_calls_do_not_reach_delegate() -> None:
    calls = []
    def writer(row: Any) -> None:
        calls.append(row)
        with pytest.raises(OverlapError):
            watch(attempt(event_id="nested"))
    watch = observer(writer=writer)
    watch(attempt())
    failures = []
    def elsewhere() -> None:
        try:
            watch(attempt(event_id="thread"))
        except OverlapError:
            failures.append(True)
    thread = Thread(target=elsewhere)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and failures == [True] and len(calls) == 1


def test_context_rejected_before_any_writer_and_unknown_source_fields_rejected_after() -> None:
    calls = []
    wrong = context()
    wrong["runtime_permission_granted"] = True
    with pytest.raises(ValidationError):
        WorkerAttemptObserver(authority=grant(), context=wrong, writer=calls.append)
    assert calls == []
    watch = observer(writer=calls.append)
    with pytest.raises(ValidationError):
        watch(attempt(extra="private-detail"))
    assert len(calls) == 1


@pytest.mark.parametrize("interrupt", [False, True])
def test_capture_failure_latches_without_claiming_a_writer_call(interrupt: bool) -> None:
    class Uncopyable:
        def __deepcopy__(self, memo: Any) -> Any:
            if interrupt:
                raise KeyboardInterrupt
            raise RuntimeError("private-capture-detail")
    writes = []
    watch = observer(writer=writes.append)
    with pytest.raises(KeyboardInterrupt if interrupt else ValidationError):
        watch(attempt(payload_sha256=Uncopyable()))
    report = watch.snapshot()
    assert writes == [] and report["writer_calls"] == 0
    assert report["failure_code"] == "capture_failed_before_writer"
    assert report["failure_history_complete"] is False
    with pytest.raises(ValidationError):
        watch(attempt())


def test_interruption_after_writer_return_latches_attribution_failure(monkeypatch: Any) -> None:
    from src.orchestration import notification_worker_attempt_observer as module
    writes = []
    watch = observer(writer=writes.append)
    def interrupt(*args: Any) -> Any:
        raise KeyboardInterrupt
    monkeypatch.setattr(module, "_receipt", interrupt)
    with pytest.raises(KeyboardInterrupt):
        watch(attempt())
    report = watch.seal()
    assert len(writes) == report["writer_returns"] == report["unattributed_writer_returns"] == 1
    assert report["attributed_attempts"] == 0
    assert report["failure_code"] == "attribution_rejected_after_writer"
    assert report["database_commit_verified"] is False
    with pytest.raises(ValidationError):
        watch(attempt())
