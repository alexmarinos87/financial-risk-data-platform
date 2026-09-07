"""Opt-in attribution at the existing attempt-writer seam, not a runtime gate."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from threading import get_ident
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes, utc
from src.orchestration.notification_worker_execution_context import validate_worker_execution_context

MODEL_VERSION = "portfolio-risk-worker-attempt-observation-v1"
ATTEMPT_FIELDS = {
    "attempt_id", "model_version", "event_id", "channel", "attempt_number",
    "idempotency_key", "attempted_at", "outcome", "http_status", "error_code",
    "endpoint_host", "payload_sha256",
}


def _text(value: Any) -> str:
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value) > 512 or any(ord(char) < 32 for char in value)):
        raise ValidationError("worker attempt text is invalid")
    return value


def _receipt(context: Mapping[str, Any], attempt: Mapping[str, Any]) -> dict[str, Any]:
    # Reuse the established producer identity rather than inventing attempt IDs.
    from src.orchestration.deliver_portfolio_risk_notifications import MODEL_VERSION as DELIVERY_MODEL
    from src.orchestration.deliver_portfolio_risk_notifications import _attempt_id

    if not isinstance(attempt, Mapping) or set(attempt) != ATTEMPT_FIELDS:
        raise ValidationError("worker attempt fields are not exact")
    event_id = _text(attempt["event_id"])
    number = attempt["attempt_number"]
    if type(number) is not int or not 1 <= number <= 10:
        raise ValidationError("worker attempt number is invalid")
    if ((context["execution_kind"] == "initial" and number != 1)
            or (context["execution_kind"] == "retry" and number < 2)):
        raise ValidationError("worker attempt kind contradicts its context")
    if (attempt["model_version"] != DELIVERY_MODEL or attempt["channel"] != "webhook"
            or attempt["idempotency_key"] != event_id
            or attempt["attempt_id"] != _attempt_id(event_id, number)):
        raise ValidationError("worker attempt source identity is invalid")
    when = utc(attempt["attempted_at"], "attempted_at")
    if not utc(context["started_at"], "started_at") <= when < utc(context["expires_at"], "expires_at"):
        raise ValidationError("worker attempt time is outside its context")
    status, error, outcome = attempt["http_status"], attempt["error_code"], attempt["outcome"]
    if status is not None and (type(status) is not int or not 100 <= status <= 599):
        raise ValidationError("worker attempt HTTP status is invalid")
    succeeded = status is not None and 200 <= status < 300
    if succeeded:
        valid = outcome == "succeeded" and error is None
    else:
        valid = outcome == "failed" and error == ("network_error" if status is None else f"http_{status}")
    if not valid:
        raise ValidationError("worker attempt outcome contradicts its transport evidence")
    digest = attempt["payload_sha256"]
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValidationError("worker attempt payload digest is invalid")
    _text(attempt["endpoint_host"])
    canonical = {**attempt, "attempted_at": when.isoformat()}
    identity = {
        "model_version": MODEL_VERSION, "context_id": context["context_id"],
        "scope_id": context["scope_id"], "worker_id": context["worker_id"],
        "destination_id": context["destination_id"], "execution_kind": context["execution_kind"],
        "attempt_id": attempt["attempt_id"], "event_id": event_id, "attempt_number": number,
        "attempted_at": when.isoformat(), "outcome": outcome,
        "source_attempt_sha256": hashlib.sha256(canonical_bytes(canonical)).hexdigest(),
        "attribution_basis": "caller_supplied_execution_context",
    }
    return {"receipt_id": f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}", **identity}


class WorkerAttemptObserver:
    """Single-thread, non-reentrant attempt-writer decorator with in-memory receipts.

    The delegate is explicitly supplied and must return None. A return is not an
    independently verified commit. Caller must preserve existing execution gates,
    stop on errors and retain/report evidence; this class never retries or sends.
    """

    def __init__(
        self, *, authority: Mapping[str, Any], context: Mapping[str, Any],
        writer: Callable[[Mapping[str, Any]], object],
    ) -> None:
        self._context = validate_worker_execution_context(context, authority=authority)
        if not callable(writer):
            raise ValidationError("worker attempt observer requires a writer")
        self._writer = writer
        self._owner = get_ident()
        self._busy = False
        self._sealed = False
        self._fault: str | None = None
        self._uncertain = False
        self._calls = 0
        self._returns = 0
        self._receipts: list[dict[str, Any]] = []
        self._events: set[str] = set()

    def _access(self) -> None:
        if get_ident() != self._owner or self._busy:
            raise OverlapError("worker attempt observer requires exclusive synchronous use")

    def __call__(self, attempt: Mapping[str, Any]) -> None:
        self._access()
        if self._sealed or self._fault is not None:
            raise ValidationError("worker attempt observer is no longer accepting callbacks")
        self._busy = True
        try:
            try:
                source = copy.deepcopy(dict(attempt))
                forwarded = copy.deepcopy(source)
            except BaseException as exc:
                self._fault = "capture_failed_before_writer"
                if not isinstance(exc, Exception):
                    raise
                raise ValidationError("worker attempt capture failed before writer") from None
            self._calls += 1
            try:
                result = self._writer(forwarded)
            except BaseException as exc:
                self._fault = "writer_outcome_uncertain"
                self._uncertain = True
                if not isinstance(exc, Exception):
                    raise
                raise StorageError("worker attempt writer failed; persistence is unconfirmed") from None
            self._returns += 1
            if result is not None:
                self._fault = "writer_contract_invalid"
                self._uncertain = True
                raise StorageError("worker attempt writer returned an unsupported acknowledgement")
            # Transport has already happened before this callback. Never suppress
            # its base write merely because attribution cannot be established.
            try:
                receipt = _receipt(self._context, source)
                if len(self._receipts) >= self._context["max_events"] or receipt["event_id"] in self._events:
                    raise ValidationError("worker attempt observation exceeds its event scope")
                self._receipts.append(receipt)
                self._events.add(receipt["event_id"])
            except BaseException as exc:
                self._fault = "attribution_rejected_after_writer"
                if not isinstance(exc, Exception):
                    raise
                raise ValidationError("worker attempt attribution failed after writer returned") from None
        finally:
            self._busy = False

    def snapshot(self) -> dict[str, Any]:
        """Report observations only; empty or sealed does not mean healthy/complete."""
        self._access()
        identity = {
            "model_version": MODEL_VERSION, "context": self._context,
            "status": "observation_failed" if self._fault else "sealed" if self._sealed else "open",
            "sealed": self._sealed, "failure_code": self._fault,
            "writer_calls": self._calls, "writer_returns": self._returns,
            "attributed_attempts": len(self._receipts),
            "unattributed_writer_returns": self._returns - len(self._receipts),
            "observed_succeeded_attempts": sum(row["outcome"] == "succeeded" for row in self._receipts),
            "observed_failed_attempts": sum(row["outcome"] == "failed" for row in self._receipts),
            "writer_outcome_uncertain": self._uncertain, "receipts": self._receipts,
            "attribution_durable": False, "database_commit_verified": False,
            "failure_history_complete": False, "runtime_permission_granted": False,
        }
        result = {"report_id": f"{MODEL_VERSION}-report-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}", **identity}
        return json.loads(canonical_bytes(result))

    def seal(self) -> dict[str, Any]:
        self._access()
        self._sealed = True
        return self.snapshot()
