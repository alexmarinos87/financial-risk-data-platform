"""Atomically retain a worker stop and its suspension evidence; never operate a scheduler."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, canonical_bytes, utc, validate_worker_authority_transition,
)
from src.orchestration.notification_worker_suspension_transition import validate_worker_suspension_bundle
from src.warehouse import notification_worker_authority_history as authority_history


TABLE = "risk_platform.notification_worker_suspension_evidence"


def _decode(row: Any) -> dict[str, Any]:
    if not isinstance(row, (tuple, list)) or len(row) != 2:
        raise StorageError("retained worker suspension row is invalid")
    try:
        result = validate_worker_suspension_bundle(row[0])
    except ValidationError:
        raise StorageError("retained worker suspension bundle is invalid") from None
    if hashlib.sha256(canonical_bytes(result)).hexdigest() != row[1]:
        raise StorageError("retained worker suspension digest differs")
    return result


def _assert_source(cursor: Any, expected: Mapping[str, Any]) -> None:
    cursor.execute(
        "SELECT document_json, document_sha256 FROM "
        "risk_platform.notification_worker_authority_history WHERE transition_id = %s",
        (expected["transition_id"],),
    )
    row = cursor.fetchone()
    if not isinstance(row, (tuple, list)) or len(row) != 2:
        raise StorageError("worker suspension retained source is missing")
    try:
        actual = validate_worker_authority_transition(row[0])
    except (ValidationError, ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise StorageError("worker suspension retained source is invalid") from None
    if actual != expected or hashlib.sha256(canonical_bytes(actual)).hexdigest() != row[1]:
        raise StorageError("worker suspension retained source differs")


def record_worker_suspension_with_cursor(
    cursor: Any, *, bundle: Mapping[str, Any],
) -> dict[str, Any]:
    """Use one caller-owned READ COMMITTED transaction; any exception requires rollback.

    New decisions require the exact current active predecessor and a recent,
    nonfuture evaluation on the database clock. Exact historical replay is read-only.
    Source health provenance and operator authentication remain caller obligations.
    """
    document = validate_worker_suspension_bundle(bundle)
    prior, decision, transition = (document[key] for key in ("authority", "decision", "transition"))
    encoded = canonical_bytes(document)
    digest = hashlib.sha256(encoded).hexdigest()
    worker_id = decision["worker_id"]
    cursor.execute("SET LOCAL lock_timeout = '5s'")
    cursor.execute("SET LOCAL statement_timeout = '10s'")
    cursor.execute(authority_history.LOCK_SQL, (authority_history.LOCK_PREFIX + worker_id,))
    cursor.execute(
        f"SELECT bundle_json, bundle_sha256 FROM {TABLE} WHERE decision_id = %s",
        (decision["decision_id"],),
    )
    row = cursor.fetchone()
    if row is not None:
        if _decode(row) != document:
            raise ValidationError("decision_id already retains different suspension evidence")
        _assert_source(cursor, prior)
        _assert_source(cursor, transition)
        result = authority_history.record_worker_authority_with_cursor(cursor, transition=transition)
        if result["created"] is not False:
            raise StorageError("suspension replay cannot create missing authority")
    else:
        cursor.execute(
            "SELECT transition_id FROM risk_platform.notification_worker_authority_history "
            "WHERE request_id = %s", (transition["request_id"],),
        )
        if cursor.fetchone() is not None:
            raise ValidationError("existing stop without decision evidence cannot be backfilled")
        current = authority_history.read_current_worker_authority_with_cursor(cursor, worker_id=worker_id)
        if current["transition"] != prior:
            raise ValidationError("worker suspension predecessor is not the exact current head")
        cursor.execute("SELECT clock_timestamp()")
        clock = cursor.fetchone()
        if not isinstance(clock, (tuple, list)) or len(clock) != 1:
            raise StorageError("worker suspension database clock is unavailable")
        now = utc(clock[0], "database clock")
        age = (now - utc(decision["evaluated_at"], "evaluated_at")).total_seconds()
        if not 0 <= age <= prior["plan"]["readiness"]["max_age_seconds"]:
            raise ValidationError("worker suspension decision is stale or future-dated")
        if authority_state(prior, as_of=now) != "active":
            raise ValidationError("worker suspension predecessor is no longer active")
        result = authority_history.record_worker_authority_with_cursor(cursor, transition=transition)
        if result["created"] is not True:
            raise ValidationError("worker suspension and authority replay states differ")
        text = encoded.decode("utf-8")
        cursor.execute(
            f"INSERT INTO {TABLE} (decision_id, transition_id, previous_transition_id, "
            "worker_id, destination_id, evaluated_at, bundle_json, canonical_bundle, bundle_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s) "
            "RETURNING bundle_json, bundle_sha256",
            (decision["decision_id"], transition["transition_id"], prior["transition_id"],
             worker_id, decision["destination_id"], decision["evaluated_at"], text, text, digest),
        )
        if _decode(cursor.fetchone()) != document:
            raise StorageError("retained suspension evidence differs from requested bundle")
    return {**result, "decision_id": decision["decision_id"], "bundle_sha256": digest,
            "suspension_evidence_recorded": True, "runtime_permission_granted": False}


def record_worker_suspension(*, dsn: str, bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Explicit database operation, with atomic commit/rollback and sanitized failure."""
    document = validate_worker_suspension_bundle(bundle)
    try:
        # Reuse the established connection timeout and DSN validation policy.
        with authority_history._connect(dsn) as connection:
            with connection.cursor() as cursor:
                result = record_worker_suspension_with_cursor(cursor, bundle=document)
        return result
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("worker suspension transaction failed; no success is confirmed") from None
