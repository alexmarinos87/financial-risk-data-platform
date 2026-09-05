"""Read one clocked authority head; a snapshot never grants runtime permission."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, canonical_bytes, identifier, utc,
)
from src.warehouse.notification_worker_authority_history import _connect, _decode, _validated

MODEL_VERSION = "portfolio-risk-worker-authority-snapshot-v1"
MAX_SNAPSHOT_BYTES = 1_048_576
SNAPSHOT_FIELDS = frozenset({
    "snapshot_id", "model_version", "worker_id", "observed_at", "transition",
    "document_sha256", "authority_sequence", "recorded_at", "authority_state",
    "read_consistency", "runtime_permission_granted",
})
CURRENT_SNAPSHOT_SQL = """
SELECT statement_timestamp(), head.document_json, head.document_sha256,
       head.authority_sequence, head.recorded_at
FROM (SELECT 1) AS observation
LEFT JOIN LATERAL (
    SELECT document_json, document_sha256, authority_sequence, recorded_at
    FROM risk_platform.notification_worker_authority_history
    WHERE worker_id = %s
    ORDER BY authority_sequence DESC LIMIT 1
) AS head ON TRUE
"""


def _build_snapshot(
    *, worker_id: Any, observed_at: Any, transition: Any,
    document_sha256: Any, authority_sequence: Any, recorded_at: Any,
) -> dict[str, Any]:
    selected = identifier(worker_id, "worker_id")
    observed = utc(observed_at, "observed_at")
    document = None
    recorded = None
    if transition is None:
        if any(value is not None for value in (document_sha256, authority_sequence, recorded_at)):
            raise ValidationError("missing authority cannot carry retained row metadata")
    else:
        document = _validated(transition)
        if document["plan"]["worker"]["worker_id"] != selected:
            raise ValidationError("authority snapshot worker scope differs")
        if type(authority_sequence) is not int or not 1 <= authority_sequence < 2**63:
            raise ValidationError("authority snapshot sequence is invalid")
        if document_sha256 != hashlib.sha256(canonical_bytes(document)).hexdigest():
            raise ValidationError("authority snapshot document digest differs")
        recorded = utc(recorded_at, "recorded_at")
        effective = utc(document["effective_at"], "effective_at")
        if not effective <= recorded <= observed:
            raise ValidationError("authority snapshot chronology is invalid")
    identity = {
        "model_version": MODEL_VERSION, "worker_id": selected,
        "observed_at": observed.isoformat(), "transition": document,
        "document_sha256": document_sha256, "authority_sequence": authority_sequence,
        "recorded_at": None if recorded is None else recorded.isoformat(),
        "authority_state": authority_state(document, as_of=observed),
        "read_consistency": "single_statement", "runtime_permission_granted": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    result = {"snapshot_id": f"{MODEL_VERSION}-{digest}", **identity}
    if len(canonical_bytes(result)) > MAX_SNAPSHOT_BYTES:
        raise ValidationError("authority snapshot exceeds 1 MB")
    return result


def validate_worker_authority_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
    """Recompute a bounded detached snapshot, not database provenance or freshness."""
    try:
        if not isinstance(value, Mapping) or set(value) != SNAPSHOT_FIELDS:
            raise ValidationError("authority snapshot fields are not exact")
        encoded = canonical_bytes(value)
        if len(encoded) > MAX_SNAPSHOT_BYTES:
            raise ValidationError("authority snapshot exceeds 1 MB")
        snapshot = json.loads(encoded)
        rebuilt = _build_snapshot(**{key: snapshot[key] for key in (
            "worker_id", "observed_at", "transition", "document_sha256",
            "authority_sequence", "recorded_at",
        )})
        if encoded != canonical_bytes(rebuilt):
            raise ValidationError("authority snapshot differs from its retained evidence")
        return rebuilt
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("authority snapshot is malformed") from None


def read_worker_authority_snapshot_with_cursor(cursor: Any, *, worker_id: str) -> dict[str, Any]:
    """SELECT only; the caller owns transaction isolation and lifecycle."""
    selected = identifier(worker_id, "worker_id")
    cursor.execute("SET LOCAL statement_timeout = '10s'")
    cursor.execute(CURRENT_SNAPSHOT_SQL, (selected,))
    row = cursor.fetchone()
    try:
        if not isinstance(row, (tuple, list)) or len(row) != 5:
            raise StorageError("authority snapshot row is invalid")
        if not isinstance(row[0], datetime):
            raise StorageError("authority snapshot database clock is invalid")
        if row[1] is not None:
            _decode(row[1:])
            if not isinstance(row[4], datetime):
                raise StorageError("authority snapshot recording clock is invalid")
        return _build_snapshot(
            worker_id=selected, observed_at=row[0], transition=row[1],
            document_sha256=row[2], authority_sequence=row[3], recorded_at=row[4],
        )
    except (ValidationError, StorageError, TypeError, ValueError, RecursionError, OverflowError):
        raise StorageError("retained authority snapshot failed validation") from None


def read_worker_authority_snapshot(*, dsn: str, worker_id: str) -> dict[str, Any]:
    """Capture one head in a new bounded READ COMMITTED, READ ONLY transaction."""
    selected = identifier(worker_id, "worker_id")
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    try:
        with _connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
                cursor.execute("SET LOCAL lock_timeout = '5s'")
                result = read_worker_authority_snapshot_with_cursor(cursor, worker_id=selected)
        return result
    except Exception:
        # Include connection/exit failures without exposing DSNs or provider diagnostics.
        raise StorageError("unable to read a verified worker authority snapshot") from None
