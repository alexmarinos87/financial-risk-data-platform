"""Append-only worker authority history; no delivery or scheduling side effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, canonical_bytes, identifier,
    validate_worker_authority_chain, validate_worker_authority_transition,
)

MAX_DOCUMENT_BYTES = 1_048_576
LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"
LOCK_PREFIX = "notification-worker-authority:"
ROW_COLUMNS = "document_json, document_sha256, authority_sequence, recorded_at"


def _validated(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        result = validate_worker_authority_transition(value)
        if len(canonical_bytes(result)) > MAX_DOCUMENT_BYTES:
            raise ValidationError("worker authority document exceeds 1 MB")
        return result
    except (ValueError, TypeError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker authority document is malformed") from None


def _decode(row: Any) -> dict[str, Any]:
    if not isinstance(row, (tuple, list)) or len(row) != 4:
        raise StorageError("retained worker authority row is invalid")
    try:
        document = _validated(row[0])
    except ValidationError:
        raise StorageError("retained worker authority document is invalid") from None
    digest = hashlib.sha256(canonical_bytes(document)).hexdigest()
    if digest != row[1] or type(row[2]) is not int or row[2] < 1:
        raise StorageError("retained worker authority integrity check failed")
    return {
        "transition": document, "document_sha256": digest,
        "authority_sequence": row[2], "recorded_at": row[3],
    }


def _summary(retained: Mapping[str, Any], *, created: bool) -> dict[str, Any]:
    document = retained["transition"]
    return {
        "transition_id": document["transition_id"], "request_id": document["request_id"],
        "worker_id": document["plan"]["worker"]["worker_id"],
        "destination_id": document["plan"]["destination"]["destination_id"],
        "authority_sequence": retained["authority_sequence"],
        "document_sha256": retained["document_sha256"], "created": created,
        "runtime_permission_granted": False,
    }


def record_worker_authority_with_cursor(
    cursor: Any, *, transition: Mapping[str, Any],
) -> dict[str, Any]:
    """Append or exactly replay using the caller's transaction (READ COMMITTED)."""
    document = _validated(transition)
    encoded = canonical_bytes(document)
    digest = hashlib.sha256(encoded).hexdigest()
    worker_id = document["plan"]["worker"]["worker_id"]
    cursor.execute("SET LOCAL lock_timeout = '5s'")
    cursor.execute("SET LOCAL statement_timeout = '10s'")
    cursor.execute(LOCK_SQL, (LOCK_PREFIX + worker_id,))
    cursor.execute(
        f"SELECT {ROW_COLUMNS} FROM risk_platform.notification_worker_authority_history "
        "WHERE request_id = %s", (document["request_id"],),
    )
    row = cursor.fetchone()
    if row is not None:
        retained = _decode(row)
        if retained["document_sha256"] != digest or retained["transition"] != document:
            raise ValidationError("request_id already retains different worker authority")
        # Historical exact replay must not promote an old grant to current head.
        return _summary(retained, created=False)
    cursor.execute(
        f"SELECT {ROW_COLUMNS} FROM risk_platform.notification_worker_authority_history "
        "WHERE worker_id = %s ORDER BY authority_sequence DESC LIMIT 1", (worker_id,),
    )
    prior_row = cursor.fetchone()
    previous = None if prior_row is None else _decode(prior_row)["transition"]
    validate_worker_authority_chain(document, previous)
    text = encoded.decode("utf-8")
    cursor.execute(
        """
        INSERT INTO risk_platform.notification_worker_authority_history (
            transition_id, request_id, worker_id, destination_id, plan_id,
            previous_transition_id, authority_sequence, action, from_state, to_state,
            requested_at, effective_at, expires_at, document_json,
            canonical_document, document_sha256
        ) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s)
        RETURNING document_json, document_sha256, authority_sequence, recorded_at
        """,
        (
            document["transition_id"], document["request_id"], worker_id,
            document["plan"]["destination"]["destination_id"], document["plan"]["plan_id"],
            document["previous_transition_id"], document["action"], document["from_state"],
            document["to_state"], document["requested_at"], document["effective_at"],
            document["expires_at"], text, text, digest,
        ),
    )
    retained = _decode(cursor.fetchone())
    if retained["transition"] != document:
        raise StorageError("retained worker authority differs from requested evidence")
    return _summary(retained, created=True)


def _connect(dsn: str) -> Any:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    try:
        import psycopg
    except ImportError:
        raise StorageError("worker authority history requires psycopg") from None
    return psycopg.connect(dsn, connect_timeout=5)


def record_worker_authority(*, dsn: str, transition: Mapping[str, Any]) -> dict[str, Any]:
    document = _validated(transition)
    try:
        with _connect(dsn) as connection:
            with connection.cursor() as cursor:
                result = record_worker_authority_with_cursor(cursor, transition=document)
        return result
    except (ValidationError, StorageError):
        raise
    except Exception:
        # Do not expose DSNs, JSON documents or database exception details.
        raise StorageError("worker authority transaction failed; no success is confirmed") from None


def read_current_worker_authority_with_cursor(cursor: Any, *, worker_id: str) -> dict[str, Any]:
    selected = identifier(worker_id, "worker_id")
    cursor.execute("SET LOCAL statement_timeout = '10s'")
    cursor.execute(
        f"SELECT {ROW_COLUMNS}, statement_timestamp() "
        "FROM risk_platform.notification_worker_authority_history "
        "WHERE worker_id = %s ORDER BY authority_sequence DESC LIMIT 1", (selected,),
    )
    row = cursor.fetchone()
    if row is None:
        return {"worker_id": selected, "authority_state": "inactive", "transition": None,
                "runtime_permission_granted": False}
    if not isinstance(row, (tuple, list)) or len(row) != 5:
        raise StorageError("current worker authority row is invalid")
    retained = _decode(row[:4])
    return {**_summary(retained, created=False), "transition": retained["transition"],
            "authority_state": authority_state(retained["transition"], as_of=row[4])}


def read_current_worker_authority(*, dsn: str, worker_id: str) -> dict[str, Any]:
    selected = identifier(worker_id, "worker_id")
    try:
        with _connect(dsn) as connection:
            with connection.cursor() as cursor:
                return read_current_worker_authority_with_cursor(cursor, worker_id=selected)
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("unable to read current worker authority") from None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("worker authority JSON contains duplicate fields")
        result[key] = value
    return result


def load_worker_authority(path: Path) -> dict[str, Any]:
    # Parent directory must be trusted. O_NOFOLLOW protects the final component;
    # O_NONBLOCK permits rejecting a FIFO without blocking while opening it.
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    if path.is_symlink():
        raise ValidationError("worker authority input must not be a symbolic link")
    try:
        with os.fdopen(os.open(path, flags), "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise ValidationError("worker authority input must be a regular file")
            raw = handle.read(MAX_DOCUMENT_BYTES + 1)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValidationError("worker authority input exceeds 1 MB")
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (OSError, ValueError, UnicodeError, RecursionError):
        raise ValidationError("unable to read worker authority input") from None
    return _validated(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate or explicitly retain worker authority evidence.")
    parser.add_argument("--transition", required=True, type=Path)
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--dsn", default=os.environ.get("WAREHOUSE_POSTGRES_DSN"))
    args = parser.parse_args(argv)
    try:
        document = load_worker_authority(args.transition)
        result = record_worker_authority(dsn=args.dsn, transition=document) if args.record else {
            "transition_id": document["transition_id"], "validated": True, "persisted": False,
            "runtime_permission_granted": False,
        }
    except (ValidationError, StorageError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
