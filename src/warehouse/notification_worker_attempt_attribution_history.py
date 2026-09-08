"""Explicit atomic source-attempt and attribution persistence; no execution path."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes, utc
from src.warehouse import notification_worker_authority_history as authority_history
from src.warehouse.notification_worker_attempt_attribution_contract import (
    validate_worker_attempt_attribution,
)

CONTEXT_TABLE = "risk_platform.notification_worker_execution_contexts"
ATTRIBUTION_TABLE = "risk_platform.notification_worker_attempt_attributions"
ATTEMPT_TABLE = "risk_platform.portfolio_risk_notification_delivery_attempts"
ATTEMPT_COLUMNS = (
    "attempt_id", "model_version", "event_id", "channel", "attempt_number",
    "idempotency_key", "attempted_at", "outcome", "http_status", "error_code",
    "endpoint_host", "payload_sha256",
)
SOURCE_COLUMNS = ", ".join(ATTEMPT_COLUMNS)


def _sha(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _exact_document(row: Any, expected: Mapping[str, Any], label: str) -> None:
    if not isinstance(row, (tuple, list)) or len(row) != 2:
        raise StorageError(f"retained attribution {label} is missing or malformed")
    try:
        equal = canonical_bytes(row[0]) == canonical_bytes(expected)
    except (ValidationError, TypeError, ValueError, RecursionError):
        raise StorageError(f"retained attribution {label} is malformed") from None
    if not equal or row[1] != _sha(expected):
        raise StorageError(f"retained attribution {label} differs from requested evidence")


def _source(row: Any, expected: Mapping[str, Any]) -> None:
    if not isinstance(row, (tuple, list)) or len(row) != len(ATTEMPT_COLUMNS):
        raise StorageError("retained attribution source attempt is missing or malformed")
    actual = dict(zip(ATTEMPT_COLUMNS, row))
    actual["attempted_at"] = utc(actual["attempted_at"], "retained attempted_at").isoformat()
    if canonical_bytes(actual) != canonical_bytes(expected):
        raise StorageError("retained attribution source attempt differs")


def _context(cursor: Any, expected: Mapping[str, Any], *, create: bool) -> None:
    cursor.execute(
        f"SELECT context_json, context_sha256 FROM {CONTEXT_TABLE} "
        "WHERE context_id = %s OR scope_id = %s OR request_id = %s LIMIT 3",
        (expected["context_id"], expected["scope_id"], expected["request_id"]),
    )
    rows = cursor.fetchall()
    if rows:
        if len(rows) != 1:
            raise ValidationError("attribution context identities conflict")
        _exact_document(rows[0], expected, "context")
        return
    if not create:
        raise StorageError("retained attribution context is missing")
    text = canonical_bytes(expected).decode("utf-8")
    cursor.execute(
        f"INSERT INTO {CONTEXT_TABLE} (context_id, scope_id, request_id, "
        "authority_transition_id, worker_id, destination_id, max_events, "
        "context_json, canonical_context, context_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s) "
        "ON CONFLICT DO NOTHING RETURNING context_json, context_sha256",
        (expected["context_id"], expected["scope_id"], expected["request_id"],
         expected["authority_transition_id"], expected["worker_id"], expected["destination_id"],
         expected["max_events"], text, text, _sha(expected)),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValidationError("attribution context identities conflict")
    _exact_document(row, expected, "context")


def record_worker_attempt_attribution_with_cursor(
    cursor: Any, *, record: Mapping[str, Any],
) -> dict[str, Any]:
    """Retain all new rows in an operation savepoint within the caller's transaction.

    Audit history may be recorded after its authority has expired or stopped.
    This is not current-authority verification or a post-transport writer adapter.
    """
    document = validate_worker_attempt_attribution(record)
    connection = getattr(cursor, "connection", None)
    if getattr(connection, "autocommit", None) is not False:
        raise ValidationError("attribution recording requires a caller-owned transaction")
    cursor.execute("SELECT current_setting('transaction_isolation')")
    if cursor.fetchone() != ("read committed",):
        raise ValidationError("attribution recording requires READ COMMITTED")
    # The SELECT above starts the caller transaction if needed. This block is a
    # savepoint, so an operation error cannot leave only some of the rows written.
    with connection.transaction():
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute("SET LOCAL statement_timeout = '10s'")
        selected, attempt = document["context"], document["attempt"]
        cursor.execute(authority_history.LOCK_SQL, (authority_history.LOCK_PREFIX + selected["worker_id"],))
        cursor.execute(
            "SELECT document_json, document_sha256, clock_timestamp() FROM "
            "risk_platform.notification_worker_authority_history WHERE transition_id = %s FOR SHARE",
            (selected["authority_transition_id"],),
        )
        authority_row = cursor.fetchone()
        if not isinstance(authority_row, (tuple, list)) or len(authority_row) != 3:
            raise StorageError("retained attribution authority is missing")
        _exact_document(authority_row[:2], document["authority"], "authority")
        if utc(attempt["attempted_at"], "attempted_at") > utc(authority_row[2], "database clock"):
            raise ValidationError("attribution source attempt is future-dated")
        cursor.execute(
            f"SELECT record_json, record_sha256 FROM {ATTRIBUTION_TABLE} WHERE attempt_id = %s",
            (attempt["attempt_id"],),
        )
        retained = cursor.fetchone()
        created = retained is None
        if not created:
            _exact_document(retained, document, "record")
            _context(cursor, selected, create=False)
            cursor.execute(
                f"SELECT {SOURCE_COLUMNS} FROM {ATTEMPT_TABLE} WHERE attempt_id = %s FOR SHARE",
                (attempt["attempt_id"],),
            )
            _source(cursor.fetchone(), attempt)
        else:
            _context(cursor, selected, create=True)
            cursor.execute(
                f"SELECT COUNT(*) FROM {ATTRIBUTION_TABLE} WHERE context_id = %s",
                (selected["context_id"],),
            )
            count = cursor.fetchone()
            if (not isinstance(count, (tuple, list)) or len(count) != 1
                    or type(count[0]) is not int or not 0 <= count[0] < selected["max_events"]):
                raise ValidationError("attribution context event limit reached or invalid")
            cursor.execute(
                f"INSERT INTO {ATTEMPT_TABLE} ({SOURCE_COLUMNS}) VALUES "
                f"({', '.join(['%s'] * len(ATTEMPT_COLUMNS))}) "
                f"ON CONFLICT DO NOTHING RETURNING {SOURCE_COLUMNS}",
                tuple(attempt[column] for column in ATTEMPT_COLUMNS),
            )
            source_row = cursor.fetchone()
            if source_row is None:
                raise ValidationError("source attempt conflict or legacy row; attribution cannot be backfilled")
            _source(source_row, attempt)
            text = canonical_bytes(document).decode("utf-8")
            cursor.execute(
                f"INSERT INTO {ATTRIBUTION_TABLE} (attribution_id, attempt_id, context_id, "
                "worker_id, destination_id, event_id, record_json, canonical_record, record_sha256) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s) "
                "RETURNING record_json, record_sha256",
                (document["attribution_id"], attempt["attempt_id"], selected["context_id"],
                 selected["worker_id"], selected["destination_id"], attempt["event_id"],
                 text, text, _sha(document)),
            )
            _exact_document(cursor.fetchone(), document, "record")
    return {
        "attribution_id": document["attribution_id"], "attempt_id": attempt["attempt_id"],
        "context_id": selected["context_id"], "record_sha256": _sha(document), "created": created,
        "source_and_attribution_reconciled": True, "commit_acknowledged": False,
        "failure_history_complete": False, "runtime_permission_granted": False,
    }


def record_worker_attempt_attribution(*, dsn: str, record: Mapping[str, Any]) -> dict[str, Any]:
    """Explicit connection-owning operation; report acknowledgement only after commit."""
    document = validate_worker_attempt_attribution(record)
    try:
        with authority_history._connect(dsn) as connection:
            with connection.cursor() as cursor:
                result = record_worker_attempt_attribution_with_cursor(cursor, record=document)
        return {**result, "commit_acknowledged": True}
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("attempt attribution transaction failed; commit is unconfirmed") from None
