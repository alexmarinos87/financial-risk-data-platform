"""Bounded, single-statement PostgreSQL readiness-source selection; never delivery."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import validate_worker_authority_transition
from src.warehouse.notification_worker_readiness_snapshot import REVIEW_FIELDS, build_worker_readiness_snapshot
from src.warehouse.notification_worker_readiness_source import source_bytes, source_identifier, source_time

# Only reviewed constant column names enter SQL. Worker identity remains a parameter.
_REVIEW_JSON = ", ".join(f"'{field}', v.{field}" for field in REVIEW_FIELDS)
READINESS_SOURCE_SQL = f"""
WITH selected_authority AS MATERIALIZED (
    SELECT CASE WHEN octet_length(document_json::TEXT) <= 1048576
                THEN document_json ELSE NULL END AS document,
           document_sha256, authority_sequence,
           octet_length(document_json::TEXT) > 1048576 AS oversized
    FROM risk_platform.notification_worker_authority_history
    WHERE worker_id = %s
    ORDER BY authority_sequence DESC LIMIT 1
), selected_sources AS (
    SELECT work.item ->> 'execution_kind' AS execution_kind,
           CASE WHEN octet_length(r.record_json::TEXT) <= 1048576
                THEN r.record_json ELSE NULL END AS record,
           r.document_sha256,
           CASE WHEN v.destination_id IS NULL THEN NULL
                ELSE jsonb_build_object({_REVIEW_JSON}) END AS review,
           COALESCE(octet_length(r.record_json::TEXT) > 1048576, FALSE) AS oversized
    FROM selected_authority a
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(a.document -> 'plan' -> 'execution' -> 'work_items') = 'array'
             THEN a.document -> 'plan' -> 'execution' -> 'work_items'
             ELSE '[]'::JSONB END
    ) work(item)
    LEFT JOIN risk_platform.current_notification_execution_readiness_review v
      ON v.destination_id = a.document -> 'plan' -> 'destination' ->> 'destination_id'
     AND v.execution_kind = work.item ->> 'execution_kind'
    LEFT JOIN risk_platform.notification_execution_readiness_decisions r
      ON r.record_id = v.readiness_record_id
    ORDER BY work.item ->> 'execution_kind'
    LIMIT 3
)
SELECT statement_timestamp(), transaction_timestamp(),
       current_setting('transaction_isolation'), current_setting('transaction_read_only'),
       (SELECT jsonb_build_object('document', document, 'document_sha256', document_sha256,
                                 'authority_sequence', authority_sequence, 'oversized', oversized)
        FROM selected_authority),
       COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.execution_kind)
                 FROM selected_sources s), '[]'::JSONB)
"""


def read_worker_readiness_with_cursor(cursor: Any, *, worker_id: str) -> dict[str, Any]:
    """Require a fresh read-only READ COMMITTED statement, not an older transaction.

    The owning API supplies an autocommit connection. This variant does not change
    caller transaction settings, acquire locks, or silently accept stale clocks.
    """
    selected = source_identifier(worker_id)
    try:
        cursor.execute(READINESS_SOURCE_SQL, (selected,))
        row = cursor.fetchone()
        if not isinstance(row, (tuple, list)) or len(row) != 6:
            raise StorageError("worker readiness source query returned an invalid envelope")
        instant = source_time(row[0])
        if source_time(row[1]) != instant or row[2] != "read committed" or row[3] != "on":
            raise StorageError("worker readiness sources require a fresh read-only statement")
        retained, entries = row[4], row[5]
        if not isinstance(entries, list) or len(entries) > 2:
            raise StorageError("worker readiness source inventory is oversized or ambiguous")
        result: dict[str, Any] = {
            "model_version": "portfolio-risk-worker-readiness-read-v1",
            "worker_id": selected, "observed_at": instant.isoformat(),
            "database_read_performed": True, "single_statement_read_only": True,
            "failure_history_verified": False, "runtime_permission_granted": False,
            "notification_delivery_performed": False, "scheduler_mutated": False,
        }
        if retained is None:
            if entries:
                raise StorageError("worker readiness sources exist without selected authority")
            return {**result, "status": "authority_missing", "authority_sequence": None,
                    "authority_transition_id": None, "snapshot": None}
        if not isinstance(retained, dict) or set(retained) != {
            "document", "document_sha256", "authority_sequence", "oversized",
        } or retained["oversized"] is not False:
            raise StorageError("worker readiness authority source is invalid or oversized")
        document = validate_worker_authority_transition(retained["document"])
        digest = hashlib.sha256(source_bytes(document)).hexdigest()
        sequence = retained["authority_sequence"]
        if (retained["document_sha256"] != digest or type(sequence) is not int or sequence < 1
                or document["plan"]["worker"]["worker_id"] != selected
                or source_time(document["effective_at"]) > instant):
            raise StorageError("worker readiness authority integrity or selected identity differs")
        sources: list[Mapping[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "execution_kind", "record", "document_sha256", "review", "oversized",
            } or entry["oversized"] is not False:
                raise StorageError("worker readiness retained source is invalid or oversized")
            sources.append({key: value for key, value in entry.items() if key != "oversized"})
        snapshot = build_worker_readiness_snapshot(authority=document, sources=sources, observed_at=instant)
        return {**result, "status": snapshot["outcome"], "authority_sequence": sequence,
                "authority_transition_id": document["transition_id"], "snapshot": snapshot}
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("worker readiness source lookup failed") from None


def _connect(dsn: str) -> Any:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("worker readiness reader requires an explicit PostgreSQL DSN")
    try:
        import psycopg
    except ImportError:
        raise StorageError("worker readiness reader requires psycopg") from None
    return psycopg.connect(dsn, connect_timeout=5, autocommit=True)


def read_current_worker_readiness(*, dsn: str, worker_id: str) -> dict[str, Any]:
    """Open a dedicated read-only session, select once, validate and close it."""
    selected = source_identifier(worker_id)
    try:
        with _connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET default_transaction_read_only = on")
                cursor.execute("SET default_transaction_isolation = 'read committed'")
                cursor.execute("SET statement_timeout = '10s'")
                return read_worker_readiness_with_cursor(cursor, worker_id=selected)
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("worker readiness read-only session failed") from None
