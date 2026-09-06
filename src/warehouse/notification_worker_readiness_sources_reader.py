"""Read bounded readiness records together, without operating worker authority."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.plan_notification_worker import validate_notification_worker_plan
from src.warehouse.notification_worker_authority_history import _connect
from src.warehouse.notification_worker_readiness_sources import (
    MAX_SOURCE_BYTES, build_worker_readiness_sources,
)

READINESS_SOURCES_SQL = """
WITH selected_kinds AS (SELECT unnest(%s::text[]) AS execution_kind)
SELECT statement_timestamp(), selected.execution_kind, review.destination_id,
       review.readiness_record_id, review.readiness_review_status,
       review.execution_ready, review.decision_matches_current_evidence,
       CASE WHEN octet_length(retained.record_json::text) <= %s
            THEN retained.record_json ELSE NULL END AS record_json,
       retained.document_sha256,
       octet_length(retained.record_json::text) AS source_bytes
FROM selected_kinds selected
LEFT JOIN risk_platform.current_notification_execution_readiness_review review
  ON review.destination_id = %s AND review.execution_kind = selected.execution_kind
LEFT JOIN risk_platform.notification_execution_readiness_decisions retained
  ON retained.record_id = review.readiness_record_id
ORDER BY selected.execution_kind, review.readiness_record_id
LIMIT 3
"""


def read_worker_readiness_sources_with_cursor(
    cursor: Any, *, plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Read one source statement; the caller owns its transaction and lifetime."""
    selected = validate_notification_worker_plan(plan)
    kinds = [item["execution_kind"] for item in selected["execution"]["work_items"]]
    destination_id = selected["destination"]["destination_id"]
    try:
        cursor.execute("SET LOCAL statement_timeout = '10s'")
        cursor.execute(READINESS_SOURCES_SQL, (kinds, MAX_SOURCE_BYTES, destination_id))
        rows = cursor.fetchmany(3)
        if not isinstance(rows, (list, tuple)) or len(rows) != len(kinds):
            raise StorageError("readiness source grain is not unique or complete")
        observed = None
        sources = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) != 10:
                raise StorageError("readiness source row is invalid")
            if not isinstance(row[0], datetime):
                raise StorageError("readiness source database clock is invalid")
            if observed is None:
                observed = row[0]
            elif row[0] != observed:
                raise StorageError("readiness sources do not share an observation clock")
            if row[2] is None:
                if any(value is not None for value in row[3:]):
                    raise StorageError("missing readiness view has retained metadata")
                sources.append({
                    "destination_id": destination_id, "execution_kind": row[1],
                    "readiness_record_id": None, "readiness_review_status": "decision_missing",
                    "execution_ready": False, "decision_matches_current_evidence": False,
                    "record_json": None, "document_sha256": None,
                })
                continue
            if row[3] is not None:
                if type(row[9]) is not int or not 1 <= row[9] <= MAX_SOURCE_BYTES:
                    raise StorageError("readiness source record is missing or oversized")
                if row[7] is None:
                    raise StorageError("readiness serving row has no retained document")
            elif row[9] is not None:
                raise StorageError("missing readiness record carries document metadata")
            sources.append({
                "destination_id": row[2], "execution_kind": row[1],
                "readiness_record_id": row[3], "readiness_review_status": row[4],
                "execution_ready": row[5], "decision_matches_current_evidence": row[6],
                "record_json": row[7], "document_sha256": row[8],
            })
        if observed is None:
            raise StorageError("readiness observation clock is missing")
        return build_worker_readiness_sources(plan=selected, sources=sources, observed_at=observed)
    except Exception:
        # No provider diagnostics, document contents or credential-bearing strings.
        raise StorageError("unable to capture verified worker readiness sources") from None


def read_worker_readiness_sources(*, dsn: str, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Open a bounded READ ONLY transaction; return only after successful exit.

    This reads readiness, not current worker authority or complete failure
    history. Callers must not combine independent snapshots as an atomic runtime
    permission. No advisory lock, source refresh, transition or transport runs.
    """
    selected = validate_notification_worker_plan(plan)
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    try:
        with _connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
                cursor.execute("SET LOCAL lock_timeout = '5s'")
                result = read_worker_readiness_sources_with_cursor(cursor, plan=selected)
        return result
    except Exception:
        raise StorageError("unable to capture verified worker readiness sources") from None
