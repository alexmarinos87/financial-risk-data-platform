"""Rollback-scoped proof of the explicit attempt-attribution recorder."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.orchestration.deliver_portfolio_risk_notifications import MODEL_VERSION as DELIVERY_MODEL, _attempt_id
from src.orchestration.notification_worker_execution_context import build_worker_execution_context
from src.orchestration.notification_worker_authority_contract import utc
from src.warehouse.notification_worker_attempt_attribution_contract import build_worker_attempt_attribution
from src.warehouse.notification_worker_attempt_attribution_history import (
    ATTEMPT_COLUMNS, ATTEMPT_TABLE, ATTRIBUTION_TABLE, CONTEXT_TABLE, SOURCE_COLUMNS,
    record_worker_attempt_attribution_with_cursor,
)

EXPECTED_CHECKS = {
    "attribution_atomic_creation", "attribution_final_write_rollback",
    "attribution_exact_replay", "attribution_no_legacy_backfill",
    "attribution_scope_conflict", "attribution_source_conflict",
    "attribution_event_capacity", "attribution_append_only",
    "attribution_historical_head_unchanged", "attribution_fixture_rolled_back",
}


def _reject(connection: Any, operation: Callable[[], Any], message: str) -> None:
    try:
        with connection.transaction():
            operation()
    except Exception as exc:
        if message not in str(exc):
            raise AssertionError("attribution rejection occurred at an unexpected boundary") from exc
    else:
        raise AssertionError("invalid attempt attribution was accepted")


def _copy_fixture_event(cursor: Any, *, event_id: str, seed_event_id: str) -> None:
    cursor.execute(
        "INSERT INTO risk_platform.portfolio_risk_notification_outbox "
        "SELECT (jsonb_populate_record(NULL::risk_platform.portfolio_risk_notification_outbox, "
        "to_jsonb(o) || jsonb_build_object('event_id', %s::TEXT, 'payload_json', "
        "o.payload_json || jsonb_build_object('event_id', %s::TEXT)))).* "
        "FROM risk_platform.portfolio_risk_notification_outbox o WHERE o.event_id = %s "
        "ON CONFLICT DO NOTHING", (event_id, event_id, seed_event_id),
    )
    # Initial and retry cases may share an event, but an absent seed must never
    # silently create zero rows and fail later at the source-attempt foreign key.
    cursor.execute(
        "SELECT event_id FROM risk_platform.portfolio_risk_notification_outbox WHERE event_id = %s",
        (event_id,),
    )
    if cursor.fetchone() != (event_id,):
        raise AssertionError("attribution fixture outbox event was not materialized")


def check_worker_attempt_attribution_contract(
    connection: Any, cursor: Any, authority: Mapping[str, Any],
) -> dict[str, bool]:
    """Apply schema only in a forced-rollback savepoint of the disposable fixture.

    No transport is performed. Synthetic attempts describe a historical authority
    whose current chain is already stopped, so recording cannot imply execution.
    """
    from psycopg.types.json import Jsonb
    from src.warehouse.notification_retry_follow_up_contract_check import _insert_evaluation_and_event

    prefix = "attribution-proof-" + uuid4().hex
    checks: dict[str, bool] = {}
    cursor.execute("SELECT to_regclass(%s), to_regclass(%s)", (CONTEXT_TABLE, ATTRIBUTION_TABLE))
    previous_tables = cursor.fetchone()
    worker_id = authority["plan"]["worker"]["worker_id"]
    cursor.execute(
        "SELECT transition_id FROM risk_platform.notification_worker_authority_history "
        "WHERE worker_id = %s ORDER BY authority_sequence DESC LIMIT 1", (worker_id,),
    )
    previous_head = cursor.fetchone()
    with connection.transaction(force_rollback=True):
        cursor.execute(Path("sql/notification_worker_attempt_attribution_schema.sql").read_text(encoding="utf-8"))
        cursor.execute(
            "SELECT to_jsonb(a) FROM risk_platform.portfolio_risk_attribution a "
            "ORDER BY ts_event DESC, calculation_id DESC LIMIT 1"
        )
        attribution_row = cursor.fetchone()
        if not isinstance(attribution_row, (tuple, list)) or len(attribution_row) != 1 or not isinstance(attribution_row[0], dict):
            raise AssertionError("portfolio attribution fixture is unavailable")
        seed = _insert_evaluation_and_event(
            cursor, attribution=attribution_row[0], suffix=prefix + "-seed",
            event_time=utc(authority["plan"]["planned_at"], "planned_at") - timedelta(seconds=2),
            jsonb=Jsonb,
        )

        def candidate(index: int, *, kind: str = "initial", request: str | None = None) -> dict[str, Any]:
            event_id = prefix + "-" + str(index)
            _copy_fixture_event(cursor, event_id=event_id, seed_event_id=seed["event_id"])
            selected = build_worker_execution_context(
                authority=authority, request_id=request or prefix + "-" + kind,
                execution_kind=kind, started_at=authority["plan"]["schedule"]["scheduled_for"],
            )
            number = 1 if kind == "initial" else 2
            attempt = {
                "attempt_id": _attempt_id(event_id, number), "model_version": DELIVERY_MODEL,
                "event_id": event_id, "channel": "webhook", "attempt_number": number,
                "idempotency_key": event_id, "attempted_at": selected["started_at"],
                "outcome": "failed", "http_status": 503, "error_code": "http_503",
                "endpoint_host": "receiver.example.invalid", "payload_sha256": "a" * 64,
            }
            return build_worker_attempt_attribution(authority=authority, context=selected, attempt=attempt)

        first = candidate(0)

        def final_write_failure() -> None:
            cursor.execute("""
                CREATE FUNCTION pg_temp.reject_attribution_fixture() RETURNS TRIGGER LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'attribution fixture final-write failure'; END; $$;
                CREATE TRIGGER attribution_fixture_rejection
                BEFORE INSERT ON risk_platform.notification_worker_attempt_attributions
                FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_attribution_fixture();
            """)
            record_worker_attempt_attribution_with_cursor(cursor, record=first)

        _reject(connection, final_write_failure, "attribution fixture final-write failure")
        cursor.execute(f"SELECT COUNT(*) FROM {ATTEMPT_TABLE} WHERE attempt_id = %s", (first["attempt"]["attempt_id"],))
        if cursor.fetchone() != (0,):
            raise AssertionError("final-write failure left a source attempt")
        cursor.execute(f"SELECT COUNT(*) FROM {CONTEXT_TABLE} WHERE context_id = %s", (first["context"]["context_id"],))
        if cursor.fetchone() != (0,):
            raise AssertionError("final-write failure left a context")
        cursor.execute(f"SELECT COUNT(*) FROM {ATTRIBUTION_TABLE} WHERE attempt_id = %s", (first["attempt"]["attempt_id"],))
        if cursor.fetchone() != (0,):
            raise AssertionError("final-write failure left attribution")
        checks["attribution_final_write_rollback"] = True

        retained = record_worker_attempt_attribution_with_cursor(cursor, record=first)
        retry = candidate(0, kind="retry")
        retry_result = record_worker_attempt_attribution_with_cursor(cursor, record=retry)
        if retained["created"] is not True or retry_result["created"] is not True:
            raise AssertionError("initial and retry evidence were not created")
        if retained["commit_acknowledged"] is not False:
            raise AssertionError("cursor evidence falsely acknowledged a commit")
        checks["attribution_atomic_creation"] = True
        replay = record_worker_attempt_attribution_with_cursor(cursor, record=first)
        if replay["created"] is not False or replay["attribution_id"] != retained["attribution_id"]:
            raise AssertionError("exact attribution replay did not converge")
        checks["attribution_exact_replay"] = True

        changed = build_worker_attempt_attribution(
            authority=authority, context=first["context"],
            attempt={**first["attempt"], "payload_sha256": "b" * 64},
        )
        _reject(connection, lambda: record_worker_attempt_attribution_with_cursor(cursor, record=changed), "record differs")
        checks["attribution_source_conflict"] = True
        other_context = candidate(1, request=prefix + "-conflicting-context")
        _reject(connection, lambda: record_worker_attempt_attribution_with_cursor(cursor, record=other_context), "context differs")
        checks["attribution_scope_conflict"] = True

        legacy = candidate(2)
        cursor.execute(
            f"INSERT INTO {ATTEMPT_TABLE} ({SOURCE_COLUMNS}) VALUES "
            f"({', '.join(['%s'] * len(ATTEMPT_COLUMNS))})",
            tuple(legacy["attempt"][column] for column in ATTEMPT_COLUMNS),
        )
        _reject(connection, lambda: record_worker_attempt_attribution_with_cursor(cursor, record=legacy), "cannot be backfilled")
        cursor.execute(f"SELECT COUNT(*) FROM {ATTEMPT_TABLE} WHERE attempt_id = %s", (legacy["attempt"]["attempt_id"],))
        if cursor.fetchone() != (1,):
            raise AssertionError("legacy source was altered by rejected attribution")
        checks["attribution_no_legacy_backfill"] = True

        for index in range(1, first["context"]["max_events"]):
            record_worker_attempt_attribution_with_cursor(cursor, record=candidate(index + 10))
        overflow = candidate(1000)
        _reject(connection, lambda: record_worker_attempt_attribution_with_cursor(cursor, record=overflow), "event limit")
        cursor.execute(f"SELECT COUNT(*) FROM {ATTEMPT_TABLE} WHERE attempt_id = %s", (overflow["attempt"]["attempt_id"],))
        if cursor.fetchone() != (0,):
            raise AssertionError("capacity rejection wrote a source attempt")
        checks["attribution_event_capacity"] = True

        for table in (CONTEXT_TABLE, ATTRIBUTION_TABLE):
            for statement in (f"UPDATE {table} SET worker_id = worker_id WHERE worker_id = %s",
                              f"DELETE FROM {table} WHERE worker_id = %s"):
                def mutate(sql: str = statement) -> None:
                    cursor.execute(sql, (worker_id,))
                _reject(connection, mutate, "append-only")
        _reject(connection, lambda: cursor.execute(f"TRUNCATE {CONTEXT_TABLE}, {ATTRIBUTION_TABLE}"), "append-only")
        checks["attribution_append_only"] = True
        cursor.execute(
            "SELECT transition_id FROM risk_platform.notification_worker_authority_history "
            "WHERE worker_id = %s ORDER BY authority_sequence DESC LIMIT 1", (worker_id,),
        )
        if cursor.fetchone() != previous_head:
            raise AssertionError("attribution recording changed current authority")
        checks["attribution_historical_head_unchanged"] = True
    cursor.execute("SELECT to_regclass(%s), to_regclass(%s)", (CONTEXT_TABLE, ATTRIBUTION_TABLE))
    if cursor.fetchone() != previous_tables:
        raise AssertionError("attribution fixture schema survived rollback")
    cursor.execute(
        "SELECT COUNT(*) FROM risk_platform.portfolio_risk_notification_outbox "
        "WHERE event_id LIKE %s OR event_id = %s", (prefix + "%", seed["event_id"]),
    )
    if cursor.fetchone() != (0,):
        raise AssertionError("attribution fixture data survived rollback")
    cursor.execute(
        "SELECT COUNT(*) FROM risk_platform.portfolio_risk_limit_evaluations WHERE calculation_id = %s",
        (seed["evaluation_id"],),
    )
    if cursor.fetchone() != (0,):
        raise AssertionError("attribution fixture seed evaluation survived rollback")
    checks["attribution_fixture_rolled_back"] = True
    if set(checks) != EXPECTED_CHECKS or any(value is not True for value in checks.values()):
        raise AssertionError("attribution fixture evidence is incomplete")
    return checks
