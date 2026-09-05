"""Disposable, transaction-scoped PostgreSQL proof of atomic suspension persistence."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from src.orchestration.notification_worker_authority_contract import build_worker_authority_transition
from src.orchestration.notification_worker_suspension import evaluate_worker_suspension
from src.orchestration.notification_worker_suspension_transition import build_worker_suspension_bundle
from src.warehouse.notification_worker_authority_history import (
    read_current_worker_authority_with_cursor, record_worker_authority_with_cursor,
)
from src.warehouse.notification_worker_suspension_history import record_worker_suspension_with_cursor


def _reject(connection: Any, operation: Callable[[], Any], expected: str) -> None:
    try:
        with connection.transaction():
            operation()
    except Exception as exc:
        if expected not in str(exc):
            raise AssertionError("suspension rejection occurred at an unexpected boundary") from exc
    else:
        raise AssertionError("invalid worker suspension operation was accepted")


def _clock(cursor: Any) -> Any:
    cursor.execute("SELECT clock_timestamp()")
    row = cursor.fetchone()
    if row is None:
        raise AssertionError("suspension fixture requires the database clock")
    return row[0]


def check_worker_suspension_contract(
    connection: Any, cursor: Any, active: Mapping[str, Any],
) -> dict[str, bool]:
    evaluated = evaluate_worker_suspension(authority=active, observation=None, evaluated_at=_clock(cursor))
    bundle = build_worker_suspension_bundle(authority=active, decision=evaluated, operator_id="fixture-health-observer")
    worker_id = evaluated["worker_id"]
    results: dict[str, bool] = {}

    def fail_second_write() -> None:
        # DDL and both attempted writes are inside the same rejected savepoint.
        cursor.execute("""
            CREATE FUNCTION pg_temp.reject_worker_suspension_fixture()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'suspension fixture second-write failure'; END;
            $$
        """)
        cursor.execute("""
            CREATE TRIGGER suspension_fixture_second_write
            BEFORE INSERT ON risk_platform.notification_worker_suspension_evidence
            FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_worker_suspension_fixture()
        """)
        record_worker_suspension_with_cursor(cursor, bundle=bundle)

    _reject(connection, fail_second_write, "suspension fixture second-write failure")
    current = read_current_worker_authority_with_cursor(cursor, worker_id=worker_id)
    cursor.execute("SELECT COUNT(*) FROM risk_platform.notification_worker_suspension_evidence WHERE worker_id = %s", (worker_id,))
    if current["transition"] != active or current["authority_sequence"] != 1 or cursor.fetchone() != (0,):
        raise AssertionError("second-write failure did not roll back both suspension writes")
    results["suspension_second_write_rollback"] = True

    class RollbackFixture(Exception):
        pass

    try:
        with connection.transaction():
            record_worker_authority_with_cursor(cursor, transition=bundle["transition"])
            _reject(connection, lambda: record_worker_suspension_with_cursor(cursor, bundle=bundle), "cannot be backfilled")
            raise RollbackFixture
    except RollbackFixture:
        pass
    results["suspension_no_legacy_backfill"] = True

    retained = record_worker_suspension_with_cursor(cursor, bundle=bundle)
    if retained["created"] is not True or retained["authority_sequence"] != 2:
        raise AssertionError("suspension bundle did not create exactly one authority successor")
    cursor.execute(
        "SELECT suspension_evidence_status, decision_id, runtime_permission_granted "
        "FROM risk_platform.current_notification_worker_suspension_review WHERE worker_id = %s", (worker_id,),
    )
    if cursor.fetchone() != ("bound", evaluated["decision_id"], False):
        raise AssertionError("current suspension evidence was not bound to its exact stop")
    results["suspension_atomic_creation_and_bound_view"] = True

    changed = build_worker_suspension_bundle(authority=active, decision=evaluated, operator_id="different-fixture-observer")
    _reject(connection, lambda: record_worker_suspension_with_cursor(cursor, bundle=changed), "different suspension evidence")
    stale = build_worker_suspension_bundle(
        authority=active,
        decision=evaluate_worker_suspension(authority=active, observation=None, evaluated_at=_clock(cursor)),
        operator_id="fixture-health-observer",
    )
    _reject(connection, lambda: record_worker_suspension_with_cursor(cursor, bundle=stale), "exact current head")
    results["suspension_conflict_and_stale_head_rejected"] = True

    for sql in (
        "UPDATE risk_platform.notification_worker_suspension_evidence SET worker_id = worker_id WHERE worker_id = %s",
        "DELETE FROM risk_platform.notification_worker_suspension_evidence WHERE worker_id = %s",
    ):
        def mutate(statement: str = sql) -> None:
            cursor.execute(statement, (worker_id,))
        _reject(connection, mutate, "append-only")
    _reject(connection, lambda: cursor.execute("TRUNCATE risk_platform.notification_worker_suspension_evidence"), "append-only")
    results["suspension_update_delete_truncate_rejected"] = True

    stopped = bundle["transition"]
    now = _clock(cursor)
    disabled = build_worker_authority_transition(
        plan=stopped["plan"], request_id=worker_id + "-after-suspension", operator_id="fixture-operator",
        action="disable", requested_at=now, effective_at=now, previous=stopped,
        reason_codes=["operator_request"],
    )
    record_worker_authority_with_cursor(cursor, transition=disabled)
    replay = record_worker_suspension_with_cursor(cursor, bundle=bundle)
    if replay["created"] is not False or replay["authority_sequence"] != 2:
        raise AssertionError("historical suspension replay did not converge")
    current = read_current_worker_authority_with_cursor(cursor, worker_id=worker_id)
    cursor.execute(
        "SELECT suspension_evidence_status, decision_id FROM "
        "risk_platform.current_notification_worker_suspension_review WHERE worker_id = %s", (worker_id,),
    )
    if current["transition"] != disabled or cursor.fetchone() != ("not_applicable", None):
        raise AssertionError("historical suspension leaked into newer current authority")
    results["suspension_historical_replay_without_promotion"] = True
    return results
