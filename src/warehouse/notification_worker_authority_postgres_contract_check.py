"""Transaction-scoped PostgreSQL proof for worker authority history."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import yaml

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition, canonical_bytes,
)
from src.orchestration.plan_notification_worker import plan_notification_worker
from src.warehouse.notification_worker_authority_history import (
    LOCK_PREFIX, LOCK_SQL, read_current_worker_authority_with_cursor,
    record_worker_authority_with_cursor,
)

from src.warehouse.notification_worker_suspension_postgres_contract_check import check_worker_suspension_contract


def _plan(directory: Path, worker_id: str, planned_at: datetime) -> dict[str, Any]:
    configs = {
        name: yaml.safe_load(Path(f"config/{name}.yaml").read_text(encoding="utf-8"))
        for name in ("notification_workers", "notification_delivery", "notification_destinations")
    }
    worker = configs["notification_workers"]["workers"].pop("risk-operations-managed")
    worker["enabled"] = True
    configs["notification_workers"]["workers"] = {worker_id: worker}
    delivery = configs["notification_delivery"]["delivery"]
    delivery["webhook"]["enabled"] = True
    delivery["retry_execution"]["enabled"] = True
    activation = configs["notification_destinations"]["destinations"][worker["destination_id"]]["activation"]
    activation.update({
        "enabled": True, "change_request_id": "AUTHORITY-HISTORY-FIXTURE",
        "reviewed_by": ["independent-reviewer"],
        "reviewed_at": (planned_at - timedelta(days=1)).isoformat(),
        "review_expires_at": (planned_at + timedelta(days=1)).isoformat(),
    })
    for name, config in configs.items():
        (directory / f"{name}.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    return plan_notification_worker(
        worker_id=worker_id, planned_at=planned_at,
        worker_config_path=directory / "notification_workers.yaml",
        delivery_config_path=directory / "notification_delivery.yaml",
        destination_config_path=directory / "notification_destinations.yaml",
    )


def _grant(plan: Mapping[str, Any], request_id: str, previous: Mapping[str, Any] | None = None) -> dict[str, Any]:
    instant = datetime.fromisoformat(plan["planned_at"]) + timedelta(seconds=1)
    return build_worker_authority_transition(
        plan=plan, request_id=request_id, operator_id="fixture-operator",
        reviewed_by=["independent-reviewer"], action="activate" if previous is None else "resume",
        requested_at=instant, effective_at=instant, previous=previous,
        expires_at=datetime.fromisoformat(plan["schedule"]["scheduled_for"]) + timedelta(seconds=plan["execution"]["execution_timeout_seconds"]),
    )


def _stop(previous: Mapping[str, Any], request_id: str, action: str) -> dict[str, Any]:
    instant = datetime.fromisoformat(previous["effective_at"]) + timedelta(seconds=1)
    return build_worker_authority_transition(
        plan=previous["plan"], request_id=request_id, operator_id="fixture-operator",
        action=action, requested_at=instant, effective_at=instant,
        reason_codes=["operator_request"], previous=previous,
    )


def _reject(connection: Any, operation: Callable[[], Any], message: str) -> None:
    try:
        with connection.transaction():
            operation()
    except Exception as exc:
        if message not in str(exc):
            raise AssertionError("rejection occurred at an unexpected contract boundary") from exc
    else:
        raise AssertionError("invalid authority operation was accepted")


def _raw_insert(cursor: Any, document: Mapping[str, Any]) -> None:
    text = canonical_bytes(document).decode("utf-8")
    cursor.execute(
        """
        INSERT INTO risk_platform.notification_worker_authority_history (
            transition_id, request_id, worker_id, destination_id, plan_id,
            previous_transition_id, authority_sequence, action, from_state, to_state,
            requested_at, effective_at, expires_at, document_json, canonical_document, document_sha256
        ) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s)
        """,
        (document["transition_id"], document["request_id"], document["plan"]["worker"]["worker_id"],
         document["plan"]["destination"]["destination_id"], document["plan"]["plan_id"],
         document["previous_transition_id"], document["action"], document["from_state"],
         document["to_state"], document["requested_at"], document["effective_at"],
         document["expires_at"], text, text, hashlib.sha256(text.encode()).hexdigest()),
    )


def run_contract_check(dsn: str) -> dict[str, Any]:
    import psycopg

    worker_id = "authority-contract-" + uuid4().hex
    base = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    results: dict[str, Any] = {}
    with TemporaryDirectory(prefix="worker-authority-contract-") as temporary:
        directory = Path(temporary)
        connection = psycopg.connect(dsn, connect_timeout=5)
        competitor = psycopg.connect(dsn, connect_timeout=5)
        try:
            with connection.cursor() as cursor, competitor.cursor() as second:
                second.execute("SET LOCAL lock_timeout = '5s'")
                first = _grant(_plan(directory, worker_id, base), worker_id + "-activate")
                suspended = _stop(first, worker_id + "-suspend", "suspend")
                resumed = _grant(
                    _plan(directory, worker_id, base + timedelta(minutes=20)),
                    worker_id + "-resume", suspended,
                )
                disabled = _stop(resumed, worker_id + "-disable", "disable")
                for sequence, (document, expected) in enumerate(
                    ((first, "expired"), (suspended, "suspended"), (resumed, "expired"), (disabled, "disabled")), 1,
                ):
                    retained = record_worker_authority_with_cursor(cursor, transition=document)
                    if retained["created"] is not True or retained["authority_sequence"] != sequence:
                        raise AssertionError("authority sequence did not advance exactly once")
                    current = read_current_worker_authority_with_cursor(cursor, worker_id=worker_id)
                    cursor.execute(
                        "SELECT authority_state, runtime_permission_granted "
                        "FROM risk_platform.current_notification_worker_authority WHERE worker_id = %s",
                        (worker_id,),
                    )
                    if current["authority_state"] != expected or cursor.fetchone() != (expected, False):
                        raise AssertionError("SQL and canonical current states differ")
                results["lifecycle_and_sequence"] = True
                replay = record_worker_authority_with_cursor(cursor, transition=first)
                if replay["created"] is not False or replay["authority_sequence"] != 1:
                    raise AssertionError("historical exact replay did not converge")
                if read_current_worker_authority_with_cursor(cursor, worker_id=worker_id)["transition"]["transition_id"] != disabled["transition_id"]:
                    raise AssertionError("historical replay promoted an old authority")
                results["historical_replay_without_promotion"] = True
                different = build_worker_authority_transition(
                    plan=first["plan"], request_id=first["request_id"], operator_id="other-operator",
                    action="activate", requested_at=first["requested_at"], effective_at=first["effective_at"],
                    expires_at=first["expires_at"], reviewed_by=["independent-reviewer"],
                )
                _reject(connection, lambda: record_worker_authority_with_cursor(cursor, transition=different), "different worker authority")
                fork = _stop(first, worker_id + "-fork", "disable")
                _reject(connection, lambda: record_worker_authority_with_cursor(cursor, transition=fork), "predecessor")
                _reject(connection, lambda: _raw_insert(cursor, fork), "head or chronology conflict")
                results["request_conflict_and_fork_rejected"] = True
                for operation in (
                    "UPDATE risk_platform.notification_worker_authority_history SET plan_id = plan_id WHERE worker_id = %s",
                    "DELETE FROM risk_platform.notification_worker_authority_history WHERE worker_id = %s",
                ):
                    def mutate(sql: str = operation) -> None:
                        cursor.execute(sql, (worker_id,))
                    _reject(connection, mutate, "append-only")
                _reject(connection, lambda: cursor.execute("TRUNCATE risk_platform.notification_worker_authority_history, risk_platform.notification_worker_suspension_evidence"), "append-only")
                results["update_delete_truncate_rejected"] = True
                cursor.execute("SELECT clock_timestamp()")
                clock_row = cursor.fetchone()
                if clock_row is None:
                    raise AssertionError("database clock is unavailable")
                now = clock_row[0]
                active_worker = worker_id + "-active"
                active = _grant(_plan(directory, active_worker, now - timedelta(seconds=10)), active_worker)
                record_worker_authority_with_cursor(cursor, transition=active)
                if read_current_worker_authority_with_cursor(cursor, worker_id=active_worker)["authority_state"] != "active":
                    raise AssertionError("current grant was not active")
                results["active_state"] = True
                results.update(check_worker_suspension_contract(connection, cursor, active))
                future = _grant(_plan(directory, worker_id + "-future", now + timedelta(days=1)), worker_id + "-future")
                _reject(connection, lambda: record_worker_authority_with_cursor(cursor, transition=future), "check constraint")
                results["future_head_rejected"] = True
                if read_current_worker_authority_with_cursor(cursor, worker_id=worker_id + "-missing")["authority_state"] != "inactive":
                    raise AssertionError("unknown worker was not inactive")
                results["unknown_worker_inactive"] = True
                cursor.execute(
                    "SELECT COUNT(*), MAX(authority_sequence) FROM "
                    "risk_platform.notification_worker_authority_history WHERE worker_id = %s", (worker_id,),
                )
                if cursor.fetchone() != (4, 4):
                    raise AssertionError("rejected requests changed the authority chain")
                results["reconciled_history_count"] = True
                second.execute("SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))", (LOCK_PREFIX + worker_id,))
                if second.fetchone() != (False,):
                    raise AssertionError("second session acquired held worker lock")
                connection.rollback()
                second.execute(LOCK_SQL, (LOCK_PREFIX + worker_id,))
                competitor.rollback()
                results["two_session_lock_exclusion_and_release"] = True
                cursor.execute(
                    "SELECT COUNT(*) FROM risk_platform.notification_worker_authority_history WHERE worker_id = %s OR worker_id = %s",
                    (worker_id, active_worker),
                )
                if cursor.fetchone() != (0,):
                    raise AssertionError("fixture history survived transaction rollback")
                results["fixture_rolled_back"] = True
                cursor.execute(
                    "SELECT COUNT(*) FROM risk_platform.notification_worker_suspension_evidence WHERE worker_id = %s",
                    (active_worker,),
                )
                if cursor.fetchone() != (0,):
                    raise AssertionError("suspension evidence survived fixture rollback")
                results["suspension_fixture_rolled_back"] = True
        finally:
            connection.rollback()
            competitor.rollback()
            connection.close()
            competitor.close()
    return {"model_version": "notification-worker-authority-postgres-contract-v1", "checks": results,
            "external_request_performed": False, "scheduler_mutated": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()
    try:
        result = run_contract_check(args.dsn)
    except ValidationError:
        print("Worker authority PostgreSQL contract rejected fixture evidence", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Worker authority PostgreSQL contract failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
