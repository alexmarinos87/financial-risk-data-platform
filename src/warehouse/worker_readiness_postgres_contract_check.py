"""Read-only adapter proof against the disposable database seeded by the existing CI target."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

from src.common.exceptions import StorageError
from src.orchestration.notification_worker_authority_contract import (
    PLAN_MODEL_VERSION, build_worker_authority_transition, canonical_bytes,
)
from src.warehouse.notification_execution_readiness_postgres_contract_check import DESTINATION_ID
from src.warehouse.notification_worker_authority_history import record_worker_authority
from src.warehouse.notification_worker_authority_postgres_contract_check import _grant, _plan
from src.warehouse.notification_worker_readiness_reader import (
    read_current_worker_readiness, read_worker_readiness_with_cursor,
)


def run_contract_check(dsn: str) -> dict[str, Any]:
    import psycopg

    worker_id = "readiness-source-proof-" + uuid4().hex
    checks: dict[str, bool] = {}
    if read_current_worker_readiness(dsn=dsn, worker_id=worker_id)["status"] != "authority_missing":
        raise AssertionError("unknown worker produced readiness")
    checks["unknown_worker_blocks"] = True
    with psycopg.connect(dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT r.record_json FROM risk_platform.current_notification_execution_readiness_review v "
                "JOIN risk_platform.notification_execution_readiness_decisions r ON r.record_id = v.readiness_record_id "
                "WHERE v.destination_id = %s AND v.execution_kind = 'initial'", (DESTINATION_ID,),
            )
            source = cursor.fetchone()
            if source is None:
                raise AssertionError("existing readiness fixture must run before source-reader proof")
            decision = source[0]["decision"]
            with TemporaryDirectory(prefix="worker-readiness-source-proof-") as temporary:
                p = _plan(Path(temporary), worker_id, datetime.now(timezone.utc) - timedelta(seconds=10))
            p["destination"]["destination_id"] = decision["destination"]["destination_id"]
            p["destination"]["fingerprint"] = decision["destination"]["fingerprint"]
            p["destination"]["endpoint_environment_variable"] = decision["destination"]["endpoint_environment_variable"]
            configuration = decision["configuration"]
            for target, field in (("delivery_fingerprint", "delivery_fingerprint"),
                                  ("retry_planning_policy_fingerprint", "retry_policy_fingerprint"),
                                  ("retry_execution_policy_fingerprint", "retry_execution_policy_fingerprint")):
                p["delivery"][target] = configuration[field]
            identity = {key: value for key, value in p.items() if key != "plan_id"}
            p["plan_id"] = f"{PLAN_MODEL_VERSION}-plan-{hashlib.sha256(canonical_bytes(identity)).hexdigest()[:24]}"
            active = _grant(p, worker_id)
            record_worker_authority(dsn=dsn, transition=active)
            first = read_current_worker_readiness(dsn=dsn, worker_id=worker_id)
            if first["status"] != "blocked" or first["authority_sequence"] != 1:
                raise AssertionError("superseded readiness sources did not block")
            observed_rows = first["snapshot"]["readiness"]
            if len(observed_rows) != 2 or any(row["status"] != "superseded" for row in observed_rows):
                raise AssertionError("actual current-review replacement was not independently detected")
            cursor.execute(
                "SELECT readiness_record_id FROM risk_platform.current_notification_execution_readiness_review "
                "WHERE destination_id = %s ORDER BY execution_kind", (DESTINATION_ID,),
            )
            if [row["record_id"] for row in observed_rows] != [row[0] for row in cursor.fetchall()]:
                raise AssertionError("reader selected different retained readiness sources")
            checks["real_records_reopened_and_supersession_detected"] = True
            cursor.execute("SELECT COUNT(*) FROM risk_platform.notification_worker_authority_history")
            before = cursor.fetchone()
            read_current_worker_readiness(dsn=dsn, worker_id=worker_id)
            cursor.execute("SELECT COUNT(*) FROM risk_platform.notification_worker_authority_history")
            if cursor.fetchone() != before:
                raise AssertionError("readiness reader wrote authority history")
            checks["read_only_repeat_does_not_write"] = True
            now = datetime.now(timezone.utc)
            stopped = build_worker_authority_transition(
                plan=p, request_id=worker_id + "-stop", operator_id="fixture-operator",
                action="suspend", requested_at=now, effective_at=now, previous=active,
                reason_codes=["operator_request"],
            )
            record_worker_authority(dsn=dsn, transition=stopped)
            latest = read_current_worker_readiness(dsn=dsn, worker_id=worker_id)
            if latest["authority_transition_id"] != stopped["transition_id"] or latest["authority_sequence"] != 2:
                raise AssertionError("reader reused an older active worker head")
            if "worker_authority_not_active" not in latest["snapshot"]["blocking_reasons"]:
                raise AssertionError("stopped authority was not blocked")
            checks["newer_stop_replaces_old_active_head"] = True
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SELECT 1")
            try:
                read_worker_readiness_with_cursor(cursor, worker_id=worker_id)
            except StorageError as exc:
                if "fresh read-only statement" not in str(exc):
                    raise
            else:
                raise AssertionError("older caller transaction was accepted")
            finally:
                cursor.execute("ROLLBACK")
            checks["older_transaction_clock_rejected"] = True
    return {"checks": checks, "fixture_authority_records": 2,
            "fixture_cleanup": "disposable_database_teardown", "runtime_permission_granted": False,
            "notification_delivery_performed": False, "scheduler_mutated": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()
    try:
        result = run_contract_check(args.dsn)
    except Exception:
        print("Worker readiness PostgreSQL source proof failed", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
