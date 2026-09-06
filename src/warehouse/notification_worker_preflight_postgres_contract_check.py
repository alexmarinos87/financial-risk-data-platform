"""Explicit disposable PostgreSQL proof for the complete worker preflight path."""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sys
from collections.abc import Iterator, Mapping
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch
from uuid import uuid4

DATABASE_PREFIX = "worker_preflight_contract_"
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _check_local_parameters(parameters: Mapping[str, object]) -> None:
    host = parameters.get("host")
    hostaddr = parameters.get("hostaddr")
    if (not isinstance(host, str) or host not in LOCAL_HOSTS or "service" in parameters
            or ("hostaddr" in parameters and (not isinstance(hostaddr, str)
                or hostaddr not in LOCAL_HOSTS - {"localhost"}))):
        raise ValueError("disposable preflight proof requires an explicit loopback database host")


def _fixture_connection_overrides(
    parameters: Mapping[str, object], environment: Mapping[str, str],
) -> dict[str, str | int]:
    """Pin routing independently of libpq defaults; do not alter credential inputs."""
    _check_local_parameters(parameters)
    if any(environment.get(key) for key in ("PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR")):
        raise ValueError("disposable preflight proof does not accept service configuration")
    host = str(parameters["host"])
    address = parameters.get("hostaddr", "127.0.0.1" if host == "localhost" else host)
    if host != "localhost" and address != host:
        raise ValueError("disposable preflight proof requires matching loopback addresses")
    port = parameters.get("port")
    if type(port) is int:
        port = str(port)
    if (not isinstance(port, str) or re.fullmatch(r"[1-9][0-9]{0,4}", port) is None
            or not 1 <= int(port) <= 65535):
        raise ValueError("disposable preflight proof requires one explicit bounded port")
    names: dict[str, str] = {}
    for key in ("dbname", "user"):
        value = parameters.get(key)
        if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,62}", value) is None:
            raise ValueError("disposable preflight proof requires explicit simple database and user names")
        names[key] = value
    return {**names, "host": host, "hostaddr": str(address), "port": port,
            "connect_timeout": 5, "options": "-c statement_timeout=15000 -c lock_timeout=5000"}


def _pinned_fixture_dsn(dsn: str) -> str:
    """Build one explicit local target for admin and child connections, without I/O."""
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    return make_conninfo(dsn, **_fixture_connection_overrides(conninfo_to_dict(dsn), os.environ))


def _owned_database_name(token: str) -> str:
    if not isinstance(token, str) or re.fullmatch(r"[0-9a-f]{32}", token) is None:
        raise ValueError("invalid generated fixture database identity")
    return DATABASE_PREFIX + token


@contextlib.contextmanager
def disposable_database(dsn: str, *, allow_disposable_database: bool = False) -> Iterator[str]:
    if allow_disposable_database is not True:
        raise ValueError("explicit disposable database acknowledgement is required")
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValueError("a local administration DSN is required")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    pinned_dsn = _pinned_fixture_dsn(dsn)
    name = _owned_database_name(uuid4().hex)
    with psycopg.connect(pinned_dsn, autocommit=True, connect_timeout=5) as admin:
        with admin.cursor() as cursor:
            cursor.execute("SET statement_timeout = '15s'")
            cursor.execute("SET lock_timeout = '5s'")
            created = False
            try:
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
                created = True
                yield make_conninfo(
                    pinned_dsn, dbname=name, connect_timeout=5,
                    options="-c statement_timeout=10000 -c lock_timeout=5000",
                )
            finally:
                # Never drop a caller-selected name or a database whose creation failed.
                if created:
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                    if cursor.fetchone() is not None:
                        raise AssertionError("fixture database cleanup was not confirmed")


def _run_command(dsn: str, arguments: list[str]) -> tuple[int, dict[str, Any]]:
    from src.orchestration.check_notification_worker_preflight import main

    stdout, stderr = io.StringIO(), io.StringIO()
    with patch.dict(os.environ, {"WAREHOUSE_POSTGRES_DSN": dsn}):
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(arguments)
    if stderr.getvalue():
        raise AssertionError("worker preflight command rejected integration fixture")
    report = json.loads(stdout.getvalue())
    if report["runtime_permission_granted"] is not False:
        raise AssertionError("operator report granted runtime permission")
    return code, report


def _exercise(dsn: str, directory: Path) -> dict[str, bool]:
    import psycopg
    import yaml

    from src.orchestration import check_notification_worker_preflight as command
    from src.orchestration.plan_notification_worker import plan_notification_worker
    from src.warehouse import notification_worker_authority_snapshot as snapshots
    from src.warehouse.notification_worker_authority_history import record_worker_authority
    from src.warehouse.notification_worker_authority_postgres_contract_check import _grant, _plan, _stop

    checks: dict[str, bool] = {}
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(Path("sql/notification_worker_authority_schema.sql").read_text(encoding="utf-8"))
        cursor.execute("SELECT clock_timestamp()")
        clock = cursor.fetchone()
        if clock is None:
            raise AssertionError("fixture database clock is unavailable")
        planned = clock[0] - timedelta(seconds=61)
    worker_id = "preflight-integration-worker"
    _plan(directory, worker_id, planned)
    worker_path = directory / "notification_workers.yaml"
    configuration = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    worker = configuration["workers"][worker_id]
    worker["schedule"].update(interval_seconds=60, jitter_seconds=0)
    worker["limits"]["execution_timeout_seconds"] = 300
    worker_path.write_text(yaml.safe_dump(configuration), encoding="utf-8")
    paths = {
        "worker_config_path": worker_path,
        "delivery_config_path": directory / "notification_delivery.yaml",
        "destination_config_path": directory / "notification_destinations.yaml",
    }
    plan = plan_notification_worker(worker_id=worker_id, planned_at=planned, **paths)
    active = _grant(plan, "PREFLIGHT-INTEGRATION-ACTIVATE")
    record_worker_authority(dsn=dsn, transition=active)
    original_reader = snapshots.read_worker_authority_snapshot_with_cursor

    def audited_read(cursor: Any, *, worker_id: str) -> dict[str, Any]:
        captured = original_reader(cursor, worker_id=worker_id)
        cursor.execute("SHOW transaction_read_only")
        if cursor.fetchone() != ("on",):
            raise AssertionError("public snapshot transaction was not read-only")
        cursor.execute("SHOW transaction_isolation")
        if cursor.fetchone() != ("read committed",):
            raise AssertionError("public snapshot isolation was not READ COMMITTED")
        try:
            with cursor.connection.transaction():
                cursor.execute("UPDATE risk_platform.notification_worker_authority_history SET plan_id = plan_id WHERE FALSE")
        except psycopg.errors.ReadOnlySqlTransaction:
            checks["public_reader_write_rejected"] = True
        else:
            raise AssertionError("read-only transaction accepted a table write")
        return captured

    with patch.object(snapshots, "read_worker_authority_snapshot_with_cursor", audited_read):
        captured = snapshots.read_worker_authority_snapshot(dsn=dsn, worker_id=worker_id)
    if captured["transition"] != active or captured["authority_state"] != "active":
        raise AssertionError("separate public reader did not observe committed active authority")
    checks["committed_active_public_read"] = True
    selection = [
        "--worker-id", worker_id, "--selected-transition-id", active["transition_id"],
        "--scheduled-for", plan["schedule"]["scheduled_for"],
        "--worker-config", str(paths["worker_config_path"]),
        "--delivery-config", str(paths["delivery_config_path"]),
        "--destination-config", str(paths["destination_config_path"]),
    ]
    code, live_report = _run_command(dsn, ["--read-current", *selection])
    if (code != 0 or live_report["source_mode"] != "live_database_read"
            or live_report["result"]["preflight"]["outcome"] != "eligible_for_health_review"):
        raise AssertionError("committed due slot did not pass the real operator preflight")
    checks["live_due_operator_path"] = True
    stopped = _stop(active, "PREFLIGHT-INTEGRATION-DISABLE", "disable")
    record_worker_authority(dsn=dsn, transition=stopped)
    code, stop_report = _run_command(dsn, ["--read-current", *selection])
    reasons = stop_report["result"]["preflight"]["reasons"]
    if code != 4 or not {"authority_superseded", "authority_disabled"}.issubset(reasons):
        raise AssertionError("new committed stop did not block the selected old grant")
    checks["committed_stop_blocks_old_selection"] = True
    replay = record_worker_authority(dsn=dsn, transition=active)
    current = snapshots.read_worker_authority_snapshot(dsn=dsn, worker_id=worker_id)
    if replay["created"] is not False or current["transition"] != stopped:
        raise AssertionError("historical replay promoted an old grant")
    checks["historical_replay_does_not_promote"] = True
    report_path = directory / "captured-report.json"
    report_path.write_text(json.dumps(live_report), encoding="utf-8")
    with patch.object(command, "read_worker_authority_snapshot", side_effect=AssertionError("offline read")):
        code, replayed_report = _run_command(dsn, ["--report", str(report_path), *selection])
    if (code != 0 or replayed_report["result"] != live_report["result"]
            or replayed_report["source_mode"] != "retained_report"
            or replayed_report["database_read_performed"] is not False):
        raise AssertionError("offline replay changed capture or implied a fresh database read")
    checks["offline_capture_is_not_current_authority"] = True
    missing = snapshots.read_worker_authority_snapshot(dsn=dsn, worker_id="missing-worker")
    if missing["transition"] is not None or not missing["observed_at"]:
        raise AssertionError("missing worker was not observed on the database clock")
    checks["missing_worker_clock"] = True
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM risk_platform.notification_worker_authority_history")
        if cursor.fetchone() != (2,):
            raise AssertionError("diagnostic checks changed committed authority history")
    checks["only_two_intended_authority_records"] = True
    return checks


def run_contract_check(dsn: str, *, allow_disposable_database: bool = False) -> dict[str, Any]:
    with disposable_database(dsn, allow_disposable_database=allow_disposable_database) as fixture_dsn:
        with TemporaryDirectory(prefix="worker-preflight-integration-") as temporary:
            checks = _exercise(fixture_dsn, Path(temporary))
    checks["owned_fixture_database_dropped"] = True
    return {"model_version": "worker-preflight-operator-postgres-contract-v1", "checks": checks,
            "runtime_permission_granted": False, "external_request_performed": False,
            "scheduler_mutated": False}


def main() -> int:
    from src.orchestration.notification_worker_cli_parser import WorkerPreflightParser

    parser = WorkerPreflightParser(prog="worker-preflight-postgres-contract", allow_abbrev=False)
    parser.add_argument("--allow-disposable-database", action="store_true")
    arguments = parser.parse_args()
    try:
        result = run_contract_check(
            os.environ.get("WAREHOUSE_POSTGRES_DSN", ""),
            allow_disposable_database=arguments.allow_disposable_database,
        )
    except Exception:
        print("Worker preflight disposable PostgreSQL proof failed; inspect fixture cleanup", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
