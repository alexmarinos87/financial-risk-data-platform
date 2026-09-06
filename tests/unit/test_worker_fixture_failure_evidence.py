from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from src.warehouse import notification_worker_preflight_postgres_contract_check as proof


@pytest.mark.parametrize("value", [None, [], {}, {"a": True, "extra": True},
                                   {"a": False}, {"a": 1}, {"a": "true"}, {"a": None}])
def test_incomplete_extra_and_non_boolean_proofs_are_rejected(value: Any) -> None:
    with pytest.raises(AssertionError, match="proof groups"):
        proof._verified_checks(value, frozenset({"a"}))


def test_proof_manifest_is_exact_sorted_and_detached() -> None:
    source = {"b": True, "a": True}
    result = proof._verified_checks(source, frozenset(source))
    source["a"] = False
    assert list(result) == ["a", "b"]
    assert result == {"a": True, "b": True}
    assert len(proof.OPERATOR_CHECKS) == 8 and len(proof.FAILURE_CHECKS) == 3
    assert len(proof.FINAL_CHECKS) == 12


@pytest.mark.parametrize("bad_stage", [None, "operator", "cleanup", "failure_proofs"])
def test_result_requires_complete_proofs_and_successful_context_exit(
    monkeypatch: pytest.MonkeyPatch, bad_stage: str | None,
) -> None:
    events: list[str] = []
    supplied = {key: True for key in proof.OPERATOR_CHECKS}
    if bad_stage == "operator":
        supplied.pop(next(iter(supplied)))

    @contextlib.contextmanager
    def database(dsn: str, *, allow_disposable_database: bool = False) -> Iterator[str]:
        assert allow_disposable_database is True
        events.append("enter")
        try:
            yield "generated-fixture"
        finally:
            events.append("cleanup")
            if bad_stage == "cleanup":
                raise RuntimeError("injected cleanup failure")

    def failure_proofs(dsn: str) -> dict[str, bool]:
        assert events == ["enter", "cleanup"]
        events.append("failure-probes")
        return {} if bad_stage == "failure_proofs" else {key: True for key in proof.FAILURE_CHECKS}

    monkeypatch.setattr(proof, "disposable_database", database)
    monkeypatch.setattr(proof, "_exercise", lambda dsn, directory: supplied)
    monkeypatch.setattr(proof, "_prove_failure_cleanup", failure_proofs)
    if bad_stage is not None:
        with pytest.raises((AssertionError, RuntimeError)):
            proof.run_contract_check("injected", allow_disposable_database=True)
        assert "cleanup" in events
        if bad_stage in {"operator", "cleanup"}:
            assert "failure-probes" not in events
    else:
        result = proof.run_contract_check("injected", allow_disposable_database=True)
        assert result["model_version"] == "worker-preflight-operator-postgres-contract-v2"
        assert result["checks"] == {key: True for key in proof.FINAL_CHECKS}
        assert set(supplied) == proof.OPERATOR_CHECKS
        assert result["checks"] is not supplied
        assert result["runtime_permission_granted"] is False


@pytest.mark.parametrize("fail_at", [None, "create", "marker", "drop", "catalog"])
def test_failure_probes_accept_only_their_own_sentinel_and_confirm_catalog_cleanup(
    monkeypatch: pytest.MonkeyPatch, fail_at: str | None,
) -> None:
    """Fake control-flow challenge; real commit/drop execution is required in CI."""
    databases: set[str] = set()
    marker_databases: set[str] = set()
    calls: list[tuple[str, str]] = []
    connected: list[dict[str, Any]] = []
    driver = ModuleType("psycopg")
    conninfo = ModuleType("psycopg.conninfo")

    class Cursor:
        def __init__(self, name: str) -> None:
            self.name = name
            self.row: Any = None

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def execute(self, statement: str, params: Any = None) -> None:
            calls.append((self.name, statement))
            if statement.startswith("CREATE DATABASE"):
                if fail_at == "create":
                    raise RuntimeError("provider creation failure")
                databases.add(statement.split('"')[1])
            elif statement.startswith("DROP DATABASE"):
                if fail_at == "drop":
                    raise RuntimeError("provider cleanup failure")
                databases.remove(statement.split('"')[1])
            elif statement.startswith("CREATE TABLE"):
                assert self.name in databases
                if fail_at == "marker":
                    raise RuntimeError("provider marker failure")
            elif statement.startswith("INSERT INTO"):
                marker_databases.add(self.name)
            elif statement.startswith("SELECT current_database()"):
                self.row = (self.name, 1 if self.name in marker_databases else 0)
            elif statement.startswith("SELECT 1 FROM pg_database"):
                self.row = (1,) if params[0] in databases or fail_at == "catalog" else None

        def fetchone(self) -> Any:
            return self.row

    class Connection:
        def __init__(self, name: str) -> None:
            self.name = name

        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor(self.name)

    def connect(dsn: str, **kwargs: Any) -> Connection:
        parameters = json.loads(dsn)
        connected.append(parameters)
        assert parameters["hostaddr"] == "127.0.0.1"
        assert parameters["port"] == "5433" and parameters["user"] == "fixture"
        return Connection(parameters["dbname"])

    monkeypatch.setattr(driver, "connect", connect, raising=False)
    monkeypatch.setattr(driver, "sql", SimpleNamespace(SQL=str, Identifier=lambda name: f'"{name}"'), raising=False)
    monkeypatch.setattr(conninfo, "conninfo_to_dict", json.loads, raising=False)
    monkeypatch.setattr(conninfo, "make_conninfo", lambda dsn, **kwargs: json.dumps({**json.loads(dsn), **kwargs}), raising=False)
    monkeypatch.setitem(sys.modules, "psycopg", driver)
    monkeypatch.setitem(sys.modules, "psycopg.conninfo", conninfo)
    for key in ("PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR"):
        monkeypatch.delenv(key, raising=False)
    before = dict(os.environ)
    dsn = json.dumps({"host": "localhost", "port": "5433", "dbname": "postgres", "user": "fixture"})
    if fail_at:
        with pytest.raises((RuntimeError, AssertionError)):
            proof._prove_failure_cleanup(dsn)
    else:
        assert proof._prove_failure_cleanup(dsn) == {key: True for key in proof.FAILURE_CHECKS}
        assert databases == set() and len(marker_databases) == 1
        assert len([sql for _, sql in calls if sql.startswith("CREATE DATABASE")]) == 2
        assert len([sql for _, sql in calls if sql.startswith("DROP DATABASE")]) == 2
    assert dict(os.environ) == before
    assert all(name != "postgres" for name, sql in calls if sql.startswith(("CREATE TABLE", "INSERT INTO")))
    assert all("FORCE" not in sql and "pg_terminate_backend" not in sql for _, sql in calls)
    if fail_at == "create":
        assert not any(sql.startswith("DROP DATABASE") for _, sql in calls)
    if fail_at == "marker":
        assert databases == set()


def test_existing_make_invocation_runs_the_strict_result_contract() -> None:
    source = Path("src/warehouse/notification_worker_preflight_postgres_contract_check.py").read_text()
    assert '_verified_checks(_exercise(fixture_dsn, Path(temporary)), OPERATOR_CHECKS)' in source
    assert '_verified_checks(_prove_failure_cleanup(dsn), FAILURE_CHECKS)' in source
    assert '_verified_checks(checks, FINAL_CHECKS)' in source
