from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from src.warehouse.notification_worker_preflight_postgres_contract_check import (
    DATABASE_PREFIX, _check_local_parameters, _owned_database_name, disposable_database,
)


@pytest.mark.parametrize("acknowledgement", [False, None, 0, 1, "true"])
def test_database_creation_requires_strict_explicit_ack_before_driver_import(
    monkeypatch: pytest.MonkeyPatch, acknowledgement: Any,
) -> None:
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(ValueError, match="acknowledgement"):
        with disposable_database("unused", allow_disposable_database=acknowledgement):
            raise AssertionError("database context must not be entered")


@pytest.mark.parametrize("dsn", [None, "", "  ", 1])
def test_dsn_must_exist_before_driver_import(monkeypatch: pytest.MonkeyPatch, dsn: Any) -> None:
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(ValueError, match="DSN"):
        with disposable_database(dsn, allow_disposable_database=True):
            raise AssertionError("database context must not be entered")


@pytest.mark.parametrize("parameters", [
    {}, {"host": "production.example"}, {"host": "/tmp"},
    {"host": "localhost,production.example"}, {"host": "localhost", "service": "hidden"},
    {"host": "localhost", "hostaddr": "192.0.2.1"},
])
def test_nonlocal_or_indirect_administration_hosts_are_rejected(parameters: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="loopback"):
        _check_local_parameters(parameters)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_explicit_loopback_hosts_are_allowed(host: str) -> None:
    _check_local_parameters({"host": host})


@pytest.mark.parametrize("token", ["risk_platform", "", "a" * 31, "A" * 32, "'; DROP DATABASE postgres", None])
def test_fixture_database_name_cannot_be_supplied_or_injected(token: Any) -> None:
    with pytest.raises(ValueError):
        _owned_database_name(token)


def test_generated_name_is_bounded_and_distinct_from_application_databases() -> None:
    name = _owned_database_name("a" * 32)
    assert name == DATABASE_PREFIX + "a" * 32
    assert len(name) < 64
    assert name not in {"postgres", "template0", "template1", "risk_platform"}


def test_disposable_proof_is_wired_into_existing_ci_target() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")
    target = makefile.split("\npostgres-contract-check:\n", 1)[1].split("\nlocal-db-up:", 1)[0]
    assert "src.warehouse.notification_worker_preflight_postgres_contract_check" in target
    assert "--allow-disposable-database" in target
    assert 'WAREHOUSE_POSTGRES_DSN="$(LOCAL_POSTGRES_DSN)"' in target


@pytest.mark.parametrize("fail_at", [None, "create", "body"])
def test_only_successfully_created_database_is_dropped(
    monkeypatch: pytest.MonkeyPatch, fail_at: str | None,
) -> None:
    from types import ModuleType, SimpleNamespace

    calls: list[str] = []

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def execute(self, statement: str, params: Any = None) -> None:
            calls.append(statement)
            if fail_at == "create" and statement.startswith("CREATE DATABASE"):
                raise RuntimeError("injected creation failure")

        def fetchone(self) -> None:
            return None

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor()

    driver = ModuleType("psycopg")
    monkeypatch.setattr(driver, "connect", lambda *args, **kwargs: Connection(), raising=False)
    monkeypatch.setattr(driver, "sql", SimpleNamespace(SQL=str, Identifier=lambda name: f'"{name}"'), raising=False)
    conninfo = ModuleType("psycopg.conninfo")
    monkeypatch.setattr(conninfo, "conninfo_to_dict", lambda dsn: {"host": "localhost"}, raising=False)
    monkeypatch.setattr(conninfo, "make_conninfo", lambda dsn, **kwargs: kwargs["dbname"], raising=False)
    monkeypatch.setitem(sys.modules, "psycopg", driver)
    monkeypatch.setitem(sys.modules, "psycopg.conninfo", conninfo)
    try:
        with disposable_database("synthetic-administration-dsn", allow_disposable_database=True) as name:
            assert name.startswith(DATABASE_PREFIX)
            if fail_at == "body":
                raise RuntimeError("injected body failure")
    except RuntimeError:
        assert fail_at is not None
    else:
        assert fail_at is None
    creates = [sql for sql in calls if sql.startswith("CREATE DATABASE")]
    drops = [sql for sql in calls if sql.startswith("DROP DATABASE")]
    assert len(creates) == 1
    if fail_at == "create":
        assert not drops
    else:
        assert len(drops) == 1
        assert creates[0].split('"')[1] == drops[0].split('"')[1]


@pytest.mark.parametrize("parameters", [
    {"host": None}, {"host": 1}, {"host": []},
    {"host": "localhost", "hostaddr": None},
    {"host": "localhost", "hostaddr": 1},
    {"host": "localhost", "hostaddr": []},
])
def test_non_text_host_values_fail_closed(parameters: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="loopback"):
        _check_local_parameters(parameters)


def test_non_host_driver_values_do_not_weaken_or_break_host_checks() -> None:
    _check_local_parameters({"host": "localhost", "port": 5433, "connect_timeout": None})
