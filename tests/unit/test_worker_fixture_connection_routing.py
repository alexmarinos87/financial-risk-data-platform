from __future__ import annotations

import copy
import json
import os
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from src.warehouse import notification_worker_preflight_postgres_contract_check as proof

TARGET = {"host": "localhost", "port": "5433", "dbname": "postgres", "user": "fixture"}


@pytest.mark.parametrize(("host", "address"), [
    ("localhost", "127.0.0.1"), ("127.0.0.1", "127.0.0.1"), ("::1", "::1"),
])
def test_route_is_pinned_without_dns_or_environment_defaults(host: str, address: str) -> None:
    parameters = {**TARGET, "host": host, "password": "synthetic-not-a-real-secret"}
    environment = {"PGHOST": "remote.invalid", "PGHOSTADDR": "invalid-address",
                   "PGPORT": "invalid-port", "PGDATABASE": "other", "PGUSER": "other"}
    before = copy.deepcopy((parameters, environment))
    result = proof._fixture_connection_overrides(parameters, environment)
    assert result == {**TARGET, "host": host, "hostaddr": address, "connect_timeout": 5,
                      "options": "-c statement_timeout=15000 -c lock_timeout=5000"}
    assert (parameters, environment) == before
    assert "password" not in result


def test_explicit_ipv6_for_localhost_is_preserved() -> None:
    assert proof._fixture_connection_overrides({**TARGET, "hostaddr": "::1"}, {})["hostaddr"] == "::1"


@pytest.mark.parametrize("key", ["PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR"])
def test_service_environment_is_rejected_without_echo(key: str) -> None:
    with pytest.raises(ValueError) as error:
        proof._fixture_connection_overrides(TARGET, {key: "synthetic-sensitive-value"})
    assert str(error.value) == "disposable preflight proof does not accept service configuration"


@pytest.mark.parametrize("port", [None, "", "0", "65536", "5433,5434", " 5433", "05433", "+5433", True, [], "１２"])
def test_missing_or_noncanonical_port_is_rejected(port: Any) -> None:
    with pytest.raises(ValueError, match="bounded port"):
        proof._fixture_connection_overrides({**TARGET, "port": port}, {})


@pytest.mark.parametrize("port", [1, 65535, "1", "65535"])
def test_single_port_bounds_allow_strings_and_driver_integers(port: Any) -> None:
    assert proof._fixture_connection_overrides({**TARGET, "port": port}, {})["port"] == str(port)


@pytest.mark.parametrize("key", ["dbname", "user"])
@pytest.mark.parametrize("value", [None, "", " ", "host=remote.invalid", "postgresql://remote.invalid", "a" * 64, True])
def test_names_cannot_use_defaults_or_nested_connection_strings(key: str, value: Any) -> None:
    with pytest.raises(ValueError, match="explicit simple"):
        proof._fixture_connection_overrides({**TARGET, key: value}, {})


def test_conflicting_numeric_addresses_are_rejected() -> None:
    with pytest.raises(ValueError, match="matching loopback"):
        proof._fixture_connection_overrides({**TARGET, "host": "::1", "hostaddr": "127.0.0.1"}, {})


def test_absent_target_fields_cannot_fall_back_to_environment() -> None:
    for key in TARGET:
        parameters = {name: value for name, value in TARGET.items() if name != key}
        with pytest.raises(ValueError):
            proof._fixture_connection_overrides(parameters, {})


@pytest.mark.parametrize("invalid", [False, True])
def test_actual_context_uses_pinned_admin_and_child_dsn_or_never_connects(
    monkeypatch: pytest.MonkeyPatch, invalid: bool,
) -> None:
    calls: list[dict[str, Any]] = []
    driver = ModuleType("psycopg")
    conninfo = ModuleType("psycopg.conninfo")

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def execute(self, *args: Any) -> None:
            return None

        def fetchone(self) -> None:
            return None

    class Connection(Cursor):
        def cursor(self) -> Cursor:
            return Cursor()

    def connect(dsn: str, **kwargs: Any) -> Connection:
        calls.append(json.loads(dsn))
        return Connection()

    def merge(dsn: str, **kwargs: Any) -> str:
        return json.dumps({**json.loads(dsn), **kwargs})

    monkeypatch.setattr(driver, "connect", connect, raising=False)
    monkeypatch.setattr(driver, "sql", SimpleNamespace(SQL=str, Identifier=lambda name: f'"{name}"'), raising=False)
    monkeypatch.setattr(conninfo, "conninfo_to_dict", json.loads, raising=False)
    monkeypatch.setattr(conninfo, "make_conninfo", merge, raising=False)
    monkeypatch.setitem(sys.modules, "psycopg", driver)
    monkeypatch.setitem(sys.modules, "psycopg.conninfo", conninfo)
    for key in ("PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PGHOSTADDR", "invalid-address")
    monkeypatch.setenv("PGPORT", "invalid-port")
    before = dict(os.environ)
    source = {**TARGET, "password": "synthetic", "sslmode": "require"}
    if invalid:
        source.pop("port")
        with pytest.raises(ValueError):
            with proof.disposable_database(json.dumps(source), allow_disposable_database=True):
                raise AssertionError("must reject before connection")
        assert calls == []
    else:
        with proof.disposable_database(json.dumps(source), allow_disposable_database=True) as child_dsn:
            child = json.loads(child_dsn)
            assert child["dbname"].startswith(proof.DATABASE_PREFIX)
            assert child["hostaddr"] == "127.0.0.1"
            assert child["port"] == "5433"
            assert child["options"] == "-c statement_timeout=10000 -c lock_timeout=5000"
            assert child["sslmode"] == "require" and child["password"] == "synthetic"
        assert len(calls) == 1
        assert calls[0]["dbname"] == "postgres" and calls[0]["hostaddr"] == "127.0.0.1"
        assert calls[0]["options"] == "-c statement_timeout=15000 -c lock_timeout=5000"
    assert dict(os.environ) == before


def test_real_conninfo_preserves_credentials_but_pins_routing(monkeypatch: pytest.MonkeyPatch) -> None:
    from psycopg.conninfo import conninfo_to_dict

    for key in ("PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PGHOSTADDR", "invalid-address")
    parsed = conninfo_to_dict(proof._pinned_fixture_dsn(
        "postgresql://fixture:synthetic@localhost:5433/postgres?sslmode=require"
    ))
    assert parsed["hostaddr"] == "127.0.0.1"
    assert parsed["port"] == "5433" and parsed["dbname"] == "postgres"
    assert parsed["user"] == "fixture" and parsed["password"] == "synthetic"
    assert parsed["sslmode"] == "require" and parsed["connect_timeout"] == "5"
