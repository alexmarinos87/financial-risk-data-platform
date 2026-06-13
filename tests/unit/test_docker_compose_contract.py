from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _load_compose() -> dict[str, Any]:
    with Path("docker-compose.yml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_local_database_services_are_present() -> None:
    compose = _load_compose()

    assert {"postgres", "mongo"} <= set(compose["services"])


def test_local_database_ports_are_bound_to_loopback_only() -> None:
    services = _load_compose()["services"]

    assert services["postgres"]["ports"] == ["127.0.0.1:5433:5432"]
    assert services["mongo"]["ports"] == ["127.0.0.1:27018:27017"]


def test_local_database_seed_mounts_are_read_only() -> None:
    services = _load_compose()["services"]

    assert services["postgres"]["volumes"] == [
        "./sql/postgres_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql:ro",
        "./sql/postgres_demo_data.sql:/docker-entrypoint-initdb.d/02_demo_data.sql:ro",
    ]
    assert services["mongo"]["volumes"] == ["./mongo/init:/docker-entrypoint-initdb.d:ro"]


def test_local_database_services_have_healthchecks() -> None:
    services = _load_compose()["services"]

    assert "pg_isready -U risk_user -d risk_platform" in services["postgres"]["healthcheck"]["test"]
    assert services["mongo"]["healthcheck"]["test"][0] == "CMD-SHELL"
    assert "db.adminCommand({ ping: 1 }).ok" in services["mongo"]["healthcheck"]["test"][1]


def test_postgres_uses_demo_only_credentials() -> None:
    environment = _load_compose()["services"]["postgres"]["environment"]

    assert environment == {
        "POSTGRES_DB": "risk_platform",
        "POSTGRES_USER": "risk_user",
        "POSTGRES_PASSWORD": "risk_password",
    }
