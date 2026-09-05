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


def test_local_database_seed_mounts_are_read_only_and_ordered() -> None:
    services = _load_compose()["services"]
    assert services["postgres"]["volumes"] == [
        "./sql/postgres_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql:ro",
        "./sql/postgres_demo_data.sql:/docker-entrypoint-initdb.d/02_demo_data.sql:ro",
        "./sql/portfolio_schema.sql:/docker-entrypoint-initdb.d/03_portfolio_schema.sql:ro",
        "./sql/portfolio_attribution_schema.sql:/docker-entrypoint-initdb.d/04_portfolio_attribution_schema.sql:ro",
        "./sql/portfolio_risk_limits_schema.sql:/docker-entrypoint-initdb.d/05_portfolio_risk_limits_schema.sql:ro",
        "./sql/portfolio_risk_breach_lifecycle_schema.sql:/docker-entrypoint-initdb.d/06_portfolio_risk_breach_lifecycle_schema.sql:ro",
        "./sql/portfolio_risk_notification_outbox_schema.sql:/docker-entrypoint-initdb.d/07_portfolio_risk_notification_outbox_schema.sql:ro",
        "./sql/market_freshness_schema.sql:/docker-entrypoint-initdb.d/08_market_freshness_schema.sql:ro",
        "./sql/portfolio_risk_notification_delivery_schema.sql:/docker-entrypoint-initdb.d/09_portfolio_risk_notification_delivery_schema.sql:ro",
        "./sql/portfolio_covariance_method_schema.sql:/docker-entrypoint-initdb.d/10_portfolio_covariance_method_schema.sql:ro",
        "./sql/portfolio_risk_limits_method_schema.sql:/docker-entrypoint-initdb.d/11_portfolio_risk_limits_method_schema.sql:ro",
        "./sql/model_approval_schema.sql:/docker-entrypoint-initdb.d/12_model_approval_schema.sql:ro",
        "./sql/operational_service_levels_schema.sql:/docker-entrypoint-initdb.d/13_operational_service_levels_schema.sql:ro",
        "./sql/operational_service_level_objectives_schema.sql:/docker-entrypoint-initdb.d/14_operational_service_level_objectives_schema.sql:ro",
        "./sql/operational_readiness_decisions_schema.sql:/docker-entrypoint-initdb.d/15_operational_readiness_decisions_schema.sql:ro",
        "./sql/operational_review_schema.sql:/docker-entrypoint-initdb.d/16_operational_review_schema.sql:ro",
        "./sql/operational_readiness_overrides_schema.sql:/docker-entrypoint-initdb.d/17_operational_readiness_overrides_schema.sql:ro",
        "./sql/local_schedule_runs_schema.sql:/docker-entrypoint-initdb.d/18_local_schedule_runs_schema.sql:ro",
        "./sql/portfolio_risk_notification_retry_execution_schema.sql:/docker-entrypoint-initdb.d/19_portfolio_risk_notification_retry_execution_schema.sql:ro",
        "./sql/portfolio_risk_notification_retry_destination_binding_schema.sql:/docker-entrypoint-initdb.d/19b_portfolio_risk_notification_retry_destination_binding_schema.sql:ro",
        "./sql/portfolio_risk_notification_retry_follow_up_schema.sql:/docker-entrypoint-initdb.d/20_portfolio_risk_notification_retry_follow_up_schema.sql:ro",
        "./sql/portfolio_risk_notification_retry_destination_follow_up_schema.sql:/docker-entrypoint-initdb.d/21_portfolio_risk_notification_retry_destination_follow_up_schema.sql:ro",
        "./sql/controlled_notification_receiver_rehearsal_schema.sql:/docker-entrypoint-initdb.d/22_controlled_notification_receiver_rehearsal_schema.sql:ro",
        "./sql/controlled_notification_receiver_review_schema.sql:/docker-entrypoint-initdb.d/23_controlled_notification_receiver_review_schema.sql:ro",
        "./sql/notification_destination_transition_rehearsal_schema.sql:/docker-entrypoint-initdb.d/24_notification_destination_transition_rehearsal_schema.sql:ro",
        "./sql/notification_execution_readiness_schema.sql:/docker-entrypoint-initdb.d/25_notification_execution_readiness_schema.sql:ro",
        "./sql/notification_retry_readiness_binding_schema.sql:/docker-entrypoint-initdb.d/26_notification_retry_readiness_binding_schema.sql:ro",
        "./sql/notification_retry_readiness_follow_up_schema.sql:/docker-entrypoint-initdb.d/27_notification_retry_readiness_follow_up_schema.sql:ro",
        "./sql/notification_worker_authority_schema.sql:/docker-entrypoint-initdb.d/28_notification_worker_authority_schema.sql:ro",
    ]
    assert services["mongo"]["volumes"] == [
        "./mongo/init:/docker-entrypoint-initdb.d:ro"
    ]


def test_local_database_services_have_healthchecks() -> None:
    services = _load_compose()["services"]
    assert (
        "pg_isready -U risk_user -d risk_platform"
        in services["postgres"]["healthcheck"]["test"]
    )
    assert services["mongo"]["healthcheck"]["test"][0] == "CMD-SHELL"
    assert (
        "db.adminCommand({ ping: 1 }).ok"
        in services["mongo"]["healthcheck"]["test"][1]
    )


def test_postgres_uses_demo_only_credentials() -> None:
    environment = _load_compose()["services"]["postgres"]["environment"]
    assert environment == {
        "POSTGRES_DB": "risk_platform",
        "POSTGRES_USER": "risk_user",
        "POSTGRES_PASSWORD": "risk_password",
    }
