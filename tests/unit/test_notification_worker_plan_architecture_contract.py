from pathlib import Path


def test_managed_notification_worker_is_plan_only_bounded_and_disabled() -> None:
    source = Path("src/orchestration/plan_notification_worker.py").read_text(
        encoding="utf-8"
    )
    config = Path("config/notification_workers.yaml").read_text(
        encoding="utf-8"
    )
    docs = Path("docs/managed-notification-worker-plan.md").read_text(
        encoding="utf-8"
    )
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "portfolio-risk-notification-worker-config-v1",
        "portfolio-risk-notification-worker-plan-v1",
        "build_notification_worker_plan",
        "plan_notification_worker",
        "validate_notification_worker_plan",
        "worker_disabled",
        "delivery_disabled",
        "retry_execution_disabled",
        "destination_not_active",
        "endpoint_environment_mismatch",
        "max_concurrency",
        "refresh_under_shared_lock",
        "cloud_schedule_activated",
        "terraform_apply_performed",
    ):
        assert required in source

    assert "risk-operations-managed:" in config
    assert "enabled: false" in config
    assert "max_concurrency: 1" in config
    assert "required_status: allowed" in config
    assert "block_on_readiness_failure: true" in config
    assert "block_on_persistence_ambiguity: true" in config
    assert "block_on_expired_review: true" in config

    for required in (
        "primary arc42 block: `orchestration`",
        "there is no `--execute` option",
        "fixed utc interval",
        "deterministic jitter",
        "max_concurrency` is exactly one",
        "mandatory suspension on unresolved persistence ambiguity",
        "does not create a cloud or local schedule",
        "does not read postgresql evidence",
        "does not contain an endpoint value",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "add_argument(\"--execute\"",
        "requests.",
        "urllib.request",
        "httpx.",
        "socket.",
        "psycopg",
        "subprocess",
    ):
        assert forbidden not in source.casefold()
