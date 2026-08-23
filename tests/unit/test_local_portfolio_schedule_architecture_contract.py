from pathlib import Path


def test_local_scheduler_documents_disabled_checkpointed_execution() -> None:
    source = Path(
        "src/orchestration/run_local_portfolio_schedule.py"
    ).read_text(encoding="utf-8")
    optional = Path(
        "src/orchestration/run_optional_portfolio_risk_notification_outbox.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/local-portfolio-scheduling.md").read_text(
        encoding="utf-8"
    )
    config = Path("config/local_portfolio_schedules.yaml").read_text(
        encoding="utf-8"
    )

    for required in (
        "maximum_catch_up_sessions",
        "local-schedule/",
        "schedule_fingerprint",
        "last_successful_session",
        "run_market_freshness",
        "run_governed_portfolio_cycle",
        "portfolio-risk-limits-warehouse-load",
        "run_optional_portfolio_risk_notification_outbox",
        "portfolio_risk_notification_outbox_loader",
        "provider_request_performed",
        "cloud_schedule_activated",
    ):
        assert required in source

    assert "no_actionable_transitions" in optional
    assert "enabled: false" in config
    for required in (
        "Disabled-by-default",
        "checkpoint advances",
        "maximum_catch_up_sessions",
        "no Kubernetes CronJob",
        "never included in plan or run summaries",
    ):
        assert required.lower() in docs.lower()
