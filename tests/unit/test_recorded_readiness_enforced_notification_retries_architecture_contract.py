from pathlib import Path


def test_recorded_readiness_retry_is_replay_safe_and_atomically_governed() -> None:
    runner = Path(
        "src/orchestration/"
        "run_recorded_readiness_enforced_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/recorded-readiness-enforced-notification-retries.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "execute_readiness_enforced_portfolio_risk_notification_retries",
        "validate_notification_execution_readiness_enforcement",
        "build_notification_retry_readiness_binding",
        "record_notification_retry_governance_bundle",
        "validate_notification_retry_governance_bundle",
        "read_notification_retry_execution_request",
        "read_notification_retry_readiness_binding",
        "retained retry terminal has no readiness binding",
        "external_request_replayed",
        "atomic_commit",
    ):
        assert required in runner

    assert runner.index("selected_history_reader(") < runner.index(
        "selected_executor("
    )
    assert runner.index("observing_readiness_enforcer") < runner.index(
        "tracking_transport"
    )
    assert runner.index("build_notification_retry_readiness_binding(") < (
        runner.index("selected_bundle_recorder(")
    )

    for required in (
        "primary arc42 blocks: `orchestration` and `warehouse`",
        "atomic terminal-plus-readiness commit",
        "failure before readiness authority exists",
        "no external request is replayed",
        "does not infer or backfill readiness authority",
        "ordinary ci uses injected readiness, transport, attempt, history and persistence functions",
        "performs no network request",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in runner
