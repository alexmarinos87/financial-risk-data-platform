from pathlib import Path


def test_retry_readiness_enforcement_is_under_one_lock_and_secret_safe() -> None:
    source = Path(
        "src/orchestration/"
        "run_readiness_enforced_portfolio_risk_notification_retries.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-retry-readiness-enforcement.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for required in (
        "enforce_notification_execution_readiness",
        'execution_kind="retry"',
        "with selected_lock_factory(dsn=dsn)",
        "lock_factory=reuse_held_lock",
        "nested_lock_reacquisition_performed",
        "outer_lock_released",
        "validate_notification_execution_readiness_enforcement",
    ):
        assert required in source

    assert source.index("selected_enforcer(") < source.index("selected_executor(")

    for forbidden in (
        "urllib.request",
        "requests.",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in source

    for required in (
        "primary arc42 blocks: `orchestration` and `warehouse`",
        "one physical advisory-lock acquisition",
        "retained retry allow decision",
        "fresh p4d5a evaluation",
        "before the first request",
        "committed configuration remains disabled",
        "no endpoint value",
        "terraform apply",
    ):
        assert required in normalized_docs
