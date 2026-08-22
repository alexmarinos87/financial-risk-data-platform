from pathlib import Path


def test_governed_history_plan_documents_plan_only_temporal_contract() -> None:
    source = Path(
        "src/orchestration/plan_governed_portfolio_history.py"
    ).read_text(encoding="utf-8")
    analytics = Path(
        "src/analytics/governed_portfolio_segments.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/governed-portfolio-history-plan.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "plan_governed_portfolio_segments",
        "parse_portfolio_mandates",
        "parse_portfolio_risk_limit_policies",
        '"performed": False',
        '"reason": "plan_only"',
    ):
        assert required in source

    for required in (
        "governed-portfolio-segments-v1",
        "coverage gap",
        "max_segments",
        "segment_id",
        "plan_id",
    ):
        assert required in analytics

    for required in (
        "plan-only",
        "covered exactly once",
        "does not acquire a lock",
        "Automatic multi-segment execution is intentionally separate",
    ):
        assert required in docs
