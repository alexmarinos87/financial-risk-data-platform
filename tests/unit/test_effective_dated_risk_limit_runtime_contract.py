from pathlib import Path


def test_effective_dated_risk_limit_runtime_is_documented_and_wired() -> None:
    runner = Path(
        "src/orchestration/run_portfolio_risk_limits.py"
    ).read_text(encoding="utf-8")
    documentation = " ".join(
        Path("docs/effective-dated-risk-limit-runtime.md")
        .read_text(encoding="utf-8")
        .split()
    )

    for required in (
        "load_effective_portfolio_risk_limit_policy",
        "validate_policy_range",
        "policy_metadata",
        "selected_policy_loader(",
        "policy_id,",
        "end_date,",
        '"policy_version"',
    ):
        assert required in runner

    for required in (
        "selects the policy version valid at the requested end date",
        "rejects a bounded request that crosses its temporal boundary",
        "Boundary rejection happens before local attribution input is read",
        "policy_version_id",
        "limit_definition_fingerprint",
        "does not automatically segment one request",
    ):
        assert required in documentation
