from pathlib import Path


def test_model_approval_gate_is_documented_at_the_runtime_boundary() -> None:
    documentation = Path("docs/model-approval-evidence.md").read_text(
        encoding="utf-8"
    )
    runner = Path(
        "src/orchestration/run_method_aware_portfolio_risk_limits.py"
    ).read_text(encoding="utf-8")
    contract_check = Path(
        "src/warehouse/model_approval_contract_check.py"
    ).read_text(encoding="utf-8")

    for required in (
        "model-approval-gate-v1",
        "baseline_exempt",
        "MODEL_APPROVAL_POSTGRES_DSN",
        "model_approval_gate",
        "before attribution Parquet is read",
        "portfolio-risk-limit-evaluation",
        "current_model_approval_status",
    ):
        assert required in documentation

    assert "resolve_model_approval_gate" in runner
    assert runner.index("approval_gate = _resolve_approval_gate(") < runner.index(
        "selected_reader = reader or collect_attribution_records"
    )
    assert "missing_gate_rejected" in contract_check
    assert "revoked_gate_rejected" in contract_check

    stale = (
        "does **not** yet require an approval during risk-limit execution",
        "The following increment will require current approval",
    )
    for statement in stale:
        assert statement not in documentation
