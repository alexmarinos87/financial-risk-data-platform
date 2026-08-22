from pathlib import Path


def test_controlled_workflow_plan_has_no_executor_path() -> None:
    planner = Path(
        "src/orchestration/portfolio_risk_workflow_plan.py"
    ).read_text(encoding="utf-8")
    cli = Path(
        "src/orchestration/plan_portfolio_risk_workflow.py"
    ).read_text(encoding="utf-8")
    documentation = " ".join(
        Path("docs/controlled-portfolio-risk-plan.md")
        .read_text(encoding="utf-8")
        .split()
    )

    combined_source = planner + cli
    for prohibited in (
        "import subprocess",
        "from subprocess",
        "os.system(",
        "shell=True",
        "Popen(",
        "check_call(",
    ):
        assert prohibited not in combined_source

    for required in (
        '"execution_authorized": False',
        '"requires_human_review": True',
        '"executor_included": False',
        '"provider_requests": 0',
        '"external_delivery_attempts": 0',
        '"cloud_mutations": 0',
        '"terraform_apply": False',
        "write_portfolio_risk_workflow_plan",
        "configuration_evidence",
        "declared_effects",
    ):
        assert required in planner

    for required in (
        "The plan is control-plane evidence",
        "It is not an executor",
        "does not import a subprocess executor",
        "execution_authorized",
        "requires_human_review",
        "requires separate operator authorization",
        "does not include a plan executor",
    ):
        assert required in documentation
