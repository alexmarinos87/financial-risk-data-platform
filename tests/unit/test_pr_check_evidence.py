from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from scripts import pr_check_evidence as evidence

HEAD = "a" * 40


def pages() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    checks = [{"id": i, "name": name, "app": {"id": app}, "head_sha": HEAD,
               "status": "completed", "conclusion": "success"}
              for i, (app, name) in enumerate(sorted(evidence.REQUIRED_CHECKS), 1)]
    return [{"total_count": len(checks), "check_runs": checks}], [{"sha": HEAD, "total_count": 0, "statuses": []}]


def summarize(checks: Any, statuses: Any) -> dict[str, Any]:
    return evidence.summarize_checks(head_sha=HEAD, check_pages=checks, status_pages=statuses)


def test_green_inventory_is_not_engineer_acceptance_and_drops_provider_content() -> None:
    checks, statuses = pages()
    checks[0]["check_runs"][0]["output"] = {"text": "private-provider-content"}
    checks[0]["check_runs"][0]["details_url"] = "https://untrusted.invalid/private"
    before = copy.deepcopy((checks, statuses))
    result = summarize(checks, statuses)
    assert result["outcome"] == "passed" and result["all_reported_checks_passed"] is True
    assert result["merge_authorized"] is False and result["engineer_acceptance"] == "pending"
    assert result["branch_rules_verified"] is False
    assert "private" not in json.dumps(result)
    result["entries"][0]["name"] = "changed"
    assert (checks, statuses) == before


def test_four_green_workflow_jobs_cannot_hide_gitguardian_failure() -> None:
    checks, statuses = pages()
    scanner = next(row for row in checks[0]["check_runs"] if row["app"]["id"] == 46505)
    scanner["conclusion"] = "failure"
    result = summarize(checks, statuses)
    assert result["outcome"] == "failed" and result["all_reported_checks_passed"] is False
    assert [row["name"] for row in result["entries"] if row["outcome"] == "failed"] == ["GitGuardian Security Checks"]


@pytest.mark.parametrize("conclusion", ["neutral", "skipped", "cancelled", "timed_out", "action_required", "startup_failure", "stale"])
def test_non_success_conclusions_never_pass(conclusion: str) -> None:
    checks, statuses = pages()
    checks[0]["check_runs"][0]["conclusion"] = conclusion
    assert summarize(checks, statuses)["all_reported_checks_passed"] is False


@pytest.mark.parametrize("state", ["queued", "in_progress", "pending", "waiting", "requested"])
def test_unfinished_checks_are_incomplete(state: str) -> None:
    checks, statuses = pages()
    checks[0]["check_runs"][0].update(status=state, conclusion=None)
    assert summarize(checks, statuses)["outcome"] == "incomplete"


def test_same_named_other_app_cannot_satisfy_requirement() -> None:
    checks, statuses = pages()
    checks[0]["check_runs"][0]["app"]["id"] = 999
    result = summarize(checks, statuses)
    assert result["outcome"] == "incomplete"
    assert len(result["missing_required_checks"]) == 1


def test_extra_failing_app_is_not_ignored() -> None:
    checks, statuses = pages()
    extra = {**checks[0]["check_runs"][0], "id": 99, "app": {"id": 999}, "conclusion": "failure"}
    checks[0]["check_runs"].append(extra)
    checks[0]["total_count"] += 1
    assert summarize(checks, statuses)["outcome"] == "failed"


@pytest.mark.parametrize("state", ["success", "failure", "error", "pending"])
def test_commit_statuses_are_reconciled_separately(state: str) -> None:
    checks, statuses = pages()
    statuses[0].update(total_count=1, statuses=[{"id": 91, "context": "external-audit", "state": state}])
    result = summarize(checks, statuses)
    assert result["outcome"] == ("passed" if state == "success" else "incomplete" if state == "pending" else "failed")


def test_full_pagination_and_order_independence() -> None:
    checks, statuses = pages()
    baseline = summarize(checks, statuses)
    rows = checks[0]["check_runs"]
    split = [{"total_count": 5, "check_runs": rows[:2]}, {"total_count": 5, "check_runs": list(reversed(rows[2:]))}]
    assert summarize(split, statuses) == baseline


@pytest.mark.parametrize("mutation", ["missing_page", "changed_total", "duplicate_id", "wrong_head", "wrong_status_head",
                                      "boolean_total", "boolean_id", "bad_app", "bad_name", "bad_state", "bad_conclusion", "unfinished_conclusion"])
def test_malformed_or_partial_evidence_fails_closed(mutation: str) -> None:
    checks, statuses = pages()
    row = checks[0]["check_runs"][0]
    if mutation == "missing_page":
        checks[0]["total_count"] = 6
    elif mutation == "changed_total":
        checks.append({"total_count": 6, "check_runs": []})
    elif mutation == "duplicate_id":
        checks[0]["check_runs"][1]["id"] = row["id"]
    elif mutation == "wrong_head":
        row["head_sha"] = "b" * 40
    elif mutation == "wrong_status_head":
        statuses[0]["sha"] = "b" * 40
    elif mutation == "boolean_total":
        checks[0]["total_count"] = True
    elif mutation == "boolean_id":
        row["id"] = True
    elif mutation == "bad_app":
        row["app"] = None
    elif mutation == "bad_name":
        row["name"] = "control\ncharacter"
    elif mutation == "bad_state":
        row["status"] = []
    elif mutation == "bad_conclusion":
        row["conclusion"] = "new-unknown-result"
    else:
        row["status"] = "queued"
    with pytest.raises(evidence.EvidenceError):
        summarize(checks, statuses)


@pytest.mark.parametrize("kind", ["check", "status"])
def test_duplicate_latest_identities_are_ambiguous_not_guessed(kind: str) -> None:
    checks, statuses = pages()
    if kind == "check":
        checks[0]["check_runs"].append({**checks[0]["check_runs"][0], "id": 99})
        checks[0]["total_count"] += 1
    else:
        statuses[0].update(total_count=2, statuses=[{"id": i, "context": "same", "state": "success"} for i in (98, 99)])
    result = summarize(checks, statuses)
    assert result["ambiguous_latest_identities"] is True
    assert result["outcome"] == "incomplete"


@pytest.mark.parametrize("value", [None, "main", "a" * 39, "A" * 40, 1])
def test_explicit_full_head_is_required(value: Any) -> None:
    with pytest.raises(evidence.EvidenceError):
        evidence.summarize_checks(head_sha=value, check_pages=[], status_pages=[])


@pytest.mark.parametrize("value", [None, [], [{}], [{"total_count": 1001, "check_runs": []}]])
def test_missing_or_unbounded_pages_are_rejected(value: Any) -> None:
    _, statuses = pages()
    with pytest.raises(evidence.EvidenceError):
        summarize(value, statuses)


def test_empty_inventory_is_missing_not_green() -> None:
    _, statuses = pages()
    result = summarize([{"total_count": 0, "check_runs": []}], statuses)
    assert result["outcome"] == "incomplete"
    assert len(result["missing_required_checks"]) == 5
