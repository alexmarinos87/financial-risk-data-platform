from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from scripts import pr_check_evidence as evidence
from scripts import pr_stack_review as review
from test_pr_check_evidence import HEAD, pages, summarize
from test_pr_stack_review import REPO, Reader, pr


def status_page(states: list[str], aggregate: Any, *, head: str = HEAD) -> dict[str, Any]:
    return {"sha": head, "state": aggregate, "total_count": len(states),
            "statuses": [{"id": 900 + i, "context": f"context-{i}", "state": state}
                         for i, state in enumerate(states)]}


@pytest.mark.parametrize(("states", "aggregate", "outcome"), [
    ([], "pending", "passed"),
    (["success"], "success", "passed"),
    (["success", "success"], "success", "passed"),
    (["pending"], "pending", "incomplete"),
    (["success", "pending"], "pending", "incomplete"),
    (["failure"], "failure", "failed"),
    (["error"], "failure", "failed"),
    (["success", "failure"], "failure", "failed"),
    (["pending", "failure"], "failure", "failed"),
    (["error", "pending", "success"], "failure", "failed"),
])
def test_combined_state_reconciles_all_contexts_without_replacing_check_policy(
    states: list[str], aggregate: str, outcome: str,
) -> None:
    checks, _ = pages()
    statuses = [status_page(states, aggregate)]
    before = copy.deepcopy((checks, statuses))
    result = summarize(checks, statuses)
    assert result["outcome"] == outcome
    assert result["combined_status_state"] == aggregate
    assert result["engineer_acceptance"] == "pending" and result["merge_authorized"] is False
    assert result["branch_rules_verified"] is False
    assert (checks, statuses) == before


@pytest.mark.parametrize(("states", "aggregate"), [
    ([], "success"), ([], "failure"), (["success"], "failure"),
    (["success"], "pending"), (["pending"], "success"),
    (["error"], "pending"), (["failure"], "success"),
])
def test_contradictory_aggregate_cannot_produce_a_report(states: list[str], aggregate: str) -> None:
    checks, _ = pages()
    with pytest.raises(evidence.EvidenceError, match="contradicts"):
        summarize(checks, [status_page(states, aggregate)])


@pytest.mark.parametrize("aggregate", [None, True, 1, [], {}, "error", "unknown", "PRIVATE_VALUE"])
def test_missing_or_invalid_aggregate_is_rejected_without_echo(aggregate: Any) -> None:
    checks, _ = pages()
    page = status_page([], aggregate)
    if aggregate is None:
        page.pop("state")
    with pytest.raises(evidence.EvidenceError) as caught:
        summarize(checks, [page])
    assert str(caught.value) == "invalid combined commit-status state"


def test_global_aggregate_is_checked_after_all_pages_not_per_slice() -> None:
    checks, _ = pages()
    complete = status_page(["success", "pending", "error"], "failure")
    split = [{**complete, "statuses": [row]} for row in complete["statuses"]]
    expected = summarize(checks, [complete])
    assert summarize(checks, split) == expected
    assert summarize(checks, list(reversed(split))) == expected


def test_same_totals_with_changing_page_aggregate_are_rejected() -> None:
    checks, _ = pages()
    complete = status_page(["success", "failure"], "failure")
    split = [{**complete, "statuses": [row]} for row in complete["statuses"]]
    split[0]["state"] = "success"
    with pytest.raises(evidence.EvidenceError, match="contradicts"):
        summarize(checks, split)


def test_pending_empty_status_inventory_does_not_hide_failed_scanner() -> None:
    checks, _ = pages()
    next(row for row in checks[0]["check_runs"] if row["app"]["id"] == 46505)["conclusion"] = "failure"
    assert summarize(checks, [status_page([], "pending")])["outcome"] == "failed"


def test_captured_live_empty_status_shape_is_accepted_not_promoted_to_approval() -> None:
    # Projected GET response captured for PR #203 through the connected reader.
    # This does not call GitHub or demonstrate the urllib transport live.
    head = "456aad6af0ac292b908f97128ccab5b38ceedb13"
    captured = {"state": "pending", "statuses": [], "sha": head, "total_count": 0}
    result = evidence.summarize_checks(
        head_sha=head, check_pages=[{"total_count": 0, "check_runs": []}], status_pages=[captured],
    )
    assert result["combined_status_state"] == "pending"
    assert result["outcome"] == "incomplete"  # Required check evidence was not supplied.
    assert result["merge_authorized"] is False


def test_actual_collector_rejects_contradiction_and_cli_emits_no_success_json(
    monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "/status?" in suffix:
            return status_page(["success"], "failure", head=value["sha"])
        return value

    with pytest.raises(evidence.EvidenceError, match="contradicts"):
        review.collect_stack(REPO, [1], read)
    assert not any(path.startswith("compare/") for path in reader.calls)
    monkeypatch.setattr(review, "GitHubReadOnly", lambda *args, **kwargs: read)
    assert review.main(["--read-github", "--repository", REPO, "--pr", "1"]) == 1
    output = capsys.readouterr()
    assert output.out == "" and output.err == "PR stack review failed; no changes made\n"


def test_validated_aggregate_does_not_copy_provider_metadata() -> None:
    checks, _ = pages()
    page = status_page(["success"], "success")
    page.update(repository={"description": "PRIVATE_VALUE"}, url="https://private.invalid")
    result = summarize(checks, [page])
    page["statuses"][0]["state"] = "failure"
    assert result["combined_status_state"] == "success"
    assert "PRIVATE_VALUE" not in json.dumps(result) and "private.invalid" not in json.dumps(result)
