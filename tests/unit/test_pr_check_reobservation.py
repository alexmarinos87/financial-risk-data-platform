from __future__ import annotations

import copy
import json
from collections import Counter
from typing import Any

import pytest

from scripts import pr_stack_review as review
from scripts.pr_check_evidence import EvidenceError, MAX_PAGES, REQUIRED_CHECKS
from test_pr_stack_review import REPO, Reader, pr


def test_same_head_failure_after_initial_read_is_detected() -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if suffix.startswith("compare/"):
            reader.failure = 1
        return value

    with pytest.raises(EvidenceError, match="check evidence changed"):
        review.collect_stack(REPO, [1], read)
    assert sum("check-runs?" in path for path in reader.calls) == 2
    assert reader.reads == {1: 1}  # Drift aborts before final reference reads.


@pytest.mark.parametrize("mutation", ["conclusion", "queued", "id", "app", "name", "add", "remove"])
def test_second_observation_compares_identity_and_state_not_just_outcome(mutation: str) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "check-runs?" in suffix and reader.calls.count(suffix) == 2:
            rows = value["check_runs"]
            if mutation == "conclusion":
                rows[0]["conclusion"] = "failure"
            elif mutation == "queued":
                rows[0].update(status="queued", conclusion=None)
            elif mutation == "id":
                rows[0]["id"] = 1001
            elif mutation == "app":
                rows[0]["app"]["id"] = 999
            elif mutation == "name":
                rows[0]["name"] = "changed-check"
            elif mutation == "add":
                rows.append({**rows[0], "id": 1001, "name": "additional-passing-check"})
                value["total_count"] += 1
            else:
                rows.pop()
                value["total_count"] -= 1
        return value

    with pytest.raises(EvidenceError, match="check evidence changed"):
        review.collect_stack(REPO, [1], read)


@pytest.mark.parametrize("mutation", ["failure", "pending", "id", "context", "add", "remove"])
def test_commit_status_changes_are_reobserved(mutation: str) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "/status?" in suffix:
            rows = [{"id": 901, "context": "external-audit", "state": "success"}]
            if reader.calls.count(suffix) == 2:
                if mutation in {"failure", "pending"}:
                    rows[0]["state"] = mutation
                elif mutation == "id":
                    rows[0]["id"] = 902
                elif mutation == "context":
                    rows[0]["context"] = "another-audit"
                elif mutation == "add":
                    rows.append({"id": 902, "context": "another-audit", "state": "success"})
                else:
                    rows.clear()
            aggregate = "failure" if mutation == "failure" and reader.calls.count(suffix) == 2 else (
                "pending" if not rows or any(row["state"] == "pending" for row in rows) else "success"
            )
            return {**value, "state": aggregate, "statuses": rows, "total_count": len(rows)}
        return value

    with pytest.raises(EvidenceError, match="check evidence changed"):
        review.collect_stack(REPO, [1], read)


def test_reused_mutable_provider_rows_cannot_rewrite_the_first_capture() -> None:
    reader = Reader([pr(1)])
    retained: dict[str, Any] = {}

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "check-runs?" in suffix:
            if not retained:
                retained.update(value)
            return retained
        if suffix.startswith("compare/"):
            retained["check_runs"][0]["conclusion"] = "failure"
        return value

    with pytest.raises(EvidenceError, match="check evidence changed"):
        review.collect_stack(REPO, [1], read)
    assert retained["check_runs"][0]["conclusion"] == "failure"


def test_order_and_unused_provider_metadata_do_not_create_false_drift() -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "check-runs?" in suffix and reader.calls.count(suffix) == 2:
            value["check_runs"].reverse()
            for row in value["check_runs"]:
                row.update(output={"text": "PRIVATE_PROVIDER_TEXT"}, details_url="https://private.invalid")
        return value

    result = review.collect_stack(REPO, [1], read)
    assert result["all_technical_checks_passed"] is True
    assert result["check_inventory_observations"] == 2
    assert result["check_inventory_consistency"] == "two_equal_observations_non_atomic"
    assert "PRIVATE_PROVIDER_TEXT" not in json.dumps(result)
    assert result["merge_authorized"] is False


@pytest.mark.parametrize("state", ["failed", "missing"])
def test_stable_blockers_still_propagate_to_children(state: str) -> None:
    parent = pr(1)
    reader = Reader([parent, pr(2, base="feature/1", base_sha=parent["head"]["sha"])])
    if state == "failed":
        reader.failure = 1

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if state == "missing" and suffix.startswith(f"commits/{parent['head']['sha']}/check-runs?"):
            return {"total_count": 0, "check_runs": []}
        return value

    result = review.collect_stack(REPO, [2, 1], read)
    assert result["all_technical_checks_passed"] is False
    assert result["candidates"][1]["technical_blockers"] == ["predecessor_technical_blocked"]
    assert result["candidates"][1]["predecessor_merge_pending"] is True
    assert reader.calls[-3:] == ["pulls/1", "pulls/2", "git/ref/heads/main"]


@pytest.mark.parametrize("change", ["head", "draft", "main"])
def test_ref_changes_during_second_inventory_pass_are_checked_afterward(change: str) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "/status?" in suffix and reader.calls.count(suffix) == 2:
            reader.change = change
        return value

    with pytest.raises(EvidenceError, match="changed"):
        review.collect_stack(REPO, [1], read)


@pytest.mark.parametrize("bad", ["provider", "pagination", "aggregate", "duplicate", "wrong_head"])
def test_second_pass_failures_never_fall_back_to_earlier_green_evidence(bad: str) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if "check-runs?" in suffix and reader.calls.count(suffix) == 2:
            if bad == "provider":
                raise EvidenceError("PRIVATE_PROVIDER_ERROR")
            if bad == "pagination":
                return {"total_count": 6, "check_runs": []}
            if bad == "duplicate":
                value["check_runs"][1]["id"] = value["check_runs"][0]["id"]
            if bad == "wrong_head":
                value["check_runs"][0]["head_sha"] = "f" * 40
        if bad == "aggregate" and "/status?" in suffix and reader.calls.count(suffix) == 2:
            value["state"] = "failure"
        return value

    with pytest.raises(EvidenceError):
        review.collect_stack(REPO, [1], read)
    assert all(count <= 2 for count in Counter(reader.calls).values())


def test_both_observations_follow_every_check_and_status_page() -> None:
    reader = Reader([pr(1)])
    head = pr(1)["head"]["sha"]

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if suffix.startswith("commits/"):
            page = int(suffix.rsplit("=", 1)[1])
            if "check-runs?" in suffix:
                rows = value["check_runs"] + [
                    {"id": 1000 + n, "name": f"extra-{n}", "app": {"id": 999}, "head_sha": head,
                     "status": "completed", "conclusion": "success"} for n in range(96)
                ]
                return {"total_count": len(rows), "check_runs": rows[(page - 1) * 100:page * 100]}
            rows = [{"id": n, "context": f"context-{n}", "state": "success"} for n in range(1, 102)]
            return {"sha": head, "state": "success", "total_count": len(rows),
                    "statuses": rows[(page - 1) * 100:page * 100]}
        return value

    result = review.collect_stack(REPO, [1], read)
    assert result["all_technical_checks_passed"] is True
    inventory_calls = Counter(path for path in reader.calls if path.startswith("commits/"))
    assert len(inventory_calls) == 4 and set(inventory_calls.values()) == {2}
    assert len(result["candidates"][0]["checks"]["entries"]) == 202


def test_maximum_selection_has_bounded_doubled_read_cost_without_retries() -> None:
    numbers = list(range(1, review.MAX_PRS + 1))
    reader = Reader([pr(n) for n in numbers])
    result = review.collect_stack(REPO, numbers, reader)
    assert len(reader.calls) == 2 + 7 * review.MAX_PRS
    assert sum("check-runs?" in p for p in reader.calls) == 2 * review.MAX_PRS
    assert sum("/status?" in p for p in reader.calls) == 2 * review.MAX_PRS
    assert result["review_order"] == numbers and result["merge_authorized"] is False
    assert MAX_PAGES == 10 and len(REQUIRED_CHECKS) == 5


def test_cli_drift_emits_only_fixed_failure_diagnostic(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    reader = Reader([pr(1)])

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        if suffix.startswith("compare/"):
            reader.failure = 1
        return value

    monkeypatch.setattr(review, "GitHubReadOnly", lambda *args, **kwargs: read)
    assert review.main(["--read-github", "--repository", REPO, "--pr", "1"]) == 1
    output = capsys.readouterr()
    assert output.out == "" and output.err == "PR stack review failed; no changes made\n"


def test_report_remains_detached_after_successful_reobservation() -> None:
    reader = Reader([pr(1)])
    retained: list[dict[str, Any]] = []

    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        retained.append(value)
        return value

    result = review.collect_stack(REPO, [1], read)
    expected = copy.deepcopy(result)
    for value in retained:
        value.clear()
    assert result == expected
