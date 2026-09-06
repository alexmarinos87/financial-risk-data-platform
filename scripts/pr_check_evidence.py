"""Conservative exact-head check inventory; never a merge/approval decision."""
from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from typing import Any

MAX_ROWS = 1000
MAX_PAGES = 10
REQUIRED_CHECKS = frozenset({
    (15368, "Python readiness"), (15368, "PostgreSQL contract"),
    (15368, "Security guardrails"), (15368, "Infrastructure validation"),
    (46505, "GitGuardian Security Checks"),
})
FAILURES = frozenset({"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"})
CONCLUSIONS = FAILURES | {"success", "neutral", "skipped"}


class EvidenceError(ValueError):
    """Invalid or incomplete evidence; diagnostics never include provider content."""


def full_sha(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise EvidenceError("a full lowercase commit SHA is required")
    return value


def positive_id(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise EvidenceError("invalid evidence identifier")
    return value


def display_name(value: Any) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= 128
            or value != value.strip() or any(not 32 <= ord(char) <= 126 for char in value)):
        raise EvidenceError("unsupported evidence name")
    return value


def complete_rows(pages: Any, key: str, *, head_sha: str | None = None) -> list[Mapping[str, Any]]:
    """Reconcile all page totals and IDs, rather than accepting a partial first page."""
    if not isinstance(pages, list) or not 1 <= len(pages) <= MAX_PAGES:
        raise EvidenceError("missing or excessive evidence pages")
    rows: list[Mapping[str, Any]] = []
    total = None
    seen: set[int] = set()
    for page in pages:
        if not isinstance(page, Mapping):
            raise EvidenceError("invalid evidence page")
        count = page.get("total_count")
        items = page.get(key)
        if (type(count) is not int or not 0 <= count <= MAX_ROWS
                or not isinstance(items, list) or len(items) > 100):
            raise EvidenceError("invalid evidence page inventory")
        if total is not None and count != total:
            raise EvidenceError("evidence inventory changed during pagination")
        total = count
        if head_sha is not None and page.get("sha") != head_sha:
            raise EvidenceError("commit-status page belongs to another head")
        for row in items:
            if not isinstance(row, Mapping):
                raise EvidenceError("invalid evidence row")
            row_id = positive_id(row.get("id"))
            if row_id in seen:
                raise EvidenceError("duplicate evidence row")
            seen.add(row_id)
            rows.append(row)
    if len(rows) != total:
        raise EvidenceError("incomplete evidence pagination")
    return rows


def summarize_checks(*, head_sha: str, check_pages: Any, status_pages: Any) -> dict[str, Any]:
    """Summarize latest check runs and combined latest-per-context commit statuses.

    The caller must obtain filter=latest check pages and combined status pages
    for this exact SHA. API totals cannot authenticate offline captures or prove
    the absence of checks GitHub has not yet created. Duplicate latest identities
    remain ambiguous instead of guessing which rerun to discard.
    """
    head = full_sha(head_sha)
    checks = complete_rows(check_pages, "check_runs")
    statuses = complete_rows(status_pages, "statuses", head_sha=head)
    entries: list[dict[str, Any]] = []
    identities: Counter[tuple[int, str]] = Counter()
    for row in checks:
        if row.get("head_sha") != head:
            raise EvidenceError("check run belongs to another head")
        app = row.get("app")
        if not isinstance(app, Mapping):
            raise EvidenceError("check run has no app identity")
        app_id = positive_id(app.get("id"))
        name = display_name(row.get("name"))
        state, conclusion = row.get("status"), row.get("conclusion")
        if not isinstance(state, str) or state not in {"queued", "in_progress", "completed", "pending", "waiting", "requested"}:
            raise EvidenceError("unknown check-run state")
        if state == "completed":
            if not isinstance(conclusion, str) or conclusion not in CONCLUSIONS:
                raise EvidenceError("unknown completed check conclusion")
            outcome = "passed" if conclusion == "success" else "failed" if conclusion in FAILURES else "incomplete"
        else:
            if conclusion is not None:
                raise EvidenceError("unfinished check has a conclusion")
            outcome = "incomplete"
        identities[(app_id, name)] += 1
        entries.append({"kind": "check_run", "id": row["id"], "app_id": app_id,
                        "name": name, "state": state, "conclusion": conclusion, "outcome": outcome})
    contexts: Counter[str] = Counter()
    for row in statuses:
        name = display_name(row.get("context"))
        state = row.get("state")
        if not isinstance(state, str) or state not in {"success", "failure", "error", "pending"}:
            raise EvidenceError("unknown commit-status state")
        contexts[name] += 1
        entries.append({"kind": "commit_status", "id": row["id"], "app_id": None,
                        "name": name, "state": state, "conclusion": None,
                        "outcome": "passed" if state == "success" else "incomplete" if state == "pending" else "failed"})
    missing = [{"app_id": app_id, "name": name}
               for app_id, name in sorted(REQUIRED_CHECKS - identities.keys())]
    ambiguous = any(count > 1 for count in identities.values()) or any(count > 1 for count in contexts.values())
    failed = any(row["outcome"] == "failed" for row in entries)
    incomplete = bool(missing) or ambiguous or any(row["outcome"] == "incomplete" for row in entries)
    outcome = "failed" if failed else "incomplete" if incomplete else "passed"
    return {
        "head_sha": head, "outcome": outcome, "all_reported_checks_passed": outcome == "passed",
        "entries": sorted(entries, key=lambda row: (row["kind"], row["app_id"] or 0, row["name"], row["id"])),
        "missing_required_checks": missing, "ambiguous_latest_identities": ambiguous,
        "evidence_scope": "captured_exact_head", "branch_rules_verified": False,
        "engineer_acceptance": "pending", "merge_authorized": False,
    }
