"""Read-only, bounded GitHub PR stack diagnostics; never accepts or merges code."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from scripts.pr_check_evidence import (
    MAX_PAGES, MAX_ROWS, EvidenceError, display_name, full_sha, summarize_checks,
)

MAX_PRS = 25
MAX_RESPONSE_BYTES = 1_048_576
ReadJSON = Callable[[str], Mapping[str, Any]]
SAFE_SUFFIX = re.compile(
    r"(?:git/ref/heads/main|pulls/[1-9][0-9]*|"
    r"compare/[0-9a-f]{40}\.\.\.[0-9a-f]{40}\?per_page=1|"
    r"commits/[0-9a-f]{40}/(?:check-runs\?filter=latest&|status\?)"
    r"per_page=100&page=(?:[1-9]|10))"
)


def repository_name(value: Any) -> str:
    if (not isinstance(value, str) or len(value) > 200
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", value) is None):
        raise EvidenceError("invalid repository identity")
    return value


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise EvidenceError("GitHub redirects are not accepted")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise EvidenceError("duplicate API field")
        value[key] = item
    return value


def _reject_constant(value: str) -> Any:
    raise EvidenceError("non-finite API value")


class GitHubReadOnly:
    def __init__(self, repository: str, *, token: str | None = None) -> None:
        self.repository = repository_name(repository)
        if token is not None and (not isinstance(token, str) or not 1 <= len(token) <= 2048
                                  or re.fullmatch(r"[A-Za-z0-9_]+", token) is None):
            raise EvidenceError("invalid environment authentication value")
        self._token = token

    def __call__(self, suffix: str) -> Mapping[str, Any]:
        if not isinstance(suffix, str) or SAFE_SUFFIX.fullmatch(suffix) is None:
            raise EvidenceError("unsupported read-only GitHub endpoint")
        headers = {"Accept": "application/vnd.github+json", "User-Agent": "pr-stack-review",
                   "X-GitHub-Api-Version": "2022-11-28"}
        if self._token is not None:
            headers["Authorization"] = "Bearer " + self._token
        request = Request(f"https://api.github.com/repos/{self.repository}/{suffix}", headers=headers, method="GET")
        try:
            # Fixed TLS origin; do not follow provider URLs or inherited proxies.
            opener = build_opener(NoRedirects(), ProxyHandler({}))
            with opener.open(request, timeout=15) as response:
                if response.status != 200:
                    raise EvidenceError("unexpected GitHub response")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise EvidenceError("GitHub response exceeded the size limit")
            value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant)
            if not isinstance(value, dict):
                raise EvidenceError("GitHub response is not an object")
            return value
        except Exception:
            raise EvidenceError("unable to obtain bounded GitHub evidence") from None


def _pages(read: ReadJSON, endpoint: str, key: str) -> list[Mapping[str, Any]]:
    pages: list[Mapping[str, Any]] = []
    collected = 0
    for number in range(1, MAX_PAGES + 1):
        separator = "&" if "?" in endpoint else "?"
        page = read(f"{endpoint}{separator}per_page=100&page={number}")
        count, rows = page.get("total_count"), page.get(key)
        if (type(count) is not int or not 0 <= count <= MAX_ROWS
                or not isinstance(rows, list) or len(rows) > 100):
            raise EvidenceError("invalid paginated GitHub inventory")
        pages.append(page)
        collected += len(rows)
        if collected >= count:
            return pages  # The pure verifier checks exact totals/IDs on all pages.
        if not rows:
            raise EvidenceError("GitHub pagination ended before its total")
    raise EvidenceError("GitHub pagination exceeded the page limit")


def _pr_identity(raw: Mapping[str, Any], repository: str, number: int) -> dict[str, Any]:
    try:
        if (raw["number"] != number or type(raw["number"]) is not int
                or raw["state"] != "open" or raw["merged"] is not False
                or type(raw["draft"]) is not bool):
            raise EvidenceError("selection is not an open unmerged PR")
        if raw["base"]["repo"]["full_name"] != repository or raw["head"]["repo"]["full_name"] != repository:
            raise EvidenceError("fork or cross-repository PR is unsupported")
        return {"number": number, "head_sha": full_sha(raw["head"]["sha"]),
                "base_sha": full_sha(raw["base"]["sha"]),
                "head_ref": display_name(raw["head"]["ref"]),
                "base_ref": display_name(raw["base"]["ref"]), "draft": raw["draft"]}
    except (KeyError, TypeError):
        raise EvidenceError("PR identity is incomplete") from None


def _main_sha(read: ReadJSON) -> str:
    raw = read("git/ref/heads/main")
    try:
        if raw["ref"] != "refs/heads/main" or raw["object"]["type"] != "commit":
            raise EvidenceError("main reference is not a commit")
        return full_sha(raw["object"]["sha"])
    except (KeyError, TypeError):
        raise EvidenceError("main identity is incomplete") from None


def collect_stack(repository: str, numbers: Sequence[int], read: ReadJSON) -> dict[str, Any]:
    repository = repository_name(repository)
    if (not isinstance(numbers, (list, tuple)) or not 1 <= len(numbers) <= MAX_PRS
            or any(type(n) is not int or n <= 0 for n in numbers)
            or len(set(numbers)) != len(numbers)):
        raise EvidenceError("select one to twenty-five distinct PR numbers")
    started = datetime.now(timezone.utc).isoformat()
    main_sha = _main_sha(read)
    candidates: dict[int, dict[str, Any]] = {}
    for number in sorted(numbers):
        identity = _pr_identity(read(f"pulls/{number}"), repository, number)
        head, base = identity["head_sha"], identity["base_sha"]
        checks = summarize_checks(
            head_sha=head, check_pages=_pages(read, f"commits/{head}/check-runs?filter=latest", "check_runs"),
            status_pages=_pages(read, f"commits/{head}/status", "statuses"),
        )
        comparison = read(f"compare/{base}...{head}?per_page=1")
        try:
            behind = comparison["behind_by"]
            merge_base = full_sha(comparison["merge_base_commit"]["sha"])
            if (comparison["base_commit"]["sha"] != base or type(behind) is not int or behind < 0
                    or (behind == 0 and merge_base != base)):
                raise EvidenceError("comparison does not reconcile the selected base")
        except (KeyError, TypeError):
            raise EvidenceError("comparison evidence is incomplete") from None
        candidates[number] = {**identity, "checks": checks, "behind_base_by": behind}
    # Re-read every PR after all collection, not just immediately after its checks.
    for number, candidate in candidates.items():
        final = _pr_identity(read(f"pulls/{number}"), repository, number)
        if any(final[key] != candidate[key] for key in final):
            raise EvidenceError("PR references changed during collection")
    if _main_sha(read) != main_sha:
        raise EvidenceError("main changed during collection")
    heads = {row["head_ref"]: n for n, row in candidates.items()}
    if len(heads) != len(candidates) or "main" in heads:
        raise EvidenceError("selected PR heads are ambiguous")
    order: list[int] = []
    visiting: set[int] = set()

    def visit(number: int) -> None:
        if number in visiting:
            raise EvidenceError("PR dependency cycle detected")
        if number in order:
            return
        visiting.add(number)
        row = candidates[number]
        parent = heads.get(row["base_ref"]) if row["base_ref"] != "main" else None
        blockers: list[str] = []
        if row["checks"]["outcome"] != "passed":
            blockers.append("checks_" + row["checks"]["outcome"])
        if row["behind_base_by"]:
            blockers.append("head_behind_base")
        if row["base_ref"] == "main":
            if row["base_sha"] != main_sha:
                blockers.append("base_main_mismatch")
        elif parent is None:
            blockers.append("predecessor_not_selected")
        else:
            visit(parent)
            if row["base_sha"] != candidates[parent]["head_sha"]:
                blockers.append("predecessor_head_mismatch")
            if candidates[parent]["technical_blockers"]:
                blockers.append("predecessor_technical_blocked")
        row.update(predecessor_pr=parent, technical_blockers=sorted(blockers),
                   engineer_acceptance="pending", predecessor_merge_pending=parent is not None,
                   merge_authorized=False)
        visiting.remove(number)
        order.append(number)

    for number in sorted(candidates):
        visit(number)
    return {"model_version": "pr-stack-review-v1", "repository": repository, "main_sha": main_sha,
            "collection_started_at": started, "collection_finished_at": datetime.now(timezone.utc).isoformat(),
            "collection_consistency": "ref_stable_non_atomic", "review_order": order,
            "candidates": [candidates[n] for n in order],
            "all_technical_checks_passed": all(not row["technical_blockers"] for row in candidates.values()),
            "independent_review_verified": False, "tested_merge_tree_verified": False,
            "engineer_acceptance": "pending", "merge_authorized": False}


class ReviewParser(argparse.ArgumentParser):
    def error(self, message: str) -> Any:
        self.exit(2, "PR stack review usage is invalid; no changes made\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = ReviewParser(prog="pr-stack-review", description=__doc__, allow_abbrev=False)
    parser.add_argument("--read-github", action="store_true", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr", type=int, action="append", required=True)
    arguments = parser.parse_args(argv)
    try:
        reader = GitHubReadOnly(arguments.repository, token=os.environ.get("GH_TOKEN") or None)
        result = collect_stack(arguments.repository, arguments.pr, reader)
        encoded = json.dumps(result, sort_keys=True, allow_nan=False)
        if len(encoded.encode("utf-8")) > MAX_RESPONSE_BYTES:
            raise EvidenceError("review report exceeds size limit")
    except Exception:
        print("PR stack review failed; no changes made", file=sys.stderr)
        return 1
    print(encoded)
    return 0 if result["all_technical_checks_passed"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
