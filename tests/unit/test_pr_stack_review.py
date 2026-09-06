from __future__ import annotations

import copy
import io
import json
from typing import Any

import pytest

from scripts import pr_stack_review as review
from scripts.pr_check_evidence import EvidenceError, REQUIRED_CHECKS

REPO = "owner/repository"
MAIN = "a" * 40


def pr(number: int, *, base: str = "main", base_sha: str = MAIN) -> dict[str, Any]:
    head = f"{number:040x}"
    return {"number": number, "state": "open", "merged": False, "draft": True,
            "head": {"ref": f"feature/{number}", "sha": head, "repo": {"full_name": REPO}},
            "base": {"ref": base, "sha": base_sha, "repo": {"full_name": REPO}},
            "body": "not-evidence-and-must-not-be-output"}


class Reader:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = {row["number"]: row for row in rows}
        self.calls: list[str] = []
        self.failure: int | None = None
        self.behind = 0
        self.change: str | None = None
        self.reads: dict[int, int] = {}

    def __call__(self, suffix: str) -> dict[str, Any]:
        self.calls.append(suffix)
        assert review.SAFE_SUFFIX.fullmatch(suffix), suffix
        if suffix == "git/ref/heads/main":
            sha = "e" * 40 if self.change == "main" and self.calls.count(suffix) > 1 else MAIN
            return {"ref": "refs/heads/main", "object": {"type": "commit", "sha": sha}}
        if suffix.startswith("pulls/"):
            number = int(suffix.split("/")[1])
            self.reads[number] = self.reads.get(number, 0) + 1
            row = copy.deepcopy(self.rows[number])
            if self.change == "head" and self.reads[number] > 1:
                row["head"]["sha"] = "f" * 40
            if self.change == "draft" and self.reads[number] > 1:
                row["draft"] = False
            return row
        if suffix.startswith("compare/"):
            base = suffix.split("/")[1].split("...")[0]
            return {"base_commit": {"sha": base}, "merge_base_commit": {"sha": "b" * 40 if self.behind else base},
                    "behind_by": self.behind}
        sha = suffix.split("/")[1]
        if "/status?" in suffix:
            return {"sha": sha, "total_count": 0, "statuses": []}
        rows = [{"id": i, "head_sha": sha, "name": name, "app": {"id": app},
                 "status": "completed", "conclusion": "failure" if self.failure == int(sha, 16) and app == 46505 else "success"}
                for i, (app, name) in enumerate(sorted(REQUIRED_CHECKS), 1)]
        return {"total_count": len(rows), "check_runs": rows}


def test_collector_propagates_scanner_failure_through_actual_dependency_chain() -> None:
    parent = pr(191)
    child = pr(193, base="feature/191", base_sha=parent["head"]["sha"])
    reader = Reader([parent, child])
    reader.failure = 191
    result = review.collect_stack(REPO, [193, 191], reader)
    assert result["review_order"] == [191, 193]
    assert result["all_technical_checks_passed"] is False
    first, second = result["candidates"]
    assert first["technical_blockers"] == ["checks_failed"]
    assert second["checks"]["outcome"] == "passed"
    assert second["technical_blockers"] == ["predecessor_technical_blocked"]
    assert second["predecessor_merge_pending"] is True
    assert all(row["merge_authorized"] is False for row in result["candidates"])
    assert result["merge_authorized"] is False
    assert reader.reads == {191: 2, 193: 2}
    assert reader.calls[-1] == "git/ref/heads/main"
    assert "not-evidence" not in json.dumps(result)


def test_green_stack_still_requires_acceptance_and_predecessor_merge() -> None:
    first = pr(3)
    reader = Reader([first, pr(1, base="feature/3", base_sha=first["head"]["sha"])])
    result = review.collect_stack(REPO, [1, 3], reader)
    assert result["review_order"] == [3, 1]
    assert result["all_technical_checks_passed"] is True
    assert result["engineer_acceptance"] == "pending" and result["merge_authorized"] is False
    assert result["candidates"][1]["predecessor_merge_pending"] is True
    assert result["independent_review_verified"] is False
    assert result["collection_consistency"] == "ref_stable_non_atomic"


@pytest.mark.parametrize("change", ["main", "head", "draft"])
def test_moving_refs_or_candidate_state_invalidate_collection(change: str) -> None:
    reader = Reader([pr(1)])
    reader.change = change
    with pytest.raises(EvidenceError, match="changed"):
        review.collect_stack(REPO, [1], reader)


def test_missing_predecessor_is_not_assumed_to_be_main() -> None:
    result = review.collect_stack(REPO, [2], Reader([pr(2, base="feature/absent")]))
    assert result["candidates"][0]["technical_blockers"] == ["predecessor_not_selected"]


def test_behind_base_is_explicit_even_with_passing_checks() -> None:
    reader = Reader([pr(1)])
    reader.behind = 2
    result = review.collect_stack(REPO, [1], reader)
    assert result["candidates"][0]["behind_base_by"] == 2
    assert result["candidates"][0]["technical_blockers"] == ["head_behind_base"]


def test_cycle_is_rejected() -> None:
    first, second = pr(1), pr(2)
    first["base"].update(ref="feature/2", sha=second["head"]["sha"])
    second["base"].update(ref="feature/1", sha=first["head"]["sha"])
    with pytest.raises(EvidenceError, match="cycle"):
        review.collect_stack(REPO, [1, 2], Reader([first, second]))


@pytest.mark.parametrize("mutation", ["fork", "closed", "merged", "bad_draft", "bad_sha", "wrong_number", "missing_repo"])
def test_unsupported_or_malformed_prs_fail_closed(mutation: str) -> None:
    row = pr(1)
    if mutation == "fork":
        row["head"]["repo"]["full_name"] = "other/repository"
    elif mutation == "closed":
        row["state"] = "closed"
    elif mutation == "merged":
        row["merged"] = True
    elif mutation == "bad_draft":
        row["draft"] = 1
    elif mutation == "bad_sha":
        row["head"]["sha"] = "short"
    elif mutation == "wrong_number":
        row["number"] = 2
    else:
        row["head"]["repo"] = None
    with pytest.raises(EvidenceError):
        review._pr_identity(row, REPO, 1)


@pytest.mark.parametrize("numbers", [[], [1, 1], [0], [True], list(range(1, 27)), "1"])
def test_invalid_selection_never_reaches_io(numbers: Any) -> None:
    reader = Reader([pr(1)])
    with pytest.raises(EvidenceError):
        review.collect_stack(REPO, numbers, reader)
    assert reader.calls == []


@pytest.mark.parametrize("repository", ["../x", "a/b?token=x", "https://github.com/a/b", "a/b/c", "a/..", "a/b\n", None])
def test_invalid_repository_never_reaches_io(repository: Any) -> None:
    reader = Reader([pr(1)])
    with pytest.raises(EvidenceError):
        review.collect_stack(repository, [1], reader)
    assert reader.calls == []


def test_pagination_visits_every_page_and_uses_explicit_latest_filter() -> None:
    calls: list[str] = []

    def read(suffix: str) -> dict[str, Any]:
        calls.append(suffix)
        page = int(suffix.rsplit("=", 1)[1])
        return {"total_count": 101, "check_runs": [{"id": n} for n in (range(1, 101) if page == 1 else [101])]}

    result = review._pages(read, "commits/" + "a" * 40 + "/check-runs?filter=latest", "check_runs")
    assert len(result) == 2 and calls[-1].endswith("&per_page=100&page=2")


@pytest.mark.parametrize("page", [{"total_count": 1, "check_runs": []}, {"total_count": 1001, "check_runs": []},
                                  {"total_count": True, "check_runs": []}, {"total_count": 1, "check_runs": None}])
def test_incomplete_or_excessive_api_pages_fail(page: dict[str, Any]) -> None:
    with pytest.raises(EvidenceError):
        review._pages(lambda path: page, "unused", "check_runs")


@pytest.mark.parametrize("suffix", ["https://evil.invalid", "../issues", "issues/1", "pulls/1/merge",
                                    "pulls/1?token=x", "git/ref/heads/main#fragment", "pulls/01"])
def test_transport_rejects_every_non_read_endpoint_before_opener(monkeypatch: pytest.MonkeyPatch, suffix: str) -> None:
    def forbidden(*args: Any) -> Any:
        raise AssertionError("opener must not be reached")
    monkeypatch.setattr(review, "build_opener", forbidden)
    with pytest.raises(EvidenceError, match="endpoint"):
        review.GitHubReadOnly(REPO)(suffix)


def test_transport_uses_fixed_origin_get_bounds_and_environment_token_only(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, Any] = {}

    class Response(io.BytesIO):
        status = 200
        def read(self, size: int = -1) -> bytes:
            observed["read_limit"] = size
            return super().read(size)

    class Opener:
        def open(self, request: Any, timeout: int) -> Response:
            observed.update(url=request.full_url, method=request.method, headers=dict(request.headers), timeout=timeout)
            return Response(b'{"total_count":0,"check_runs":[]}')

    def build(*handlers: Any) -> Opener:
        assert any(isinstance(h, review.NoRedirects) for h in handlers)
        assert next(h for h in handlers if isinstance(h, review.ProxyHandler)).proxies == {}
        return Opener()

    monkeypatch.setattr(review, "build_opener", build)
    reader = review.GitHubReadOnly(REPO, token="TEST_ONLY_VALUE")
    assert reader("pulls/1")["total_count"] == 0
    assert observed["url"] == "https://api.github.com/repos/owner/repository/pulls/1"
    assert observed["method"] == "GET" and observed["timeout"] == 15
    assert observed["read_limit"] == review.MAX_RESPONSE_BYTES + 1
    assert observed["headers"]["Authorization"] == "Bearer TEST_ONLY_VALUE"
    with pytest.raises(EvidenceError):
        review.NoRedirects().redirect_request(None)


@pytest.mark.parametrize("payload", [b"[]", b'{"x":1,"x":2}', b'{"x":NaN}', b"\xff", b"[" * 1500,
                                     b" " * (review.MAX_RESPONSE_BYTES + 1)])
def test_transport_rejects_bad_response_without_echo(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> None:
    class Response(io.BytesIO):
        status = 200
    class Opener:
        def open(self, *args: Any, **kwargs: Any) -> Response:
            return Response(payload)
    monkeypatch.setattr(review, "build_opener", lambda *args: Opener())
    with pytest.raises(EvidenceError) as caught:
        review.GitHubReadOnly(REPO)("pulls/1")
    assert str(caught.value) == "unable to obtain bounded GitHub evidence"


@pytest.mark.parametrize("token", ["", "bad\nheader", "bad value", "x" * 2049, 42])
def test_invalid_environment_token_is_not_echoed(token: Any) -> None:
    with pytest.raises(EvidenceError) as caught:
        review.GitHubReadOnly(REPO, token=token)
    assert str(caught.value) == "invalid environment authentication value"


def test_cli_returns_blocked_report_without_accepting_or_writing(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    reader = Reader([pr(191)])
    reader.failure = 191
    monkeypatch.setattr(review, "GitHubReadOnly", lambda *args, **kwargs: reader)
    assert review.main(["--read-github", "--repository", REPO, "--pr", "191"]) == 3
    result = json.loads(capsys.readouterr().out)
    assert result["merge_authorized"] is False and result["candidates"][0]["technical_blockers"] == ["checks_failed"]


@pytest.mark.parametrize("extra", [["--token", "DO_NOT_ECHO"], ["--execute"], ["--read"], ["--read-github=DO_NOT_ECHO"]])
def test_cli_usage_errors_are_redacted_before_io(extra: list[str], monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("reader must not be constructed")
    monkeypatch.setattr(review, "GitHubReadOnly", forbidden)
    with pytest.raises(SystemExit) as caught:
        review.main(["--read-github", "--repository", REPO, "--pr", "1", *extra])
    assert caught.value.code == 2
    output = capsys.readouterr()
    assert output.out == "" and output.err == "PR stack review usage is invalid; no changes made\n"


def test_cli_provider_failure_is_redacted(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    def read(path: str) -> Any:
        raise RuntimeError("DO_NOT_ECHO")
    monkeypatch.setattr(review, "GitHubReadOnly", lambda *args, **kwargs: read)
    assert review.main(["--read-github", "--repository", REPO, "--pr", "1"]) == 1
    output = capsys.readouterr()
    assert output.out == "" and output.err == "PR stack review failed; no changes made\n"


def test_predecessor_head_and_main_mismatches_remain_blockers() -> None:
    first = pr(1, base_sha="b" * 40)
    second = pr(2, base="feature/1", base_sha="c" * 40)
    result = review.collect_stack(REPO, [1, 2], Reader([first, second]))
    assert result["candidates"][0]["technical_blockers"] == ["base_main_mismatch"]
    assert "predecessor_head_mismatch" in result["candidates"][1]["technical_blockers"]
    assert result["tested_merge_tree_verified"] is False


def test_duplicate_selected_head_branches_are_rejected() -> None:
    first, second = pr(1), pr(2)
    second["head"]["ref"] = first["head"]["ref"]
    with pytest.raises(EvidenceError, match="ambiguous"):
        review.collect_stack(REPO, [1, 2], Reader([first, second]))


@pytest.mark.parametrize("bad", [{"behind_by": True}, {"base_commit": {"sha": "f" * 40}},
                                 {"merge_base_commit": {"sha": "e" * 40}}])
def test_comparison_must_reconcile_selected_base(bad: dict[str, Any]) -> None:
    reader = Reader([pr(1)])
    def read(suffix: str) -> dict[str, Any]:
        value = reader(suffix)
        return {**value, **bad} if suffix.startswith("compare/") else value
    with pytest.raises(EvidenceError):
        review.collect_stack(REPO, [1], read)


def test_command_needs_explicit_read_flag_and_help_does_not_read(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no I/O")
    monkeypatch.setattr(review, "GitHubReadOnly", forbidden)
    for args, code in ((["--repository", REPO, "--pr", "1"], 2), (["--help"], 0)):
        with pytest.raises(SystemExit) as caught:
            review.main(args)
        assert caught.value.code == code
    assert "pr-stack-review" in capsys.readouterr().out


def test_transport_http_error_does_not_echo_provider_or_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    class Opener:
        def open(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("SENSITIVE_HTTP_ERROR")
    monkeypatch.setattr(review, "build_opener", lambda *args: Opener())
    with pytest.raises(EvidenceError) as caught:
        review.GitHubReadOnly(REPO)("pulls/1")
    assert str(caught.value) == "unable to obtain bounded GitHub evidence"
