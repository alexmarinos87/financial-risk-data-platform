from __future__ import annotations

import http.client
import io
import json
import socket
from collections import Counter
from collections.abc import Callable
from typing import Any

import pytest

from scripts import pr_stack_review as review
from scripts.pr_check_evidence import EvidenceError, REQUIRED_CHECKS

REPO = "owner/repository"
MAIN, HEAD = "a" * 40, "b" * 40


def response_bytes(body: bytes, status: int = 200, *, chunked: bool = False) -> bytes:
    header = f"HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\n".encode()
    if 300 <= status < 400:
        header += b"Location: https://unused.invalid/PRIVATE_REDIRECT\r\n"
    if chunked:
        return (header + b"Transfer-Encoding: chunked\r\n\r\n"
                + f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n")
    return header + f"Content-Length: {len(body)}\r\n\r\n".encode() + body


class WireStream(io.BytesIO):
    def __init__(self, content: bytes, fail_close: bool = False) -> None:
        super().__init__(content)
        self.read_sizes: list[int] = []
        self.fail_close = fail_close

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return super().read(size)

    def close(self) -> None:
        super().close()
        if self.fail_close:
            raise OSError("PRIVATE_CLEANUP_ERROR")


class ProtocolTransport:
    """Keep urllib and HTTPResponse real; substitute only the connection/bytes."""
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.requests: list[dict[str, Any]] = []
        self.streams: list[WireStream] = []
        self.reply: Callable[[str], bytes] = lambda path: response_bytes(b"{}")
        self.fail_close = False
        transport = self

        class Connection(http.client.HTTPSConnection):
            sock = None
            debuglevel = 0

            def __init__(self, host: str, timeout: int, **kwargs: Any) -> None:
                assert host == "api.github.com"
                assert timeout == 15
                self.path = ""

            def set_debuglevel(self, level: int) -> None:
                pass

            def request(self, method: str, path: str, body: Any, headers: Any,
                        **kwargs: Any) -> None:
                assert method == "GET" and body is None
                self.path = path
                transport.requests.append({"path": path, "headers": dict(headers)})

            def getresponse(self) -> http.client.HTTPResponse:
                stream = WireStream(transport.reply(self.path), transport.fail_close)
                transport.streams.append(stream)

                class ByteSource:
                    def makefile(self, mode: str) -> WireStream:
                        return stream

                response = http.client.HTTPResponse(ByteSource())
                response.begin()
                return response

            def close(self) -> None:
                pass

        monkeypatch.setattr(http.client, "HTTPSConnection", Connection)


@pytest.fixture
def protocol(monkeypatch: pytest.MonkeyPatch) -> ProtocolTransport:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("outbound network is forbidden in protocol tests")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("https_proxy", "http://unused.invalid:9999")
    return ProtocolTransport(monkeypatch)


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308, 401, 403, 404, 429, 500])
def test_real_handlers_reject_and_close_without_reading_or_forwarding(
    protocol: ProtocolTransport, status: int,
) -> None:
    protocol.reply = lambda path: response_bytes(b"PRIVATE_PROVIDER_BODY", status)
    with pytest.raises(EvidenceError) as caught:
        review.GitHubReadOnly(REPO, token="TEST_ONLY_VALUE")("pulls/1")
    assert str(caught.value) == "unable to obtain bounded GitHub evidence"
    assert len(protocol.requests) == 1  # No redirect or retry with authentication.
    assert protocol.requests[0]["headers"]["Authorization"] == "Bearer TEST_ONLY_VALUE"
    assert protocol.streams[0].closed
    assert protocol.streams[0].read_sizes == []


@pytest.mark.parametrize("chunked", [False, True])
def test_real_parser_accepts_json_and_closes_response(
    protocol: ProtocolTransport, chunked: bool,
) -> None:
    protocol.reply = lambda path: response_bytes(b'{"ok":true}', chunked=chunked)
    assert review.GitHubReadOnly(REPO)("pulls/1") == {"ok": True}
    assert protocol.streams[0].closed
    assert "Authorization" not in protocol.requests[0]["headers"]


@pytest.mark.parametrize("body", [b"{", b'{"a":1,"a":2}', b'{"a":NaN}', b"[]",
                                  b" " * (review.MAX_RESPONSE_BYTES + 1)])
def test_real_parser_rejects_invalid_or_oversize_body_and_closes(
    protocol: ProtocolTransport, body: bytes,
) -> None:
    protocol.reply = lambda path: response_bytes(body)
    with pytest.raises(EvidenceError, match="unable to obtain bounded GitHub evidence"):
        review.GitHubReadOnly(REPO)("pulls/1")
    assert protocol.streams[0].closed
    assert max(protocol.streams[0].read_sizes) <= review.MAX_RESPONSE_BYTES + 1


@pytest.mark.parametrize("status", [302, 403])
def test_cleanup_error_still_emits_only_fixed_diagnostic(
    protocol: ProtocolTransport, status: int,
) -> None:
    protocol.fail_close = True
    protocol.reply = lambda path: response_bytes(b"PRIVATE_BODY", status)
    with pytest.raises(EvidenceError) as caught:
        review.GitHubReadOnly(REPO)("pulls/1")
    assert str(caught.value) == "unable to obtain bounded GitHub evidence"
    assert protocol.streams[0].closed


@pytest.mark.parametrize(("mode", "exit_code"), [("passed", 0), ("failed", 3), ("drift", 1)])
def test_actual_cli_collector_transport_and_http_parser_compose(
    protocol: ProtocolTransport, capsys: Any, mode: str, exit_code: int,
) -> None:
    counts: Counter[str] = Counter()

    def reply(path: str) -> bytes:
        suffix = path.removeprefix(f"/repos/{REPO}/")
        counts[suffix] += 1
        if suffix == "git/ref/heads/main":
            payload: dict[str, Any] = {
                "ref": "refs/heads/main", "object": {"type": "commit", "sha": MAIN},
            }
        elif suffix == "pulls/1":
            payload = {
                "number": 1, "state": "open", "merged": False, "draft": True,
                "head": {"ref": "feature/1", "sha": HEAD, "repo": {"full_name": REPO}},
                "base": {"ref": "main", "sha": MAIN, "repo": {"full_name": REPO}},
            }
        elif suffix.startswith("compare/"):
            payload = {"base_commit": {"sha": MAIN}, "merge_base_commit": {"sha": MAIN},
                       "behind_by": 0}
        elif "/check-runs?" in suffix:
            failed = mode == "failed" or (mode == "drift" and counts[suffix] == 2)
            rows = [
                {"id": i, "name": name, "app": {"id": app}, "head_sha": HEAD,
                 "status": "completed",
                 "conclusion": "failure" if failed and app == 46505 else "success"}
                for i, (app, name) in enumerate(sorted(REQUIRED_CHECKS), 1)
            ]
            payload = {"total_count": len(rows), "check_runs": rows}
        elif "/status?" in suffix:
            payload = {"sha": HEAD, "state": "pending", "total_count": 0, "statuses": []}
        else:
            raise AssertionError("unexpected protocol request")
        return response_bytes(json.dumps(payload).encode())

    protocol.reply = reply
    assert review.main(["--read-github", "--repository", REPO, "--pr", "1"]) == exit_code
    output = capsys.readouterr()
    if mode == "drift":
        assert output.out == "" and output.err == "PR stack review failed; no changes made\n"
        assert len(protocol.requests) == 7
    else:
        report = json.loads(output.out)
        assert output.err == ""
        assert report["check_inventory_observations"] == 2
        assert report["all_technical_checks_passed"] is (mode == "passed")
        assert report["engineer_acceptance"] == "pending" and report["merge_authorized"] is False
        assert len(protocol.requests) == 9
    assert all(stream.closed for stream in protocol.streams)


def test_cli_http_failure_closes_response_and_emits_no_provider_content(
    protocol: ProtocolTransport, capsys: Any,
) -> None:
    protocol.reply = lambda path: response_bytes(b"PRIVATE_RATE_LIMIT", 429)
    assert review.main(["--read-github", "--repository", REPO, "--pr", "1"]) == 1
    output = capsys.readouterr()
    assert output.out == "" and output.err == "PR stack review failed; no changes made\n"
    assert len(protocol.requests) == 1 and protocol.streams[0].closed
