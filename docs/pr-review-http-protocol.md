# PR review HTTP response ownership and protocol evidence

Primary arc42 block: `engineering-controls`. Goal #231 follows #227.

## Reproduced failure and repair

An HTTP error can be raised by `opener.open` before the response `with` block is
entered. Likewise, rejecting a redirect inside `redirect_request` interrupts the
standard handler before its normal response cleanup. The previous implementation
returned its redacted error with those response streams still open. Ten offline
protocol cases reproduced this at the response-closed assertion against the
unchanged source, after the request and real HTTP handling had both occurred.

The client now closes `HTTPError` responses explicitly. The no-redirect handler
closes its supplied response before raising. Both use a fixed failure even when
cleanup itself raises. Error bodies are not read to construct diagnostics, no
redirect is followed, no request is retried and no authentication is forwarded.
The successful-response context manager, endpoint allowlist, proxy/TLS policy,
byte limits, observation logic and report schema are unchanged.

## What the tests actually execute

`tests/unit/test_pr_review_http_protocol.py` keeps the real `build_opener`,
`OpenerDirector`, HTTPS handler, redirect/error handlers and `HTTPResponse` parser.
Only the connection/response-byte source is substituted. The parser consumes real
HTTP/1.1 status/header/body bytes, including chunked framing. Outbound socket
connections and DNS lookup are guarded; only synthetic authentication is used.

Coverage includes five redirect statuses, five HTTP failures, bounded success,
chunked JSON, malformed/duplicate/non-finite/oversized JSON, and failure while
closing. End-to-end tests use the real CLI, collector, HTTP client and parser for
passing checks, a stable failed scanner, and changed second-observation evidence.
They verify request counts, response closure, redacted diagnostics and continued
`merge_authorized=false`. No fake opener or fake parsed dictionary replaces that
composition. The connection seam deliberately does not perform TLS or networking.

```bash
python -m pytest -q tests/unit/test_pr_review_http_protocol.py
make type-check
make quality-check
make readiness-check
```

## Limits and acceptance

This is offline protocol-integration evidence, not a live GitHub smoke test,
DNS verification, TLS-handshake validation or proof of production endpoint
behavior. Releasing a response is attempted before failure returns; a failing
close still cannot be represented as successful collection. Static checks and
protocol tests do not authenticate review approval or make observations atomic.

No dependency, workflow, database, schema, notification or activation-default
change. No scanner finding is dismissed, predecessor merged or deployment run.
Independent review and explicit final-diff engineer acceptance remain pending.

Primary references: Python's file-like HTTPError and handler-chain behavior:
<https://docs.python.org/3.11/library/urllib.error.html#urllib.error.HTTPError>
and <https://docs.python.org/3.11/library/urllib.request.html>.
