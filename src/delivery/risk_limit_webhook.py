from __future__ import annotations

import hashlib
import json
import time as time_module
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ..common.config import load_yaml
from ..common.exceptions import StorageError, ValidationError

MAX_ATTEMPT_FILES = 100_000
MAX_NOTIFICATIONS_PER_RUN = 1_000


@dataclass(frozen=True, slots=True)
class WebhookDeliveryConfig:
    adapter_id: str
    enabled: bool
    endpoint_env: str
    authorization_env: str | None
    timeout_seconds: int
    max_attempts: int
    initial_backoff_seconds: int
    max_backoff_seconds: int
    max_notifications_per_run: int


@dataclass(frozen=True, slots=True)
class WebhookResponse:
    status_code: int


Transport = Callable[..., WebhookResponse]
Sleeper = Callable[[float], None]
Clock = Callable[[], datetime]


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _bounded_integer(
    value: Any,
    label: str,
    *,
    minimum: int = 1,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def parse_webhook_delivery_config(
    payload: Mapping[str, Any],
    adapter_id: str,
) -> WebhookDeliveryConfig:
    adapters = payload.get("adapters")
    if not isinstance(adapters, Mapping):
        raise ValidationError("delivery configuration must define adapters")
    candidate = adapters.get(adapter_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"delivery adapter '{adapter_id}' is not configured")
    enabled = candidate.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("delivery adapter enabled must be true or false")
    if candidate.get("adapter_type") != "webhook":
        raise ValidationError("only the webhook adapter type is supported")

    authorization_raw = candidate.get("authorization_env")
    authorization_env = (
        None
        if authorization_raw in {None, ""}
        else _required_text(authorization_raw, "authorization_env")
    )
    return WebhookDeliveryConfig(
        adapter_id=_required_text(adapter_id, "adapter_id"),
        enabled=enabled,
        endpoint_env=_required_text(candidate.get("endpoint_env"), "endpoint_env"),
        authorization_env=authorization_env,
        timeout_seconds=_bounded_integer(
            candidate.get("timeout_seconds"),
            "timeout_seconds",
            maximum=120,
        ),
        max_attempts=_bounded_integer(
            candidate.get("max_attempts"),
            "max_attempts",
            maximum=10,
        ),
        initial_backoff_seconds=_bounded_integer(
            candidate.get("initial_backoff_seconds"),
            "initial_backoff_seconds",
            minimum=0,
            maximum=300,
        ),
        max_backoff_seconds=_bounded_integer(
            candidate.get("max_backoff_seconds"),
            "max_backoff_seconds",
            minimum=0,
            maximum=3600,
        ),
        max_notifications_per_run=_bounded_integer(
            candidate.get("max_notifications_per_run"),
            "max_notifications_per_run",
            maximum=MAX_NOTIFICATIONS_PER_RUN,
        ),
    )


def load_webhook_delivery_config(
    path: Path,
    adapter_id: str,
) -> WebhookDeliveryConfig:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("delivery configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("delivery configuration must be a mapping")
    return parse_webhook_delivery_config(payload, adapter_id)


def default_webhook_transport(
    *,
    endpoint: str,
    headers: Mapping[str, str],
    body: bytes,
    timeout_seconds: int,
) -> WebhookResponse:
    request = Request(
        endpoint,
        data=body,
        headers=dict(headers),
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            return WebhookResponse(status_code=int(response.status))
    except HTTPError as exc:
        return WebhookResponse(status_code=int(exc.code))


def _status_for_http(status_code: int) -> str:
    if 200 <= status_code <= 299:
        return "delivered"
    if status_code in {408, 425, 429} or 500 <= status_code <= 599:
        return "retryable_failure"
    return "permanent_failure"


def _aware_now(clock: Clock) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise StorageError("delivery attempt clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _required_notification_text(
    notification: Mapping[str, Any],
    field: str,
) -> str:
    value = notification.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"notification {field} must be non-empty text")
    return value.strip()


def _payload(notification: Mapping[str, Any]) -> Mapping[str, Any]:
    value = notification.get("payload_json")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            raise ValidationError("notification payload_json is invalid") from None
    if value is None:
        return {
            key: item
            for key, item in notification.items()
            if key not in {"authorization", "endpoint", "secret"}
        }
    if not isinstance(value, Mapping):
        raise ValidationError("notification payload_json must be an object")
    return value


def _stable_path_segment(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _attempt_id(
    *,
    adapter_id: str,
    notification_id: str,
    attempt_number: int,
) -> str:
    payload = {
        "adapter_id": adapter_id,
        "attempt_number": attempt_number,
        "notification_id": notification_id,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"risk-limit-delivery-attempt-{digest}"


def _attempt_directory(
    attempts_dir: Path,
    *,
    adapter_id: str,
    notification_id: str,
) -> Path:
    if attempts_dir.is_symlink():
        raise StorageError("delivery attempt directory must not be a symbolic link")
    return (
        attempts_dir
        / _stable_path_segment(adapter_id)
        / _stable_path_segment(notification_id)
    )


def _read_attempts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_dir():
        raise StorageError("delivery attempt path must be a regular directory")
    try:
        files = sorted(path.glob("attempt-*.json"))
    except OSError:
        raise StorageError("delivery attempts could not be inventoried") from None
    if len(files) > MAX_ATTEMPT_FILES:
        raise StorageError("delivery attempt history exceeds the file bound")
    attempts: list[dict[str, Any]] = []
    for file_path in files:
        if file_path.is_symlink() or not file_path.is_file():
            raise StorageError("delivery attempt history contains an unsafe file")
        try:
            value = json.loads(file_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise StorageError("delivery attempt history is invalid") from None
        if not isinstance(value, dict):
            raise StorageError("delivery attempt history is invalid")
        attempts.append(value)
    attempts.sort(key=lambda item: int(item.get("attempt_number", -1)))
    return attempts


def _write_attempt(path: Path, attempt: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise StorageError("delivery attempt path must not be a symbolic link")
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(attempt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("delivery attempt could not be persisted") from None


def _backoff_seconds(config: WebhookDeliveryConfig, attempt_number: int) -> int:
    return min(
        config.max_backoff_seconds,
        config.initial_backoff_seconds * (2 ** max(0, attempt_number - 1)),
    )


def deliver_risk_limit_notifications(
    notifications: Sequence[Mapping[str, Any]],
    *,
    config: WebhookDeliveryConfig,
    attempts_dir: Path,
    endpoint: str,
    authorization: str | None,
    transport: Transport | None = None,
    sleeper: Sleeper = time_module.sleep,
    clock: Clock = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    if not config.enabled:
        raise ValidationError("delivery adapter is disabled")
    endpoint = _required_text(endpoint, "webhook endpoint")
    if not endpoint.startswith(("https://", "http://")):
        raise ValidationError("webhook endpoint must use HTTP or HTTPS")
    if len(notifications) > config.max_notifications_per_run:
        raise ValidationError("notification selection exceeds the configured bound")
    if len(notifications) > MAX_NOTIFICATIONS_PER_RUN:
        raise ValidationError("notification selection exceeds the hard bound")

    selected_transport = transport or default_webhook_transport
    counts = {
        "delivered": 0,
        "permanent_failure": 0,
        "retryable_failure": 0,
        "skipped_terminal": 0,
        "attempts_written": 0,
    }
    notification_results: list[dict[str, Any]] = []

    for notification in notifications:
        notification_id = _required_notification_text(
            notification,
            "notification_id",
        )
        deduplication_id = notification.get("deduplication_id", notification_id)
        if not isinstance(deduplication_id, str) or not deduplication_id.strip():
            raise ValidationError(
                "notification deduplication_id must be non-empty text"
            )
        history_path = _attempt_directory(
            attempts_dir,
            adapter_id=config.adapter_id,
            notification_id=notification_id,
        )
        history = _read_attempts(history_path)
        terminal = next(
            (
                item
                for item in reversed(history)
                if item.get("status") in {"delivered", "permanent_failure"}
            ),
            None,
        )
        if terminal is not None:
            counts["skipped_terminal"] += 1
            notification_results.append(
                {
                    "notification_id": notification_id,
                    "status": "skipped_terminal",
                    "terminal_attempt_id": terminal.get("attempt_id"),
                }
            )
            continue

        starting_attempt = len(history) + 1
        final_status = "retryable_failure"
        final_attempt_id: str | None = None
        for attempt_number in range(starting_attempt, config.max_attempts + 1):
            attempted_at = _aware_now(clock)
            headers = {
                "Content-Type": "application/json",
                "Idempotency-Key": deduplication_id.strip(),
                "User-Agent": "financial-risk-data-platform/1",
            }
            if authorization is not None:
                headers["Authorization"] = authorization
            body = json.dumps(
                {
                    "notification_id": notification_id,
                    "payload": _payload(notification),
                },
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")

            http_status: int | None = None
            error_class: str | None = None
            try:
                response = selected_transport(
                    endpoint=endpoint,
                    headers=headers,
                    body=body,
                    timeout_seconds=config.timeout_seconds,
                )
                http_status = int(response.status_code)
                final_status = _status_for_http(http_status)
            except (OSError, TimeoutError):
                final_status = "retryable_failure"
                error_class = "transport_error"
            except Exception:
                final_status = "permanent_failure"
                error_class = "unexpected_transport_error"

            attempt_id = _attempt_id(
                adapter_id=config.adapter_id,
                notification_id=notification_id,
                attempt_number=attempt_number,
            )
            final_attempt_id = attempt_id
            attempt = {
                "adapter_id": config.adapter_id,
                "attempt_id": attempt_id,
                "attempt_number": attempt_number,
                "attempted_at": attempted_at.isoformat(),
                "deduplication_id": deduplication_id.strip(),
                "error_class": error_class,
                "http_status": http_status,
                "notification_id": notification_id,
                "status": final_status,
            }
            _write_attempt(
                history_path / f"attempt-{attempt_number:02d}.json",
                attempt,
            )
            counts["attempts_written"] += 1

            if final_status in {"delivered", "permanent_failure"}:
                break
            if attempt_number < config.max_attempts:
                sleeper(float(_backoff_seconds(config, attempt_number)))

        counts[final_status] += 1
        notification_results.append(
            {
                "final_attempt_id": final_attempt_id,
                "notification_id": notification_id,
                "status": final_status,
            }
        )

    return {
        "adapter_id": config.adapter_id,
        "counts": counts,
        "delivery_performed": True,
        "notifications_selected": len(notifications),
        "results": notification_results,
    }
