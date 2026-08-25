from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from src.common.config import load_yaml
from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    DeliveryLockFactory,
    acquire_notification_delivery_lock,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "portfolio-risk-webhook-delivery-v1"
CHANNEL = "webhook"
MAX_BATCH_EVENTS = 100
MAX_ATTEMPTS_PER_EVENT = 10
MAX_TIMEOUT_SECONDS = 30
MAX_BACKOFF_SECONDS = 60

CandidateReader = Callable[..., list[dict[str, Any]]]
AttemptWriter = Callable[[Mapping[str, Any]], None]
Transport = Callable[[str, bytes, Mapping[str, str], float], int]


class DeliveryTransportError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WebhookDeliveryConfig:
    enabled: bool
    endpoint_env: str
    timeout_seconds: int
    max_batch_events: int
    max_attempts_per_event: int
    initial_backoff_seconds: int

    @property
    def fingerprint(self) -> str:
        payload = {
            "channel": CHANNEL,
            "enabled": self.enabled,
            "endpoint_env": self.endpoint_env,
            "initial_backoff_seconds": self.initial_backoff_seconds,
            "max_attempts_per_event": self.max_attempts_per_event,
            "max_batch_events": self.max_batch_events,
            "model_version": MODEL_VERSION,
            "timeout_seconds": self.timeout_seconds,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"webhook-delivery-{digest}"


def _required_text(value: Any, label: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if len(parsed) > maximum or any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} is invalid")
    return parsed


def _bounded_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def parse_webhook_delivery_config(
    payload: Mapping[str, Any],
) -> WebhookDeliveryConfig:
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    delivery = payload.get("delivery")
    if not isinstance(delivery, Mapping):
        raise ValidationError("notification delivery configuration is missing delivery")
    webhook = delivery.get("webhook")
    if not isinstance(webhook, Mapping):
        raise ValidationError("notification delivery configuration is missing webhook")
    enabled = webhook.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("webhook enabled must be boolean")
    return WebhookDeliveryConfig(
        enabled=enabled,
        endpoint_env=_required_text(webhook.get("endpoint_env"), "endpoint_env"),
        timeout_seconds=_bounded_integer(
            webhook.get("timeout_seconds"),
            "timeout_seconds",
            MAX_TIMEOUT_SECONDS,
        ),
        max_batch_events=_bounded_integer(
            webhook.get("max_batch_events"),
            "max_batch_events",
            MAX_BATCH_EVENTS,
        ),
        max_attempts_per_event=_bounded_integer(
            webhook.get("max_attempts_per_event"),
            "max_attempts_per_event",
            MAX_ATTEMPTS_PER_EVENT,
        ),
        initial_backoff_seconds=_bounded_integer(
            webhook.get("initial_backoff_seconds"),
            "initial_backoff_seconds",
            MAX_BACKOFF_SECONDS,
        ),
    )


def load_webhook_delivery_config(path: Path) -> WebhookDeliveryConfig:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "notification delivery configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    return parse_webhook_delivery_config(payload)


def _endpoint(value: str) -> tuple[str, str]:
    endpoint = _required_text(value, "webhook endpoint", maximum=2_048)
    parsed = urlparse(endpoint)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValidationError(
            "webhook endpoint must be an HTTPS URL without credentials or fragment"
        )
    return endpoint, parsed.hostname.lower()


def read_pending_delivery_candidates(
    *,
    dsn: str,
    max_events: int,
    schema_name: str = "risk_platform",
) -> list[dict[str, Any]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    max_events = _bounded_integer(max_events, "max_events", MAX_BATCH_EVENTS)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL delivery reading requires psycopg. Run `make setup` first."
        ) from exc

    schema = '"' + schema_name.replace('"', '""') + '"'
    statement = f"""
        SELECT
            pending.event_id,
            pending.event_type,
            pending.transition_type,
            pending.policy_id,
            pending.policy_fingerprint,
            pending.portfolio_id,
            pending.definition_fingerprint,
            pending.metric_name,
            pending.subject_type,
            pending.subject_key,
            pending.current_status,
            pending.ts_event,
            pending.payload_json,
            COALESCE(MAX(attempt.attempt_number), 0) AS attempts_so_far
        FROM {schema}.portfolio_risk_notification_pending pending
        LEFT JOIN {schema}.portfolio_risk_notification_delivery_attempts attempt
          ON attempt.event_id = pending.event_id
         AND attempt.channel = %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {schema}.portfolio_risk_notification_delivery_attempts success
            WHERE success.event_id = pending.event_id
              AND success.channel = %s
              AND success.outcome = 'succeeded'
        )
        GROUP BY
            pending.event_id,
            pending.event_type,
            pending.transition_type,
            pending.policy_id,
            pending.policy_fingerprint,
            pending.portfolio_id,
            pending.definition_fingerprint,
            pending.metric_name,
            pending.subject_type,
            pending.subject_key,
            pending.current_status,
            pending.ts_event,
            pending.payload_json
        HAVING COALESCE(MAX(attempt.attempt_number), 0) = 0
        ORDER BY pending.ts_event, pending.event_id
        LIMIT %s
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, [CHANNEL, CHANNEL, max_events + 1])
                records = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read pending notification delivery candidates"
        ) from None
    if len(records) > max_events:
        raise ValidationError(
            "pending notification delivery exceeds max_events; reduce the batch"
        )
    return records


def _attempt_id(event_id: str, attempt_number: int) -> str:
    payload = {
        "attempt_number": attempt_number,
        "channel": CHANNEL,
        "event_id": event_id,
        "model_version": MODEL_VERSION,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-attempt-{digest}"


def _canonical_payload(candidate: Mapping[str, Any]) -> bytes:
    raw_payload = candidate.get("payload_json")
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except ValueError:
            raise ValidationError("notification payload_json is invalid") from None
    if not isinstance(raw_payload, Mapping):
        raise ValidationError("notification payload_json must be an object")
    ts_event = candidate.get("ts_event")
    if not isinstance(ts_event, datetime) or ts_event.tzinfo is None:
        raise ValidationError("notification ts_event must be timezone-aware")
    envelope = {
        "event_id": _required_text(candidate.get("event_id"), "event_id", 512),
        "event_type": _required_text(candidate.get("event_type"), "event_type"),
        "metric_name": _required_text(candidate.get("metric_name"), "metric_name"),
        "payload": dict(raw_payload),
        "policy_id": _required_text(candidate.get("policy_id"), "policy_id"),
        "portfolio_id": _required_text(
            candidate.get("portfolio_id"),
            "portfolio_id",
        ),
        "status": _required_text(candidate.get("current_status"), "current_status"),
        "subject_key": _required_text(
            candidate.get("subject_key"),
            "subject_key",
            512,
        ),
        "ts_event": ts_event.astimezone(timezone.utc).isoformat(),
    }
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _default_transport(
    endpoint: str,
    payload: bytes,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> int:
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers=dict(headers),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise DeliveryTransportError("network_error") from None


def write_delivery_attempt(
    attempt: Mapping[str, Any],
    *,
    dsn: str,
    schema_name: str = "risk_platform",
) -> None:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL attempt writing requires psycopg. Run `make setup` first."
        ) from exc
    schema = '"' + schema_name.replace('"', '""') + '"'
    columns = (
        "attempt_id",
        "model_version",
        "event_id",
        "channel",
        "attempt_number",
        "idempotency_key",
        "attempted_at",
        "outcome",
        "http_status",
        "error_code",
        "endpoint_host",
        "payload_sha256",
    )
    quoted = ", ".join('"' + column + '"' for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    statement = (
        f"INSERT INTO {schema}.portfolio_risk_notification_delivery_attempts "
        f"({quoted}) VALUES ({placeholders})"
    )
    values = tuple(attempt[column] for column in columns)
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, values)
            connection.commit()
    except Exception:
        raise StorageError("Unable to persist notification delivery attempt") from None


def _read_and_validate_candidates(
    *,
    reader: CandidateReader,
    dsn: str,
    max_events: int,
) -> list[dict[str, Any]]:
    candidates = reader(dsn=dsn, max_events=max_events)
    if not isinstance(candidates, list):
        raise StorageError("notification delivery reader returned invalid evidence")
    if len(candidates) > max_events:
        raise ValidationError("notification delivery exceeds max_batch_events")
    return candidates


def _delivery_summary(
    *,
    config: WebhookDeliveryConfig,
    endpoint_host: str | None,
    candidates: list[dict[str, Any]],
    execute: bool,
    lock_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    plan = [
        {
            "attempts_so_far": int(candidate.get("attempts_so_far", 0)),
            "event_id": candidate.get("event_id"),
            "event_type": candidate.get("event_type"),
            "metric_name": candidate.get("metric_name"),
            "status": candidate.get("current_status"),
        }
        for candidate in candidates
    ]
    concurrency_control = {
        "performed": lock_evidence is not None,
        "acquired": lock_evidence is not None,
        "released": False,
        "held_through_attempt_persistence": lock_evidence is not None,
        "model_version": (
            lock_evidence.get("model_version") if lock_evidence is not None else None
        ),
        "scope": lock_evidence.get("scope") if lock_evidence is not None else None,
        "key_fingerprint": (
            lock_evidence.get("key_fingerprint") if lock_evidence is not None else None
        ),
    }
    return {
        "run_id": str(uuid4()),
        "config_fingerprint": config.fingerprint,
        "channel": CHANNEL,
        "endpoint": {
            "configured": endpoint_host is not None,
            "host": endpoint_host,
        },
        "selection": {
            "candidates_selected": len(candidates),
            "max_batch_events": config.max_batch_events,
            "initial_attempts_only": True,
        },
        "plan": plan,
        "execution": {
            "requested": execute,
            "performed": False,
            "succeeded": 0,
            "failed": 0,
            "exhausted": 0,
            "attempts_recorded": 0,
        },
        "concurrency_control": concurrency_control,
        "response_bodies_recorded": False,
    }


def _execute_initial_attempts(
    *,
    candidates: list[dict[str, Any]],
    endpoint: str,
    endpoint_host: str,
    config: WebhookDeliveryConfig,
    writer: AttemptWriter,
    transport: Transport,
) -> dict[str, int | bool]:
    for candidate in candidates:
        attempts_so_far = candidate.get("attempts_so_far", 0)
        if type(attempts_so_far) is not int or attempts_so_far < 0:
            raise ValidationError("attempts_so_far must be a non-negative integer")
        if attempts_so_far != 0:
            raise ValidationError(
                "events with existing delivery attempts require an exact retry plan"
            )

    succeeded = 0
    failed = 0
    attempts_recorded = 0
    for candidate in candidates:
        event_id = _required_text(candidate.get("event_id"), "event_id", 512)
        payload = _canonical_payload(candidate)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        attempted_at = datetime.now(timezone.utc)
        outcome = "failed"
        http_status: int | None = None
        error_code: str | None = None
        try:
            response_status = transport(
                endpoint,
                payload,
                {
                    "Content-Type": "application/json",
                    "Idempotency-Key": event_id,
                    "User-Agent": "financial-risk-data-platform/1",
                },
                float(config.timeout_seconds),
            )
            if type(response_status) is not int or not 100 <= response_status <= 599:
                raise ValidationError("webhook transport returned an invalid HTTP status")
            http_status = response_status
            if 200 <= response_status < 300:
                outcome = "succeeded"
            else:
                error_code = f"http_{response_status}"
        except DeliveryTransportError:
            error_code = "network_error"
        attempt = {
            "attempt_id": _attempt_id(event_id, 1),
            "model_version": MODEL_VERSION,
            "event_id": event_id,
            "channel": CHANNEL,
            "attempt_number": 1,
            "idempotency_key": event_id,
            "attempted_at": attempted_at,
            "outcome": outcome,
            "http_status": http_status,
            "error_code": error_code,
            "endpoint_host": endpoint_host,
            "payload_sha256": payload_sha256,
        }
        writer(attempt)
        attempts_recorded += 1
        if outcome == "succeeded":
            succeeded += 1
        else:
            failed += 1

    return {
        "requested": True,
        "performed": bool(candidates),
        "succeeded": succeeded,
        "failed": failed,
        "exhausted": 0,
        "attempts_recorded": attempts_recorded,
    }


def deliver_portfolio_risk_notifications(
    *,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    environment: Mapping[str, str] | None = None,
    reader: CandidateReader | None = None,
    attempt_writer: AttemptWriter | None = None,
    transport: Transport | None = None,
    lock_factory: DeliveryLockFactory | None = None,
) -> dict[str, Any]:
    config = load_webhook_delivery_config(config_path)
    selected_environment = environment if environment is not None else os.environ
    raw_endpoint = selected_environment.get(config.endpoint_env)
    endpoint_host: str | None = None
    endpoint_value: str | None = None
    if raw_endpoint:
        endpoint_value, endpoint_host = _endpoint(raw_endpoint)
    if execute:
        if not config.enabled:
            raise ValidationError(
                "webhook delivery is disabled in reviewed configuration"
            )
        if endpoint_value is None or endpoint_host is None:
            raise ValidationError(
                f"webhook endpoint environment variable {config.endpoint_env} is not set"
            )

    selected_reader = reader or read_pending_delivery_candidates
    if not execute:
        candidates = _read_and_validate_candidates(
            reader=selected_reader,
            dsn=dsn,
            max_events=config.max_batch_events,
        )
        return _delivery_summary(
            config=config,
            endpoint_host=endpoint_host,
            candidates=candidates,
            execute=False,
        )

    if endpoint_value is None or endpoint_host is None:
        raise ValidationError("webhook endpoint was not resolved for execution")
    selected_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )
    selected_transport = transport or _default_transport
    selected_lock_factory = lock_factory or acquire_notification_delivery_lock

    with selected_lock_factory(dsn=dsn) as lock_evidence:
        candidates = _read_and_validate_candidates(
            reader=selected_reader,
            dsn=dsn,
            max_events=config.max_batch_events,
        )
        summary = _delivery_summary(
            config=config,
            endpoint_host=endpoint_host,
            candidates=candidates,
            execute=True,
            lock_evidence=lock_evidence,
        )
        summary["execution"] = _execute_initial_attempts(
            candidates=candidates,
            endpoint=endpoint_value,
            endpoint_host=endpoint_host,
            config=config,
            writer=selected_writer,
            transport=selected_transport,
        )

    summary["concurrency_control"]["released"] = True
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or manually make the first delivery attempt for pending "
            "portfolio risk notifications."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("Unable to write webhook delivery summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = deliver_portfolio_risk_notifications(
            config_path=args.config,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except OverlapError:
        print(
            "Webhook delivery rejected: another notification delivery execution "
            "is already active",
            file=sys.stderr,
        )
        return 1
    except ValidationError:
        print(
            "Webhook delivery failed: configuration, endpoint, candidate, or "
            "options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Webhook delivery failed: PostgreSQL, lock, or attempt persistence "
            "failed; remote receivers should deduplicate by Idempotency-Key",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Webhook delivery failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
